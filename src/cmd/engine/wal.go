package engine

import (
	"bufio"
	"encoding/binary"
	"hash/crc32"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

type WAL struct {
	mu       sync.Mutex
	file     *os.File
	writer   *bufio.Writer
	path     string
	size     int64
	maxSize  int64
	syncMode SyncMode

	pending    int32
	lastSync   time.Time
	syncTimer  *time.Timer
	syncNotify chan struct{}

	closed int32
}

type SyncMode int

const (
	SyncNone SyncMode = iota
	SyncInterval
	SyncBatch
	SyncEvery
)

const (
	walMagic        = 0x57414C31
	walEntryHeader  = 12
	defaultMaxSize  = 64 * 1024 * 1024
	syncIntervalMs  = 10
	writeBufferSize = 256 * 1024
)

func NewWAL(dir string, mode SyncMode) (*WAL, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	path := filepath.Join(dir, "wal.log")
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}

	stat, _ := file.Stat()
	size := stat.Size()

	w := &WAL{
		file:       file,
		writer:     bufio.NewWriterSize(file, writeBufferSize),
		path:       path,
		size:       size,
		maxSize:    defaultMaxSize,
		syncMode:   mode,
		syncNotify: make(chan struct{}, 1),
	}

	if mode == SyncInterval {
		w.syncTimer = time.AfterFunc(syncIntervalMs*time.Millisecond, w.timerSync)
	}

	return w, nil
}

func (w *WAL) Write(key, value []byte) error {
	if atomic.LoadInt32(&w.closed) == 1 {
		return os.ErrClosed
	}

	entryLen := len(key) + len(value) + 8
	entry := make([]byte, walEntryHeader+entryLen)

	binary.LittleEndian.PutUint32(entry[0:4], walMagic)
	binary.LittleEndian.PutUint32(entry[4:8], uint32(entryLen))

	binary.LittleEndian.PutUint32(entry[walEntryHeader:walEntryHeader+4], uint32(len(key)))
	copy(entry[walEntryHeader+4:], key)
	binary.LittleEndian.PutUint32(entry[walEntryHeader+4+len(key):], uint32(len(value)))
	copy(entry[walEntryHeader+8+len(key):], value)

	crc := crc32.ChecksumIEEE(entry[walEntryHeader:])
	binary.LittleEndian.PutUint32(entry[8:12], crc)

	w.mu.Lock()
	_, err := w.writer.Write(entry)
	w.size += int64(len(entry))
	atomic.AddInt32(&w.pending, 1)
	w.mu.Unlock()

	if err != nil {
		return err
	}

	if w.syncMode == SyncEvery {
		return w.Sync()
	}

	return nil
}

func (w *WAL) WriteBatch(kvs [][2][]byte) error {
	if atomic.LoadInt32(&w.closed) == 1 {
		return os.ErrClosed
	}

	if len(kvs) == 0 {
		return nil
	}

	totalSize := 0
	for _, kv := range kvs {
		totalSize += walEntryHeader + 8 + len(kv[0]) + len(kv[1])
	}

	batch := make([]byte, 0, totalSize)

	for _, kv := range kvs {
		key, value := kv[0], kv[1]
		entryLen := len(key) + len(value) + 8
		entry := make([]byte, walEntryHeader+entryLen)

		binary.LittleEndian.PutUint32(entry[0:4], walMagic)
		binary.LittleEndian.PutUint32(entry[4:8], uint32(entryLen))

		binary.LittleEndian.PutUint32(entry[walEntryHeader:walEntryHeader+4], uint32(len(key)))
		copy(entry[walEntryHeader+4:], key)
		binary.LittleEndian.PutUint32(entry[walEntryHeader+4+len(key):], uint32(len(value)))
		copy(entry[walEntryHeader+8+len(key):], value)

		crc := crc32.ChecksumIEEE(entry[walEntryHeader:])
		binary.LittleEndian.PutUint32(entry[8:12], crc)

		batch = append(batch, entry...)
	}

	w.mu.Lock()
	_, err := w.writer.Write(batch)
	w.size += int64(len(batch))
	atomic.AddInt32(&w.pending, int32(len(kvs)))
	w.mu.Unlock()

	if err != nil {
		return err
	}

	if w.syncMode == SyncBatch || w.syncMode == SyncEvery {
		return w.Sync()
	}

	return nil
}

func (w *WAL) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.writer.Flush(); err != nil {
		return err
	}

	if err := w.file.Sync(); err != nil {
		return err
	}

	atomic.StoreInt32(&w.pending, 0)
	w.lastSync = time.Now()

	select {
	case w.syncNotify <- struct{}{}:
	default:
	}

	return nil
}

func (w *WAL) timerSync() {
	if atomic.LoadInt32(&w.closed) == 1 {
		return
	}

	if atomic.LoadInt32(&w.pending) > 0 {
		w.Sync()
	}

	w.syncTimer.Reset(syncIntervalMs * time.Millisecond)
}

func (w *WAL) Rotate() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.writer.Flush(); err != nil {
		return err
	}
	if err := w.file.Sync(); err != nil {
		return err
	}
	w.file.Close()

	newPath := w.path + ".old"
	os.Remove(newPath)
	os.Rename(w.path, newPath)

	file, err := os.OpenFile(w.path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return err
	}

	w.file = file
	w.writer = bufio.NewWriterSize(file, writeBufferSize)
	w.size = 0

	return nil
}

func (w *WAL) ShouldRotate() bool {
	return w.size >= w.maxSize
}

func (w *WAL) Close() error {
	if !atomic.CompareAndSwapInt32(&w.closed, 0, 1) {
		return nil
	}

	if w.syncTimer != nil {
		w.syncTimer.Stop()
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.writer.Flush(); err != nil {
		return err
	}
	if err := w.file.Sync(); err != nil {
		return err
	}

	return w.file.Close()
}

func (w *WAL) Replay(fn func(key, value []byte) error) error {
	file, err := os.Open(w.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer file.Close()

	header := make([]byte, walEntryHeader)
	for {
		n, err := file.Read(header)
		if err != nil || n < walEntryHeader {
			break
		}

		magic := binary.LittleEndian.Uint32(header[0:4])
		if magic != walMagic {
			break
		}

		entryLen := binary.LittleEndian.Uint32(header[4:8])
		storedCRC := binary.LittleEndian.Uint32(header[8:12])

		data := make([]byte, entryLen)
		n, err = file.Read(data)
		if err != nil || n < int(entryLen) {
			break
		}

		if crc32.ChecksumIEEE(data) != storedCRC {
			break
		}

		keyLen := binary.LittleEndian.Uint32(data[0:4])
		key := data[4 : 4+keyLen]
		valueLen := binary.LittleEndian.Uint32(data[4+keyLen : 8+keyLen])
		value := data[8+keyLen : 8+keyLen+valueLen]

		if err := fn(key, value); err != nil {
			return err
		}
	}

	return nil
}

func (w *WAL) Clear() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.file.Truncate(0); err != nil {
		return err
	}
	if _, err := w.file.Seek(0, 0); err != nil {
		return err
	}
	w.writer.Reset(w.file)
	w.size = 0
	return nil
}
