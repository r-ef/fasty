package engine

import (
	"encoding/binary"
	"sync"
	"sync/atomic"
	"time"
)

type DurableWriter struct {
	db  *RelationalDB
	wal *WAL

	buffer   [][2][]byte
	bufferMu sync.Mutex

	flushChan chan [][2][]byte
	doneChan  chan struct{}
	flushWg   sync.WaitGroup

	pending uint64
	flushed uint64

	maxBatchSize  int
	flushInterval time.Duration
	flushTicker   *time.Ticker
	numFlushers   int
}

func NewDurableWriter(db *RelationalDB, walDir string) (*DurableWriter, error) {
	wal, err := NewWAL(walDir, SyncBatch)
	if err != nil {
		return nil, err
	}

	dw := &DurableWriter{
		db:            db,
		wal:           wal,
		buffer:        make([][2][]byte, 0, 50000),
		flushChan:     make(chan [][2][]byte, 100),
		doneChan:      make(chan struct{}),
		maxBatchSize:  10000,
		flushInterval: 100 * time.Millisecond,
		numFlushers:   4,
	}

	if err := dw.recoverFromWAL(); err != nil {
		wal.Close()
		return nil, err
	}

	for i := 0; i < dw.numFlushers; i++ {
		dw.flushWg.Add(1)
		go dw.flushWorker()
	}

	dw.flushTicker = time.NewTicker(dw.flushInterval)
	go dw.timerFlusher()

	return dw, nil
}

func (dw *DurableWriter) recoverFromWAL() error {
	batch := make([][2][]byte, 0, 10000)

	err := dw.wal.Replay(func(key, value []byte) error {
		keyCopy := make([]byte, len(key))
		valueCopy := make([]byte, len(value))
		copy(keyCopy, key)
		copy(valueCopy, value)
		batch = append(batch, [2][]byte{keyCopy, valueCopy})

		if len(batch) >= 10000 {
			dw.db.kv.BatchSet(batch)
			batch = batch[:0]
		}
		return nil
	})

	if err != nil {
		return err
	}

	if len(batch) > 0 {
		dw.db.kv.BatchSet(batch)
	}

	dw.db.kv.Flush()
	dw.wal.Clear()

	return nil
}

func (dw *DurableWriter) flushWorker() {
	defer dw.flushWg.Done()

	for batch := range dw.flushChan {
		if len(batch) > 0 {
			dw.db.kv.BatchSet(batch)
			atomic.AddUint64(&dw.flushed, uint64(len(batch)))
		}
	}
}

func (dw *DurableWriter) timerFlusher() {
	for {
		select {
		case <-dw.flushTicker.C:
			dw.FlushToLSM()
		case <-dw.doneChan:
			return
		}
	}
}

func (dw *DurableWriter) Write(key, value []byte) error {
	if err := dw.wal.Write(key, value); err != nil {
		return err
	}

	keyCopy := make([]byte, len(key))
	valueCopy := make([]byte, len(value))
	copy(keyCopy, key)
	copy(valueCopy, value)

	dw.bufferMu.Lock()
	dw.buffer = append(dw.buffer, [2][]byte{keyCopy, valueCopy})
	shouldFlush := len(dw.buffer) >= dw.maxBatchSize
	atomic.AddUint64(&dw.pending, 1)
	dw.bufferMu.Unlock()

	if shouldFlush {
		dw.FlushToLSM()
	}

	return nil
}

func (dw *DurableWriter) WriteBatch(kvs [][2][]byte) error {
	if err := dw.wal.WriteBatch(kvs); err != nil {
		return err
	}

	dw.bufferMu.Lock()
	for _, kv := range kvs {
		keyCopy := make([]byte, len(kv[0]))
		valueCopy := make([]byte, len(kv[1]))
		copy(keyCopy, kv[0])
		copy(valueCopy, kv[1])
		dw.buffer = append(dw.buffer, [2][]byte{keyCopy, valueCopy})
	}
	shouldFlush := len(dw.buffer) >= dw.maxBatchSize
	atomic.AddUint64(&dw.pending, uint64(len(kvs)))
	dw.bufferMu.Unlock()

	if shouldFlush {
		dw.FlushToLSM()
	}

	return nil
}

func (dw *DurableWriter) FlushToLSM() {
	dw.bufferMu.Lock()
	if len(dw.buffer) == 0 {
		dw.bufferMu.Unlock()
		return
	}

	batch := dw.buffer
	dw.buffer = make([][2][]byte, 0, dw.maxBatchSize)
	dw.bufferMu.Unlock()

	select {
	case dw.flushChan <- batch:
	default:
		dw.db.kv.BatchSet(batch)
		atomic.AddUint64(&dw.flushed, uint64(len(batch)))
	}

	if dw.wal.ShouldRotate() {
		dw.db.kv.Flush()
		dw.wal.Rotate()
	}
}

func (dw *DurableWriter) FlushSync() {
	dw.FlushToLSM()

	for {
		if atomic.LoadUint64(&dw.flushed) >= atomic.LoadUint64(&dw.pending) {
			break
		}
		time.Sleep(time.Millisecond)
	}

	dw.db.kv.Flush()
}

func (dw *DurableWriter) Close() error {
	dw.flushTicker.Stop()
	close(dw.doneChan)
	dw.FlushSync()
	close(dw.flushChan)
	dw.flushWg.Wait()
	dw.wal.Clear()
	return dw.wal.Close()
}

type DurableInserter struct {
	db           *RelationalDB
	schema       *TableSchema
	durWriter    *DurableWriter
	inserted     uint64
}

func (db *RelationalDB) NewDurableInserter(tableName string, walDir string) (*DurableInserter, error) {
	schema, ok := db.catalog[tableName]
	if !ok {
		return nil, ErrTableNotFound
	}

	durWriter, err := NewDurableWriter(db, walDir)
	if err != nil {
		return nil, err
	}

	return &DurableInserter{
		db:        db,
		schema:    schema,
		durWriter: durWriter,
	}, nil
}

func (di *DurableInserter) InsertBatch(rows [][]interface{}) error {
	if len(rows) == 0 {
		return nil
	}

	kvs := make([][2][]byte, len(rows))

	for i, data := range rows {
		pk := di.db.generatePK()

		key := make([]byte, 12)
		binary.BigEndian.PutUint32(key[:4], di.schema.ID)
		binary.BigEndian.PutUint64(key[4:12], pk)

		row := Row{PrimaryKey: pk, Data: data}
		val, _ := di.db.encoder.EncodeValue(row)

		kvs[i] = [2][]byte{key, val}
	}

	if err := di.durWriter.WriteBatch(kvs); err != nil {
		return err
	}

	atomic.AddUint64(&di.inserted, uint64(len(rows)))
	return nil
}

func (di *DurableInserter) InsertMaps(maps []map[string]interface{}) error {
	rows := make([][]interface{}, len(maps))
	for i, m := range maps {
		rowData := make([]interface{}, len(di.schema.Columns))
		for j, colName := range di.schema.Columns {
			if val, ok := m[colName]; ok {
				rowData[j] = val
			}
		}
		rows[i] = rowData
	}
	return di.InsertBatch(rows)
}

func (di *DurableInserter) Flush() {
	di.durWriter.FlushSync()
}

func (di *DurableInserter) Close() error {
	return di.durWriter.Close()
}

func (di *DurableInserter) Stats() (inserted, pending uint64) {
	return atomic.LoadUint64(&di.inserted), atomic.LoadUint64(&di.durWriter.pending)
}

