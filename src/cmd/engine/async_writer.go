package engine

import (
	"encoding/binary"
	"sync"
	"sync/atomic"
	"time"
)

type AsyncWriter struct {
	db *RelationalDB

	buffer     [][2][]byte
	bufferMu   sync.Mutex
	bufferSize int

	flushChan   chan [][2][]byte
	doneChan    chan struct{}
	flushWg     sync.WaitGroup
	flushTicker *time.Ticker

	pending   uint64
	flushed   uint64
	batchSize int

	maxBatchSize   int
	flushInterval  time.Duration
	numFlushers    int
}

func NewAsyncWriter(db *RelationalDB) *AsyncWriter {
	aw := &AsyncWriter{
		db:             db,
		buffer:         make([][2][]byte, 0, 50000),
		flushChan:      make(chan [][2][]byte, 100),
		doneChan:       make(chan struct{}),
		maxBatchSize:   10000, 
		flushInterval:  50 * time.Millisecond,
		numFlushers:    4, 
	}

	for i := 0; i < aw.numFlushers; i++ {
		aw.flushWg.Add(1)
		go aw.flushWorker()
	}

	aw.flushTicker = time.NewTicker(aw.flushInterval)
	go aw.timerFlusher()

	return aw
}

func (aw *AsyncWriter) flushWorker() {
	defer aw.flushWg.Done()

	for batch := range aw.flushChan {
		if len(batch) > 0 {
			aw.db.kv.BatchSet(batch)
			atomic.AddUint64(&aw.flushed, uint64(len(batch)))
		}
	}
}

func (aw *AsyncWriter) timerFlusher() {
	for {
		select {
		case <-aw.flushTicker.C:
			aw.FlushAsync()
		case <-aw.doneChan:
			return
		}
	}
}

func (aw *AsyncWriter) Write(key, value []byte) {
	aw.bufferMu.Lock()

	keyCopy := make([]byte, len(key))
	copy(keyCopy, key)
	valCopy := make([]byte, len(value))
	copy(valCopy, value)

	aw.buffer = append(aw.buffer, [2][]byte{keyCopy, valCopy})
	shouldFlush := len(aw.buffer) >= aw.maxBatchSize

	aw.bufferMu.Unlock()

	atomic.AddUint64(&aw.pending, 1)

	if shouldFlush {
		aw.FlushAsync()
	}
}

func (aw *AsyncWriter) WriteNoCopy(key, value []byte) {
	aw.bufferMu.Lock()
	aw.buffer = append(aw.buffer, [2][]byte{key, value})
	shouldFlush := len(aw.buffer) >= aw.maxBatchSize
	aw.bufferMu.Unlock()

	atomic.AddUint64(&aw.pending, 1)

	if shouldFlush {
		aw.FlushAsync()
	}
}

func (aw *AsyncWriter) WriteBatch(kvs [][2][]byte) {
	if len(kvs) == 0 {
		return
	}

	aw.bufferMu.Lock()
	aw.buffer = append(aw.buffer, kvs...)
	shouldFlush := len(aw.buffer) >= aw.maxBatchSize
	aw.bufferMu.Unlock()

	atomic.AddUint64(&aw.pending, uint64(len(kvs)))

	if shouldFlush {
		aw.FlushAsync()
	}
}

func (aw *AsyncWriter) FlushAsync() {
	aw.bufferMu.Lock()
	if len(aw.buffer) == 0 {
		aw.bufferMu.Unlock()
		return
	}

	batch := aw.buffer
	aw.buffer = make([][2][]byte, 0, aw.maxBatchSize)
	aw.bufferMu.Unlock()

	select {
	case aw.flushChan <- batch:
	default:
		aw.db.kv.BatchSet(batch)
		atomic.AddUint64(&aw.flushed, uint64(len(batch)))
	}
}

func (aw *AsyncWriter) FlushSync() {
	aw.FlushAsync()

	for {
		if atomic.LoadUint64(&aw.flushed) >= atomic.LoadUint64(&aw.pending) {
			break
		}
		time.Sleep(time.Millisecond)
	}
}

func (aw *AsyncWriter) Close() {
	aw.flushTicker.Stop()
	close(aw.doneChan)
	aw.FlushSync()
	close(aw.flushChan)
	aw.flushWg.Wait()
}

func (aw *AsyncWriter) Stats() map[string]uint64 {
	return map[string]uint64{
		"pending": atomic.LoadUint64(&aw.pending),
		"flushed": atomic.LoadUint64(&aw.flushed),
	}
}

type HighSpeedInserter struct {
	db          *RelationalDB
	schema      *TableSchema
	asyncWriter *AsyncWriter

	encoders sync.Pool

	inserted uint64
}

type encoderBuffer struct {
	keyBuf []byte
	valBuf []byte
}

func (db *RelationalDB) NewHighSpeedInserter(tableName string) (*HighSpeedInserter, error) {
	db.mu.RLock()
	schema, exists := db.catalog[tableName]
	db.mu.RUnlock()

	if !exists {
		return nil, ErrTableNotFound
	}

	hsi := &HighSpeedInserter{
		db:          db,
		schema:      schema,
		asyncWriter: NewAsyncWriter(db),
		encoders: sync.Pool{
			New: func() interface{} {
				return &encoderBuffer{
					keyBuf: make([]byte, 12),
					valBuf: make([]byte, 0, 4096),
				}
			},
		},
	}

	return hsi, nil
}

func (hsi *HighSpeedInserter) Insert(data []interface{}) {
	pk := hsi.db.generatePK()

	enc := hsi.encoders.Get().(*encoderBuffer)

	binary.BigEndian.PutUint32(enc.keyBuf[:4], hsi.schema.ID)
	binary.BigEndian.PutUint64(enc.keyBuf[4:12], pk)

	row := Row{PrimaryKey: pk, Data: data}
	val, _ := hsi.db.encoder.EncodeValue(row)

	keyCopy := make([]byte, 12)
	copy(keyCopy, enc.keyBuf)

	hsi.encoders.Put(enc)

	hsi.asyncWriter.WriteNoCopy(keyCopy, val)
	atomic.AddUint64(&hsi.inserted, 1)
}

func (hsi *HighSpeedInserter) InsertBatch(rows [][]interface{}) {
	if len(rows) == 0 {
		return
	}

	kvs := make([][2][]byte, len(rows))

	for i, data := range rows {
		pk := hsi.db.generatePK()

		key := make([]byte, 12)
		binary.BigEndian.PutUint32(key[:4], hsi.schema.ID)
		binary.BigEndian.PutUint64(key[4:12], pk)

		row := Row{PrimaryKey: pk, Data: data}
		val, _ := hsi.db.encoder.EncodeValue(row)

		kvs[i] = [2][]byte{key, val}
	}

	hsi.asyncWriter.WriteBatch(kvs)
	atomic.AddUint64(&hsi.inserted, uint64(len(rows)))
}

func (hsi *HighSpeedInserter) InsertMap(data map[string]interface{}) {
	rowData := make([]interface{}, len(hsi.schema.Columns))
	for i, colName := range hsi.schema.Columns {
		if val, ok := data[colName]; ok {
			rowData[i] = val
		}
	}
	hsi.Insert(rowData)
}

func (hsi *HighSpeedInserter) InsertMaps(rows []map[string]interface{}) {
	if len(rows) == 0 {
		return
	}

	dataRows := make([][]interface{}, len(rows))
	for i, row := range rows {
		rowData := make([]interface{}, len(hsi.schema.Columns))
		for j, colName := range hsi.schema.Columns {
			if val, ok := row[colName]; ok {
				rowData[j] = val
			}
		}
		dataRows[i] = rowData
	}

	hsi.InsertBatch(dataRows)
}

func (hsi *HighSpeedInserter) Flush() {
	hsi.asyncWriter.FlushSync()
}

func (hsi *HighSpeedInserter) Close() uint64 {
	hsi.asyncWriter.Close()
	return hsi.inserted
}

func (hsi *HighSpeedInserter) Count() uint64 {
	return atomic.LoadUint64(&hsi.inserted)
}

