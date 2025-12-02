package engine

import (
	"encoding/binary"
	"sync"
	"sync/atomic"
	"time"
)

func (db *RelationalDB) generatePK() uint64 {
	return uint64(atomic.AddUint32(&db.nextTableID, 1)) + uint64(time.Now().UnixNano()%1000000)
}

func (db *RelationalDB) BatchInsert(tableName string, rows []map[string]interface{}) (int, error) {
	db.mu.RLock()
	schema, exists := db.catalog[tableName]
	db.mu.RUnlock()

	if !exists {
		return 0, ErrTableNotFound
	}

	kvPairs := make([][2][]byte, 0, len(rows))

	for _, rowData := range rows {
		var pk uint64
		if id, ok := rowData["id"]; ok {
			switch v := id.(type) {
			case int:
				pk = uint64(v)
			case int64:
				pk = uint64(v)
			case float64:
				pk = uint64(v)
			case uint64:
				pk = v
			}
		}
		if pk == 0 {
			pk = db.generatePK()
		}

		data := make([]interface{}, len(schema.Columns))
		for i, colName := range schema.Columns {
			if val, ok := rowData[colName]; ok {
				data[i] = val
			}
		}

		row := Row{PrimaryKey: pk, Data: data}
		key := db.encoder.EncodeKey(schema.ID, pk)
		val, err := db.encoder.EncodeValue(row)
		if err != nil {
			continue
		}

		kvPairs = append(kvPairs, [2][]byte{key, val})
	}

	if err := db.kv.BatchSet(kvPairs); err != nil {
		return 0, err
	}

	return len(kvPairs), nil
}

func (db *RelationalDB) BatchInsertRaw(tableName string, columnData [][]interface{}) (int, error) {
	db.mu.RLock()
	schema, exists := db.catalog[tableName]
	db.mu.RUnlock()

	if !exists {
		return 0, ErrTableNotFound
	}

	kvPairs := make([][2][]byte, 0, len(columnData))

	for _, data := range columnData {
		pk := db.generatePK()
		row := Row{PrimaryKey: pk, Data: data}
		key := db.encoder.EncodeKey(schema.ID, pk)
		val, err := db.encoder.EncodeValue(row)
		if err != nil {
			continue
		}
		kvPairs = append(kvPairs, [2][]byte{key, val})
	}

	if err := db.kv.BatchSet(kvPairs); err != nil {
		return 0, err
	}

	return len(kvPairs), nil
}

func (db *RelationalDB) DirectInsert(tableName string, data map[string]interface{}) error {
	db.mu.RLock()
	schema, exists := db.catalog[tableName]
	db.mu.RUnlock()

	if !exists {
		return ErrTableNotFound
	}

	var pk uint64
	if id, ok := data["id"]; ok {
		switch v := id.(type) {
		case int:
			pk = uint64(v)
		case int64:
			pk = uint64(v)
		case float64:
			pk = uint64(v)
		}
	}
	if pk == 0 {
		pk = db.generatePK()
	}

	rowData := make([]interface{}, len(schema.Columns))
	for i, colName := range schema.Columns {
		if val, ok := data[colName]; ok {
			rowData[i] = val
		}
	}

	row := Row{PrimaryKey: pk, Data: rowData}
	key := db.encoder.EncodeKey(schema.ID, pk)
	val, err := db.encoder.EncodeValue(row)
	if err != nil {
		return err
	}

	return db.kv.Set(key, val)
}

type BatchProcessor struct {
	db       *RelationalDB
	schema   *TableSchema
	workers  int
	jobChan  chan []interface{}
	wg       sync.WaitGroup
	count    uint64
	errors   uint64
}

func (db *RelationalDB) NewBatchProcessor(tableName string, workers int) (*BatchProcessor, error) {
	db.mu.RLock()
	schema, exists := db.catalog[tableName]
	db.mu.RUnlock()

	if !exists {
		return nil, ErrTableNotFound
	}

	if workers <= 0 {
		workers = 8
	}

	bp := &BatchProcessor{
		db:      db,
		schema:  schema,
		workers: workers,
		jobChan: make(chan []interface{}, workers*1000),
	}

	for i := 0; i < workers; i++ {
		bp.wg.Add(1)
		go bp.worker()
	}

	return bp, nil
}

func (bp *BatchProcessor) worker() {
	defer bp.wg.Done()

	batch := make([][2][]byte, 0, 100)

	for data := range bp.jobChan {
		pk := bp.db.generatePK()
		row := Row{PrimaryKey: pk, Data: data}
		key := bp.db.encoder.EncodeKey(bp.schema.ID, pk)
		val, err := bp.db.encoder.EncodeValue(row)
		if err != nil {
			atomic.AddUint64(&bp.errors, 1)
			continue
		}

		batch = append(batch, [2][]byte{key, val})

		if len(batch) >= 100 {
			if err := bp.db.kv.BatchSet(batch); err != nil {
				atomic.AddUint64(&bp.errors, uint64(len(batch)))
			} else {
				atomic.AddUint64(&bp.count, uint64(len(batch)))
			}
			batch = batch[:0]
		}
	}

	if len(batch) > 0 {
		if err := bp.db.kv.BatchSet(batch); err != nil {
			atomic.AddUint64(&bp.errors, uint64(len(batch)))
		} else {
			atomic.AddUint64(&bp.count, uint64(len(batch)))
		}
	}
}

func (bp *BatchProcessor) Insert(data []interface{}) {
	bp.jobChan <- data
}

func (bp *BatchProcessor) Close() (inserted uint64, errors uint64) {
	close(bp.jobChan)
	bp.wg.Wait()
	return bp.count, bp.errors
}

type StreamInsert struct {
	db        *RelationalDB
	schema    *TableSchema
	buffer    [][2][]byte
	mu        sync.Mutex
	flushSize int
}

func (db *RelationalDB) NewStreamInsert(tableName string, bufferSize int) (*StreamInsert, error) {
	db.mu.RLock()
	schema, exists := db.catalog[tableName]
	db.mu.RUnlock()

	if !exists {
		return nil, ErrTableNotFound
	}

	if bufferSize <= 0 {
		bufferSize = 10000
	}

	return &StreamInsert{
		db:        db,
		schema:    schema,
		buffer:    make([][2][]byte, 0, bufferSize),
		flushSize: bufferSize / 10,
	}, nil
}

func (s *StreamInsert) Add(data []interface{}) error {
	pk := s.db.generatePK()
	row := Row{PrimaryKey: pk, Data: data}
	key := s.db.encoder.EncodeKey(s.schema.ID, pk)
	val, err := s.db.encoder.EncodeValue(row)
	if err != nil {
		return err
	}

	s.mu.Lock()
	s.buffer = append(s.buffer, [2][]byte{key, val})
	shouldFlush := len(s.buffer) >= s.flushSize
	s.mu.Unlock()

	if shouldFlush {
		return s.Flush()
	}
	return nil
}

func (s *StreamInsert) Flush() error {
	s.mu.Lock()
	if len(s.buffer) == 0 {
		s.mu.Unlock()
		return nil
	}

	batch := make([][2][]byte, len(s.buffer))
	copy(batch, s.buffer)
	s.buffer = s.buffer[:0]
	s.mu.Unlock()

	return s.db.kv.BatchSet(batch)
}

func (s *StreamInsert) Close() error {
	return s.Flush()
}

type ColumnBuffer struct {
	db      *RelationalDB
	schema  *TableSchema
	columns [][]interface{}
	count   int
	cap     int
}

func (db *RelationalDB) NewColumnBuffer(tableName string, capacity int) (*ColumnBuffer, error) {
	db.mu.RLock()
	schema, exists := db.catalog[tableName]
	db.mu.RUnlock()

	if !exists {
		return nil, ErrTableNotFound
	}

	if capacity <= 0 {
		capacity = 10000
	}

	columns := make([][]interface{}, len(schema.Columns))
	for i := range columns {
		columns[i] = make([]interface{}, 0, capacity)
	}

	return &ColumnBuffer{
		db:      db,
		schema:  schema,
		columns: columns,
		cap:     capacity,
	}, nil
}

func (cb *ColumnBuffer) AddRow(values ...interface{}) {
	for i := 0; i < len(cb.columns) && i < len(values); i++ {
		cb.columns[i] = append(cb.columns[i], values[i])
	}
	cb.count++
}

func (cb *ColumnBuffer) Flush() (int, error) {
	if cb.count == 0 {
		return 0, nil
	}

	batch := make([][2][]byte, 0, cb.count)

	for rowIdx := 0; rowIdx < cb.count; rowIdx++ {
		pk := cb.db.generatePK()

		data := make([]interface{}, len(cb.columns))
		for col := 0; col < len(cb.columns); col++ {
			if rowIdx < len(cb.columns[col]) {
				data[col] = cb.columns[col][rowIdx]
			}
		}

		row := Row{PrimaryKey: pk, Data: data}
		key := cb.db.encoder.EncodeKey(cb.schema.ID, pk)
		val, err := cb.db.encoder.EncodeValue(row)
		if err != nil {
			continue
		}

		batch = append(batch, [2][]byte{key, val})
	}

	if err := cb.db.kv.BatchSet(batch); err != nil {
		return 0, err
	}

	inserted := len(batch)

	for i := range cb.columns {
		cb.columns[i] = cb.columns[i][:0]
	}
	cb.count = 0

	return inserted, nil
}

type UltraFastInsert struct {
	db      *RelationalDB
	schema  *TableSchema
	keyBuf  []byte
	batch   [][2][]byte
	pending int
}

func (db *RelationalDB) NewUltraFastInsert(tableName string) (*UltraFastInsert, error) {
	db.mu.RLock()
	schema, exists := db.catalog[tableName]
	db.mu.RUnlock()

	if !exists {
		return nil, ErrTableNotFound
	}

	return &UltraFastInsert{
		db:     db,
		schema: schema,
		keyBuf: make([]byte, 12),
		batch:  make([][2][]byte, 0, 1000),
	}, nil
}

func (u *UltraFastInsert) Insert(data []interface{}) error {
	pk := u.db.generatePK()

	binary.BigEndian.PutUint32(u.keyBuf[:4], u.schema.ID)
	binary.BigEndian.PutUint64(u.keyBuf[4:12], pk)

	row := Row{PrimaryKey: pk, Data: data}
	val, err := u.db.encoder.EncodeValue(row)
	if err != nil {
		return err
	}

	keyCopy := make([]byte, 12)
	copy(keyCopy, u.keyBuf)

	u.batch = append(u.batch, [2][]byte{keyCopy, val})
	u.pending++

	if u.pending >= 1000 {
		return u.Flush()
	}
	return nil
}

func (u *UltraFastInsert) Flush() error {
	if u.pending == 0 {
		return nil
	}

	err := u.db.kv.BatchSet(u.batch)

	u.batch = u.batch[:0]
	u.pending = 0

	return err
}

func (u *UltraFastInsert) Close() error {
	return u.Flush()
}

