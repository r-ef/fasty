package tree

import (
	"bytes"
	"fmt"
	"path/filepath"
	"sync"

	"github.com/couchbase/moss"
)

var bufferPool = sync.Pool{
	New: func() interface{} {
		return bytes.NewBuffer(make([]byte, 0, 256))
	},
}

func getBuffer() *bytes.Buffer {
	buf := bufferPool.Get().(*bytes.Buffer)
	buf.Reset()
	return buf
}

func putBuffer(buf *bytes.Buffer) {
	if buf.Cap() < 64*1024 { 
		bufferPool.Put(buf)
	}
}

type DB struct {
	mu sync.RWMutex

	store      *moss.Store
	collection moss.Collection
	directory  string
	inMemory   bool

	writeMu     sync.Mutex
	writeBatch  moss.Batch
	writeCount  int
	maxBatchOps int
}

func OpenInMemory() (*DB, error) {
	collection, err := moss.NewCollection(moss.CollectionOptions{
		MergerIdleRunTimeoutMS: 50,
		MaxPreMergerBatches:    128,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create in-memory collection: %w", err)
	}

	if err := collection.Start(); err != nil {
		return nil, fmt.Errorf("failed to start collection: %w", err)
	}

	return &DB{
		collection:  collection,
		inMemory:    true,
		maxBatchOps: 1000, 
	}, nil
}

func Open(dataDir string) (*DB, error) {
	absPath, err := filepath.Abs(dataDir)
	if err != nil {
		return nil, fmt.Errorf("failed to get the absolute path: %w", err)
	}

	storeOptions := moss.StoreOptions{
		CollectionOptions: moss.CollectionOptions{
			MergerIdleRunTimeoutMS: 10,
			MaxPreMergerBatches: 256,
			MinMergePercentage: 0.0,
		},
		CompactionPercentage:  0.3,
		CompactionBufferPages: 4096, 
		CompactionSync:        false,
		OpenFile:              nil,
	}

	persistOps := moss.StorePersistOptions{
		NoSync:            true, 
		CompactionConcern: moss.CompactionDisable, 
	}

	store, collection, err := moss.OpenStoreCollection(
		absPath,
		storeOptions,
		persistOps,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to open moss store: %w", err)
	}

	return &DB{
		store:       store,
		collection:  collection,
		directory:   absPath,
		maxBatchOps: 5000, 
	}, nil
}

func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	db.flushBatchLocked()

	if db.collection != nil {
		if !db.inMemory && db.store != nil {
			ss, err := db.collection.Snapshot()
			if err != nil {
				return fmt.Errorf("failed to snapshot collection: %w", err)
			}
			defer ss.Close()

			_, err = db.store.Persist(ss, moss.StorePersistOptions{
				NoSync:            false,
				CompactionConcern: moss.CompactionAllow,
			})
			if err != nil {
				return fmt.Errorf("failed to persist collection: %w", err)
			}
		}
		db.collection.Close()
	}

	if db.store != nil {
		if err := db.store.Close(); err != nil {
			return fmt.Errorf("failed to close moss store: %w", err)
		}
	}
	return nil
}

func (db *DB) flushBatchLocked() error {
	if db.writeBatch == nil || db.writeCount == 0 {
		return nil
	}

	err := db.collection.ExecuteBatch(db.writeBatch, moss.WriteOptions{})
	db.writeBatch.Close()
	db.writeBatch = nil
	db.writeCount = 0
	return err
}

func (db *DB) Set(key, value []byte) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	return db.setLocked(key, value)
}

func (db *DB) setLocked(key, value []byte) error {
	if db.writeBatch == nil {
		size := (len(key) + len(value)) * db.maxBatchOps
		batch, err := db.collection.NewBatch(db.maxBatchOps, size)
		if err != nil {
			return fmt.Errorf("failed to create batch: %w", err)
		}
		db.writeBatch = batch
	}

	if err := db.writeBatch.Set(key, value); err != nil {
		return fmt.Errorf("failed to set key: %w", err)
	}
	db.writeCount++

	if db.writeCount >= db.maxBatchOps {
		return db.flushBatchLocked()
	}
	return nil
}

func (db *DB) SetImmediate(key, value []byte) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if err := db.flushBatchLocked(); err != nil {
		return err
	}

	batch, err := db.collection.NewBatch(1, len(key)+len(value))
	if err != nil {
		return fmt.Errorf("failed to create batch: %w", err)
	}
	defer batch.Close()

	if err := batch.Set(key, value); err != nil {
		return fmt.Errorf("failed to set key: %w", err)
	}

	return db.collection.ExecuteBatch(batch, moss.WriteOptions{})
}

func (db *DB) Get(key []byte) ([]byte, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	value, err := db.collection.Get(key, moss.ReadOptions{})
	if err != nil {
		return nil, fmt.Errorf("couldn't get key: %w", err)
	}

	if value == nil {
		return nil, nil
	}
	result := make([]byte, len(value))
	copy(result, value)
	return result, nil
}

func (db *DB) Delete(key []byte) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if err := db.flushBatchLocked(); err != nil {
		return err
	}

	batch, err := db.collection.NewBatch(1, len(key))
	if err != nil {
		return fmt.Errorf("failed to create batch: %w", err)
	}
	defer batch.Close()

	if err := batch.Del(key); err != nil {
		return fmt.Errorf("failed to delete key: %w", err)
	}

	return db.collection.ExecuteBatch(batch, moss.WriteOptions{})
}

type snapshotIterator struct {
	moss.Iterator
	ss moss.Snapshot
}

func (si *snapshotIterator) Close() error {
	err := si.Iterator.Close()
	if ssErr := si.ss.Close(); ssErr != nil && err == nil {
		err = ssErr
	}
	return err
}

func (db *DB) NewIterator() (moss.Iterator, error) {
	db.mu.Lock()
	if err := db.flushBatchLocked(); err != nil {
		db.mu.Unlock()
		return nil, err
	}
	db.mu.Unlock()

	db.mu.RLock()
	defer db.mu.RUnlock()

	ss, err := db.collection.Snapshot()
	if err != nil {
		return nil, fmt.Errorf("failed to take snapshot: %w", err)
	}

	iter, err := ss.StartIterator(nil, nil, moss.IteratorOptions{})
	if err != nil {
		ss.Close()
		return nil, fmt.Errorf("failed to start iterator: %w", err)
	}

	return &snapshotIterator{
		Iterator: iter,
		ss:       ss,
	}, nil
}

func (db *DB) NewPrefixIterator(prefix []byte) (moss.Iterator, error) {
	db.mu.Lock()
	if err := db.flushBatchLocked(); err != nil {
		db.mu.Unlock()
		return nil, err
	}
	db.mu.Unlock()

	db.mu.RLock()
	defer db.mu.RUnlock()

	ss, err := db.collection.Snapshot()
	if err != nil {
		return nil, fmt.Errorf("failed to take snapshot: %w", err)
	}

	endKey := make([]byte, len(prefix))
	copy(endKey, prefix)
	for i := len(endKey) - 1; i >= 0; i-- {
		if endKey[i] < 0xFF {
			endKey[i]++
			break
		}
		endKey[i] = 0
	}

	iter, err := ss.StartIterator(prefix, endKey, moss.IteratorOptions{})
	if err != nil {
		ss.Close()
		return nil, fmt.Errorf("failed to start prefix iterator: %w", err)
	}

	return &snapshotIterator{
		Iterator: iter,
		ss:       ss,
	}, nil
}

func (db *DB) BatchSet(kvs [][2][]byte) error {
	if len(kvs) == 0 {
		return nil
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	if err := db.flushBatchLocked(); err != nil {
		return err
	}

	totalBytes := 0
	for _, kv := range kvs {
		totalBytes += len(kv[0]) + len(kv[1])
	}

	batch, err := db.collection.NewBatch(len(kvs), totalBytes)
	if err != nil {
		return fmt.Errorf("failed to create batch: %w", err)
	}
	defer batch.Close()

	for _, kv := range kvs {
		if err := batch.Set(kv[0], kv[1]); err != nil {
			return err
		}
	}

	return db.collection.ExecuteBatch(batch, moss.WriteOptions{})
}

func (db *DB) BatchWrite(sets [][2][]byte, deletes [][]byte) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if err := db.flushBatchLocked(); err != nil {
		return err
	}

	totalOps := len(sets) + len(deletes)
	if totalOps == 0 {
		return nil
	}

	totalBytes := 0
	for _, kv := range sets {
		totalBytes += len(kv[0]) + len(kv[1])
	}
	for _, k := range deletes {
		totalBytes += len(k)
	}

	batch, err := db.collection.NewBatch(totalOps, totalBytes)
	if err != nil {
		return fmt.Errorf("failed to create batch: %w", err)
	}
	defer batch.Close()

	for _, kv := range sets {
		if err := batch.Set(kv[0], kv[1]); err != nil {
			return err
		}
	}

	for _, k := range deletes {
		if err := batch.Del(k); err != nil {
			return err
		}
	}

	return db.collection.ExecuteBatch(batch, moss.WriteOptions{})
}

func (db *DB) BatchSetMap(kvs map[string][]byte) error {
	if len(kvs) == 0 {
		return nil
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	if err := db.flushBatchLocked(); err != nil {
		return err
	}

	totalBytes := 0
	for k, v := range kvs {
		totalBytes += len(k) + len(v)
	}

	batch, err := db.collection.NewBatch(len(kvs), totalBytes)
	if err != nil {
		return fmt.Errorf("failed to create batch: %w", err)
	}
	defer batch.Close()

	for k, v := range kvs {
		if err := batch.Set([]byte(k), v); err != nil {
			return err
		}
	}

	return db.collection.ExecuteBatch(batch, moss.WriteOptions{})
}

func (db *DB) Flush() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if err := db.flushBatchLocked(); err != nil {
		return err
	}

	ss, err := db.collection.Snapshot()
	if err != nil {
		return err
	}
	defer ss.Close()

	_, err = db.store.Persist(ss, moss.StorePersistOptions{
		NoSync:            false,
		CompactionConcern: moss.CompactionAllow,
	})
	return err
}

func (db *DB) Stats() map[string]interface{} {
	db.mu.RLock()
	defer db.mu.RUnlock()

	stats := make(map[string]interface{})
	if s, err := db.collection.Stats(); err == nil {
		stats["collection"] = s
	}
	if s, err := db.store.Stats(); err == nil {
		stats["store"] = s
	}
	return stats
}
