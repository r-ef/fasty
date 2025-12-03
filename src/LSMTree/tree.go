package tree

import (
	"container/list"
	"fmt"
	"path/filepath"
	"sync"

	"github.com/klauspost/compress/zstd"
	"github.com/couchbase/moss"
)

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

	encoder *zstd.Encoder
	decoder *zstd.Decoder

	cache    map[string]*list.Element
	lruList  *list.List
	cacheMax int
	cacheMu  sync.RWMutex
}

func OpenInMemory() (*DB, error) {
	collection, err := moss.NewCollection(moss.CollectionOptions{
		MergerIdleRunTimeoutMS: 50,
		MaxPreMergerBatches:    2048,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create in-memory collection: %w", err)
	}

	if err := collection.Start(); err != nil {
		return nil, fmt.Errorf("failed to start collection: %w", err)
	}

	enc, _ := zstd.NewWriter(nil)
	dec, _ := zstd.NewReader(nil)

	return &DB{
		collection:  collection,
		inMemory:    true,
		maxBatchOps: 100000,
		encoder:     enc,
		decoder:     dec,
		cache:       make(map[string]*list.Element),
		lruList:     list.New(),
		cacheMax:    10000,
	}, nil
}

func Open(dataDir string) (*DB, error) {
	absPath, err := filepath.Abs(dataDir)
	if err != nil {
		return nil, fmt.Errorf("failed to get the absolute path: %w", err)
	}

	storeOptions := moss.StoreOptions{
		CollectionOptions: moss.CollectionOptions{
			MergerIdleRunTimeoutMS: 200,
			MaxPreMergerBatches:    1024,
			MinMergePercentage:     0.8,
		},
		CompactionPercentage:  0.3,
		CompactionBufferPages: 8192,
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

	enc, _ := zstd.NewWriter(nil)
	dec, _ := zstd.NewReader(nil)

	return &DB{
		store:       store,
		collection:  collection,
		directory:   absPath,
		maxBatchOps: 25000,
		encoder:     enc,
		decoder:     dec,
		cache:       make(map[string]*list.Element),
		lruList:     list.New(),
		cacheMax:    10000,
	}, nil
}

type cacheItem struct {
	key   string
	value []byte
}

func (db *DB) addToCache(key string, value []byte) {
	db.cacheMu.Lock()
	defer db.cacheMu.Unlock()

	if elem, ok := db.cache[key]; ok {
		db.lruList.MoveToFront(elem)
		elem.Value.(*cacheItem).value = value
		return
	}

	if db.lruList.Len() >= db.cacheMax {
		elem := db.lruList.Back()
		if elem != nil {
			db.lruList.Remove(elem)
			delete(db.cache, elem.Value.(*cacheItem).key)
		}
	}

	item := &cacheItem{key: key, value: value}
	elem := db.lruList.PushFront(item)
	db.cache[key] = elem
}

func (db *DB) getFromCache(key string) ([]byte, bool) {
	db.cacheMu.RLock()
	defer db.cacheMu.RUnlock()

	if elem, ok := db.cache[key]; ok {
		return elem.Value.(*cacheItem).value, true
	}
	return nil, false
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
	compressed := db.encoder.EncodeAll(value, make([]byte, 0, len(value)))

	if db.writeBatch == nil {
		estSize := (len(key) + len(compressed)) * db.maxBatchOps
		if estSize < 1024*1024 {
			estSize = 1024 * 1024
		}
		batch, err := db.collection.NewBatch(db.maxBatchOps, estSize)
		if err != nil {
			return fmt.Errorf("failed to create batch: %w", err)
		}
		db.writeBatch = batch
	}

	if err := db.writeBatch.Set(key, compressed); err != nil {
		return fmt.Errorf("failed to set key: %w", err)
	}
	db.writeCount++

	db.addToCache(string(key), value)

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

	compressed := db.encoder.EncodeAll(value, make([]byte, 0, len(value)))

	batch, err := db.collection.NewBatch(1, len(key)+len(compressed))
	if err != nil {
		return fmt.Errorf("failed to create batch: %w", err)
	}
	defer batch.Close()

	if err := batch.Set(key, compressed); err != nil {
		return fmt.Errorf("failed to set key: %w", err)
	}

	db.addToCache(string(key), value)

	return db.collection.ExecuteBatch(batch, moss.WriteOptions{})
}

func (db *DB) Get(key []byte) ([]byte, error) {
	if val, ok := db.getFromCache(string(key)); ok {
		return val, nil
	}

	db.mu.RLock()
	defer db.mu.RUnlock()

	value, err := db.collection.Get(key, moss.ReadOptions{})
	if err != nil {
		return nil, fmt.Errorf("couldn't get key: %w", err)
	}

	if value == nil {
		return nil, nil
	}

	decompressed, err := db.decoder.DecodeAll(value, nil)
	if err != nil {
		result := make([]byte, len(value))
		copy(result, value)
		db.addToCache(string(key), result)
		return result, nil
	}

	db.addToCache(string(key), decompressed)
	return decompressed, nil
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

	db.cacheMu.Lock()
	if elem, ok := db.cache[string(key)]; ok {
		db.lruList.Remove(elem)
		delete(db.cache, string(key))
	}
	db.cacheMu.Unlock()

	return db.collection.ExecuteBatch(batch, moss.WriteOptions{})
}

type snapshotIterator struct {
	moss.Iterator
	ss      moss.Snapshot
	decoder *zstd.Decoder
}

func (si *snapshotIterator) Close() error {
	err := si.Iterator.Close()
	if ssErr := si.ss.Close(); ssErr != nil && err == nil {
		err = ssErr
	}
	return err
}

func (si *snapshotIterator) Current() ([]byte, []byte, error) {
	k, v, err := si.Iterator.Current()
	if err != nil {
		return nil, nil, err
	}
	decompressed, err := si.decoder.DecodeAll(v, nil)
	if err != nil {
		return k, v, nil
	}
	return k, decompressed, nil
}

func (si *snapshotIterator) CurrentEx() (moss.EntryEx, []byte, []byte, error) {
	ex, k, v, err := si.Iterator.CurrentEx()
	if err != nil {
		return ex, nil, nil, err
	}
	decompressed, err := si.decoder.DecodeAll(v, nil)
	if err != nil {
		return ex, k, v, nil
	}
	return ex, k, decompressed, nil
}

type AsyncPrefetchIterator struct {
	wrapped *snapshotIterator
	ch      chan itemResult
	done    chan struct{}
	curKey  []byte
	curVal  []byte
	curEx   moss.EntryEx
	curErr  error
}

type itemResult struct {
	key []byte
	val []byte
	ex  moss.EntryEx
	err error
}

func NewAsyncPrefetchIterator(wrapped *snapshotIterator) *AsyncPrefetchIterator {
	api := &AsyncPrefetchIterator{
		wrapped: wrapped,
		ch:      make(chan itemResult, 100),
		done:    make(chan struct{}),
	}

	ex, k, v, err := wrapped.CurrentEx()
	api.curKey = k
	api.curVal = v
	api.curEx = ex
	api.curErr = err

	go api.prefetchLoop()
	return api
}

func (api *AsyncPrefetchIterator) prefetchLoop() {
	defer close(api.ch)
	for {
		select {
		case <-api.done:
			return
		default:
			err := api.wrapped.Next()
			if err == moss.ErrIteratorDone {
				return
			}
			if err != nil {
				api.ch <- itemResult{err: err}
				return
			}
			ex, k, v, err := api.wrapped.CurrentEx()

			kc := make([]byte, len(k))
			copy(kc, k)
			vc := make([]byte, len(v))
			copy(vc, v)

			select {
			case api.ch <- itemResult{key: kc, val: vc, ex: ex, err: err}:
			case <-api.done:
				return
			}
		}
	}
}

func (api *AsyncPrefetchIterator) Close() error {
	close(api.done)
	return api.wrapped.Close()
}

func (api *AsyncPrefetchIterator) Current() ([]byte, []byte, error) {
	return api.curKey, api.curVal, api.curErr
}

func (api *AsyncPrefetchIterator) CurrentEx() (moss.EntryEx, []byte, []byte, error) {
	return api.curEx, api.curKey, api.curVal, api.curErr
}

func (api *AsyncPrefetchIterator) Next() error {
	res, ok := <-api.ch
	if !ok {
		return moss.ErrIteratorDone
	}
	api.curKey = res.key
	api.curVal = res.val
	api.curEx = res.ex
	api.curErr = res.err
	return res.err
}

func (api *AsyncPrefetchIterator) SeekTo(key []byte) error {
	return fmt.Errorf("SeekTo not supported on async iterator")
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

	si := &snapshotIterator{
		Iterator: iter,
		ss:       ss,
		decoder:  db.decoder,
	}

	return NewAsyncPrefetchIterator(si), nil
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

	si := &snapshotIterator{
		Iterator: iter,
		ss:       ss,
		decoder:  db.decoder,
	}

	return NewAsyncPrefetchIterator(si), nil
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

	compressedKVs := make([][2][]byte, len(kvs))
	totalBytes := 0

	for i, kv := range kvs {
		cVal := db.encoder.EncodeAll(kv[1], nil)
		compressedKVs[i] = [2][]byte{kv[0], cVal}
		totalBytes += len(kv[0]) + len(cVal)
	}

	if totalBytes < 1024*1024 {
		totalBytes = 1024 * 1024
	}

	batch, err := db.collection.NewBatch(len(kvs), totalBytes)
	if err != nil {
		return fmt.Errorf("failed to create batch: %w", err)
	}
	defer batch.Close()

	for _, kv := range compressedKVs {
		if err := batch.Set(kv[0], kv[1]); err != nil {
			return err
		}
	}

	db.cacheMu.Lock()
	for _, kv := range kvs {
		if elem, ok := db.cache[string(kv[0])]; ok {
			db.lruList.MoveToFront(elem)
			elem.Value.(*cacheItem).value = kv[1]
		} else {
			if db.lruList.Len() >= db.cacheMax {
				elem := db.lruList.Back()
				if elem != nil {
					db.lruList.Remove(elem)
					delete(db.cache, elem.Value.(*cacheItem).key)
				}
			}
			item := &cacheItem{key: string(kv[0]), value: kv[1]}
			elem := db.lruList.PushFront(item)
			db.cache[string(kv[0])] = elem
		}
	}
	db.cacheMu.Unlock()

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

	compressedSets := make([][2][]byte, len(sets))
	totalBytes := 0
	for i, kv := range sets {
		cVal := db.encoder.EncodeAll(kv[1], nil)
		compressedSets[i] = [2][]byte{kv[0], cVal}
		totalBytes += len(kv[0]) + len(cVal)
	}
	for _, k := range deletes {
		totalBytes += len(k)
	}

	if totalBytes < 1024*1024 {
		totalBytes = 1024 * 1024
	}

	batch, err := db.collection.NewBatch(totalOps, totalBytes)
	if err != nil {
		return fmt.Errorf("failed to create batch: %w", err)
	}
	defer batch.Close()

	for _, kv := range compressedSets {
		if err := batch.Set(kv[0], kv[1]); err != nil {
			return err
		}
	}

	for _, k := range deletes {
		if err := batch.Del(k); err != nil {
			return err
		}
	}

	err = db.collection.ExecuteBatch(batch, moss.WriteOptions{})
	if err != nil {
		return err
	}

	db.cacheMu.Lock()
	for _, kv := range sets {
		key := string(kv[0])
		if elem, ok := db.cache[key]; ok {
			db.lruList.MoveToFront(elem)
			elem.Value.(*cacheItem).value = kv[1]
		} else {
			if db.lruList.Len() >= db.cacheMax {
				e := db.lruList.Back()
				if e != nil {
					db.lruList.Remove(e)
					delete(db.cache, e.Value.(*cacheItem).key)
				}
			}
			item := &cacheItem{key: key, value: kv[1]}
			elem := db.lruList.PushFront(item)
			db.cache[key] = elem
		}
	}
	for _, k := range deletes {
		key := string(k)
		if elem, ok := db.cache[key]; ok {
			db.lruList.Remove(elem)
			delete(db.cache, key)
		}
	}
	db.cacheMu.Unlock()

	return nil
}

func (db *DB) BatchSetMap(kvs map[string][]byte) error {
	if len(kvs) == 0 {
		return nil
	}

	s := make([][2][]byte, 0, len(kvs))
	for k, v := range kvs {
		s = append(s, [2][]byte{[]byte(k), v})
	}
	return db.BatchSet(s)
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
