package memstore

import (
	"encoding/binary"
	"hash/maphash"
	"sync"
	"sync/atomic"
)

const (
	numShards    = 256 
	shardMask    = numShards - 1
	initialSize  = 1024
)

type Shard struct {
	mu   sync.RWMutex
	data map[string][]byte
}

type MemStore struct {
	shards  [numShards]*Shard
	seed    maphash.Seed
	count   uint64
	size    uint64
}

func New() *MemStore {
	m := &MemStore{
		seed: maphash.MakeSeed(),
	}
	for i := 0; i < numShards; i++ {
		m.shards[i] = &Shard{
			data: make(map[string][]byte, initialSize),
		}
	}
	return m
}

func (m *MemStore) getShard(key []byte) *Shard {
	var h maphash.Hash
	h.SetSeed(m.seed)
	h.Write(key)
	return m.shards[h.Sum64()&shardMask]
}

func (m *MemStore) Set(key, value []byte) error {
	shard := m.getShard(key)
	shard.mu.Lock()

	k := string(key)
	old, exists := shard.data[k]

	v := make([]byte, len(value))
	copy(v, value)
	shard.data[k] = v

	shard.mu.Unlock()

	if !exists {
		atomic.AddUint64(&m.count, 1)
		atomic.AddUint64(&m.size, uint64(len(key)+len(value)))
	} else {
		atomic.AddUint64(&m.size, uint64(len(value)-len(old)))
	}

	return nil
}

func (m *MemStore) SetNoCopy(key, value []byte) error {
	shard := m.getShard(key)
	shard.mu.Lock()

	k := string(key)
	_, exists := shard.data[k]
	shard.data[k] = value

	shard.mu.Unlock()

	if !exists {
		atomic.AddUint64(&m.count, 1)
		atomic.AddUint64(&m.size, uint64(len(key)+len(value)))
	}

	return nil
}

func (m *MemStore) Get(key []byte) ([]byte, error) {
	shard := m.getShard(key)
	shard.mu.RLock()
	v, ok := shard.data[string(key)]
	shard.mu.RUnlock()

	if !ok {
		return nil, nil
	}
	return v, nil
}

func (m *MemStore) Delete(key []byte) error {
	shard := m.getShard(key)
	shard.mu.Lock()

	k := string(key)
	if v, exists := shard.data[k]; exists {
		delete(shard.data, k)
		atomic.AddUint64(&m.count, ^uint64(0)) 
		atomic.AddUint64(&m.size, ^uint64(len(k)+len(v)-1))
	}

	shard.mu.Unlock()
	return nil
}

func (m *MemStore) BatchSet(kvs [][2][]byte) error {
	if len(kvs) == 0 {
		return nil
	}

	shardBatches := make(map[int][][2][]byte)
	for _, kv := range kvs {
		var h maphash.Hash
		h.SetSeed(m.seed)
		h.Write(kv[0])
		shardIdx := int(h.Sum64() & shardMask)
		shardBatches[shardIdx] = append(shardBatches[shardIdx], kv)
	}

	var addedCount, addedSize uint64

	for shardIdx, batch := range shardBatches {
		shard := m.shards[shardIdx]
		shard.mu.Lock()

		for _, kv := range batch {
			k := string(kv[0])
			_, exists := shard.data[k]

			v := make([]byte, len(kv[1]))
			copy(v, kv[1])
			shard.data[k] = v

			if !exists {
				addedCount++
				addedSize += uint64(len(kv[0]) + len(kv[1]))
			}
		}

		shard.mu.Unlock()
	}

	atomic.AddUint64(&m.count, addedCount)
	atomic.AddUint64(&m.size, addedSize)

	return nil
}

func (m *MemStore) BatchSetNoCopy(kvs [][2][]byte) error {
	if len(kvs) == 0 {
		return nil
	}

	shardBatches := make(map[int][][2][]byte)
	for _, kv := range kvs {
		var h maphash.Hash
		h.SetSeed(m.seed)
		h.Write(kv[0])
		shardIdx := int(h.Sum64() & shardMask)
		shardBatches[shardIdx] = append(shardBatches[shardIdx], kv)
	}

	var addedCount, addedSize uint64

	for shardIdx, batch := range shardBatches {
		shard := m.shards[shardIdx]
		shard.mu.Lock()

		for _, kv := range batch {
			k := string(kv[0])
			_, exists := shard.data[k]
			shard.data[k] = kv[1]

			if !exists {
				addedCount++
				addedSize += uint64(len(kv[0]) + len(kv[1]))
			}
		}

		shard.mu.Unlock()
	}

	atomic.AddUint64(&m.count, addedCount)
	atomic.AddUint64(&m.size, addedSize)

	return nil
}

func (m *MemStore) Count() uint64 {
	return atomic.LoadUint64(&m.count)
}

func (m *MemStore) Size() uint64 {
	return atomic.LoadUint64(&m.size)
}

type Iterator struct {
	store      *MemStore
	shardIdx   int
	keys       []string
	keyIdx     int
	currentKey []byte
	currentVal []byte
}

func (m *MemStore) NewIterator() *Iterator {
	return &Iterator{
		store:    m,
		shardIdx: -1,
	}
}

func (it *Iterator) loadNextShard() bool {
	for it.shardIdx++; it.shardIdx < numShards; it.shardIdx++ {
		shard := it.store.shards[it.shardIdx]
		shard.mu.RLock()

		if len(shard.data) > 0 {
			it.keys = make([]string, 0, len(shard.data))
			for k := range shard.data {
				it.keys = append(it.keys, k)
			}
		}

		shard.mu.RUnlock()

		if len(it.keys) > 0 {
			it.keyIdx = 0
			return true
		}
	}
	return false
}

func (it *Iterator) Next() bool {
	if it.keys != nil && it.keyIdx < len(it.keys)-1 {
		it.keyIdx++
		return it.loadCurrent()
	}

	it.keys = nil
	if !it.loadNextShard() {
		return false
	}
	return it.loadCurrent()
}

func (it *Iterator) loadCurrent() bool {
	if it.keyIdx >= len(it.keys) {
		return false
	}

	key := it.keys[it.keyIdx]
	shard := it.store.shards[it.shardIdx]

	shard.mu.RLock()
	val, ok := shard.data[key]
	shard.mu.RUnlock()

	if !ok {
		return it.Next()
	}

	it.currentKey = []byte(key)
	it.currentVal = val
	return true
}

func (it *Iterator) Current() ([]byte, []byte, error) {
	return it.currentKey, it.currentVal, nil
}

func (it *Iterator) Close() error {
	it.keys = nil
	return nil
}

type PrefixIterator struct {
	store      *MemStore
	prefix     []byte
	shardIdx   int
	keys       []string
	keyIdx     int
	currentKey []byte
	currentVal []byte
}

func (m *MemStore) NewPrefixIterator(prefix []byte) *PrefixIterator {
	return &PrefixIterator{
		store:    m,
		prefix:   prefix,
		shardIdx: -1,
	}
}

func (it *PrefixIterator) loadNextShard() bool {
	prefixStr := string(it.prefix)

	for it.shardIdx++; it.shardIdx < numShards; it.shardIdx++ {
		shard := it.store.shards[it.shardIdx]
		shard.mu.RLock()

		it.keys = it.keys[:0]
		for k := range shard.data {
			if len(k) >= len(prefixStr) && k[:len(prefixStr)] == prefixStr {
				it.keys = append(it.keys, k)
			}
		}

		shard.mu.RUnlock()

		if len(it.keys) > 0 {
			it.keyIdx = 0
			return true
		}
	}
	return false
}

func (it *PrefixIterator) Next() bool {
	if it.keys != nil && it.keyIdx < len(it.keys)-1 {
		it.keyIdx++
		return it.loadCurrent()
	}

	if !it.loadNextShard() {
		return false
	}
	return it.loadCurrent()
}

func (it *PrefixIterator) loadCurrent() bool {
	if it.keyIdx >= len(it.keys) {
		return false
	}

	key := it.keys[it.keyIdx]
	shard := it.store.shards[it.shardIdx]

	shard.mu.RLock()
	val, ok := shard.data[key]
	shard.mu.RUnlock()

	if !ok {
		return it.Next()
	}

	it.currentKey = []byte(key)
	it.currentVal = val
	return true
}

func (it *PrefixIterator) Current() ([]byte, []byte, error) {
	return it.currentKey, it.currentVal, nil
}

func (it *PrefixIterator) Close() error {
	it.keys = nil
	return nil
}

func (m *MemStore) Stats() map[string]interface{} {
	return map[string]interface{}{
		"count":     m.Count(),
		"size":      m.Size(),
		"numShards": numShards,
	}
}

func (m *MemStore) Clear() {
	for i := 0; i < numShards; i++ {
		shard := m.shards[i]
		shard.mu.Lock()
		shard.data = make(map[string][]byte, initialSize)
		shard.mu.Unlock()
	}
	atomic.StoreUint64(&m.count, 0)
	atomic.StoreUint64(&m.size, 0)
}

func (m *MemStore) ForEach(fn func(key, value []byte) bool) {
	for i := 0; i < numShards; i++ {
		shard := m.shards[i]
		shard.mu.RLock()

		for k, v := range shard.data {
			if !fn([]byte(k), v) {
				shard.mu.RUnlock()
				return
			}
		}

		shard.mu.RUnlock()
	}
}

func (m *MemStore) ForEachPrefix(prefix []byte, fn func(key, value []byte) bool) {
	prefixStr := string(prefix)

	for i := 0; i < numShards; i++ {
		shard := m.shards[i]
		shard.mu.RLock()

		for k, v := range shard.data {
			if len(k) >= len(prefixStr) && k[:len(prefixStr)] == prefixStr {
				if !fn([]byte(k), v) {
					shard.mu.RUnlock()
					return
				}
			}
		}

		shard.mu.RUnlock()
	}
}

func (m *MemStore) ParallelForEachPrefix(prefix []byte, fn func(key, value []byte)) {
	prefixStr := string(prefix)
	var wg sync.WaitGroup

	for i := 0; i < numShards; i++ {
		wg.Add(1)
		go func(shardIdx int) {
			defer wg.Done()

			shard := m.shards[shardIdx]
			shard.mu.RLock()
			defer shard.mu.RUnlock()

			for k, v := range shard.data {
				if len(k) >= len(prefixStr) && k[:len(prefixStr)] == prefixStr {
					fn([]byte(k), v)
				}
			}
		}(i)
	}

	wg.Wait()
}

func (m *MemStore) Flush() error {
	return nil
}

func (m *MemStore) Close() error {
	return nil
}

func TablePrefix(tableID uint32) []byte {
	prefix := make([]byte, 4)
	binary.BigEndian.PutUint32(prefix, tableID)
	return prefix
}

