package engine

import (
	"container/list"
	"sync"
	"sync/atomic"
	"time"
)

type ResultCache struct {
	mu      sync.Mutex
	cache   map[uint64]*list.Element
	lruList *list.List
	maxSize int
	hits    uint64
	misses  uint64
	enabled bool
}

type cacheEntry struct {
	key       uint64
	result    string
	timestamp int64
	hits      uint32
}

func NewResultCache(maxSize int) *ResultCache {
	return &ResultCache{
		cache:   make(map[uint64]*list.Element, maxSize),
		lruList: list.New(),
		maxSize: maxSize,
		enabled: true,
	}
}

func hashQuery(s string) uint64 {
	h := uint64(14695981039346656037)
	for i := 0; i < len(s); i++ {
		h ^= uint64(s[i])
		h *= 1099511628211
	}
	return h
}

func (c *ResultCache) Get(query string) (string, bool) {
	if !c.enabled {
		return "", false
	}

	hash := hashQuery(query)

	c.mu.Lock()
	defer c.mu.Unlock()

	if elem, ok := c.cache[hash]; ok {
		c.lruList.MoveToFront(elem)
		entry := elem.Value.(*cacheEntry)
		atomic.AddUint64(&c.hits, 1)
		atomic.AddUint32(&entry.hits, 1)
		return entry.result, true
	}

	atomic.AddUint64(&c.misses, 1)
	return "", false
}

func (c *ResultCache) Set(query, result string) {
	if !c.enabled {
		return
	}

	if len(result) > 1<<20 {
		return
	}

	hash := hashQuery(query)

	c.mu.Lock()
	defer c.mu.Unlock()

	if elem, ok := c.cache[hash]; ok {
		c.lruList.MoveToFront(elem)
		entry := elem.Value.(*cacheEntry)
		entry.result = result
		entry.timestamp = time.Now().UnixNano()
		return
	}

	if c.lruList.Len() >= c.maxSize {
		c.evictLRU()
	}

	entry := &cacheEntry{
		key:       hash,
		result:    result,
		timestamp: time.Now().UnixNano(),
		hits:      1,
	}
	elem := c.lruList.PushFront(entry)
	c.cache[hash] = elem
}

func (c *ResultCache) evictLRU() {
	elem := c.lruList.Back()
	if elem != nil {
		c.lruList.Remove(elem)
		entry := elem.Value.(*cacheEntry)
		delete(c.cache, entry.key)
	}
}

func (c *ResultCache) Invalidate() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lruList.Init()
	c.cache = make(map[uint64]*list.Element, c.maxSize)
}

func (c *ResultCache) Stats() (hits, misses uint64) {
	return atomic.LoadUint64(&c.hits), atomic.LoadUint64(&c.misses)
}

type Arena struct {
	buf  []byte
	off  int
	size int
}

func NewArena(size int) *Arena {
	return &Arena{
		buf:  make([]byte, size),
		size: size,
	}
}

func (a *Arena) Alloc(n int) []byte {
	if a.off+n > a.size {
		a.off = 0
	}
	slice := a.buf[a.off : a.off+n]
	a.off += n
	return slice
}

func (a *Arena) Reset() {
	a.off = 0
}

type ConditionMatcher struct {
	colIdx int
	op     uint8
	intVal int64
	strVal string
	isInt  bool
}

const (
	opEq uint8 = iota
	opNe
	opGt
	opLt
	opGe
	opLe
)

func (m *ConditionMatcher) Match(data []interface{}) bool {
	if m.colIdx >= len(data) {
		return false
	}

	val := data[m.colIdx]

	if m.isInt {
		var v int64
		switch x := val.(type) {
		case int:
			v = int64(x)
		case int64:
			v = x
		case float64:
			v = int64(x)
		default:
			return false
		}

		switch m.op {
		case opEq:
			return v == m.intVal
		case opNe:
			return v != m.intVal
		case opGt:
			return v > m.intVal
		case opLt:
			return v < m.intVal
		case opGe:
			return v >= m.intVal
		case opLe:
			return v <= m.intVal
		}
	} else {
		s, ok := val.(string)
		if !ok {
			return false
		}

		switch m.op {
		case opEq:
			return s == m.strVal
		case opNe:
			return s != m.strVal
		case opGt:
			return s > m.strVal
		case opLt:
			return s < m.strVal
		case opGe:
			return s >= m.strVal
		case opLe:
			return s <= m.strVal
		}
	}

	return false
}
