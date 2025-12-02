package engine

import (
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

type ResultCache struct {
	mu       sync.RWMutex
	cache    map[uint64]*cacheEntry
	maxSize  int
	hits     uint64
	misses   uint64
	enabled  bool
}

type cacheEntry struct {
	result    string
	timestamp int64
	hits      uint32
}

func NewResultCache(maxSize int) *ResultCache {
	return &ResultCache{
		cache:   make(map[uint64]*cacheEntry, maxSize),
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

	c.mu.RLock()
	entry, ok := c.cache[hash]
	c.mu.RUnlock()

	if ok {
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

	if len(c.cache) >= c.maxSize {
		c.evictLRU()
	}

	c.cache[hash] = &cacheEntry{
		result:    result,
		timestamp: time.Now().UnixNano(),
		hits:      1,
	}
}

func (c *ResultCache) evictLRU() {
	var oldestKey uint64
	var oldestTime int64 = 1<<63 - 1
	var lowestHits uint32 = 1<<32 - 1

	for k, v := range c.cache {
		if v.hits < lowestHits || (v.hits == lowestHits && v.timestamp < oldestTime) {
			oldestKey = k
			oldestTime = v.timestamp
			lowestHits = v.hits
		}
	}

	delete(c.cache, oldestKey)
}

func (c *ResultCache) Invalidate() {
	c.mu.Lock()
	c.cache = make(map[uint64]*cacheEntry, c.maxSize)
	c.mu.Unlock()
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

func fastStringEqual(a, b string) bool {
	if len(a) != len(b) {
		return false
	}
	if len(a) == 0 {
		return true
	}
	return *(*string)(unsafe.Pointer(&a)) == *(*string)(unsafe.Pointer(&b))
}

func fastHasPrefix(s, prefix string) bool {
	if len(s) < len(prefix) {
		return false
	}
	return s[:len(prefix)] == prefix
}

var rowPools = [...]sync.Pool{
	{New: func() interface{} { r := make([]Row, 0, 16); return &r }},
	{New: func() interface{} { r := make([]Row, 0, 64); return &r }},
	{New: func() interface{} { r := make([]Row, 0, 256); return &r }},
	{New: func() interface{} { r := make([]Row, 0, 1024); return &r }},
}

func getRowSliceFromPool(hint int) *[]Row {
	idx := 0
	if hint > 16 {
		idx = 1
	}
	if hint > 64 {
		idx = 2
	}
	if hint > 256 {
		idx = 3
	}
	s := rowPools[idx].Get().(*[]Row)
	*s = (*s)[:0]
	return s
}

func putRowSliceToPool(s *[]Row, cap int) {
	if cap <= 16 {
		rowPools[0].Put(s)
	} else if cap <= 64 {
		rowPools[1].Put(s)
	} else if cap <= 256 {
		rowPools[2].Put(s)
	} else if cap <= 1024 {
		rowPools[3].Put(s)
	}
}

var bytePools = [...]sync.Pool{
	{New: func() interface{} { b := make([]byte, 0, 128); return &b }},
	{New: func() interface{} { b := make([]byte, 0, 1024); return &b }},
	{New: func() interface{} { b := make([]byte, 0, 8192); return &b }},
}

func getByteSlice(hint int) *[]byte {
	idx := 0
	if hint > 128 {
		idx = 1
	}
	if hint > 1024 {
		idx = 2
	}
	b := bytePools[idx].Get().(*[]byte)
	*b = (*b)[:0]
	return b
}

func putByteSlice(b *[]byte) {
	cap := cap(*b)
	if cap <= 128 {
		bytePools[0].Put(b)
	} else if cap <= 1024 {
		bytePools[1].Put(b)
	} else if cap <= 8192 {
		bytePools[2].Put(b)
	}
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

func compileCondition(schema *TableSchema, cond *Condition) *ConditionMatcher {
	colIdx := schema.GetColumnIndex(cond.Column)
	if colIdx == -1 {
		return nil
	}

	m := &ConditionMatcher{colIdx: colIdx}

	switch cond.Op {
	case "=":
		m.op = opEq
	case "!=":
		m.op = opNe
	case ">":
		m.op = opGt
	case "<":
		m.op = opLt
	case ">=":
		m.op = opGe
	case "<=":
		m.op = opLe
	default:
		return nil
	}

	val := cond.Value.ToInterface()
	switch v := val.(type) {
	case int:
		m.isInt = true
		m.intVal = int64(v)
	case string:
		m.strVal = v
	default:
		return nil
	}

	return m
}

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

