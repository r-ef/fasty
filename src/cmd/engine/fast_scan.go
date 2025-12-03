package engine

import (
	"container/heap"
	"encoding/binary"
	"runtime"
	"strings"
	"sync"
)

type TopKHeap struct {
	rows   []Row
	colIdx int
	desc   bool
	k      int
}

func (h TopKHeap) Len() int { return len(h.rows) }

func (h TopKHeap) Less(i, j int) bool {
	cmp := compareInterface(h.rows[i].Data[h.colIdx], h.rows[j].Data[h.colIdx])
	if h.desc {
		return cmp < 0
	}
	return cmp > 0
}

func (h TopKHeap) Swap(i, j int) { h.rows[i], h.rows[j] = h.rows[j], h.rows[i] }

func (h *TopKHeap) Push(x interface{}) { h.rows = append(h.rows, x.(Row)) }

func (h *TopKHeap) Pop() interface{} {
	old := h.rows
	n := len(old)
	x := old[n-1]
	h.rows = old[0 : n-1]
	return x
}

func (db *RelationalDB) FastQuery(schema *TableSchema, where *WhereClause, orderBy *OrderClause, limit *int, offset *int) ([]Row, error) {
	var matchers []*CompiledCondition
	if where != nil {
		matchers = db.compileWhere(schema, where)
	}

	prefix := db.encoder.EncodeKey(schema.ID, 0)[:4]
	iter, err := db.kv.NewPrefixIterator(prefix)
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	if orderBy != nil && limit != nil && *limit > 0 {
		return db.fastQueryWithHeap(iter, schema, matchers, where, orderBy, *limit, offset)
	}

	return db.fastParallelScan(iter, schema, matchers, where, limit, offset)
}

type CompiledCondition struct {
	colIdx   int
	op       uint8
	intVal   int64
	floatVal float64
	strVal   string
	boolVal  bool
	isInt    bool
	isFloat  bool
	isString bool
	isBool   bool
	isLike   bool
	pattern  string
}

func (db *RelationalDB) compileWhere(schema *TableSchema, where *WhereClause) []*CompiledCondition {
	var conditions []*CompiledCondition

	for _, orClause := range where.Or {
		for _, cond := range orClause.Conditions {
			cc := &CompiledCondition{
				colIdx: schema.GetColumnIndex(cond.Column),
			}
			if cc.colIdx == -1 {
				continue
			}

			switch cond.Op {
			case "=":
				cc.op = 0
			case "!=":
				cc.op = 1
			case ">":
				cc.op = 2
			case "<":
				cc.op = 3
			case ">=":
				cc.op = 4
			case "<=":
				cc.op = 5
			case "like":
				cc.isLike = true
				if cond.Value.String != nil {
					cc.pattern = *cond.Value.String
				}
			}

			val := cond.Value.ToInterface()
			switch v := val.(type) {
			case int:
				cc.isInt = true
				cc.intVal = int64(v)
			case float64:
				cc.isFloat = true
				cc.floatVal = v
			case string:
				cc.isString = true
				cc.strVal = v
			case bool:
				cc.isBool = true
				cc.boolVal = v
			}

			conditions = append(conditions, cc)
		}
	}

	return conditions
}

func (cc *CompiledCondition) MatchValue(val interface{}) bool {
	if cc.isLike {
		if s, ok := val.(string); ok {
			return matchLike(s, cc.pattern)
		}
		return false
	}

	if cc.isInt {
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
		switch cc.op {
		case 0:
			return v == cc.intVal
		case 1:
			return v != cc.intVal
		case 2:
			return v > cc.intVal
		case 3:
			return v < cc.intVal
		case 4:
			return v >= cc.intVal
		case 5:
			return v <= cc.intVal
		}
	}

	if cc.isFloat {
		var v float64
		switch x := val.(type) {
		case float64:
			v = x
		case int:
			v = float64(x)
		case int64:
			v = float64(x)
		default:
			return false
		}
		switch cc.op {
		case 0:
			return v == cc.floatVal
		case 1:
			return v != cc.floatVal
		case 2:
			return v > cc.floatVal
		case 3:
			return v < cc.floatVal
		case 4:
			return v >= cc.floatVal
		case 5:
			return v <= cc.floatVal
		}
	}

	if cc.isString {
		s, ok := val.(string)
		if !ok {
			return false
		}
		switch cc.op {
		case 0:
			return s == cc.strVal
		case 1:
			return s != cc.strVal
		case 2:
			return s > cc.strVal
		case 3:
			return s < cc.strVal
		case 4:
			return s >= cc.strVal
		case 5:
			return s <= cc.strVal
		}
	}

	if cc.isBool {
		b, ok := val.(bool)
		if !ok {
			return false
		}
		switch cc.op {
		case 0:
			return b == cc.boolVal
		case 1:
			return b != cc.boolVal
		}
	}

	return false
}

func (cc *CompiledCondition) Match(data []interface{}) bool {
	if cc.colIdx >= len(data) {
		return false
	}
	return cc.MatchValue(data[cc.colIdx])
}

func matchConditions(matchers []*CompiledCondition, data []interface{}) bool {
	if len(matchers) == 0 {
		return true
	}
	for _, m := range matchers {
		if m.Match(data) {
			return true
		}
	}
	return false
}

func (db *RelationalDB) matchesFilterRaw(data []byte, matchers []*CompiledCondition) bool {
	if len(matchers) == 0 {
		return true
	}
	for _, m := range matchers {
		val, err := db.encoder.DecodeValueFast(data, m.colIdx)
		if err != nil {
			return false
		}
		if !m.MatchValue(val) {
			return false
		}
	}
	return true
}

func (db *RelationalDB) fastQueryWithHeap(iter interface {
	Current() ([]byte, []byte, error)
	Next() error
}, schema *TableSchema, matchers []*CompiledCondition, where *WhereClause, orderBy *OrderClause, limit int, offset *int) ([]Row, error) {
	colIdx := schema.GetColumnIndex(orderBy.Column)
	if colIdx == -1 {
		colIdx = 0
	}

	need := limit
	if offset != nil {
		need += *offset
	}

	h := &TopKHeap{
		rows:   make([]Row, 0, need+1),
		colIdx: colIdx,
		desc:   strings.EqualFold(orderBy.Direction, "DESC"),
		k:      need,
	}
	heap.Init(h)

	for {
		key, val, err := iter.Current()
		if err != nil {
			break
		}

		decoded, err := db.encoder.DecodeValue(val)
		if err != nil {
			iter.Next()
			continue
		}

		if where != nil && !db.matchesFilterFast(schema, decoded, where, matchers) {
			iter.Next()
			continue
		}

		pk := binary.BigEndian.Uint64(key[4:12])
		row := Row{PrimaryKey: pk, Data: decoded}

		heap.Push(h, row)

		if h.Len() > need {
			heap.Pop(h)
		}

		if err := iter.Next(); err != nil {
			break
		}
	}

	result := make([]Row, h.Len())
	for i := h.Len() - 1; i >= 0; i-- {
		result[i] = heap.Pop(h).(Row)
	}

	if offset != nil && *offset > 0 {
		if *offset >= len(result) {
			return []Row{}, nil
		}
		result = result[*offset:]
	}

	if limit < len(result) {
		result = result[:limit]
	}

	return result, nil
}

func (db *RelationalDB) matchesFilterFast(schema *TableSchema, data []interface{}, where *WhereClause, matchers []*CompiledCondition) bool {
	if matchers != nil {
		return matchConditions(matchers, data)
	}
	row := Row{Data: data}
	return db.matchesFilter(schema, row, where)
}

func (db *RelationalDB) fastParallelScan(iter interface {
	Current() ([]byte, []byte, error)
	Next() error
}, schema *TableSchema, matchers []*CompiledCondition, where *WhereClause, limit *int, offset *int) ([]Row, error) {
	type kvPair struct {
		key []byte
		val []byte
	}
	var pairs []kvPair

	for {
		key, val, err := iter.Current()
		if err != nil {
			break
		}
		keyCopy := make([]byte, len(key))
		copy(keyCopy, key)
		valCopy := make([]byte, len(val))
		copy(valCopy, val)
		pairs = append(pairs, kvPair{keyCopy, valCopy})

		if err := iter.Next(); err != nil {
			break
		}
	}

	if len(pairs) == 0 {
		return []Row{}, nil
	}

	if len(pairs) < 500 {
		rows := make([]Row, 0, len(pairs))
		for _, p := range pairs {
			if matchers != nil && !db.matchesFilterRaw(p.val, matchers) {
				continue
			}

			decoded, err := db.encoder.DecodeValue(p.val)
			if err != nil {
				continue
			}

			if where != nil && matchers == nil && !db.matchesFilter(schema, Row{Data: decoded}, where) {
				continue
			}

			pk := binary.BigEndian.Uint64(p.key[4:12])
			rows = append(rows, Row{PrimaryKey: pk, Data: decoded})
		}

		if offset != nil && *offset > 0 {
			if *offset >= len(rows) {
				return []Row{}, nil
			}
			rows = rows[*offset:]
		}
		if limit != nil && *limit >= 0 && *limit < len(rows) {
			rows = rows[:*limit]
		}

		return rows, nil
	}

	numWorkers := runtime.NumCPU() * 2
	if numWorkers > 16 {
		numWorkers = 16
	}

	chunkSize := (len(pairs) + numWorkers - 1) / numWorkers
	results := make([][]Row, numWorkers)

	var wg sync.WaitGroup
	for w := 0; w < numWorkers; w++ {
		start := w * chunkSize
		end := start + chunkSize
		if end > len(pairs) {
			end = len(pairs)
		}
		if start >= len(pairs) {
			break
		}

		wg.Add(1)
		go func(workerID, start, end int) {
			defer wg.Done()

			localRows := make([]Row, 0, (end-start)/2)

			for i := start; i < end; i++ {
				p := pairs[i]
				if matchers != nil && !db.matchesFilterRaw(p.val, matchers) {
					continue
				}

				decoded, err := db.encoder.DecodeValue(p.val)
				if err != nil {
					continue
				}

				if where != nil && matchers == nil && !db.matchesFilter(schema, Row{Data: decoded}, where) {
					continue
				}

				pk := binary.BigEndian.Uint64(p.key[4:12])
				localRows = append(localRows, Row{PrimaryKey: pk, Data: decoded})
			}

			results[workerID] = localRows
		}(w, start, end)
	}

	wg.Wait()

	totalLen := 0
	for _, r := range results {
		totalLen += len(r)
	}

	rows := make([]Row, 0, totalLen)
	for _, r := range results {
		rows = append(rows, r...)
	}

	if offset != nil && *offset > 0 {
		if *offset >= len(rows) {
			return []Row{}, nil
		}
		rows = rows[*offset:]
	}
	if limit != nil && *limit >= 0 && *limit < len(rows) {
		rows = rows[:*limit]
	}

	return rows, nil
}
