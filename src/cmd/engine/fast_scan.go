package engine

import (
	"container/heap"
	"encoding/binary"
	"log"
	"strings"
	"unsafe"
)

type TopKHeap struct {
	rows   []Row
	colIdx int
	desc   bool
	k      int
}

func (h TopKHeap) Len() int { return len(h.rows) }

func (h TopKHeap) Less(i, j int) bool {
	var cmp int
	if h.colIdx < len(h.rows[i].Data) && h.colIdx < len(h.rows[j].Data) {
		cmp = compareInterface(h.rows[i].Data[h.colIdx], h.rows[j].Data[h.colIdx])
	} else if h.colIdx < len(h.rows[i].Data) {
		cmp = -1
	} else if h.colIdx < len(h.rows[j].Data) {
		cmp = 1
	} else {
		cmp = 0
	}

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
	log.Printf("FastQuery: prefix %x for table %s id %d", prefix, schema.Name, schema.ID)
	iter, err := db.kv.NewPrefixIterator(prefix)
	if err != nil {
		log.Printf("FastQuery: iterator error %v", err)
		return nil, err
	}
	defer iter.Close()

	if orderBy != nil && limit != nil {
		// Check if there's an index on the orderBy column
		if idx := db.findIndexForColumn(schema.Name, orderBy.Column); idx != nil {
			return db.fastQueryWithIndex(idx, schema, matchers, where, orderBy, *limit, offset)
		}
		return db.fastQueryWithHeap(iter, schema, matchers, where, orderBy, *limit, offset)
	}

	var results []Row
	for {
		key, val, err := iter.Current()
		if err != nil {
			log.Printf("FastQuery: iter error %v", err)
			break
		}
		log.Printf("FastQuery: processing key")

		decoded, err := db.encoder.DecodeValue(val)
		if err != nil {
			log.Printf("FastQuery: decode error %v", err)
			iter.Next()
			continue
		}
		log.Printf("FastQuery: decoded %v", decoded)

		if where != nil && !db.matchesFilterFast(schema, decoded, where, matchers) {
			iter.Next()
			continue
		}

		pk := binary.BigEndian.Uint64(key[4:12])
		row := Row{PrimaryKey: pk, Data: decoded}
		results = append(results, row)

		if limit != nil && len(results) >= *limit {
			break
		}

		if err := iter.Next(); err != nil {
			break
		}
	}
	log.Printf("FastQuery: returning %d results for table %s", len(results), schema.Name)
	return results, nil
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

	if colIdx >= len(schema.Columns) {
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

		if len(decoded) != len(schema.Columns) {
			iter.Next()
			continue
		}

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

type IndexItem struct {
	pk  uint64
	key []byte
}

type IndexHeap []IndexItem

func (h IndexHeap) Len() int { return len(h) }
func (h IndexHeap) Less(i, j int) bool {
	vi := decodeValueFromKey(h[i].key)
	vj := decodeValueFromKey(h[j].key)
	cmp := compareInterface(vi, vj)
	return cmp > 0 // for DESC: larger first
}
func (h IndexHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *IndexHeap) Push(x interface{}) { *h = append(*h, x.(IndexItem)) }
func (h *IndexHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

func decodeValueFromKey(key []byte) interface{} {
	if len(key) < 5 {
		return nil
	}
	typeByte := key[4]
	data := key[5 : len(key)-8]
	switch typeByte {
	case 1: // int
		if len(data) == 8 {
			return int(binary.BigEndian.Uint64(data))
		}
	case 2: // float
		if len(data) == 8 {
			bits := binary.BigEndian.Uint64(data)
			return *(*float64)(unsafe.Pointer(&bits))
		}
	case 3: // string
		if len(data) > 0 {
			return string(data[:len(data)-1]) // remove null
		}
	case 4: // bool
		return data[0] == 1
	}
	return nil
}

func (db *RelationalDB) fastQueryWithIndex(idx *IndexSchema, schema *TableSchema, matchers []*CompiledCondition, where *WhereClause, orderBy *OrderClause, limit int, offset *int) ([]Row, error) {
	prefix := make([]byte, 4)
	binary.BigEndian.PutUint32(prefix, idx.ID+1000)
	iter, err := db.kv.NewPrefixIterator(prefix)
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	desc := strings.EqualFold(orderBy.Direction, "DESC")
	need := limit
	if offset != nil {
		need += *offset
	}

	var items []IndexItem
	if desc {
		// For DESC, use heap to keep top K
		h := &IndexHeap{}
		heap.Init(h)

		for {
			key, val, err := iter.Current()
			if err != nil {
				break
			}

			if len(val) != 8 {
				iter.Next()
				continue
			}
			pk := binary.BigEndian.Uint64(val)

			if where != nil {
				// Fetch the full row to apply WHERE
				rowKey := db.encoder.EncodeKey(schema.ID, pk)
				rowVal, err := db.kv.Get(rowKey)
				if err != nil || rowVal == nil {
					iter.Next()
					continue
				}

				decoded, err := db.encoder.DecodeValue(rowVal)
				if err != nil {
					iter.Next()
					continue
				}

				// Apply WHERE filter
				if !db.matchesFilterFast(schema, decoded, where, matchers) {
					iter.Next()
					continue
				}
			}

			keyCopy := make([]byte, len(key))
			copy(keyCopy, key)
			item := IndexItem{pk: pk, key: keyCopy}
			heap.Push(h, item)

			if h.Len() > need {
				heap.Pop(h)
			}

			if err := iter.Next(); err != nil {
				break
			}
		}

		items = *h
	} else {
		// For ASC, collect first need
		for {
			if len(items) >= need {
				break
			}

			key, val, err := iter.Current()
			if err != nil {
				break
			}

			if len(val) != 8 {
				iter.Next()
				continue
			}
			pk := binary.BigEndian.Uint64(val)

			if where != nil {
				// Fetch the full row to apply WHERE
				rowKey := db.encoder.EncodeKey(schema.ID, pk)
				rowVal, err := db.kv.Get(rowKey)
				if err != nil || rowVal == nil {
					iter.Next()
					continue
				}

				decoded, err := db.encoder.DecodeValue(rowVal)
				if err != nil {
					iter.Next()
					continue
				}

				// Apply WHERE filter
				if !db.matchesFilterFast(schema, decoded, where, matchers) {
					iter.Next()
					continue
				}
			}

			keyCopy := make([]byte, len(key))
			copy(keyCopy, key)
			items = append(items, IndexItem{pk: pk, key: keyCopy})

			if err := iter.Next(); err != nil {
				break
			}
		}
	}

	// Apply offset
	if offset != nil && *offset > 0 {
		if *offset >= len(items) {
			return []Row{}, nil
		}
		items = items[*offset:]
	}

	// Fetch rows
	var rows []Row
	for _, item := range items {
		rowKey := db.encoder.EncodeKey(schema.ID, item.pk)
		rowVal, err := db.kv.Get(rowKey)
		if err != nil || rowVal == nil {
			continue
		}
		decoded, err := db.encoder.DecodeValue(rowVal)
		if err != nil {
			log.Printf("fastQueryWithIndex: decode error %v", err)
			continue
		}
		log.Printf("fastQueryWithIndex: decoded %v", decoded)
		row := Row{PrimaryKey: item.pk, Data: decoded}
		rows = append(rows, row)
	}

	return rows, nil
}

func (db *RelationalDB) matchesFilterFast(schema *TableSchema, data []interface{}, where *WhereClause, matchers []*CompiledCondition) bool {
	if matchers != nil {
		return matchConditions(matchers, data)
	}
	row := Row{Data: data}
	return db.matchesFilter(schema, row, where)
}
