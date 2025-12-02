package engine

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	json "github.com/goccy/go-json"

	"github.com/alecthomas/participle/v2"
	"github.com/bits-and-blooms/bloom/v3"

	tree "github.com/r-ef/fasty/src/LSMTree"
)

const (
	SystemCatalogID = 0
	IndexCatalogID  = 1
)

var (
	ErrTableNotFound = errors.New("table not found")
)

type queryCache struct {
	mu    sync.RWMutex
	cache map[string]*Query
	size  int
}

func newQueryCache(size int) *queryCache {
	return &queryCache{
		cache: make(map[string]*Query, size),
		size:  size,
	}
}

func (c *queryCache) get(q string) (*Query, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	ast, ok := c.cache[q]
	return ast, ok
}

func (c *queryCache) set(q string, ast *Query) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.cache) >= c.size {
		count := 0
		for k := range c.cache {
			delete(c.cache, k)
			count++
			if count >= c.size/2 {
				break
			}
		}
	}
	c.cache[q] = ast
}

var (
	rowSlicePool = sync.Pool{
		New: func() interface{} {
			s := make([]Row, 0, 128)
			return &s
		},
	}
	bufferPool = sync.Pool{
		New: func() interface{} {
			return make([]byte, 0, 4096)
		},
	}
)

type Query struct {
	Create      *CreateCmd      `parser:"@@"`
	CreateIndex *CreateIndexCmd `parser:"| @@"`
	Insert      *InsertCmd      `parser:"| @@"`
	Select      *SelectCmd      `parser:"| @@"`
	Find        *FindCmd        `parser:"| @@"`
	Update      *UpdateCmd      `parser:"| @@"`
	Delete      *DeleteCmd      `parser:"| @@"`
	Drop        *DropCmd        `parser:"| @@"`
	DropIndex   *DropIndexCmd   `parser:"| @@"`
}

type CreateCmd struct {
	Name string   `parser:"'create' 'table' @Ident"`
	Cols []ColDef `parser:"'{' @@ (',' @@)* '}'"`
}

type ColDef struct {
	Name string `parser:"@Ident ':'"`
	Type string `parser:"@Ident"`
}

type CreateIndexCmd struct {
	Name   string `parser:"'create' 'index' @Ident"`
	Table  string `parser:"'on' @Ident"`
	Column string `parser:"'(' @Ident ')'"`
}

type DropIndexCmd struct {
	Name string `parser:"'drop' 'index' @Ident"`
}

type InsertCmd struct {
	Table string `parser:"'insert' @Ident"`
	Pairs []KV   `parser:"'{' @@ (',' @@)* '}'"`
}

type KV struct {
	Key   string `parser:"@Ident ':'"`
	Value *Value `parser:"@@"`
}

type Value struct {
	Null   bool     `parser:"  @'null'"`
	Bool   *bool    `parser:"| @('true' | 'false')"`
	Float  *float64 `parser:"| @Float"`
	Number *int     `parser:"| @Int"`
	String *string  `parser:"| @String"`
	Object []KV     `parser:"| '{' (@@ (',' @@)*)? '}'"`
	Array  []*Value `parser:"| '[' (@@ (',' @@)*)? ']'"`
}

func (v *Value) ToInterface() interface{} {
	if v == nil || v.Null {
		return nil
	}
	if v.Bool != nil {
		return *v.Bool
	}
	if v.Float != nil {
		return *v.Float
	}
	if v.Number != nil {
		return *v.Number
	}
	if v.String != nil {
		return *v.String
	}
	if v.Object != nil {
		obj := make(map[string]interface{}, len(v.Object))
		for _, kv := range v.Object {
			obj[kv.Key] = kv.Value.ToInterface()
		}
		return obj
	}
	if v.Array != nil {
		arr := make([]interface{}, len(v.Array))
		for i, elem := range v.Array {
			arr[i] = elem.ToInterface()
		}
		return arr
	}
	return nil
}

type SelectCmd struct {
	Columns []string     `parser:"'select' (@Ident (',' @Ident)* | '*')"`
	Table   string       `parser:"'from' @Ident"`
	Where   *WhereClause `parser:"@@?"`
	OrderBy *OrderClause `parser:"@@?"`
	Limit   *int         `parser:"('limit' @Int)?"`
	Offset  *int         `parser:"('offset' @Int)?"`
}

type OrderClause struct {
	Column string `parser:"'order' 'by' @Ident"`
	Desc   bool   `parser:"@'desc'?"`
}

type FindCmd struct {
	Table   string       `parser:"'find' @Ident"`
	Where   *WhereClause `parser:"@@?"`
	OrderBy *OrderClause `parser:"@@?"`
	Limit   *int         `parser:"('limit' @Int)?"`
}

type WhereClause struct {
	Or []*AndClause `parser:"'where' @@ ('or' @@)*"`
}

type AndClause struct {
	Conditions []Condition `parser:"@@ ('and' @@)*"`
}

type Condition struct {
	Column string `parser:"@Ident"`
	Op     string `parser:"@('=' | '!=' | '>' | '<' | '>=' | '<=' | 'like')"`
	Value  *Value `parser:"@@"`
}

type UpdateCmd struct {
	Table string       `parser:"'update' @Ident"`
	Set   []KV         `parser:"'set' '{' @@ (',' @@)* '}'"`
	Where *WhereClause `parser:"@@?"`
}

type DeleteCmd struct {
	Table string       `parser:"'delete' @Ident"`
	Where *WhereClause `parser:"@@?"`
}

type DropCmd struct {
	Table string `parser:"'drop' 'table' @Ident"`
}

type IndexSchema struct {
	ID      uint32 `json:"id"`
	Name    string `json:"name"`
	Table   string `json:"table"`
	Column  string `json:"column"`
	TableID uint32 `json:"table_id"`
	ColIdx  int    `json:"col_idx"`

	bloom *bloom.BloomFilter
}

type RelationalDB struct {
	kv       *tree.DB
	encoder  Encoder
	parser   *participle.Parser[Query]
	inMemory bool

	mu          sync.RWMutex
	catalog     map[string]*TableSchema
	indexes     map[string]*IndexSchema
	tableIndex  map[string][]*IndexSchema
	nextTableID uint32
	nextIndexID uint32

	queryCache *queryCache

	resultCache *ResultCache

	workerCount int

	queryCount uint64
	cacheHits  uint64
}

func NewRelationalDB(storagePath string) (*RelationalDB, error) {
	kv, err := tree.Open(storagePath)
	if err != nil {
		return nil, err
	}

	parser, err := participle.Build[Query]()
	if err != nil {
		return nil, fmt.Errorf("failed to build parser: %w", err)
	}

	db := &RelationalDB{
		kv:          kv,
		encoder:     Encoder{},
		parser:      parser,
		catalog:     make(map[string]*TableSchema),
		indexes:     make(map[string]*IndexSchema),
		tableIndex:  make(map[string][]*IndexSchema),
		nextTableID: 1,
		nextIndexID: 1,
		queryCache:  newQueryCache(2000),
		resultCache: NewResultCache(5000),
		workerCount: runtime.NumCPU(),
	}

	if err := db.loadCatalog(); err != nil {
		return nil, err
	}
	if err := db.loadIndexes(); err != nil {
		return nil, err
	}

	return db, nil
}

func NewInMemoryDB() (*RelationalDB, error) {
	kv, err := tree.OpenInMemory()
	if err != nil {
		return nil, err
	}

	parser, err := participle.Build[Query]()
	if err != nil {
		return nil, fmt.Errorf("failed to build parser: %w", err)
	}

	db := &RelationalDB{
		kv:          kv,
		encoder:     Encoder{},
		parser:      parser,
		inMemory:    true,
		catalog:     make(map[string]*TableSchema),
		indexes:     make(map[string]*IndexSchema),
		tableIndex:  make(map[string][]*IndexSchema),
		nextTableID: 1,
		nextIndexID: 1,
		queryCache:  newQueryCache(2000),
		resultCache: NewResultCache(5000),
		workerCount: runtime.NumCPU(),
	}

	return db, nil
}

func (db *RelationalDB) Close() error {
	return db.kv.Close()
}

func (db *RelationalDB) Execute(queryStr string) (string, error) {
	atomic.AddUint64(&db.queryCount, 1)

	ast, ok := db.queryCache.get(queryStr)
	if ok {
		atomic.AddUint64(&db.cacheHits, 1)
	} else {
		var err error
		ast, err = db.parser.ParseString("", queryStr)
		if err != nil {
			return "", fmt.Errorf("syntax error: %w", err)
		}
		db.queryCache.set(queryStr, ast)
	}

	switch {
	case ast.Create != nil:
		return db.handleCreate(ast.Create)
	case ast.CreateIndex != nil:
		return db.handleCreateIndex(ast.CreateIndex)
	case ast.Insert != nil:
		return db.handleInsert(ast.Insert) 
	case ast.Select != nil:
		return db.handleSelectCached(queryStr, ast.Select)
	case ast.Find != nil:
		return db.handleFindCached(queryStr, ast.Find)
	case ast.Update != nil:
		db.resultCache.Invalidate() 
		return db.handleUpdate(ast.Update)
	case ast.Delete != nil:
		db.resultCache.Invalidate() 
		return db.handleDelete(ast.Delete)
	case ast.Drop != nil:
		db.resultCache.Invalidate() 
		return db.handleDrop(ast.Drop)
	case ast.DropIndex != nil:
		return db.handleDropIndex(ast.DropIndex)
	}

	return "", errors.New("empty query")
}

func (db *RelationalDB) handleCreate(cmd *CreateCmd) (string, error) {
	colNames := make([]string, len(cmd.Cols))
	for i, col := range cmd.Cols {
		colNames[i] = col.Name
	}

	if err := db.createTableInternal(cmd.Name, colNames); err != nil {
		return "", err
	}
	return fmt.Sprintf("Table '%s' created", cmd.Name), nil
}

func (db *RelationalDB) handleCreateIndex(cmd *CreateIndexCmd) (string, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	if _, exists := db.indexes[cmd.Name]; exists {
		return "", fmt.Errorf("index '%s' already exists", cmd.Name)
	}

	schema, exists := db.catalog[cmd.Table]
	if !exists {
		return "", fmt.Errorf("table '%s' does not exist", cmd.Table)
	}

	colIdx := schema.GetColumnIndex(cmd.Column)
	if colIdx == -1 {
		return "", fmt.Errorf("column '%s' does not exist", cmd.Column)
	}

	bloomFilter := bloom.NewWithEstimates(100000, 0.01)

	indexSchema := &IndexSchema{
		ID:      db.nextIndexID,
		Name:    cmd.Name,
		Table:   cmd.Table,
		Column:  cmd.Column,
		TableID: schema.ID,
		ColIdx:  colIdx,
		bloom:   bloomFilter,
	}
	db.nextIndexID++

	key := db.encoder.EncodeKey(IndexCatalogID, uint64(indexSchema.ID))
	val, _ := json.Marshal(indexSchema)
	if err := db.kv.Set(key, val); err != nil {
		return "", err
	}

	if err := db.buildIndexParallel(schema, indexSchema, colIdx); err != nil {
		return "", err
	}

	db.indexes[cmd.Name] = indexSchema
	db.tableIndex[cmd.Table] = append(db.tableIndex[cmd.Table], indexSchema)

	return fmt.Sprintf("Index '%s' created on %s(%s)", cmd.Name, cmd.Table, cmd.Column), nil
}

func (db *RelationalDB) buildIndexParallel(schema *TableSchema, indexSchema *IndexSchema, colIdx int) error {
	prefix := db.encoder.EncodeKey(schema.ID, 0)[:4]
	iter, err := db.kv.NewPrefixIterator(prefix)
	if err != nil {
		return err
	}
	defer iter.Close()

	var kvPairs [][2][]byte

	for {
		key, val, err := iter.Current()
		if err != nil {
			break
		}

		decoded, err := db.encoder.DecodeValue(val)
		if err == nil && colIdx < len(decoded) {
			pk := binary.BigEndian.Uint64(key[4:12])
			colVal := decoded[colIdx]
			indexKey := db.encodeIndexKey(indexSchema.ID, colVal, pk)
			pkBytes := make([]byte, 8)
			binary.BigEndian.PutUint64(pkBytes, pk)
			kvPairs = append(kvPairs, [2][]byte{indexKey, pkBytes})

			if indexSchema.bloom != nil {
				indexSchema.bloom.Add(valueToBytes(colVal))
			}
		}

		if err := iter.Next(); err != nil {
			break
		}
	}

	if len(kvPairs) > 0 {
		return db.kv.BatchSet(kvPairs)
	}
	return nil
}

func valueToBytes(v interface{}) []byte {
	switch val := v.(type) {
	case int:
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, uint64(val))
		return buf
	case string:
		return []byte(val)
	case bool:
		if val {
			return []byte{1}
		}
		return []byte{0}
	default:
		return nil
	}
}

func (db *RelationalDB) encodeIndexKey(indexID uint32, value interface{}, pk uint64) []byte {
	buf := make([]byte, 0, 32)
	buf = binary.BigEndian.AppendUint32(buf, indexID+1000)

	switch v := value.(type) {
	case int:
		buf = append(buf, 1)
		buf = binary.BigEndian.AppendUint64(buf, uint64(v))
	case float64:
		buf = append(buf, 2)
		buf = binary.BigEndian.AppendUint64(buf, *(*uint64)(unsafe.Pointer(&v)))
	case string:
		buf = append(buf, 3)
		buf = append(buf, v...)
		buf = append(buf, 0)
	case bool:
		buf = append(buf, 4)
		if v {
			buf = append(buf, 1)
		} else {
			buf = append(buf, 0)
		}
	default:
		buf = append(buf, 0)
	}

	buf = binary.BigEndian.AppendUint64(buf, pk)
	return buf
}

func (db *RelationalDB) handleDropIndex(cmd *DropIndexCmd) (string, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	indexSchema, exists := db.indexes[cmd.Name]
	if !exists {
		return "", fmt.Errorf("index '%s' does not exist", cmd.Name)
	}

	prefix := make([]byte, 4)
	binary.BigEndian.PutUint32(prefix, indexSchema.ID+1000)

	iter, err := db.kv.NewPrefixIterator(prefix)
	if err != nil {
		return "", err
	}

	var keysToDelete [][]byte
	for {
		key, _, err := iter.Current()
		if err != nil {
			break
		}
		keyCopy := make([]byte, len(key))
		copy(keyCopy, key)
		keysToDelete = append(keysToDelete, keyCopy)
		if err := iter.Next(); err != nil {
			break
		}
	}
	iter.Close()

	if len(keysToDelete) > 0 {
		db.kv.BatchWrite(nil, keysToDelete)
	}

	catalogKey := db.encoder.EncodeKey(IndexCatalogID, uint64(indexSchema.ID))
	db.kv.Delete(catalogKey)

	delete(db.indexes, cmd.Name)
	tableIndexes := db.tableIndex[indexSchema.Table]
	for i, idx := range tableIndexes {
		if idx.Name == cmd.Name {
			db.tableIndex[indexSchema.Table] = append(tableIndexes[:i], tableIndexes[i+1:]...)
			break
		}
	}

	return fmt.Sprintf("Index '%s' dropped", cmd.Name), nil
}

func (db *RelationalDB) handleInsert(cmd *InsertCmd) (string, error) {
	db.mu.RLock()
	schema, exists := db.catalog[cmd.Table]
	tableIndexes := db.tableIndex[cmd.Table]
	db.mu.RUnlock()

	if !exists {
		return "", fmt.Errorf("table '%s' does not exist", cmd.Table)
	}

	rowData := make([]interface{}, len(schema.Columns))
	pk := uint64(0)

	for _, pair := range cmd.Pairs {
		colIdx := schema.GetColumnIndex(pair.Key)
		if colIdx == -1 {
			return "", fmt.Errorf("unknown column: %s", pair.Key)
		}

		val := pair.Value.ToInterface()
		rowData[colIdx] = val

		if pair.Key == "id" {
			switch v := val.(type) {
			case int:
				pk = uint64(v)
			case float64:
				pk = uint64(v)
			}
		}
	}

	if pk == 0 {
		pk = uint64(atomic.AddUint32(&db.nextTableID, 1)) + uint64(time.Now().UnixNano()%1000000)
	}

	row := Row{PrimaryKey: pk, Data: rowData}
	keyBytes := db.encoder.EncodeKey(schema.ID, pk)
	valBytes, err := db.encoder.EncodeValue(row)
	if err != nil {
		return "", err
	}

	var kvPairs [][2][]byte
	kvPairs = append(kvPairs, [2][]byte{keyBytes, valBytes})

	for _, idx := range tableIndexes {
		if idx.ColIdx >= 0 && idx.ColIdx < len(rowData) {
			colVal := rowData[idx.ColIdx]
			indexKey := db.encodeIndexKey(idx.ID, colVal, pk)
			pkBytes := make([]byte, 8)
			binary.BigEndian.PutUint64(pkBytes, pk)
			kvPairs = append(kvPairs, [2][]byte{indexKey, pkBytes})

			if idx.bloom != nil {
				idx.bloom.Add(valueToBytes(colVal))
			}
		}
	}

	if err := db.kv.BatchSet(kvPairs); err != nil {
		return "", err
	}

	return "Insert successful", nil
}

func (db *RelationalDB) handleSelect(cmd *SelectCmd) (string, error) {
	rows, err := db.selectWithOptions(cmd.Table, cmd.Where, cmd.OrderBy, cmd.Limit, cmd.Offset)
	if err != nil {
		return "", err
	}

	if len(cmd.Columns) > 0 && cmd.Columns[0] != "*" {
		db.mu.RLock()
		schema := db.catalog[cmd.Table]
		db.mu.RUnlock()

		projected := make([]map[string]interface{}, len(rows))
		for i, row := range rows {
			obj := make(map[string]interface{}, len(cmd.Columns))
			for _, col := range cmd.Columns {
				if idx := schema.GetColumnIndex(col); idx >= 0 && idx < len(row.Data) {
					obj[col] = row.Data[idx]
				}
			}
			projected[i] = obj
		}
		out, _ := json.Marshal(projected)
		return unsafeString(out), nil
	}

	out, _ := json.Marshal(rows)
	return unsafeString(out), nil
}

func (db *RelationalDB) handleSelectCached(queryStr string, cmd *SelectCmd) (string, error) {
	if cached, ok := db.resultCache.Get(queryStr); ok {
		return cached, nil
	}

	result, err := db.handleSelect(cmd)
	if err != nil {
		return "", err
	}

	db.resultCache.Set(queryStr, result)
	return result, nil
}

func (db *RelationalDB) handleFind(cmd *FindCmd) (string, error) {
	rows, err := db.selectWithOptions(cmd.Table, cmd.Where, cmd.OrderBy, cmd.Limit, nil)
	if err != nil {
		return "", err
	}
	out, _ := json.Marshal(rows)
	return unsafeString(out), nil
}

func (db *RelationalDB) handleFindCached(queryStr string, cmd *FindCmd) (string, error) {
	if cached, ok := db.resultCache.Get(queryStr); ok {
		return cached, nil
	}

	result, err := db.handleFind(cmd)
	if err != nil {
		return "", err
	}

	db.resultCache.Set(queryStr, result)
	return result, nil
}

func (db *RelationalDB) selectWithOptions(tableName string, where *WhereClause, orderBy *OrderClause, limit *int, offset *int) ([]Row, error) {
	db.mu.RLock()
	schema, exists := db.catalog[tableName]
	tableIndexes := db.tableIndex[tableName]
	db.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("table '%s' does not exist", tableName)
	}

	var rows []Row

	if where != nil && len(tableIndexes) > 0 {
		for _, idx := range tableIndexes {
			if pks := db.tryIndexLookup(idx, where); pks != nil {
				rows, _ = db.fetchRowsByPKs(schema, pks)
				rows = db.filterRows(schema, rows, where)
				goto applySort
			}
		}
	}

	return db.FastQuery(schema, where, orderBy, limit, offset)

applySort:
	if orderBy != nil {
		colIdx := schema.GetColumnIndex(orderBy.Column)
		if colIdx >= 0 {
			sortRows(rows, colIdx, orderBy.Desc)
		}
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

func (db *RelationalDB) parallelScan(schema *TableSchema, where *WhereClause) []Row {
	prefix := db.encoder.EncodeKey(schema.ID, 0)[:4]
	iter, err := db.kv.NewPrefixIterator(prefix)
	if err != nil {
		return nil
	}
	defer iter.Close()

	var pairs []struct {
		key []byte
		val []byte
	}

	for {
		key, val, err := iter.Current()
		if err != nil {
			break
		}
		keyCopy := make([]byte, len(key))
		copy(keyCopy, key)
		valCopy := make([]byte, len(val))
		copy(valCopy, val)
		pairs = append(pairs, struct {
			key []byte
			val []byte
		}{keyCopy, valCopy})

		if err := iter.Next(); err != nil {
			break
		}
	}

	if len(pairs) < 100 {
		rows := make([]Row, 0, len(pairs))
		for _, p := range pairs {
			decoded, err := db.encoder.DecodeValue(p.val)
			if err == nil {
				pk := binary.BigEndian.Uint64(p.key[4:12])
				row := Row{PrimaryKey: pk, Data: decoded}
				if where == nil || db.matchesFilter(schema, row, where) {
					rows = append(rows, row)
				}
			}
		}
		return rows
	}

	numWorkers := runtime.NumCPU()
	chunkSize := (len(pairs) + numWorkers - 1) / numWorkers

	var wg sync.WaitGroup
	results := make([][]Row, numWorkers)

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
			localRows := make([]Row, 0, end-start)
			for i := start; i < end; i++ {
				p := pairs[i]
				decoded, err := db.encoder.DecodeValue(p.val)
				if err == nil {
					pk := binary.BigEndian.Uint64(p.key[4:12])
					row := Row{PrimaryKey: pk, Data: decoded}
					if where == nil || db.matchesFilter(schema, row, where) {
						localRows = append(localRows, row)
					}
				}
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

	return rows
}

func sortRows(rows []Row, colIdx int, desc bool) {
	if len(rows) < 24 {
		for i := 1; i < len(rows); i++ {
			for j := i; j > 0 && compareRows(rows[j-1], rows[j], colIdx, desc) > 0; j-- {
				rows[j], rows[j-1] = rows[j-1], rows[j]
			}
		}
		return
	}

	sort.Slice(rows, func(i, j int) bool {
		cmp := compareInterface(rows[i].Data[colIdx], rows[j].Data[colIdx])
		if desc {
			return cmp > 0
		}
		return cmp < 0
	})
}

func compareRows(a, b Row, colIdx int, desc bool) int {
	cmp := compareInterface(a.Data[colIdx], b.Data[colIdx])
	if desc {
		return -cmp
	}
	return cmp
}

func (db *RelationalDB) tryIndexLookup(idx *IndexSchema, where *WhereClause) []uint64 {
	for _, orClause := range where.Or {
		for _, cond := range orClause.Conditions {
			if cond.Column == idx.Column && cond.Op == "=" {
				val := cond.Value.ToInterface()

				if idx.bloom != nil {
					valBytes := valueToBytes(val)
					if valBytes != nil && !idx.bloom.Test(valBytes) {
						return []uint64{} 
					}
				}

				return db.indexLookup(idx, val)
			}
		}
	}
	return nil
}

func (db *RelationalDB) indexLookup(idx *IndexSchema, value interface{}) []uint64 {
	prefix := make([]byte, 4)
	binary.BigEndian.PutUint32(prefix, idx.ID+1000)

	iter, err := db.kv.NewPrefixIterator(prefix)
	if err != nil {
		return nil
	}
	defer iter.Close()

	var pks []uint64
	for {
		key, val, err := iter.Current()
		if err != nil {
			break
		}
		if db.indexKeyMatches(key[4:], value) && len(val) == 8 {
			pks = append(pks, binary.BigEndian.Uint64(val))
		}
		if err := iter.Next(); err != nil {
			break
		}
	}
	return pks
}

func (db *RelationalDB) indexKeyMatches(keyPart []byte, value interface{}) bool {
	if len(keyPart) == 0 {
		return false
	}

	typeByte := keyPart[0]
	keyPart = keyPart[1:]

	switch v := value.(type) {
	case int:
		if typeByte != 1 || len(keyPart) < 8 {
			return false
		}
		return int64(binary.BigEndian.Uint64(keyPart[:8])) == int64(v)
	case string:
		if typeByte != 3 {
			return false
		}
		nullIdx := bytes.IndexByte(keyPart, 0)
		if nullIdx == -1 {
			return false
		}
		return string(keyPart[:nullIdx]) == v
	case bool:
		if typeByte != 4 || len(keyPart) < 1 {
			return false
		}
		return (keyPart[0] == 1) == v
	}
	return false
}

func (db *RelationalDB) fetchRowsByPKs(schema *TableSchema, pks []uint64) ([]Row, error) {
	rows := make([]Row, 0, len(pks))
	for _, pk := range pks {
		key := db.encoder.EncodeKey(schema.ID, pk)
		val, err := db.kv.Get(key)
		if err != nil || val == nil {
			continue
		}
		decoded, err := db.encoder.DecodeValue(val)
		if err == nil {
			rows = append(rows, Row{PrimaryKey: pk, Data: decoded})
		}
	}
	return rows, nil
}

func (db *RelationalDB) filterRows(schema *TableSchema, rows []Row, where *WhereClause) []Row {
	if where == nil {
		return rows
	}
	filtered := make([]Row, 0, len(rows))
	for _, row := range rows {
		if db.matchesFilter(schema, row, where) {
			filtered = append(filtered, row)
		}
	}
	return filtered
}

func compareInterface(a, b interface{}) int {
	aNum, aIsNum := toFloat64(a)
	bNum, bIsNum := toFloat64(b)

	if aIsNum && bIsNum {
		if aNum < bNum {
			return -1
		}
		if aNum > bNum {
			return 1
		}
		return 0
	}

	if aStr, ok := a.(string); ok {
		if bStr, ok := b.(string); ok {
			if aStr < bStr {
				return -1
			}
			if aStr > bStr {
				return 1
			}
			return 0
		}
	}

	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return 1
	}
	if b == nil {
		return -1
	}
	return 0
}

func (db *RelationalDB) handleUpdate(cmd *UpdateCmd) (string, error) {
	db.mu.RLock()
	schema, exists := db.catalog[cmd.Table]
	db.mu.RUnlock()
	if !exists {
		return "", fmt.Errorf("table '%s' does not exist", cmd.Table)
	}

	rows, keys, err := db.selectRowsWithKeys(schema, cmd.Where)
	if err != nil {
		return "", err
	}

	var kvPairs [][2][]byte
	for i, row := range rows {
		for _, kv := range cmd.Set {
			colIdx := schema.GetColumnIndex(kv.Key)
			if colIdx == -1 {
				return "", fmt.Errorf("unknown column: %s", kv.Key)
			}
			row.Data[colIdx] = kv.Value.ToInterface()
		}

		valBytes, err := db.encoder.EncodeValue(row)
		if err != nil {
			return "", err
		}
		kvPairs = append(kvPairs, [2][]byte{keys[i], valBytes})
	}

	if len(kvPairs) > 0 {
		if err := db.kv.BatchSet(kvPairs); err != nil {
			return "", err
		}
	}

	return fmt.Sprintf("Updated %d row(s)", len(rows)), nil
}

func (db *RelationalDB) handleDelete(cmd *DeleteCmd) (string, error) {
	db.mu.RLock()
	schema, exists := db.catalog[cmd.Table]
	db.mu.RUnlock()
	if !exists {
		return "", fmt.Errorf("table '%s' does not exist", cmd.Table)
	}

	_, keys, err := db.selectRowsWithKeys(schema, cmd.Where)
	if err != nil {
		return "", err
	}

	if len(keys) > 0 {
		if err := db.kv.BatchWrite(nil, keys); err != nil {
			return "", err
		}
	}

	return fmt.Sprintf("Deleted %d row(s)", len(keys)), nil
}

func (db *RelationalDB) handleDrop(cmd *DropCmd) (string, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	schema, exists := db.catalog[cmd.Table]
	if !exists {
		return "", fmt.Errorf("table '%s' does not exist", cmd.Table)
	}

	prefix := db.encoder.EncodeKey(schema.ID, 0)[:4]
	iter, err := db.kv.NewPrefixIterator(prefix)
	if err != nil {
		return "", err
	}

	var keysToDelete [][]byte
	for {
		key, _, err := iter.Current()
		if err != nil {
			break
		}
		keyCopy := make([]byte, len(key))
		copy(keyCopy, key)
		keysToDelete = append(keysToDelete, keyCopy)
		if err := iter.Next(); err != nil {
			break
		}
	}
	iter.Close()

	if len(keysToDelete) > 0 {
		db.kv.BatchWrite(nil, keysToDelete)
	}

	catalogKey := db.encoder.EncodeKey(SystemCatalogID, uint64(schema.ID))
	db.kv.Delete(catalogKey)
	delete(db.catalog, cmd.Table)

	return fmt.Sprintf("Table '%s' dropped", cmd.Table), nil
}

func (db *RelationalDB) createTableInternal(name string, colNames []string) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if _, exists := db.catalog[name]; exists {
		return fmt.Errorf("table '%s' already exists", name)
	}

	id := db.nextTableID
	db.nextTableID++

	schema := &TableSchema{ID: id, Name: name, Columns: colNames}
	schema.GetColumnIndex("") 

	key := db.encoder.EncodeKey(SystemCatalogID, uint64(id))
	val, _ := json.Marshal(schema)
	if err := db.kv.Set(key, val); err != nil {
		return err
	}
	db.catalog[name] = schema
	return nil
}

func (db *RelationalDB) selectRowsWithKeys(schema *TableSchema, where *WhereClause) ([]Row, [][]byte, error) {
	prefix := db.encoder.EncodeKey(schema.ID, 0)[:4]
	iter, err := db.kv.NewPrefixIterator(prefix)
	if err != nil {
		return nil, nil, err
	}
	defer iter.Close()

	rows := make([]Row, 0, 64)
	keys := make([][]byte, 0, 64)

	for {
		key, val, err := iter.Current()
		if err != nil {
			break
		}

		decoded, err := db.encoder.DecodeValue(val)
		if err == nil {
			pk := binary.BigEndian.Uint64(key[4:12])
			row := Row{PrimaryKey: pk, Data: decoded}

			if where == nil || db.matchesFilter(schema, row, where) {
				rows = append(rows, row)
				keyCopy := make([]byte, len(key))
				copy(keyCopy, key)
				keys = append(keys, keyCopy)
			}
		}

		if err := iter.Next(); err != nil {
			break
		}
	}
	return rows, keys, nil
}

func (db *RelationalDB) matchesFilter(schema *TableSchema, row Row, where *WhereClause) bool {
	if where == nil || len(where.Or) == 0 {
		return true
	}

	for _, andClause := range where.Or {
		if db.matchesAndClause(schema, row, andClause) {
			return true
		}
	}
	return false
}

func (db *RelationalDB) matchesAndClause(schema *TableSchema, row Row, andClause *AndClause) bool {
	for _, cond := range andClause.Conditions {
		colIdx := schema.GetColumnIndex(cond.Column)
		if colIdx == -1 || colIdx >= len(row.Data) {
			return false
		}

		if !db.compareValues(row.Data[colIdx], cond.Op, cond.Value) {
			return false
		}
	}
	return true
}

func (db *RelationalDB) compareValues(rowVal interface{}, op string, condVal *Value) bool {
	condInterface := condVal.ToInterface()

	if op == "like" {
		rowStr, ok := rowVal.(string)
		if !ok {
			return false
		}
		pattern, ok := condInterface.(string)
		if !ok {
			return false
		}
		return matchLike(rowStr, pattern)
	}

	rowNum, rowIsNum := toFloat64(rowVal)
	condNum, condIsNum := toFloat64(condInterface)

	if rowIsNum && condIsNum {
		switch op {
		case "=":
			return rowNum == condNum
		case "!=":
			return rowNum != condNum
		case ">":
			return rowNum > condNum
		case "<":
			return rowNum < condNum
		case ">=":
			return rowNum >= condNum
		case "<=":
			return rowNum <= condNum
		}
	}

	if rowStr, ok := rowVal.(string); ok {
		if condStr, ok := condInterface.(string); ok {
			switch op {
			case "=":
				return rowStr == condStr
			case "!=":
				return rowStr != condStr
			case ">":
				return rowStr > condStr
			case "<":
				return rowStr < condStr
			case ">=":
				return rowStr >= condStr
			case "<=":
				return rowStr <= condStr
			}
		}
	}

	if rowBool, ok := rowVal.(bool); ok {
		if condBool, ok := condInterface.(bool); ok {
			switch op {
			case "=":
				return rowBool == condBool
			case "!=":
				return rowBool != condBool
			}
		}
	}

	if rowVal == nil && condInterface == nil {
		return op == "="
	}
	if (rowVal == nil) != (condInterface == nil) {
		return op == "!="
	}

	return false
}

func matchLike(s, pattern string) bool {
	return matchLikeHelper(strings.ToLower(s), strings.ToLower(pattern))
}

func matchLikeHelper(s, pattern string) bool {
	for len(pattern) > 0 {
		switch pattern[0] {
		case '%':
			for len(pattern) > 0 && pattern[0] == '%' {
				pattern = pattern[1:]
			}
			if len(pattern) == 0 {
				return true
			}
			for i := 0; i <= len(s); i++ {
				if matchLikeHelper(s[i:], pattern) {
					return true
				}
			}
			return false
		case '_':
			if len(s) == 0 {
				return false
			}
			s = s[1:]
			pattern = pattern[1:]
		default:
			if len(s) == 0 || s[0] != pattern[0] {
				return false
			}
			s = s[1:]
			pattern = pattern[1:]
		}
	}
	return len(s) == 0
}

func toFloat64(v interface{}) (float64, bool) {
	switch n := v.(type) {
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	case float64:
		return n, true
	case string:
		if f, err := strconv.ParseFloat(n, 64); err == nil {
			return f, true
		}
	}
	return 0, false
}

func (db *RelationalDB) loadCatalog() error {
	prefix := db.encoder.EncodeKey(SystemCatalogID, 0)[:4]
	iter, err := db.kv.NewPrefixIterator(prefix)
	if err != nil {
		return err
	}
	defer iter.Close()

	for {
		_, val, err := iter.Current()
		if err != nil {
			break
		}
		var schema TableSchema
		if json.Unmarshal(val, &schema) == nil {
			db.catalog[schema.Name] = &schema
			if schema.ID >= db.nextTableID {
				atomic.StoreUint32(&db.nextTableID, schema.ID+1)
			}
		}
		if err := iter.Next(); err != nil {
			break
		}
	}
	return nil
}

func (db *RelationalDB) loadIndexes() error {
	prefix := db.encoder.EncodeKey(IndexCatalogID, 0)[:4]
	iter, err := db.kv.NewPrefixIterator(prefix)
	if err != nil {
		return err
	}
	defer iter.Close()

	for {
		_, val, err := iter.Current()
		if err != nil {
			break
		}
		var indexSchema IndexSchema
		if json.Unmarshal(val, &indexSchema) == nil {
			indexSchema.bloom = bloom.NewWithEstimates(100000, 0.01)
			db.indexes[indexSchema.Name] = &indexSchema
			db.tableIndex[indexSchema.Table] = append(db.tableIndex[indexSchema.Table], &indexSchema)
			if indexSchema.ID >= db.nextIndexID {
				atomic.StoreUint32(&db.nextIndexID, indexSchema.ID+1)
			}
		}
		if err := iter.Next(); err != nil {
			break
		}
	}
	return nil
}

func unsafeString(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

func (db *RelationalDB) Stats() map[string]interface{} {
	resultHits, resultMisses := db.resultCache.Stats()
	return map[string]interface{}{
		"queries":            atomic.LoadUint64(&db.queryCount),
		"query_cache_hits":   atomic.LoadUint64(&db.cacheHits),
		"result_cache_hits":  resultHits,
		"result_cache_misses": resultMisses,
		"tables":             len(db.catalog),
		"indexes":            len(db.indexes),
	}
}
