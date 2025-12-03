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

	"regexp"

	"github.com/alecthomas/participle/v2"
	"github.com/alecthomas/participle/v2/lexer"
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

type Query struct {
	Cmd *Command `parser:"@@? ';'?"`
}

type Command struct {
	Create *CreateStmt `parser:"(@@ |"`
	Insert *InsertCmd  `parser:" @@ |"`
	Select *SelectCmd  `parser:" @@ |"`
	Update *UpdateCmd  `parser:" @@ |"`
	Delete *DeleteCmd  `parser:" @@ |"`
	Drop   *DropStmt   `parser:" @@)"`
}

type CreateStmt struct {
	Table *CreateCmd      `parser:"'CREATE' ('TABLE' @@"`
	Index *CreateIndexCmd `parser:"| 'INDEX' @@)"`
}

type DropStmt struct {
	Table *DropCmd      `parser:"'DROP' ('TABLE' @@"`
	Index *DropIndexCmd `parser:"| 'INDEX' @@)"`
}

type CreateCmd struct {
	Name     string         `parser:"@Ident"`
	Elements []TableElement `parser:"('(' @@ (',' @@)* ')') | ('{' @@ (',' @@)* '}')"`
}

type TableElement struct {
	Constraint *Constraint `parser:"'CONSTRAINT' @@"`
	Column     *ColDef     `parser:"| @@"`
}

type Constraint struct {
	Name  string     `parser:"@Ident"`
	Check *CheckExpr `parser:"'CHECK' '(' @@ ')'"`
}

type CheckExpr struct {
	Left   string   `parser:"@Ident"`
	Op     string   `parser:"@('LIKE' | 'IN' | '=' | '!=' | '>' | '<' | '>=' | '<=')"`
	Right  *Value   `parser:"@@?"`
	InList []*Value `parser:"('(' @@ (',' @@)* ')')?"`
}

type ColDef struct {
	Name          string      `parser:"@Ident ':'?"`
	Type          string      `parser:"@Ident"`
	TypeParams    []string    `parser:"('(' (@String | @Ident | @Int) (',' (@String | @Ident | @Int))* ')')?"`
	PrimaryKey    bool        `parser:"@('PRIMARY' 'KEY')?"`
	AutoIncrement bool        `parser:"@('AUTO_INCREMENT')?"`
	NotNull       bool        `parser:"@('NOT' 'NULL')?"`
	Unique        bool        `parser:"@('UNIQUE')?"`
	Default       *Value      `parser:"('DEFAULT' @@)?"`
	ForeignKey    *ForeignKey `parser:"@@?"`
	Check         *CheckExpr  `parser:"('CHECK' '(' @@ ')')?"`
}

type ForeignKey struct {
	Table    string `parser:"'REFERENCES' @Ident"`
	Column   string `parser:"'(' @Ident ')'"`
	OnDelete string `parser:"('ON' 'DELETE' @('CASCADE' | 'SET' 'NULL' | 'RESTRICT' | 'NO' 'ACTION'))?"`
	OnUpdate string `parser:"('ON' 'UPDATE' @('CASCADE' | 'SET' 'NULL' | 'RESTRICT' | 'NO' 'ACTION'))?"`
}

type CreateIndexCmd struct {
	Name   string `parser:"@Ident"`
	Table  string `parser:"'ON' @Ident"`
	Column string `parser:"'(' @Ident ')'"`
}

type DropIndexCmd struct {
	Name string `parser:"@Ident"`
}

type InsertCmd struct {
	Table   string   `parser:"'INSERT' 'INTO' @Ident"`
	Columns []string `parser:"'(' @Ident (',' @Ident)* ')'"`
	Values  []*Value `parser:"'VALUES' '(' @@ (',' @@)* ')'"`
}

type KV struct {
	Key   string `parser:"@Ident ':'"`
	Value *Value `parser:"@@"`
}

type Value struct {
	Null       bool     `parser:"  @'NULL'"`
	Bool       *bool    `parser:"| @('TRUE' | 'FALSE')"`
	Float      *float64 `parser:"| @Float"`
	Number     *int     `parser:"| @Int"`
	String     *string  `parser:"| @String"`
	Identifier *string  `parser:"| @Ident"`
	Object     []KV     `parser:"| '{' (@@ (',' @@)*)? '}'"`
	Array      []*Value `parser:"| '[' (@@ (',' @@)*)? ']'"`
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
	if v.Identifier != nil {
		return *v.Identifier
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
	Selectors []Selector   `parser:"'SELECT' @@ (',' @@)*"`
	Table     *string      `parser:"('FROM' @Ident)?"`
	Joins     []*Join      `parser:"@@*"`
	Where     *WhereClause `parser:"@@?"`
	GroupBy   *GroupBy     `parser:"@@?"`
	OrderBy   *OrderClause `parser:"@@?"`
	Limit     *int         `parser:"('LIMIT' @Int)?"`
	Offset    *int         `parser:"('OFFSET' @Int)?"`
}

type Join struct {
	Type  string `parser:"@('INNER' | 'LEFT' | 'RIGHT' | 'FULL' | 'CROSS')? 'JOIN'"`
	Table string `parser:"@Ident"`
	On    *On    `parser:"'ON' @@"`
}

type On struct {
	Left  string `parser:"@Ident"`
	Op    string `parser:"@('=' | '!=' | '>' | '<' | '>=' | '<=')"`
	Right string `parser:"@Ident"`
}

type GroupBy struct {
	Columns []string `parser:"'GROUP' 'BY' @Ident (',' @Ident)*"`
}

type Selector struct {
	Star     bool            `parser:"@'*'"`
	Function *FuncWithAlias  `parser:"| @@"`
	Value    *ValueWithAlias `parser:"| @@"`
}

type FuncWithAlias struct {
	Func  *Func  `parser:"@@"`
	Alias string `parser:"( 'AS' @Ident )?"`
}

type ValueWithAlias struct {
	Value *Value `parser:"@@"`
	Alias string `parser:"( 'AS' @Ident )?"`
}

type Func struct {
	Name string `parser:"@Ident"`
	Arg  string `parser:"'(' @Ident ')'"`
}

type OrderClause struct {
	Column    string `parser:"'ORDER' 'BY' @Ident"`
	Direction string `parser:"@('DESC' | 'ASC')?"`
}

type FindCmd struct {
	Table   string       `parser:"'FIND' @Ident"`
	Where   *WhereClause `parser:"@@?"`
	OrderBy *OrderClause `parser:"@@?"`
	Limit   *int         `parser:"('LIMIT' @Int)?"`
}

type WhereClause struct {
	Or []*AndClause `parser:"'WHERE' @@ ('OR' @@)*"`
}

type AndClause struct {
	Conditions []Condition `parser:"@@ ('AND' @@)*"`
}

type Condition struct {
	Column string `parser:"@Ident"`
	Op     string `parser:"@('=' | '!=' | '>' | '<' | '>=' | '<=' | 'LIKE')"`
	Value  *Value `parser:"@@"`
}

type UpdateCmd struct {
	Table       string       `parser:"'UPDATE' @Ident"`
	Assignments []Assignment `parser:"'SET' @@ (',' @@)*"`
	Where       *WhereClause `parser:"@@?"`
}

type Assignment struct {
	Column string `parser:"@Ident '='"`
	Value  *Value `parser:"@@"`
}

type DeleteCmd struct {
	Table string       `parser:"'DELETE' 'FROM' @Ident"`
	Where *WhereClause `parser:"@@?"`
}

type DropCmd struct {
	Table string `parser:"@Ident"`
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

var (
	sqlLexer = lexer.MustSimple([]lexer.SimpleRule{
		{Name: "Comment", Pattern: `--[^\n]*`},
		{Name: "String", Pattern: `'[^']*'|"[^"]*"`},
		{Name: "Float", Pattern: `[-+]?\d+\.\d+`},
		{Name: "Int", Pattern: `[-+]?\d+`},
		{Name: "Operators", Pattern: `>=|<=|!=|<>`},
		{Name: "Punct", Pattern: `[-!()+/*,;=<>]`},
		{Name: "Ident", Pattern: `[a-zA-Z_][a-zA-Z0-9_]*(\.[a-zA-Z_][a-zA-Z0-9_]*)?`},
		{Name: "Whitespace", Pattern: `\s+`},
	})
)

func NewRelationalDB(storagePath string) (*RelationalDB, error) {
	kv, err := tree.Open(storagePath)
	if err != nil {
		return nil, err
	}

	parser, err := participle.Build[Query](
		participle.Lexer(sqlLexer),
		participle.CaseInsensitive("Ident"),
		participle.Unquote("String"),
		participle.Elide("Whitespace", "Comment"),
	)
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

	parser, err := participle.Build[Query](
		participle.Lexer(sqlLexer),
		participle.CaseInsensitive("Ident"),
		participle.Unquote("String"),
		participle.Elide("Whitespace", "Comment"),
	)
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

func (db *RelationalDB) findIndexForColumn(tableName, columnName string) *IndexSchema {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if indexes, ok := db.tableIndex[tableName]; ok {
		for _, idx := range indexes {
			if idx.Column == columnName {
				return idx
			}
		}
	}
	return nil
}

func (db *RelationalDB) Close() error {
	return db.kv.Close()
}

func (db *RelationalDB) Execute(queryStr string) (string, error) {
	atomic.AddUint64(&db.queryCount, 1)

	lower := strings.ToLower(strings.TrimSpace(queryStr))
	if lower == "show tables" {
		return db.handleShowTables()
	}
	if strings.HasPrefix(lower, "describe ") {
		tableName := strings.TrimSpace(queryStr[9:])
		return db.handleDescribe(tableName)
	}

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

	if ast.Cmd == nil {
		return "", nil
	}

	cmd := ast.Cmd
	switch {
	case cmd.Create != nil:
		if cmd.Create.Table != nil {
			return db.handleCreate(cmd.Create.Table)
		}
		if cmd.Create.Index != nil {
			return db.handleCreateIndex(cmd.Create.Index)
		}
	case cmd.Insert != nil:
		return db.handleInsert(cmd.Insert)
	case cmd.Select != nil:
		return db.handleSelectCached(queryStr, cmd.Select)
	case cmd.Update != nil:
		db.resultCache.Invalidate()
		return db.handleUpdate(cmd.Update)
	case cmd.Delete != nil:
		db.resultCache.Invalidate()
		return db.handleDelete(cmd.Delete)
	case cmd.Drop != nil:
		if cmd.Drop.Table != nil {
			db.resultCache.Invalidate()
			return db.handleDrop(cmd.Drop.Table)
		}
		if cmd.Drop.Index != nil {
			return db.handleDropIndex(cmd.Drop.Index)
		}
	}

	return "", errors.New("empty query")
}

func (db *RelationalDB) handleShowTables() (string, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	tables := make([]map[string]interface{}, 0, len(db.catalog))
	for name, schema := range db.catalog {
		tables = append(tables, map[string]interface{}{
			"name":    name,
			"columns": len(schema.Columns),
		})
	}

	data, _ := json.Marshal(tables)
	return string(data), nil
}

func (db *RelationalDB) handleDescribe(tableName string) (string, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	schema, ok := db.catalog[tableName]
	if !ok {
		return "", fmt.Errorf("table '%s' not found", tableName)
	}

	types := schema.Types
	if types == nil || len(types) == 0 {
		types = make([]string, len(schema.Columns))
		for i := range types {
			types[i] = "any"
		}
	}

	indexes := make([]string, 0)
	for _, idx := range db.tableIndex[tableName] {
		indexes = append(indexes, idx.Column)
	}

	result := map[string]interface{}{
		"name":        tableName,
		"columns":     schema.Columns,
		"types":       types,
		"indexes":     indexes,
		"primary_key": schema.PrimaryKey,
	}

	if schema.AutoIncrement {
		result["auto_increment"] = true
	}

	if len(schema.ForeignKeys) > 0 {
		fks := make(map[string]string)
		for col, ref := range schema.ForeignKeys {
			fks[col] = ref.Table + "(" + ref.Column + ")"
		}
		result["foreign_keys"] = fks
	}

	if len(schema.Constraints.NotNull) > 0 {
		result["not_null"] = schema.Constraints.NotNull
	}

	if len(schema.Constraints.Unique) > 0 {
		result["unique"] = schema.Constraints.Unique
	}

	if len(schema.Constraints.Defaults) > 0 {
		result["defaults"] = schema.Constraints.Defaults
	}

	if len(schema.Constraints.Enums) > 0 {
		result["enums"] = schema.Constraints.Enums
	}

	if len(schema.Constraints.Checks) > 0 {
		result["checks"] = schema.Constraints.Checks
	}

	data, _ := json.Marshal(result)
	return string(data), nil
}

func (db *RelationalDB) handleCreate(cmd *CreateCmd) (string, error) {
	var cols []ColDef
	for _, el := range cmd.Elements {
		if el.Column != nil {
			cols = append(cols, *el.Column)
		}
	}

	colNames := make([]string, len(cols))
	colTypes := make([]string, len(cols))
	var primaryKey string
	var autoIncrement bool
	foreignKeys := make(map[string]FKRef)
	constraints := ColumnConstraints{
		NotNull:  make(map[string]bool),
		Unique:   make(map[string]bool),
		Defaults: make(map[string]interface{}),
		Enums:    make(map[string][]string),
		Checks:   make(map[string]string),
		Lengths:  make(map[string]int),
	}

	for i, col := range cols {
		colNames[i] = col.Name
		colType := col.Type

		if strings.EqualFold(col.Type, "VARCHAR") || strings.EqualFold(col.Type, "CHAR") {
			if len(col.TypeParams) > 0 {
				if length, err := strconv.Atoi(col.TypeParams[0]); err == nil {
					constraints.Lengths[col.Name] = length
				}
			}
			colType = "string"
		} else if strings.EqualFold(col.Type, "ENUM") {
			if len(col.TypeParams) > 0 {
				enumValues := make([]string, len(col.TypeParams))
				for j, param := range col.TypeParams {
					enumValues[j] = strings.Trim(param, `"'`)
				}
				constraints.Enums[col.Name] = enumValues
			}
			colType = "string"
		} else if strings.EqualFold(col.Type, "TIMESTAMP") {
			colType = "int"
		} else if strings.EqualFold(col.Type, "BOOLEAN") {
			colType = "bool"
		} else if strings.EqualFold(col.Type, "INT") {
			colType = "int"
		}

		colTypes[i] = strings.ToLower(colType)

		if col.PrimaryKey {
			if primaryKey != "" {
				return "", fmt.Errorf("table can only have one primary key")
			}
			primaryKey = col.Name
			autoIncrement = col.AutoIncrement
		}

		if col.NotNull {
			constraints.NotNull[col.Name] = true
		}

		if col.Unique {
			constraints.Unique[col.Name] = true
		}

		if col.Default != nil {
			constraints.Defaults[col.Name] = col.Default.ToInterface()
		} else if strings.EqualFold(col.Type, "TIMESTAMP") && primaryKey != col.Name {
		}

		if col.Default != nil {
			constraints.Defaults[col.Name] = col.Default.ToInterface()
		} else if strings.EqualFold(col.Type, "timestamp") && primaryKey != col.Name {
			constraints.Defaults[col.Name] = time.Now().Unix()
		}

		if col.ForeignKey != nil {
			foreignKeys[col.Name] = FKRef{
				Table:  col.ForeignKey.Table,
				Column: col.ForeignKey.Column,
			}
		}

		if col.Check != nil {
			checkExpr := col.Check
			if checkExpr.InList != nil {
				vals := make([]string, len(checkExpr.InList))
				for i, v := range checkExpr.InList {
					vals[i] = fmt.Sprintf("%v", v.ToInterface())
					if s, ok := v.ToInterface().(string); ok {
						vals[i] = fmt.Sprintf("'%s'", s)
					}
				}
				checkBody := fmt.Sprintf("%s IN (%s)", checkExpr.Left, strings.Join(vals, ", "))
				constraints.Checks[col.Name] = checkBody
			} else {
				rightVal := checkExpr.Right.ToInterface()
				rightStr := fmt.Sprintf("%v", rightVal)
				if s, ok := rightVal.(string); ok {
					rightStr = fmt.Sprintf("'%s'", s)
				}
				checkBody := fmt.Sprintf("%s %s %s", checkExpr.Left, checkExpr.Op, rightStr)
				constraints.Checks[col.Name] = checkBody
			}
		}
	}

	for _, el := range cmd.Elements {
		if el.Constraint != nil {

			checkExpr := el.Constraint.Check
			if checkExpr == nil {
				continue
			}

			var checkBody string
			if checkExpr.InList != nil {
				vals := make([]string, len(checkExpr.InList))
				for i, v := range checkExpr.InList {
					vals[i] = fmt.Sprintf("%v", v.ToInterface())
					if s, ok := v.ToInterface().(string); ok {
						vals[i] = fmt.Sprintf("'%s'", s)
					}
				}
				checkBody = fmt.Sprintf("%s IN (%s)", checkExpr.Left, strings.Join(vals, ", "))
			} else {
				rightVal := checkExpr.Right.ToInterface()
				rightStr := fmt.Sprintf("%v", rightVal)
				if s, ok := rightVal.(string); ok {
					rightStr = fmt.Sprintf("'%s'", s)
				}
				checkBody = fmt.Sprintf("%s %s %s", checkExpr.Left, checkExpr.Op, rightStr)
			}

			for _, colName := range colNames {
				if strings.EqualFold(checkExpr.Left, colName) {
					constraints.Checks[colName] = checkBody
				}
			}
		}
	}

	if err := db.createTableFull(cmd.Name, colNames, colTypes, primaryKey, autoIncrement, foreignKeys, constraints); err != nil {
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

	if len(cmd.Columns) != len(cmd.Values) {
		return "", fmt.Errorf("column count %d does not match value count %d", len(cmd.Columns), len(cmd.Values))
	}

	rowData := make([]interface{}, len(schema.Columns))
	pk := uint64(0)
	pkCol := schema.PrimaryKey
	if pkCol == "" {
		pkCol = "id"
	}

	for i := range rowData {
		colName := schema.Columns[i]
		if def, ok := schema.Constraints.Defaults[colName]; ok {
			if s, isStr := def.(string); isStr && s == "CURRENT_TIMESTAMP" {
				rowData[i] = time.Now().Unix()
			} else {
				rowData[i] = def
			}
		}
	}

	for i, colName := range cmd.Columns {
		colIdx := schema.GetColumnIndex(colName)
		if colIdx == -1 {
			return "", fmt.Errorf("unknown column: %s", colName)
		}

		val := cmd.Values[i].ToInterface()

		if err := db.validateColumnValue(schema, colName, val); err != nil {
			return "", err
		}

		rowData[colIdx] = val

		if colName == pkCol {
			switch v := val.(type) {
			case int:
				pk = uint64(v)
			case float64:
				pk = uint64(v)
			}
		}
	}

	if schema.AutoIncrement && pk == 0 {
		pk = db.getNextAutoIncrement(schema.ID)
		if pkIdx := schema.GetColumnIndex(pkCol); pkIdx != -1 {
			rowData[pkIdx] = int(pk)
		}
	}

	for i, col := range schema.Columns {
		if rowData[i] == nil {
			if schema.Constraints.NotNull[col] {
				return "", fmt.Errorf("column '%s' cannot be null", col)
			}
		}
	}

	if schema.Constraints.Unique != nil {
		for col, _ := range schema.Constraints.Unique {
			colIdx := schema.GetColumnIndex(col)
			if colIdx >= 0 && colIdx < len(rowData) && rowData[colIdx] != nil {
				if err := db.validateUnique(schema, col, rowData[colIdx]); err != nil {
					return "", err
				}
			}
		}
	}

	if len(schema.ForeignKeys) > 0 {
		for fkCol, fkRef := range schema.ForeignKeys {
			colIdx := schema.GetColumnIndex(fkCol)
			if colIdx == -1 || colIdx >= len(rowData) {
				continue
			}
			fkVal := rowData[colIdx]
			if fkVal == nil {
				continue
			}
			if err := db.validateForeignKey(fkRef.Table, fkRef.Column, fkVal); err != nil {
				return "", fmt.Errorf("foreign key constraint failed: %w", err)
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

func (db *RelationalDB) handleSelect(cmd *SelectCmd) (string, error) {
	if cmd.Table == nil {
		row := make(map[string]interface{})
		for i, sel := range cmd.Selectors {
			var name string
			var val interface{}

			if sel.Value != nil {
				val = sel.Value.Value.ToInterface()
				if sel.Value.Alias != "" {
					name = sel.Value.Alias
				} else if sel.Value.Value.Identifier != nil {
					name = *sel.Value.Value.Identifier
				} else {
					name = fmt.Sprintf("col_%d", i)
				}
			} else if sel.Function != nil {
				name = fmt.Sprintf("%s(%s)", sel.Function.Func.Name, sel.Function.Func.Arg)
				if sel.Function.Alias != "" {
					name = sel.Function.Alias
				}
				val = 0
			} else if sel.Star {
				return "[]", nil
			} else {
				continue
			}

			if name != "" {
				row[name] = val
			}
		}
		result := []map[string]interface{}{row}
		out, _ := json.Marshal(result)
		return unsafeString(out), nil
	}

	db.mu.RLock()
	schema, exists := db.catalog[*cmd.Table]
	db.mu.RUnlock()

	if !exists {
		return "", fmt.Errorf("table '%s' does not exist", *cmd.Table)
	}

	rows, err := db.selectWithOptions(*cmd.Table, cmd.Where, cmd.OrderBy, cmd.Limit, cmd.Offset)
	if err != nil {
		return "", err
	}

	var result []map[string]interface{}
	for _, row := range rows {
		outRow := make(map[string]interface{})
		for i, sel := range cmd.Selectors {
			name := fmt.Sprintf("col_%d", i)
			var alias string
			if sel.Star {
				for j, colName := range schema.Columns {
					if j < len(row.Data) {
						outRow[colName] = row.Data[j]
					}
				}
				continue
			} else if sel.Value != nil {
				if sel.Value.Alias != "" {
					alias = sel.Value.Alias
				} else if sel.Value.Value.Identifier != nil {
					name = *sel.Value.Value.Identifier
				}
				colIdx := schema.GetColumnIndex(name)
				if colIdx >= 0 && colIdx < len(row.Data) {
					if alias != "" {
						outRow[alias] = row.Data[colIdx]
					} else {
						outRow[name] = row.Data[colIdx]
					}
				}
			} else if sel.Function != nil {
				name = fmt.Sprintf("%s(%s)", sel.Function.Func.Name, sel.Function.Func.Arg)
				if sel.Function.Alias != "" {
					alias = sel.Function.Alias
				}
				colIdx := schema.GetColumnIndex(sel.Function.Func.Arg)
				if colIdx >= 0 && colIdx < len(row.Data) {
					val := row.Data[colIdx]
					if alias != "" {
						outRow[alias] = val
					} else {
						outRow[name] = val
					}
				}
			}
		}
		if len(outRow) > 0 {
			result = append(result, outRow)
		}
	}

	out, _ := json.Marshal(result)
	return unsafeString(out), nil
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
			desc := strings.EqualFold(orderBy.Direction, "DESC")
			sortRows(rows, colIdx, desc)
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
	tableIndexes := db.tableIndex[cmd.Table]
	db.mu.RUnlock()
	if !exists {
		return "", fmt.Errorf("table '%s' does not exist", cmd.Table)
	}

	rows, err := db.FastQuery(schema, cmd.Where, nil, nil, nil)
	if err != nil {
		return "", err
	}

	var setPairs [][2][]byte
	var deleteKeys [][]byte
	for _, row := range rows {
		// Store old values for index updates
		oldRow := make([]interface{}, len(row.Data))
		copy(oldRow, row.Data)

		for _, assign := range cmd.Assignments {
			colIdx := schema.GetColumnIndex(assign.Column)
			if colIdx == -1 {
				return "", fmt.Errorf("unknown column: %s", assign.Column)
			}
			row.Data[colIdx] = assign.Value.ToInterface()
		}

		valBytes, err := db.encoder.EncodeValue(row)
		if err != nil {
			return "", err
		}
		key := db.encoder.EncodeKey(schema.ID, row.PrimaryKey)
		setPairs = append(setPairs, [2][]byte{key, valBytes})

		// Update indexes
		for _, idx := range tableIndexes {
			if idx.ColIdx >= 0 && idx.ColIdx < len(oldRow) {
				oldVal := oldRow[idx.ColIdx]
				newVal := row.Data[idx.ColIdx]

				// Delete old index entry
				if oldVal != nil {
					oldIndexKey := db.encodeIndexKey(idx.ID, oldVal, row.PrimaryKey)
					deleteKeys = append(deleteKeys, oldIndexKey)
				}

				// Add new index entry
				if newVal != nil {
					newIndexKey := db.encodeIndexKey(idx.ID, newVal, row.PrimaryKey)
					pkBytes := make([]byte, 8)
					binary.BigEndian.PutUint64(pkBytes, row.PrimaryKey)
					setPairs = append(setPairs, [2][]byte{newIndexKey, pkBytes})

					if idx.bloom != nil {
						idx.bloom.Add(valueToBytes(newVal))
					}
				}
			}
		}
	}

	if len(setPairs) > 0 {
		if err := db.kv.BatchSet(setPairs); err != nil {
			return "", err
		}
	}
	if len(deleteKeys) > 0 {
		if err := db.kv.BatchWrite(nil, deleteKeys); err != nil {
			return "", err
		}
	}

	return fmt.Sprintf("Updated %d row(s)", len(rows)), nil
}

func (db *RelationalDB) handleDelete(cmd *DeleteCmd) (string, error) {
	db.mu.RLock()
	schema, exists := db.catalog[cmd.Table]
	tableIndexes := db.tableIndex[cmd.Table]
	db.mu.RUnlock()
	if !exists {
		return "", fmt.Errorf("table '%s' does not exist", cmd.Table)
	}

	rows, err := db.FastQuery(schema, cmd.Where, nil, nil, nil)
	if err != nil {
		return "", err
	}

	var keys [][]byte
	for _, row := range rows {
		key := db.encoder.EncodeKey(schema.ID, row.PrimaryKey)
		keys = append(keys, key)

		// Delete from indexes
		for _, idx := range tableIndexes {
			if idx.ColIdx >= 0 && idx.ColIdx < len(row.Data) {
				val := row.Data[idx.ColIdx]
				if val != nil {
					indexKey := db.encodeIndexKey(idx.ID, val, row.PrimaryKey)
					keys = append(keys, indexKey)
				}
			}
		}
	}

	if len(keys) > 0 {
		if err := db.kv.BatchWrite(nil, keys); err != nil {
			return "", err
		}
	}

	return fmt.Sprintf("Deleted %d row(s)", len(rows)), nil
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

func (db *RelationalDB) createTableFull(name string, colNames []string, colTypes []string, primaryKey string, autoIncrement bool, foreignKeys map[string]FKRef, constraints ColumnConstraints) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if _, exists := db.catalog[name]; exists {
		return fmt.Errorf("table '%s' already exists", name)
	}

	if colTypes == nil {
		colTypes = make([]string, len(colNames))
		for i := range colTypes {
			colTypes[i] = "any"
		}
	}

	if primaryKey != "" {
		found := false
		for _, col := range colNames {
			if col == primaryKey {
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("primary key column '%s' not found in table", primaryKey)
		}
	}

	for fkCol, fkRef := range foreignKeys {
		refSchema, exists := db.catalog[fkRef.Table]
		if !exists {
			return fmt.Errorf("foreign key references non-existent table '%s'", fkRef.Table)
		}
		if refSchema.GetColumnIndex(fkRef.Column) == -1 {
			return fmt.Errorf("foreign key references non-existent column '%s' in table '%s'", fkRef.Column, fkRef.Table)
		}
		found := false
		for _, col := range colNames {
			if col == fkCol {
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("foreign key column '%s' not found in table", fkCol)
		}
	}

	id := db.nextTableID
	db.nextTableID++

	schema := &TableSchema{
		ID:            id,
		Name:          name,
		Columns:       colNames,
		Types:         colTypes,
		PrimaryKey:    primaryKey,
		AutoIncrement: autoIncrement,
		ForeignKeys:   foreignKeys,
		Constraints:   constraints,
	}
	schema.InitColumnIndex()

	if autoIncrement {
		key := db.encoder.EncodeKey(SystemCatalogID+1, uint64(id))
		seqVal := make([]byte, 8)
		binary.BigEndian.PutUint64(seqVal, 0)
		db.kv.Set(key, seqVal)
	}

	key := db.encoder.EncodeKey(SystemCatalogID, uint64(id))
	val, _ := json.Marshal(schema)
	if err := db.kv.Set(key, val); err != nil {
		return err
	}
	db.catalog[name] = schema
	return nil
}

func (db *RelationalDB) validateForeignKey(refTable, refColumn string, value interface{}) error {
	db.mu.RLock()
	schema, exists := db.catalog[refTable]
	db.mu.RUnlock()

	if !exists {
		return fmt.Errorf("referenced table '%s' does not exist", refTable)
	}

	colIdx := schema.GetColumnIndex(refColumn)
	if colIdx == -1 {
		return fmt.Errorf("referenced column '%s' does not exist in table '%s'", refColumn, refTable)
	}

	prefix := db.encoder.EncodeKey(schema.ID, 0)[:4]
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

		rowData, err := db.encoder.DecodeValue(val)
		if err != nil {
			iter.Next()
			continue
		}
		if colIdx < len(rowData) {
			if compareValues(rowData[colIdx], value) {
				return nil
			}
		}
		iter.Next()
	}

	return fmt.Errorf("no matching row in '%s' where %s = %v", refTable, refColumn, value)
}

func compareValues(a, b interface{}) bool {
	if a == nil || b == nil {
		return a == b
	}
	switch av := a.(type) {
	case int:
		switch bv := b.(type) {
		case int:
			return av == bv
		case float64:
			return float64(av) == bv
		}
	case float64:
		switch bv := b.(type) {
		case int:
			return av == float64(bv)
		case float64:
			return av == bv
		}
	case string:
		if bv, ok := b.(string); ok {
			return av == bv
		}
	case bool:
		if bv, ok := b.(bool); ok {
			return av == bv
		}
	}
	return fmt.Sprintf("%v", a) == fmt.Sprintf("%v", b)
}

func (db *RelationalDB) getNextAutoIncrement(tableID uint32) uint64 {
	db.mu.Lock()
	defer db.mu.Unlock()

	key := db.encoder.EncodeKey(SystemCatalogID+1, uint64(tableID))
	val, err := db.kv.Get(key)
	if err != nil || len(val) < 8 {
		seqVal := make([]byte, 8)
		binary.BigEndian.PutUint64(seqVal, 1)
		db.kv.Set(key, seqVal)
		return 1
	}

	seq := binary.BigEndian.Uint64(val)
	seq++
	seqVal := make([]byte, 8)
	binary.BigEndian.PutUint64(seqVal, seq)
	db.kv.Set(key, seqVal)
	return seq
}

func (db *RelationalDB) validateColumnValue(schema *TableSchema, colName string, value interface{}) error {
	if value == nil {
		if schema.Constraints.NotNull[colName] {
			return fmt.Errorf("column '%s' cannot be null", colName)
		}
		return nil
	}

	if enumValues, ok := schema.Constraints.Enums[colName]; ok {
		strVal := fmt.Sprintf("%v", value)
		found := false
		for _, ev := range enumValues {
			if ev == strVal {
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("column '%s' value '%v' not in enum: %v", colName, value, enumValues)
		}
	}

	if maxLen, ok := schema.Constraints.Lengths[colName]; ok {
		if strVal, ok := value.(string); ok {
			if len(strVal) > maxLen {
				return fmt.Errorf("column '%s' value exceeds maximum length of %d", colName, maxLen)
			}
		}
	}

	if checkExpr, ok := schema.Constraints.Checks[colName]; ok {
		if err := db.validateCheckConstraint(colName, value, checkExpr); err != nil {
			return err
		}
	}

	return nil
}

func (db *RelationalDB) validateCheckConstraint(colName string, value interface{}, checkExpr string) error {

	if strings.Contains(checkExpr, " IN (") {
		parts := strings.SplitN(checkExpr, " IN (", 2)
		if len(parts) == 2 {
			listStr := strings.TrimSuffix(parts[1], ")")

			var allowed []string
			var current strings.Builder
			inQuote := false
			for _, r := range listStr {
				if r == '\'' {
					inQuote = !inQuote
				}
				if r == ',' && !inQuote {
					allowed = append(allowed, strings.TrimSpace(current.String()))
					current.Reset()
				} else {
					current.WriteRune(r)
				}
			}
			if current.Len() > 0 {
				allowed = append(allowed, strings.TrimSpace(current.String()))
			}

			strVal := fmt.Sprintf("%v", value)

			found := false
			for _, item := range allowed {
				cleanItem := strings.Trim(item, "'")
				if cleanItem == strVal {
					found = true
					break
				}
			}
			if !found {
				return fmt.Errorf("check constraint failed: value '%v' not allowed", value)
			}
			return nil
		}
	}

	checkExpr = strings.Trim(checkExpr, `"'`)

	if strings.Contains(checkExpr, "LIKE") {
		pattern := extractLikePattern(checkExpr)
		if pattern != "" {
			strVal := fmt.Sprintf("%v", value)
			if !matchesLike(strVal, pattern) {
				return fmt.Errorf("check constraint failed: %s does not match pattern %s", colName, pattern)
			}
		}
		return nil
	}

	parts := strings.Fields(checkExpr)
	if len(parts) >= 3 {
		op := parts[1]
		valStr := parts[2]

		valNum, err := strconv.ParseFloat(valStr, 64)
		if err == nil {
			rowNum, isNum := toFloat64(value)
			if isNum {
				switch op {
				case ">":
					if !(rowNum > valNum) {
						return fmt.Errorf("check constraint failed: %v must be > %v", value, valNum)
					}
				case ">=":
					if !(rowNum >= valNum) {
						return fmt.Errorf("check constraint failed: %v must be >= %v", value, valNum)
					}
				case "<":
					if !(rowNum < valNum) {
						return fmt.Errorf("check constraint failed: %v must be < %v", value, valNum)
					}
				case "<=":
					if !(rowNum <= valNum) {
						return fmt.Errorf("check constraint failed: %v must be <= %v", value, valNum)
					}
				case "=":
					if !(rowNum == valNum) {
						return fmt.Errorf("check constraint failed: %v must be == %v", value, valNum)
					}
				case "!=":
					if !(rowNum != valNum) {
						return fmt.Errorf("check constraint failed: %v must be != %v", value, valNum)
					}
				}
			}
		}
	}

	return nil
}

func extractLikePattern(expr string) string {
	parts := strings.Split(expr, "LIKE")
	if len(parts) < 2 {
		return ""
	}
	pattern := strings.TrimSpace(parts[1])
	pattern = strings.Trim(pattern, `"'`)
	return pattern
}

func matchesLike(str, pattern string) bool {
	if pattern == "" {
		return true
	}

	regexPattern := "^"
	for i := 0; i < len(pattern); i++ {
		if pattern[i] == '%' {
			regexPattern += ".*"
		} else if pattern[i] == '_' {
			regexPattern += "."
		} else {
			regexPattern += regexp.QuoteMeta(string(pattern[i]))
		}
	}
	regexPattern += "$"

	matched, _ := regexp.MatchString(regexPattern, str)
	return matched
}

func (db *RelationalDB) validateUnique(schema *TableSchema, colName string, value interface{}) error {
	colIdx := schema.GetColumnIndex(colName)
	if colIdx == -1 {
		return nil
	}

	prefix := db.encoder.EncodeKey(schema.ID, 0)[:4]
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

		rowData, err := db.encoder.DecodeValue(val)
		if err != nil {
			iter.Next()
			continue
		}

		if colIdx < len(rowData) && compareValues(rowData[colIdx], value) {
			return fmt.Errorf("unique constraint violation: column '%s' value '%v' already exists", colName, value)
		}
		iter.Next()
	}

	return nil
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
			schema.InitColumnIndex()
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
		"queries":             atomic.LoadUint64(&db.queryCount),
		"query_cache_hits":    atomic.LoadUint64(&db.cacheHits),
		"result_cache_hits":   resultHits,
		"result_cache_misses": resultMisses,
		"tables":              len(db.catalog),
		"indexes":             len(db.indexes),
	}
}
