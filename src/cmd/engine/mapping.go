package engine

import (
	"encoding/binary"
	"errors"
	"fmt"
	"unsafe"

	json "github.com/goccy/go-json"
)

type DataType byte

const (
	TypeNull   DataType = 0
	TypeInt    DataType = 1
	TypeString DataType = 2
	TypeJSON   DataType = 3
	TypeFloat  DataType = 4
	TypeBool   DataType = 5
)

var (
	nullBytes  = []byte{byte(TypeNull)}
	trueBytes  = []byte{byte(TypeBool), 1}
	falseBytes = []byte{byte(TypeBool), 0}
)

type TableSchema struct {
	ID            uint32            `json:"id"`
	Name          string            `json:"name"`
	Columns       []string          `json:"columns"`
	Types         []string          `json:"types"`
	PrimaryKey    string            `json:"primary_key,omitempty"`
	AutoIncrement bool              `json:"auto_increment,omitempty"`
	ForeignKeys   map[string]FKRef  `json:"foreign_keys,omitempty"`
	Constraints   ColumnConstraints `json:"constraints,omitempty"`

	colIndex map[string]int
}

type FKRef struct {
	Table  string `json:"table"`
	Column string `json:"column"`
}

type ColumnConstraints struct {
	NotNull  map[string]bool        `json:"not_null,omitempty"`
	Unique   map[string]bool        `json:"unique,omitempty"`
	Defaults map[string]interface{} `json:"defaults,omitempty"`
	Enums    map[string][]string    `json:"enums,omitempty"`
	Checks   map[string]string      `json:"checks,omitempty"`
	Lengths  map[string]int         `json:"lengths,omitempty"`
}

func (s *TableSchema) InitColumnIndex() {
	s.colIndex = make(map[string]int, len(s.Columns))
	for i, col := range s.Columns {
		s.colIndex[col] = i
	}
}

func (s *TableSchema) GetColumnIndex(name string) int {
	if idx, ok := s.colIndex[name]; ok {
		return idx
	}
	return -1
}

type Row struct {
	PrimaryKey uint64
	Data       []interface{}
}

type Encoder struct{}

func (e *Encoder) EncodeKey(tableID uint32, primaryKey uint64) []byte {
	buf := make([]byte, 12)
	binary.BigEndian.PutUint32(buf[0:4], tableID)
	binary.BigEndian.PutUint64(buf[4:12], primaryKey)
	return buf
}

func (e *Encoder) EncodeKeyTo(buf []byte, tableID uint32, primaryKey uint64) {
	binary.BigEndian.PutUint32(buf[0:4], tableID)
	binary.BigEndian.PutUint64(buf[4:12], primaryKey)
}

func (e *Encoder) DecodeKey(key []byte) (tableID uint32, primaryKey uint64) {
	if len(key) >= 12 {
		tableID = binary.BigEndian.Uint32(key[0:4])
		primaryKey = binary.BigEndian.Uint64(key[4:12])
	}
	return
}

func (e *Encoder) EncodeValue(row Row) ([]byte, error) {
	estimatedSize := len(row.Data) * 17
	buf := make([]byte, 0, estimatedSize)

	for _, col := range row.Data {
		encoded, err := e.encodeField(col)
		if err != nil {
			return nil, err
		}
		buf = append(buf, encoded...)
	}
	return buf, nil
}

func (e *Encoder) EncodeValueTo(buf *[]byte, row Row) error {
	for _, col := range row.Data {
		if err := e.encodeFieldTo(buf, col); err != nil {
			return err
		}
	}
	return nil
}

func (e *Encoder) encodeField(col interface{}) ([]byte, error) {
	if col == nil {
		return nullBytes, nil
	}

	switch v := col.(type) {
	case bool:
		if v {
			return trueBytes, nil
		}
		return falseBytes, nil

	case int:
		buf := make([]byte, 9)
		buf[0] = byte(TypeInt)
		binary.BigEndian.PutUint64(buf[1:], uint64(v))
		return buf, nil

	case int64:
		buf := make([]byte, 9)
		buf[0] = byte(TypeInt)
		binary.BigEndian.PutUint64(buf[1:], uint64(v))
		return buf, nil

	case float64:
		buf := make([]byte, 9)
		buf[0] = byte(TypeFloat)
		binary.BigEndian.PutUint64(buf[1:], *(*uint64)(unsafe.Pointer(&v)))
		return buf, nil

	case string:
		strLen := len(v)
		buf := make([]byte, 5+strLen)
		buf[0] = byte(TypeString)
		binary.BigEndian.PutUint32(buf[1:5], uint32(strLen))
		copy(buf[5:], v)
		return buf, nil

	case map[string]interface{}, []interface{}:
		jsonBytes, err := json.Marshal(v)
		if err != nil {
			return nil, err
		}
		buf := make([]byte, 5+len(jsonBytes))
		buf[0] = byte(TypeJSON)
		binary.BigEndian.PutUint32(buf[1:5], uint32(len(jsonBytes)))
		copy(buf[5:], jsonBytes)
		return buf, nil

	default:
		return nil, errors.New("unsupported data type")
	}
}

func (e *Encoder) encodeFieldTo(buf *[]byte, col interface{}) error {
	if col == nil {
		*buf = append(*buf, byte(TypeNull))
		return nil
	}

	switch v := col.(type) {
	case bool:
		if v {
			*buf = append(*buf, byte(TypeBool), 1)
		} else {
			*buf = append(*buf, byte(TypeBool), 0)
		}

	case int:
		*buf = append(*buf, byte(TypeInt))
		*buf = binary.BigEndian.AppendUint64(*buf, uint64(v))

	case int64:
		*buf = append(*buf, byte(TypeInt))
		*buf = binary.BigEndian.AppendUint64(*buf, uint64(v))

	case float64:
		*buf = append(*buf, byte(TypeFloat))
		*buf = binary.BigEndian.AppendUint64(*buf, *(*uint64)(unsafe.Pointer(&v)))

	case string:
		*buf = append(*buf, byte(TypeString))
		*buf = binary.BigEndian.AppendUint32(*buf, uint32(len(v)))
		*buf = append(*buf, v...)

	case map[string]interface{}, []interface{}:
		jsonBytes, err := json.Marshal(v)
		if err != nil {
			return err
		}
		*buf = append(*buf, byte(TypeJSON))
		*buf = binary.BigEndian.AppendUint32(*buf, uint32(len(jsonBytes)))
		*buf = append(*buf, jsonBytes...)

	default:
		return errors.New("unsupported data type")
	}
	return nil
}

func (e *Encoder) DecodeValue(data []byte) ([]interface{}, error) {
	if len(data) == 0 {
		return nil, nil
	}

	result := make([]interface{}, 0, 8)
	pos := 0

	for pos < len(data) {
		if pos >= len(data) {
			break
		}

		typeByte := DataType(data[pos])
		pos++

		switch typeByte {
		case TypeNull:
			result = append(result, nil)

		case TypeBool:
			if pos >= len(data) {
				return nil, errors.New("truncated bool value")
			}
			result = append(result, data[pos] == 1)
			pos++

		case TypeInt:
			if pos+8 > len(data) {
				return nil, errors.New("truncated int value")
			}
			val := int64(binary.BigEndian.Uint64(data[pos : pos+8]))
			result = append(result, int(val))
			pos += 8

		case TypeFloat:
			if pos+8 > len(data) {
				return nil, errors.New("truncated float value")
			}
			bits := binary.BigEndian.Uint64(data[pos : pos+8])
			result = append(result, *(*float64)(unsafe.Pointer(&bits)))
			pos += 8

		case TypeString:
			if pos+4 > len(data) {
				return nil, errors.New("truncated string length")
			}
			length := binary.BigEndian.Uint32(data[pos : pos+4])
			pos += 4
			if pos+int(length) > len(data) {
				return nil, errors.New("truncated string value")
			}
			str := string(data[pos : pos+int(length)])
			result = append(result, str)
			pos += int(length)

		case TypeJSON:
			if pos+4 > len(data) {
				return nil, errors.New("truncated json length")
			}
			length := binary.BigEndian.Uint32(data[pos : pos+4])
			pos += 4
			if pos+int(length) > len(data) {
				return nil, errors.New("truncated json value")
			}
			var obj interface{}
			if err := json.Unmarshal(data[pos:pos+int(length)], &obj); err != nil {
				return nil, err
			}
			result = append(result, obj)
			pos += int(length)

		default:
			return nil, fmt.Errorf("unknown data type %d at position %d", typeByte, pos-1)
		}
	}

	return result, nil
}

func (e *Encoder) DecodeValueFast(data []byte, fieldIndex int) (interface{}, error) {
	pos := 0
	currentField := 0

	for pos < len(data) {
		typeByte := DataType(data[pos])
		pos++

		var fieldLen int
		switch typeByte {
		case TypeNull:
			fieldLen = 0
		case TypeBool:
			fieldLen = 1
		case TypeInt, TypeFloat:
			fieldLen = 8
		case TypeString, TypeJSON:
			if pos+4 > len(data) {
				return nil, errors.New("truncated length")
			}
			fieldLen = int(binary.BigEndian.Uint32(data[pos:pos+4])) + 4
		default:
			return nil, fmt.Errorf("unknown type %d", typeByte)
		}

		if currentField == fieldIndex {
			switch typeByte {
			case TypeNull:
				return nil, nil
			case TypeBool:
				return data[pos] == 1, nil
			case TypeInt:
				return int(int64(binary.BigEndian.Uint64(data[pos : pos+8]))), nil
			case TypeFloat:
				bits := binary.BigEndian.Uint64(data[pos : pos+8])
				return *(*float64)(unsafe.Pointer(&bits)), nil
			case TypeString:
				length := binary.BigEndian.Uint32(data[pos : pos+4])
				return string(data[pos+4 : pos+4+int(length)]), nil
			case TypeJSON:
				length := binary.BigEndian.Uint32(data[pos : pos+4])
				var obj interface{}
				if err := json.Unmarshal(data[pos+4:pos+4+int(length)], &obj); err != nil {
					return nil, err
				}
				return obj, nil
			}
		}

		pos += fieldLen
		currentField++
	}

	return nil, fmt.Errorf("field index %d out of range", fieldIndex)
}
