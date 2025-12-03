package storage

import "github.com/r-ef/moss"

type KVStore interface {
	Set(key, value []byte) error
	Get(key []byte) ([]byte, error)
	Delete(key []byte) error
	BatchSet(kvs [][2][]byte) error
	BatchWrite(sets [][2][]byte, deletes [][]byte) error
	NewPrefixIterator(prefix []byte) (moss.Iterator, error)
	NewIterator() (moss.Iterator, error)
	Close() error
	Flush() error
	Stats() map[string]interface{}
}
