package memstore

import (
	"github.com/couchbase/moss"
)

type Wrapper struct {
	*MemStore
}

func NewWrapper() *Wrapper {
	return &Wrapper{
		MemStore: New(),
	}
}

func (w *Wrapper) NewPrefixIterator(prefix []byte) (moss.Iterator, error) {
	return &MossIteratorAdapter{
		iter: w.MemStore.NewPrefixIterator(prefix),
	}, nil
}

func (w *Wrapper) NewIterator() (moss.Iterator, error) {
	return &MossIteratorAdapter{
		iter: &prefixToRegular{w.MemStore.NewIterator()},
	}, nil
}

func (w *Wrapper) BatchWrite(sets [][2][]byte, deletes [][]byte) error {
	if len(sets) > 0 {
		if err := w.MemStore.BatchSet(sets); err != nil {
			return err
		}
	}
	for _, key := range deletes {
		if err := w.MemStore.Delete(key); err != nil {
			return err
		}
	}
	return nil
}

type prefixToRegular struct {
	*Iterator
}

func (p *prefixToRegular) Next() bool {
	return p.Iterator.Next()
}

func (p *prefixToRegular) Current() ([]byte, []byte, error) {
	return p.Iterator.Current()
}

func (p *prefixToRegular) Close() error {
	return p.Iterator.Close()
}

type MossIteratorAdapter struct {
	iter interface {
		Next() bool
		Current() ([]byte, []byte, error)
		Close() error
	}
	started bool
}

func (m *MossIteratorAdapter) Close() error {
	return m.iter.Close()
}

func (m *MossIteratorAdapter) Next() error {
	if !m.iter.Next() {
		return moss.ErrIteratorDone
	}
	return nil
}

func (m *MossIteratorAdapter) Current() ([]byte, []byte, error) {
	return m.iter.Current()
}

func (m *MossIteratorAdapter) CurrentEx() (entryEx moss.EntryEx, key, val []byte, err error) {
	key, val, err = m.iter.Current()
	return moss.EntryEx{}, key, val, err
}

func (m *MossIteratorAdapter) Seek(key []byte) error {
	for {
		k, _, err := m.iter.Current()
		if err != nil {
			return err
		}
		if string(k) >= string(key) {
			return nil
		}
		if !m.iter.Next() {
			return moss.ErrIteratorDone
		}
	}
}

func (m *MossIteratorAdapter) SeekTo(key []byte) error {
	return m.Seek(key)
}

