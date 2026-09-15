package yogableve

import (
	"sync"

	store "github.com/blevesearch/upsidedown_store_api"
)

type snapshotEntry struct {
	key []byte
	val []byte
}

// Reader holds an isolated in-memory snapshot for Bleve.
type Reader struct {
	s       *Store
	entries []snapshotEntry
	values  map[string][]byte

	mu     sync.Mutex
	closed bool
}

var _ store.KVReader = (*Reader)(nil)

// Get returns an owned copy of the value associated with key.
func (r *Reader) Get(key []byte) ([]byte, error) {
	if err := r.checkOpen(); err != nil {
		return nil, err
	}
	val, found := r.values[string(key)]
	if !found {
		return nil, nil
	}
	if val == nil {
		return []byte{}, nil
	}
	return append([]byte(nil), val...), nil
}

// MultiGet retrieves multiple values in one call.
func (r *Reader) MultiGet(keys [][]byte) ([][]byte, error) {
	vals := make([][]byte, len(keys))
	for i, key := range keys {
		val, err := r.Get(key)
		if err != nil {
			return nil, err
		}
		vals[i] = val
	}
	return vals, nil
}

// PrefixIterator returns an iterator over all keys with prefix.
func (r *Reader) PrefixIterator(prefix []byte) store.KVIterator {
	return newIterator(r, prefix, nil, nil, true)
}

// RangeIterator returns an iterator over keys >= start and < end.
func (r *Reader) RangeIterator(start, end []byte) store.KVIterator {
	return newIterator(r, nil, start, end, false)
}

// Close releases the reader snapshot.
func (r *Reader) Close() error {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil
	}
	r.closed = true
	r.entries = nil
	r.values = nil
	r.mu.Unlock()

	return nil
}

func (r *Reader) checkOpen() error {
	if r == nil || r.s == nil {
		return ErrClosed
	}
	if err := r.s.checkOpen(); err != nil {
		return err
	}
	r.mu.Lock()
	closed := r.closed
	r.mu.Unlock()
	if closed {
		return ErrClosed
	}
	return nil
}
