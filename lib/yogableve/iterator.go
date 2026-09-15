package yogableve

import (
	"bytes"
	"fmt"
	"sort"

	store "github.com/blevesearch/upsidedown_store_api"
)

// Iterator adapts a YogaDB iterator to Bleve's KVIterator contract.
type Iterator struct {
	r       *Reader
	entries []snapshotEntry
	pos     int

	prefixMode bool
	prefix     []byte
	start      []byte
	end        []byte

	currentKey []byte
	currentVal []byte

	valid  bool
	err    error
	closed bool
}

var _ store.KVIterator = (*Iterator)(nil)

func newIterator(r *Reader, prefix, start, end []byte, prefixMode bool) *Iterator {
	if err := r.checkOpen(); err != nil {
		return &Iterator{r: r, err: err, closed: true}
	}
	rv := &Iterator{
		r:          r,
		entries:    r.entries,
		prefixMode: prefixMode,
		prefix:     append([]byte(nil), prefix...),
	}
	if start != nil {
		rv.start = append([]byte(nil), start...)
	}
	if end != nil {
		rv.end = append([]byte(nil), end...)
	}
	if prefixMode {
		rv.Seek(prefix)
	} else {
		rv.Seek(start)
	}
	return rv
}

// Seek positions the iterator at the first key >= key within its bounds.
func (it *Iterator) Seek(key []byte) {
	it.capture(func() {
		if it.closed {
			it.valid = false
			return
		}

		target := key
		if it.prefixMode {
			if bytes.Compare(target, it.prefix) < 0 {
				target = it.prefix
			}
		} else if it.start != nil && bytes.Compare(target, it.start) < 0 {
			target = it.start
		}

		it.pos = sort.Search(len(it.entries), func(i int) bool {
			return bytes.Compare(it.entries[i].key, target) >= 0
		})
		it.loadCurrent()
	})
}

// Next advances to the next key.
func (it *Iterator) Next() {
	it.capture(func() {
		if it.closed || !it.valid {
			it.valid = false
			return
		}
		it.pos++
		it.loadCurrent()
	})
}

// Key returns the current key. The bytes are valid until the next iterator call.
func (it *Iterator) Key() []byte {
	if it == nil || !it.valid {
		return nil
	}
	return it.currentKey
}

// Value returns the current value. The bytes are valid until the next iterator call.
func (it *Iterator) Value() []byte {
	if it == nil || !it.valid {
		return nil
	}
	return it.currentVal
}

// Valid reports whether the iterator is positioned at a valid key.
func (it *Iterator) Valid() bool {
	return it != nil && it.valid
}

// Current returns Key, Value, and Valid in one operation.
func (it *Iterator) Current() ([]byte, []byte, bool) {
	if it == nil || !it.valid {
		return nil, nil, false
	}
	return it.currentKey, it.currentVal, true
}

// Close releases iterator resources.
func (it *Iterator) Close() error {
	if it == nil {
		return nil
	}
	it.closed = true
	it.valid = false
	it.entries = nil
	return it.err
}

func (it *Iterator) loadCurrent() {
	it.valid = false
	if it.pos >= len(it.entries) {
		return
	}
	entry := it.entries[it.pos]
	raw := entry.key
	val := entry.val
	if it.outOfBounds(raw) {
		return
	}
	it.currentKey = append(it.currentKey[:0], raw...)
	it.currentVal = append(it.currentVal[:0], val...)
	it.valid = true
}

func (it *Iterator) outOfBounds(raw []byte) bool {
	if it.prefixMode {
		return !bytes.HasPrefix(raw, it.prefix)
	}
	if it.start != nil && bytes.Compare(raw, it.start) < 0 {
		return true
	}
	return it.end != nil && bytes.Compare(raw, it.end) >= 0
}

func (it *Iterator) capture(fn func()) {
	if it == nil || it.err != nil {
		if it != nil {
			it.valid = false
		}
		return
	}
	defer func() {
		if r := recover(); r != nil {
			it.err = fmt.Errorf("yogableve: iterator panic: %v", r)
			it.valid = false
		}
	}()
	fn()
}
