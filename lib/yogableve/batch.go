package yogableve

import store "github.com/blevesearch/upsidedown_store_api"

type batchOp struct {
	key    []byte
	val    []byte
	delete bool
}

type mergeGroup struct {
	key      []byte
	operands [][]byte
}

// Batch buffers a Bleve mutation batch. It owns all keys and values passed to it.
type Batch struct {
	ops         []batchOp
	merges      map[string]*mergeGroup
	mergeOrder  []string
	closed      bool
	constructor *Store
}

var _ store.KVBatch = (*Batch)(nil)

func newBatch(s *Store, options store.KVBatchOptions) *Batch {
	b := &Batch{
		ops:         make([]batchOp, 0, options.NumSets+options.NumDeletes),
		constructor: s,
	}
	if options.NumMerges > 0 {
		b.merges = make(map[string]*mergeGroup, options.NumMerges)
		b.mergeOrder = make([]string, 0, options.NumMerges)
	}
	return b
}

// Set updates the key with the specified value.
func (b *Batch) Set(key, val []byte) {
	if b == nil || b.closed {
		return
	}
	b.ops = append(b.ops, batchOp{
		key: append([]byte(nil), key...),
		val: append([]byte(nil), val...),
	})
}

// Delete removes the specified key.
func (b *Batch) Delete(key []byte) {
	if b == nil || b.closed {
		return
	}
	b.ops = append(b.ops, batchOp{
		key:    append([]byte(nil), key...),
		delete: true,
	})
}

// Merge appends a merge operand for key.
func (b *Batch) Merge(key, val []byte) {
	if b == nil || b.closed {
		return
	}
	mapKey := string(key)
	if b.merges == nil {
		b.merges = make(map[string]*mergeGroup)
	}
	g := b.merges[mapKey]
	if g == nil {
		g = &mergeGroup{key: append([]byte(nil), key...)}
		b.merges[mapKey] = g
		b.mergeOrder = append(b.mergeOrder, mapKey)
	}
	g.operands = append(g.operands, append([]byte(nil), val...))
}

// Reset clears the batch for reuse.
func (b *Batch) Reset() {
	if b == nil || b.closed {
		return
	}
	b.ops = b.ops[:0]
	clear(b.merges)
	b.mergeOrder = b.mergeOrder[:0]
}

// Close frees this batch.
func (b *Batch) Close() error {
	if b == nil {
		return nil
	}
	b.closed = true
	b.ops = nil
	b.merges = nil
	b.mergeOrder = nil
	return nil
}
