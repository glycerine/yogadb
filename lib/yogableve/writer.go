package yogableve

import (
	"fmt"

	store "github.com/blevesearch/upsidedown_store_api"
	"github.com/glycerine/yogadb"
)

// Writer mutates a YogaDB-backed Bleve store through atomic batches.
type Writer struct {
	s *Store
}

var _ store.KVWriter = (*Writer)(nil)

// NewBatch returns an empty mutation batch.
func (w *Writer) NewBatch() store.KVBatch {
	return newBatch(w.s, store.KVBatchOptions{})
}

// NewBatchEx returns a scratch byte buffer and a pre-sized batch.
func (w *Writer) NewBatchEx(options store.KVBatchOptions) ([]byte, store.KVBatch, error) {
	if options.TotalBytes < 0 {
		options.TotalBytes = 0
	}
	return make([]byte, options.TotalBytes), newBatch(w.s, options), nil
}

// ExecuteBatch applies a batch atomically.
func (w *Writer) ExecuteBatch(batch store.KVBatch) error {
	if w == nil || w.s == nil {
		return ErrNilDB
	}
	if err := w.s.checkOpen(); err != nil {
		return err
	}
	if w.s.readOnly {
		return ErrReadOnly
	}

	b, ok := batch.(*Batch)
	if !ok || b.constructor != w.s {
		return ErrWrongBatch
	}
	if b.closed {
		return ErrClosed
	}

	return w.s.db.Update(func(rw *yogadb.WriteTx) error {
		if len(b.mergeOrder) > 0 && w.s.mo == nil {
			return ErrMergeUnsupported
		}
		for _, mapKey := range b.mergeOrder {
			g := b.merges[mapKey]
			if g == nil {
				continue
			}
			oldVal, found, _, _, err := rw.Get(w.s.dbKey(g.key))
			if err != nil {
				return err
			}
			if !found {
				oldVal = nil
			}
			mergedVal, ok := w.s.mo.FullMerge(g.key, oldVal, g.operands)
			if !ok {
				return fmt.Errorf("yogableve: merge operator %q returned failure", w.s.mo.Name())
			}
			if _, err := rw.Put(w.s.dbKey(g.key), mergedVal, 0); err != nil {
				return err
			}
		}

		for _, op := range b.ops {
			if op.delete {
				if err := rw.Delete(w.s.dbKey(op.key)); err != nil {
					return err
				}
				continue
			}
			if _, err := rw.Put(w.s.dbKey(op.key), op.val, 0); err != nil {
				return err
			}
		}
		return nil
	})
}

// Close closes this writer.
func (w *Writer) Close() error {
	return nil
}
