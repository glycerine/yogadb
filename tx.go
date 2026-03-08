package yogadb

import (
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/tidwall/btree"
)

// ====================== Transactions ======================
//
// FlexDB supports serializable transactions via a single-writer mutex.
// Update transactions are serialized; View transactions happen when no
// writer is active, and many readers can read concurrently without blocking
// each other.
//
// Tx provides Commit and Cancel with first-wins semantics: whichever is
// called first takes effect, subsequent calls are no-ops (or return ErrTxDone).

const (
	txActive    int32 = 0
	txCommitted int32 = 1
	txCancelled int32 = 2
)

var (
	// ErrTxDone is returned when Commit is called on an already-cancelled transaction.
	ErrTxDone = errors.New("flexdb: transaction already finished")
	// ErrTxNotWritable is returned when Put or Delete is called on a read-only transaction.
	ErrTxNotWritable = errors.New("flexdb: cannot write in a read-only transaction")
)

// Tx represents a database transaction.
type Tx struct {
	db       *FlexDB
	writable bool
	state    int32 // atomic: txActive -> txCommitted or txCancelled

	// Read snapshot (COW btree copies + ffMu.RLock)
	mtSnaps [2]*btree.BTreeG[*KV]

	// Write buffer (writable only) — local btree, applied at Commit
	wb *btree.BTreeG[*KV]
}

func (db *FlexDB) begin(writable bool) *Tx {
	tx := &Tx{
		db:       db,
		writable: writable,
	}

	if writable {
		db.topMutRW.Lock()
	} else {
		db.topMutRW.RLock()
	}

	// Snapshot memtables via COW copies
	active := db.activeMT
	inactive := 1 - active
	tx.mtSnaps[0] = db.memtables[active].bt.Copy()
	tx.mtSnaps[1] = db.memtables[inactive].bt.Copy()

	if writable {
		tx.wb = btree.NewBTreeG[*KV](kvLess)
	}

	return tx
}

// Update executes fn within a serializable read-write transaction.
// Only one Update transaction runs at a time (serialized by db.topMutRW).
// If fn returns without calling Commit or Cancel, the transaction is auto-cancelled.
func (db *FlexDB) Update(fn func(tx *Tx) error) error {

	tx := db.begin(true)
	err := fn(tx)

	// Auto-cancel if still active
	if atomic.LoadInt32(&tx.state) == txActive {
		tx.Cancel()
	}
	return err
}

// View executes fn within a read-only transaction.
// Multiple View transactions can run concurrently.
// The transaction is auto-released when fn returns.
func (db *FlexDB) View(fn func(tx *Tx) error) error {
	tx := db.begin(false)
	err := fn(tx)

	// Auto-release if still active
	if atomic.LoadInt32(&tx.state) == txActive {
		tx.release()
		atomic.StoreInt32(&tx.state, txCancelled)
	}
	return err
}

// Get retrieves the value for key within the transaction's snapshot.
// Returns nil, false if not found or if the key is a tombstone.
func (tx *Tx) Get(key []byte) ([]byte, bool) {
	// Check write buffer first (highest priority for writable Tx)
	if tx.writable && tx.wb != nil {
		kv, ok := tx.wb.Get(&KV{Key: key})
		if ok {
			if kv.isTombstone() {
				return nil, false // tombstone in write buffer
			}
			val := make([]byte, len(kv.Value))
			copy(val, kv.Value)
			return val, true
		}
	}

	// Check memtable snapshots
	for i := 0; i < 2; i++ {
		if tx.mtSnaps[i] != nil {
			kv, ok := tx.mtSnaps[i].Get(&KV{Key: key})
			if ok {
				if kv.isTombstone() {
					return nil, false // tombstone
				}
				val, err := tx.db.resolveVPtr(kv)
				if err != nil {
					return nil, false
				}
				out := make([]byte, len(val))
				copy(out, val)
				return out, true
			}
		}
	}

	// Check FlexSpace
	return tx.db.getPassthrough(key)
}

// Put writes a key-value pair into the transaction's write buffer.
// The write is only applied to the database when Commit is called.
// Returns ErrTxNotWritable for read-only transactions.
func (tx *Tx) Put(key, value []byte) error {
	if !tx.writable {
		return ErrTxNotWritable
	}
	if atomic.LoadInt32(&tx.state) != txActive {
		return ErrTxDone
	}
	//if len(key)+len(value)+16 >= MaxKeySize {
	if len(key) > MaxKeySize {
		return fmt.Errorf("flexdb: Key too large (max %d bytes)", MaxKeySize)
	}
	tx.wb.Set(&KV{Key: dupBytes(key), Value: dupBytes(value)})
	return nil
}

// Delete marks a key for deletion in the transaction's write buffer.
// The deletion is only applied to the database when Commit is called.
// Returns ErrTxNotWritable for read-only transactions.
func (tx *Tx) Delete(key []byte) error {
	if !tx.writable {
		return ErrTxNotWritable
	}
	if atomic.LoadInt32(&tx.state) != txActive {
		return ErrTxDone
	}
	tx.wb.Set(&KV{Key: dupBytes(key), Value: nil}) // tombstone, because Value==nil && HasVPtr == 0
	return nil
}

// Commit applies all buffered writes to the database.
// Uses first-wins semantics: if Cancel was called first, returns ErrTxDone.
// Double Commit returns nil (idempotent).
func (tx *Tx) Commit() error {
	if !atomic.CompareAndSwapInt32(&tx.state, txActive, txCommitted) {
		if atomic.LoadInt32(&tx.state) == txCancelled {
			return ErrTxDone
		}
		return nil // already committed — idempotent
	}

	if !tx.writable {
		tx.release()
		return nil
	}
	defer tx.release()

	// now we still hold db.topMutRW.Lock() writer's lock.

	// Save write buffer before release (release nils it).
	wb := tx.wb

	// Apply write buffer to database
	if wb != nil {
		wb.Ascend(&KV{}, func(item *KV) bool {
			tx.db.writeLockHeldPut(item.Key, item.Value)
			return true
		})
	}

	return nil
}

// Cancel discards all buffered writes.
// Uses first-wins semantics: if Commit was called first, returns nil.
// Double Cancel returns nil (idempotent).
func (tx *Tx) Cancel() error {
	if !atomic.CompareAndSwapInt32(&tx.state, txActive, txCancelled) {
		return nil // already committed or cancelled — idempotent
	}
	tx.release()
	return nil
}

func (tx *Tx) release() {
	if tx.writable {
		tx.db.topMutRW.Unlock()
	} else {
		tx.db.topMutRW.RUnlock()
	}
	tx.mtSnaps[0] = nil
	tx.mtSnaps[1] = nil
	tx.wb = nil
}
