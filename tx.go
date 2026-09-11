package yogadb

import (
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"time"
)

// ReadOnlyDB provides read-only access to the database within a View
// transaction. Methods must not be used after the callback returns.
// ReadOnlyDB is implemented by ReadOnlyTx.
//
// To avoid indirect call overhead, this interface is
// not actually used in the View API.
// It is nice for documention purposes though; to get an
// overview of the available methods.
type ReadOnlyDB interface {
	Get(key string) ([]byte, bool, uint64, HLC, error)
	GetKV(key string) (kv *KVcloser, err error)
	Find(smod SearchModifier, key string) (kvc *KVcloser, exact bool, err error)
	FindIt(smod SearchModifier, key string) (kvc *KVcloser, exact bool, err error, it *Iter)
	FetchLarge(kv *KV) (val []byte, vtyp uint64, hlc HLC, err error)
	NewIter() *Iter
	Ascend(pivot string, callback func(key string, value []byte, vtyp uint64, hlc HLC) bool)
	Descend(pivot string, callback func(key string, value []byte, vtyp uint64, hlc HLC) bool)
	AscendRange(greaterOrEqual, lessThan string, callback func(key string, value []byte, vtyp uint64, hlc HLC) bool)
	DescendRange(lessOrEqual, greaterThan string, callback func(key string, value []byte, vtyp uint64, hlc HLC) bool)
	Len() int64
	LenBigSmall() (big, small int64)
}

// WritableDB extends ReadOnlyDB with mutation methods. Passed to
// Update transaction callbacks. Writes are applied immediately
// to the database (no buffering). Since there is only ever
// a single writer at a time, and no concurrent readers, there
// is no point in waiting to apply each action, and "reading
// your own writes" is often desired/expected/required.
// WritableDB is implemented by WriteTx.
//
// WriteTx writes are bracketed in FLEXDB.MEMWAL by lazy BEGIN/COMMIT records
// for crash recovery: recovery replays all records in a committed group, and
// discards a group whose COMMIT was not present. Writes are applied
// immediately in memory so readers inside the transaction see their own
// writes, but rollbackable writes become durable only after Commit returns.
//
// To avoid indirect call overhead, this interface is
// not actually used in the Update API.
// It is nice for documention purposes though; to get an
// overview of the available methods.
type WritableDB interface {
	ReadOnlyDB
	Put(key string, value []byte, vtyp uint64) (HLC, error)
	Delete(key string) error
	DeleteRange(includeLarge bool, begKey, endKey string, begInclusive, endInclusive bool) (n int64, allGone bool, err error)
	Clear(includeLarge bool) (allGone bool, err error)
	Merge(key string, fn func(oldVal []byte, exists bool, oldVtyp uint64) (newVal []byte, write bool, doDelete bool, newVtyp uint64)) error
	Commit() error
	Rollback() error
}

// txBase tracks iterators created within a transaction and auto-closes
// them when the transaction ends.
type txBase struct {
	db    *FlexDB
	iters []*Iter
}

func (tx *txBase) newIter() *Iter {
	it := &Iter{db: tx.db}
	tx.iters = append(tx.iters, it)
	return it
}

func (tx *txBase) closeAll() {
	for _, it := range tx.iters {
		if !it.closed {
			it.releaseIterState()
			it.valid = false
			it.closed = true
		}
	}
	tx.iters = tx.iters[:0]
}

// txFind implements Find for both WriteTx and ReadOnlyTx.
// Caller must hold topMutRW (read or write lock).
func txFind(tx *txBase, smod SearchModifier, key string) (kvc *KVcloser, exact bool, err error) {
	it := tx.newIter()
	lazyLarge := (smod&LAZY_LARGE != 0)
	lazySmall := (smod&LAZY_SMALL != 0)
	skipValues := (smod&SKIP_VALUES != 0)
	if lazyLarge || skipValues {
		it.lazyLarge = true
		smod &^= LAZY_LARGE
	}
	if skipValues {
		it.skipValues = true
		smod &^= SKIP_VALUES
	}
	smod &^= LAZY_SMALL

	var found bool
	found, exact = findSeekIter(it, smod, key)
	if found {
		zc := findBuildKV(it)
		resultKey := strings.Clone(zc.Key)
		vtyp := zc.Vtyp()
		valueFromCache := it.valueNeedsCopy

		it.Close()

		if skipValues {
			kvc = &KVcloser{KV: KV{Key: resultKey, Hlc: zc.Hlc}, db: tx.db, Vtyp: vtyp}
			return
		}

		// LAZY_SMALL path: try zero-copy via cache pinning.
		if lazySmall && valueFromCache && !zc.HasVPtr() && len(zc.Value) > 0 {
			kvc, err = tx.db.findBuildKVZeroCopy(resultKey)
			if err != nil {
				return
			}
			if kvc != nil {
				kvc.Key = resultKey
				kvc.Vtyp = vtyp
				return
			}
		}

		// Standard path: copy inline value.
		owned := KV{Vptr: zc.Vptr, Hlc: zc.Hlc}
		owned.Key = resultKey
		if !zc.HasVPtr() && len(zc.Value) > 0 {
			owned.Value = append([]byte{}, zc.Value...)
		}
		kvc = &KVcloser{KV: owned, db: tx.db, Vtyp: vtyp}

		// Auto-fetch large value unless LAZY_LARGE was requested.
		if !lazyLarge && kvc.HasVPtr() {
			val, _, _, fetchErr := tx.db.resolveVPtr(kvc.KV)
			if fetchErr != nil {
				kvc = nil
				err = fetchErr
			} else {
				kvc.Value = val
			}
		}
		return
	}
	it.Close()
	return
}

// txFindIt implements FindIt for both WriteTx and ReadOnlyTx.
// Returns a *KVcloser for the initial result (independent of the iterator)
// and an iterator positioned at the result for continued scanning.
// Caller must hold topMutRW (read or write lock).
func txFindIt(tx *txBase, smod SearchModifier, key string) (kvc *KVcloser, exact bool, err error, it *Iter) {
	it = tx.newIter()
	lazyLarge := (smod&LAZY_LARGE != 0)
	lazySmall := (smod&LAZY_SMALL != 0)
	skipValues := (smod&SKIP_VALUES != 0)
	if lazyLarge || skipValues {
		it.lazyLarge = true
		smod &^= LAZY_LARGE
	}
	if skipValues {
		it.skipValues = true
		smod &^= SKIP_VALUES
	}
	smod &^= LAZY_SMALL

	var found bool
	found, exact = findSeekIter(it, smod, key)
	if !found {
		return
	}
	zc := findBuildKV(it)
	resultKey := strings.Clone(zc.Key)
	vtyp := zc.Vtyp()
	valueFromCache := it.valueNeedsCopy

	if skipValues {
		kvc = &KVcloser{KV: KV{Key: resultKey, Hlc: zc.Hlc}, db: tx.db, Vtyp: vtyp}
		return
	}

	// LAZY_SMALL path: try zero-copy via cache pinning.
	if lazySmall && valueFromCache && !zc.HasVPtr() && len(zc.Value) > 0 {
		kvc, err = tx.db.findBuildKVZeroCopy(resultKey)
		if err != nil {
			return
		}
		if kvc != nil {
			kvc.Key = resultKey
			kvc.Vtyp = vtyp
			return
		}
	}

	// Standard path: copy inline value.
	owned := KV{Vptr: zc.Vptr, Hlc: zc.Hlc}
	owned.Key = resultKey
	if !zc.HasVPtr() && len(zc.Value) > 0 {
		owned.Value = append([]byte{}, zc.Value...)
	}
	kvc = &KVcloser{KV: owned, db: tx.db, Vtyp: vtyp}

	// Auto-fetch large value unless LAZY_LARGE was requested.
	if !lazyLarge && kvc.HasVPtr() {
		val, _, _, fetchErr := tx.db.resolveVPtr(kvc.KV)
		if fetchErr != nil {
			kvc = nil
			err = fetchErr
		} else {
			kvc.Value = val
		}
	}
	return
}

// ========== WriteTx (implements WritableDB) ==========

// WriteTx extends ReadTx with mutation methods. Passed to Update transaction
// callbacks. Writes are applied immediately to the active memtable so reads in
// the transaction see their own writes. Commit persists those writes; Rollback
// discards them from the memtable and MEMWAL.
type WriteTx struct {
	txBase
	walTxnBegun     bool
	done            bool
	managedByUpdate bool
	beginLiveKeys   int64
	beginBigKeys    int64
	beginSmallKeys  int64
	beginAutoDel    int64
	beginAutoVLOG   int64
}

var _ WritableDB = (*WriteTx)(nil)

var ErrWriteTxClosed = errors.New("yogadb: write transaction already committed or rolled back")

func (tx *WriteTx) checkOpen() error {
	if tx.done {
		return ErrWriteTxClosed
	}
	return nil
}

func (tx *WriteTx) ensureWalTxn() error {
	if err := tx.checkOpen(); err != nil {
		return err
	}
	if !tx.walTxnBegun {
		if err := tx.db.mt.logAppendWalRecordType(MEMWAL_BEGIN_TXN); err != nil {
			return err
		}
		tx.walTxnBegun = true
	}
	return nil
}

func (tx *WriteTx) commitWalTxn() error {
	if err := tx.checkOpen(); err != nil {
		return err
	}
	if tx.walTxnBegun {
		if err := tx.db.mt.logAppendWalRecordType(MEMWAL_COMMIT_TXN); err != nil {
			return err
		}
		tx.walTxnBegun = false
	}
	return nil
}

func (tx *WriteTx) finish() {
	tx.done = true
	tx.closeAll()
	if !tx.managedByUpdate {
		tx.db.topMutRW.Unlock()
	}
}

// Rollback discards all writes made in this WriteTx. It is safe to defer:
// if Commit already won, Rollback is a no-op.
func (tx *WriteTx) Rollback() error {
	if tx.done {
		return nil
	}
	err := tx.rollbackOpen()
	tx.finish()
	return err
}

func (tx *WriteTx) rollbackOpen() error {
	db := tx.db
	db.mt.ks.clear()
	db.mt.empty = true
	db.mt.size = 0
	db.liveKeys = tx.beginLiveKeys
	db.liveBigKeys = tx.beginBigKeys
	db.liveSmallKeys = tx.beginSmallKeys
	atomic.StoreInt64(&db.autoVacuumDeletedBytes, tx.beginAutoDel)
	atomic.StoreInt64(&db.autoVacuumVLOGDeletedBytes, tx.beginAutoVLOG)
	tx.walTxnBegun = false
	db.flushSeq++

	ts := uint64(time.Now().UnixNano())
	if err := db.mt.logTruncateWithVersion(ts, db.ff.tree.PersistentVersion); err != nil {
		return fmt.Errorf("flexdb: rollback truncate memwal: %w", err)
	}
	return nil
}

func (tx *WriteTx) resetRollbackBaseline() {
	tx.beginLiveKeys = tx.db.liveKeys
	tx.beginBigKeys = tx.db.liveBigKeys
	tx.beginSmallKeys = tx.db.liveSmallKeys
	tx.beginAutoDel = atomic.LoadInt64(&tx.db.autoVacuumDeletedBytes)
	tx.beginAutoVLOG = atomic.LoadInt64(&tx.db.autoVacuumVLOGDeletedBytes)
}

// Get retrieves the value for key. Returns (nil, false, nil) if not found
// or deleted. The returned []byte is a copy, safe to retain.
func (tx *WriteTx) Get(key string) (value []byte, found bool, vtyp uint64, hlc HLC, err error) {
	if err := tx.checkOpen(); err != nil {
		return nil, false, 0, 0, err
	}
	tx.db.requireReadsAllowed()
	return tx.db.someLockHeldGet(key)
}

// GetKV is equivalent to tx.Find(Exact, key).
func (tx *WriteTx) GetKV(key string) (kv *KVcloser, err error) {
	if err := tx.checkOpen(); err != nil {
		return nil, err
	}
	tx.db.requireReadsAllowed()
	kv, _, err = tx.Find(Exact, key)
	return
}

// Put writes key -> value. len(value) == 0 is fine, if desired.
// Call Delete instead of Put to delete a key and any associated value.
//
// Values of any size are accepted. Values > vlogInlineThreshold (64 bytes) are
// stored in the VLOG file; smaller values are stored inline in
// the FLEXSPACE.KV.SLOT_BLOCKS file with the keys.
//
// Large values are written exactly once: to the VLOG. The WAL stores only
// the VPtr (16 bytes), not the full value.
//
// Puts are not durably committed until Commit returns successfully.
func (tx *WriteTx) Put(key string, value []byte, vtyp uint64) (HLC, error) {
	if err := tx.checkOpen(); err != nil {
		return 0, err
	}
	return tx.db.writeLockHeldPutWithHook(tx.ensureWalTxn, key, value, vtyp, false)
}

// Delete removes key from the store.
func (tx *WriteTx) Delete(key string) error {
	if err := tx.checkOpen(); err != nil {
		return err
	}
	_, err := tx.db.writeLockHeldPutWithHook(tx.ensureWalTxn, key, nil, 0, true)
	return err
}

// Commit durably commits this WriteTx. It is terminal: if Rollback already won,
// Commit is a no-op.
func (tx *WriteTx) Commit() (err error) {
	if tx.done {
		return nil
	}
	defer func() {
		tx.done = true
		tx.closeAll()
		if tx.managedByUpdate {
			return
		}
		autoVacuumHandoff := false
		if err == nil {
			autoVacuumHandoff = tx.db.maybeStartAutoVacuumLocked()
		}
		if !autoVacuumHandoff {
			tx.db.topMutRW.Unlock()
		}
	}()
	if err := tx.commitWalTxn(); err != nil {
		return errors.Join(err, tx.rollbackOpen())
	}
	if err := tx.db.writeLockHeldSync(); err != nil {
		return err
	}
	return nil
}

// Find seeks to a key relative to the given key per smod (Exact, GTE, GT, LTE, LT)
// and returns a KVcloser. Getting back a nil *KVcloser means not found.
// The returned bool 'exact' is true when the
// returned key equals the query. The smod supports bitwise OR-ing
// with the LAZY_SMALL, LAZY_LARGE, and LAZY flags to control
// value flow. e.g. LAZY allows near-zero-copy full-table key-only scans.
//
// Warning: the user must call Close() on the kvc *KVcloser when done copying any
// value out, or else memory and resource leaks will ensue.
func (tx *WriteTx) Find(smod SearchModifier, key string) (kvc *KVcloser, exact bool, err error) {
	if err := tx.checkOpen(); err != nil {
		return nil, false, err
	}
	tx.db.requireReadsAllowed()
	kvc, exact, err = txFind(&tx.txBase, smod, key)
	return
}

// FindIt is like Find but also returns an iterator positioned at the result.
// The returned kvc *KVcloser is independent of the iterator - the user must
// call kvc.Close() when done with the initial result, and use the iterator
// for continued scanning. The iterator is auto-closed when the transaction ends.
func (tx *WriteTx) FindIt(smod SearchModifier, key string) (kvc *KVcloser, exact bool, err error, it *Iter) {
	if err := tx.checkOpen(); err != nil {
		return nil, false, err, nil
	}
	tx.db.requireReadsAllowed()
	kvc, exact, err, it = txFindIt(&tx.txBase, smod, key)
	return
}

// FetchLarge retrieves the full value for a KV. For VLOG-stored
// values it reads from disk; for inline values it returns kv.Value directly.
func (tx *WriteTx) FetchLarge(kv *KV) (val []byte, vtyp uint64, hlc HLC, err error) {
	if err := tx.checkOpen(); err != nil {
		return nil, 0, 0, err
	}
	tx.db.requireReadsAllowed()
	return tx.db.lockHeldFetchLarge(kv)
}

// NewIter returns a new iterator over the database. It is
// automatically closed when the transaction ends. It is
// legal to Close it sooner if you want to release resources
// early.
func (tx *WriteTx) NewIter() *Iter {
	if err := tx.checkOpen(); err != nil {
		panic(err)
	}
	tx.db.requireReadsAllowed()
	return tx.newIter()
}

// Len returns the total number of live keys in the database.
func (tx *WriteTx) Len() int64 {
	if err := tx.checkOpen(); err != nil {
		panic(err)
	}
	tx.db.requireReadsAllowed()
	return tx.db.liveKeys
}

// LenBigSmall returns live key counts partitioned by storage (VLOG vs inline).
func (tx *WriteTx) LenBigSmall() (big, small int64) {
	if err := tx.checkOpen(); err != nil {
		panic(err)
	}
	tx.db.requireReadsAllowed()
	return tx.db.liveBigKeys, tx.db.liveSmallKeys
}

// DeleteRange deletes keys in the range [begKey, endKey] with configurable
// inclusivity. When includeLarge is false, VLOG-stored keys are skipped.
// If allGone is true, the fast delete-all path reinitialized the database
// immediately and cannot be rolled back; previously obtained iterators and KV
// references are invalidated.
//
// Transactional caveat: when allGone is true inside a WriteTx, DeleteRange
// immediately applies this destructive operation and makes the resulting
// database state the new rollback baseline for the same WriteTx. Later writes
// in the transaction are allowed and behave as if a fresh WriteTx had just
// started: Commit keeps them, while Rollback discards only those later writes.
// The allGone delete itself is not rolled back.
func (tx *WriteTx) DeleteRange(includeLarge bool, begKey, endKey string, begInclusive, endInclusive bool) (n int64, allGone bool, err error) {
	if err := tx.checkOpen(); err != nil {
		return 0, false, err
	}
	n, allGone, err = tx.db.writeLockHeldDeleteRangeWithHook(tx.ensureWalTxn, includeLarge, begKey, endKey, begInclusive, endInclusive)
	if allGone && err == nil {
		tx.walTxnBegun = false
		tx.resetRollbackBaseline()
	}
	return
}

// Clear deletes all keys. When includeLarge is false, only inline-value
// keys are deleted; VLOG-stored keys survive. When allGone is true,
// the database was reinitialized and all previously obtained iterators
// and KV references are invalidated. The fast allGone path rewrites database
// files immediately and cannot be rolled back; Rollback can only discard later
// writes in the same WriteTx.
//
// Transactional caveat: when allGone is true inside a WriteTx, Clear
// immediately applies this destructive operation and makes the resulting
// database state the new rollback baseline for the same WriteTx. Later writes
// in the transaction are allowed and behave as if a fresh WriteTx had just
// started: Commit keeps them, while Rollback discards only those later writes.
// The allGone clear itself is not rolled back.
func (tx *WriteTx) Clear(includeLarge bool) (allGone bool, err error) {
	if err := tx.checkOpen(); err != nil {
		return false, err
	}
	allGone, err = tx.db.writeLockHeldClearWithHook(tx.ensureWalTxn, includeLarge)
	if allGone && err == nil {
		tx.walTxnBegun = false
		tx.resetRollbackBaseline()
	}
	return
}

// Merge performs an atomic read-modify-write on key. The callback
// function is always called: when the key exists,
// oldVal is its current value and exists=true;
// when the key is absent or deleted, oldVal=nil and
// exists=false (allowing conditional creation). Return write=false
// to skip the write, or doDelete=true to delete the key.
//
// This mirrors C's flexdb_merge: it looks up the old value across all
// layers (active memtable, inactive memtable, FlexSpace), applies the
// user function, and writes the result atomically.
//
// Q: When should the callback return doWrite=false, doDelete=false?
// A: This means "do nothing." The main scenarios:
//
//  1. Conditional creation: The callback inspects exists and decides
//     not to create the key. E.g., "only increment if the key
//     already exists" - if exists=false, return a bare return (all zeros = no-op).
//
//  2. Conditional update: The callback inspects the old value and
//     decides no change is needed. E.g., "set to X only if current
//     value isn't already X."
//
//  3. Read-only peek: The callback just wants to see the current
//     value (though Get is simpler for that).
//
// .
func (tx *WriteTx) Merge(key string, callback func(oldVal []byte, exists bool, oldVtyp uint64) (newVal []byte, write bool, doDelete bool, newVtype uint64)) error {
	if err := tx.checkOpen(); err != nil {
		return err
	}
	return tx.db.writeLockHeldMergeWithHook(tx.ensureWalTxn, key, callback)
}

// Ascend iterates keys >= pivot in ascending order until callback returns false.
// Use pivot="" to start from the first key.
func (tx *WriteTx) Ascend(pivot string, callback func(key string, value []byte, vtyp uint64, hlc HLC) bool) {
	if err := tx.checkOpen(); err != nil {
		panic(err)
	}
	tx.db.requireReadsAllowed()
	it := tx.newIter()
	defer it.Close()
	it.Seek(pivot)
	for it.Valid() {
		if !callback(it.Key(), it.iterResolvedValue(), it.Vtyp(), it.Hlc()) {
			return
		}
		it.Next()
	}
}

// Descend iterates keys <= pivot in descending order until callback returns false.
// Use pivot="" to start from the last key.
func (tx *WriteTx) Descend(pivot string, callback func(key string, value []byte, vtyp uint64, hlc HLC) bool) {
	if err := tx.checkOpen(); err != nil {
		panic(err)
	}
	tx.db.requireReadsAllowed()
	it := tx.newIter()
	defer it.Close()
	if pivot == "" {
		it.SeekLast()
	} else {
		it.seekLE(pivot, false)
	}
	for it.Valid() {
		if !callback(it.Key(), it.iterResolvedValue(), it.Vtyp(), it.Hlc()) {
			return
		}
		it.Prev()
	}
}

// AscendRange iterates keys in [greaterOrEqual, lessThan) in ascending order.
// Use "" for either bound to leave it open.
func (tx *WriteTx) AscendRange(greaterOrEqual, lessThan string, callback func(key string, value []byte, vtyp uint64, hlc HLC) bool) {
	if err := tx.checkOpen(); err != nil {
		panic(err)
	}
	tx.db.requireReadsAllowed()
	it := tx.newIter()
	defer it.Close()
	it.Seek(greaterOrEqual)
	for it.Valid() {
		if lessThan != "" && it.Key() >= lessThan {
			return
		}
		if !callback(it.Key(), it.iterResolvedValue(), it.Vtyp(), it.Hlc()) {
			return
		}
		it.Next()
	}
}

// DescendRange iterates keys in (greaterThan, lessOrEqual] in descending order.
// Use "" for either bound to leave it open.
func (tx *WriteTx) DescendRange(lessOrEqual, greaterThan string, callback func(key string, value []byte, vtyp uint64, hlc HLC) bool) {
	if err := tx.checkOpen(); err != nil {
		panic(err)
	}
	tx.db.requireReadsAllowed()
	it := tx.newIter()
	defer it.Close()
	if lessOrEqual == "" {
		it.SeekLast()
	} else {
		it.seekLE(lessOrEqual, false)
	}
	for it.Valid() {
		if greaterThan != "" && it.Key() <= greaterThan {
			return
		}
		if !callback(it.Key(), it.iterResolvedValue(), it.Vtyp(), it.Hlc()) {
			return
		}
		it.Prev()
	}
}

// ====================== ReadOnlyTx (implements ReadOnlyDB) ======================

var _ ReadOnlyDB = (*ReadOnlyTx)(nil)

// ReadOnlyTx provides read-only access to the database within a View
// transaction. Methods must not be used after the callback returns.
type ReadOnlyTx struct{ txBase }

// Get retrieves the value for key. Returns (nil, false, nil) if not found
// or deleted. The returned []byte is a copy, safe to retain.
func (roTx *ReadOnlyTx) Get(key string) (value []byte, found bool, vtyp uint64, hlc HLC, err error) {
	return roTx.db.someLockHeldGet(key)
}

// GetKV is equivalent to roTx.Find(Exact, key).
func (roTx *ReadOnlyTx) GetKV(key string) (kv *KVcloser, err error) {
	kv, _, err = roTx.Find(Exact, key)
	return
}

// Find seeks to a key relative to the given key per smod (Exact, GTE, GT, LTE, LT)
// and returns a KVcloser. nil KVcloser means not found. exact is true when the
// returned key equals the query. Supports LAZY_SMALL, LAZY_LARGE, and LAZY flags.
func (roTx *ReadOnlyTx) Find(smod SearchModifier, key string) (kvc *KVcloser, exact bool, err error) {
	kvc, exact, err = txFind(&roTx.txBase, smod, key)
	return
}

// FindIt is like Find but also returns an iterator positioned at the result.
// The returned KVcloser is independent of the iterator - the user must
// call kvc.Close() when done with the initial result, and use the iterator
// for continued scanning. The iterator is auto-closed when the transaction ends.
func (roTx *ReadOnlyTx) FindIt(smod SearchModifier, key string) (kvc *KVcloser, exact bool, err error, it *Iter) {
	kvc, exact, err, it = txFindIt(&roTx.txBase, smod, key)
	return
}

// FetchLarge retrieves the full value for a KV. For VLOG-stored
// values it reads from disk; for inline values it returns kv.Value directly.
func (roTx *ReadOnlyTx) FetchLarge(kv *KV) (val []byte, vtyp uint64, hlc HLC, err error) {
	return roTx.db.lockHeldFetchLarge(kv)
}

// NewIter returns a new iterator over the database. It is
// automatically closed when the transaction ends. It is
// legal to Close it sooner if you want to release resources
// early.
func (roTx *ReadOnlyTx) NewIter() *Iter {
	return roTx.newIter()
}

// Len returns the total number of live keys in the database.
// See also LenBigSmall to get the count partitioned by
// the size class (small inline, large in the VLOG).
func (roTx *ReadOnlyTx) Len() int64 {
	return roTx.db.liveKeys
}

// LenBigSmall returns live key counts partitioned by size
// class. The returned count big counts the keys in VLOG,
// while small gives the number of keys with inline values.
// See also Len to get the total directly.
func (roTx *ReadOnlyTx) LenBigSmall() (big, small int64) {
	return roTx.db.liveBigKeys, roTx.db.liveSmallKeys
}

// Ascend iterates keys >= pivot in ascending order until callback returns false.
// Use pivot="" to start from the first key.
func (roTx *ReadOnlyTx) Ascend(pivot string, callback func(key string, value []byte, vtyp uint64, hlc HLC) bool) {
	it := roTx.newIter()
	defer it.Close()
	it.Seek(pivot)
	for it.Valid() {
		if !callback(it.Key(), it.iterResolvedValue(), it.Vtyp(), it.Hlc()) {
			return
		}
		it.Next()
	}
}

// Descend iterates keys <= pivot in descending order until callback returns false.
// Use pivot="" to start from the last key.
func (roTx *ReadOnlyTx) Descend(pivot string, callback func(key string, value []byte, vtyp uint64, hlc HLC) bool) {
	it := roTx.newIter()
	defer it.Close()
	if pivot == "" {
		it.SeekLast()
	} else {
		it.seekLE(pivot, false)
	}
	for it.Valid() {
		if !callback(it.Key(), it.iterResolvedValue(), it.Vtyp(), it.Hlc()) {
			return
		}
		it.Prev()
	}
}

// AscendRange iterates keys in [greaterOrEqual, lessThan) in ascending order.
// Use "" for either bound to leave it open.
func (roTx *ReadOnlyTx) AscendRange(greaterOrEqual, lessThan string, callback func(key string, value []byte, vtyp uint64, hlc HLC) bool) {
	it := roTx.newIter()
	defer it.Close()
	it.Seek(greaterOrEqual)
	for it.Valid() {
		if lessThan != "" && it.Key() >= lessThan {
			return
		}
		if !callback(it.Key(), it.iterResolvedValue(), it.Vtyp(), it.Hlc()) {
			return
		}
		it.Next()
	}
}

// DescendRange iterates keys in (greaterThan, lessOrEqual] in descending order.
// Use "" for either bound to leave it open.
func (roTx *ReadOnlyTx) DescendRange(lessOrEqual, greaterThan string, callback func(key string, value []byte, vtyp uint64, hlc HLC) bool) {
	it := roTx.newIter()
	defer it.Close()
	if lessOrEqual == "" {
		it.SeekLast()
	} else {
		it.seekLE(lessOrEqual, false)
	}
	for it.Valid() {
		if greaterThan != "" && it.Key() <= greaterThan {
			return
		}
		if !callback(it.Key(), it.iterResolvedValue(), it.Vtyp(), it.Hlc()) {
			return
		}
		it.Prev()
	}
}

// ====================== FlexDB.Update / FlexDB.View ======================

// Update runs fn inside an exclusive write transaction. The write lock
// (topMutRW.Lock()) is held for the duration of fn, blocking all other
// readers and writers including the flush worker.
//
// Update requires AllowReads. Before AllowReads, data must be loaded with
// Batch.Set, Batch.SetBytes, and Batch.Delete only.
//
// All iterators created within fn via rwDB.NewIter() or rwDB.FindIt()
// are automatically closed when fn returns.
//
// Before fn runs, existing memtable contents are flushed so rollback can
// discard this transaction's memtable changes. If fn returns nil, Update calls
// Commit. If fn returns an error, Update calls Rollback and returns the error.
// If fn calls Clear(true) or a DeleteRange that returns allGone=true, that
// destructive operation immediately becomes the rollback baseline for the same
// WriteTx. Later writes are allowed and behave like writes in a fresh
// transaction: Commit keeps them, while Rollback discards only those later
// writes.
//
// Do NOT call db.Put/db.Get/db.Delete/db.Sync inside fn - use rw
// methods on the WriteTx instead (to avoid deadlock).
func (db *FlexDB) Update(fn func(rw *WriteTx) error) (err error) {
	db.requireReadsAllowed()
	db.topMutRW.Lock()
	tx, err := db.beginWriteTxLocked(true)
	if err != nil {
		db.topMutRW.Unlock()
		return err
	}
	autoVacuumHandoff := false
	defer func() {
		defer func() {
			if !autoVacuumHandoff {
				db.topMutRW.Unlock()
			}
		}()
		defer tx.closeAll()

		if r := recover(); r != nil {
			rollbackErr := tx.Rollback()
			if ioe, ok := r.(iterIOErr); ok {
				err = errors.Join(ioe.err, rollbackErr)
			} else {
				panic(r)
			}
			return
		}
		if tx.done {
			if err == nil {
				autoVacuumHandoff = db.maybeStartAutoVacuumLocked()
			}
			return
		}
		if err != nil {
			err = errors.Join(err, tx.Rollback())
			return
		}
		err = tx.Commit()
		if err == nil {
			autoVacuumHandoff = db.maybeStartAutoVacuumLocked()
		}
	}()
	return fn(tx)
}

func (db *FlexDB) beginWriteTxLocked(managedByUpdate bool) (*WriteTx, error) {
	if err := db.writeLockHeldSync(); err != nil {
		return nil, fmt.Errorf("flexdb: begin write transaction sync: %w", err)
	}
	return &WriteTx{
		txBase:          txBase{db: db},
		managedByUpdate: managedByUpdate,
		beginLiveKeys:   db.liveKeys,
		beginBigKeys:    db.liveBigKeys,
		beginSmallKeys:  db.liveSmallKeys,
		beginAutoDel:    atomic.LoadInt64(&db.autoVacuumDeletedBytes),
		beginAutoVLOG:   atomic.LoadInt64(&db.autoVacuumVLOGDeletedBytes),
	}, nil
}

// View runs fn inside a read-only transaction. The read lock
// (topMutRW.RLock()) is held for the duration of fn, blocking
// writers (including the flush worker) but allowing concurrent readers.
//
// All iterators created within fn via roDB.NewIter() or roDB.FindIt()
// are automatically closed when fn returns.
//
// Do NOT call db.Get inside fn - use roDB methods instead (deadlock).
func (db *FlexDB) View(fn func(ro *ReadOnlyTx) error) (err error) {
	db.requireReadsAllowed()
	db.topMutRW.RLock()
	tx := &ReadOnlyTx{txBase{db: db}}
	defer func() {
		tx.closeAll()
		db.topMutRW.RUnlock()
	}()
	defer recoverIterIOErr(&err)
	return fn(tx)
}

func (db *FlexDB) BeginUpdate() (*WriteTx, error) {
	db.requireReadsAllowed()
	db.topMutRW.Lock()
	tx, err := db.beginWriteTxLocked(false)
	if err != nil {
		db.topMutRW.Unlock()
		return nil, err
	}
	return tx, nil
}

func (db *FlexDB) BeginView() *ReadOnlyTx {
	db.requireReadsAllowed()
	db.topMutRW.RLock()
	return &ReadOnlyTx{txBase{db: db}}
}
func (rtx *ReadOnlyTx) Close() {
	rtx.closeAll()
	rtx.db.topMutRW.RUnlock()
}
