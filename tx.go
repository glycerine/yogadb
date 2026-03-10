package yogadb

// ReadOnlyDB provides read-only access to the database within a View
// transaction. Methods must not be used after the callback returns.
type ReadOnlyDB interface {
	Get(key string) ([]byte, bool)
	Find(smod SearchModifier, key string) (kv *KV, found, exact bool)
	FindIt(smod SearchModifier, key string) (kv *KV, found, exact bool, it *Iter)
	FetchLarge(kv *KV) ([]byte, error)
	NewIter() *Iter
	Ascend(pivot string, iter func(key string, value []byte) bool)
	Descend(pivot string, iter func(key string, value []byte) bool)
	AscendRange(greaterOrEqual, lessThan string, iter func(key string, value []byte) bool)
	DescendRange(lessOrEqual, greaterThan string, iter func(key string, value []byte) bool)
	Len() int64
	LenBigSmall() (big, small int64)
}

// WritableDB extends ReadOnlyDB with mutation methods. Passed to
// Update transaction callbacks. Writes are applied immediately
// to the database (no buffering). Since there is only ever
// a single writer at a time, and no concurrent readers, there
// is no point in waiting to apply each action.
type WritableDB interface {
	ReadOnlyDB
	Put(key string, value []byte) error
	Delete(key string) error
	DeleteRange(includeLarge bool, begKey, endKey string, begInclusive, endInclusive bool) (n int64, allGone bool, err error)
	Clear(includeLarge bool) (allGone bool, err error)
	Merge(key string, fn func(oldVal []byte, exists bool) (newVal []byte, write bool, doDelete bool)) error
	Sync() error
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

// ====================== WriteTx (implements WritableDB) ======================

var _ WritableDB = (*WriteTx)(nil)

type WriteTx struct{ txBase }

// Get retrieves the value for key. Returns (nil, false) if not found
// or deleted. The returned []byte is a copy, safe to retain.
func (tx *WriteTx) Get(key string) ([]byte, bool) {
	return tx.db.someLockHeldGet(key)
}

// Put writes key -> value. value==nil stores a live key with nil value
// (useful for sets); []byte{} stores a live key with zero-length value.
// Use Delete to remove a key. Not durable until Sync is called.
func (tx *WriteTx) Put(key string, value []byte) error {
	return tx.db.writeLockHeldPut(key, value, false)
}

// Delete removes key from the store.
func (tx *WriteTx) Delete(key string) error {
	return tx.db.writeLockHeldPut(key, nil, true)
}

// Sync flushes all in-memory data to disk and fsyncs.
func (tx *WriteTx) Sync() error {
	return tx.db.writeLockHeldSync()
}

// Find seeks to a key relative to the given key per smod (Exact, GTE, GT, LTE, LT)
// and returns an owned KV copy. exact is true when the returned key equals the query.
func (tx *WriteTx) Find(smod SearchModifier, key string) (kv *KV, found, exact bool) {
	it := tx.newIter()
	found, exact = findSeekIter(it, smod, key)
	if found {
		kv = findBuildKV(it)
	}
	it.Close()
	return
}

// FindIt is like Find but also returns an iterator positioned at the result.
// The iterator is auto-closed when the transaction ends.
func (tx *WriteTx) FindIt(smod SearchModifier, key string) (kv *KV, found, exact bool, it *Iter) {
	it = tx.newIter()
	found, exact = findSeekIter(it, smod, key)
	if found {
		kv = findBuildKV(it)
	}
	return
}

// FetchLarge retrieves the full value for a KV. For VLOG-stored
// values it reads from disk; for inline values it returns kv.Value directly.
func (tx *WriteTx) FetchLarge(kv *KV) ([]byte, error) {
	return tx.db.lockHeldFetchLarge(kv)
}

// NewIter returns a new iterator over the database. It is
// automatically closed when the transaction ends. It is
// legal to Close it sooner if you want to release resources
// early.
func (tx *WriteTx) NewIter() *Iter {
	return tx.newIter()
}

// Len returns the total number of live keys in the database.
func (tx *WriteTx) Len() int64 {
	return tx.db.liveKeys
}

// LenBigSmall returns live key counts partitioned by storage (VLOG vs inline).
func (tx *WriteTx) LenBigSmall() (big, small int64) {
	return tx.db.liveBigKeys, tx.db.liveSmallKeys
}

// DeleteRange deletes keys in the range [begKey, endKey] with configurable
// inclusivity. When includeLarge is false, VLOG-stored keys are skipped.
// If allGone is true, the entire database was reinitialized and all
// previously obtained iterators and KV references are invalidated.
func (tx *WriteTx) DeleteRange(includeLarge bool, begKey, endKey string, begInclusive, endInclusive bool) (n int64, allGone bool, err error) {
	return tx.db.writeLockHeldDeleteRange(includeLarge, begKey, endKey, begInclusive, endInclusive)
}

// Clear deletes all keys. When includeLarge is false, only inline-value
// keys are deleted; VLOG-stored keys survive. When allGone is true,
// the database was reinitialized and all previously obtained iterators
// and KV references are invalidated.
func (tx *WriteTx) Clear(includeLarge bool) (allGone bool, err error) {
	return tx.db.writeLockHeldClear(includeLarge)
}

// Merge performs an atomic read-modify-write on key. fn is always
// called: when the key exists, oldVal is its current value and
// exists=true; when the key is absent or deleted, oldVal=nil and
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
//     already exists" — if exists=false, return a bare return (all zeros = no-op).
//
//  2. Conditional update: The callback inspects the old value and
//     decides no change is needed. E.g., "set to X only if current
//     value isn't already X."
//
//  3. Read-only peek: The callback just wants to see the current
//     value (though Get is simpler for that).
//
// .
func (tx *WriteTx) Merge(key string, fn func(oldVal []byte, exists bool) (newVal []byte, write bool, doDelete bool)) error {
	return tx.db.writeLockHeldMerge(key, fn)
}

// Ascend iterates keys >= pivot in ascending order until iter returns false.
// Use pivot="" to start from the first key.
func (tx *WriteTx) Ascend(pivot string, iter func(key string, value []byte) bool) {
	it := tx.newIter()
	defer it.Close()
	it.Seek(pivot)
	for it.Valid() {
		if !iter(it.Key(), it.iterResolvedValue()) {
			return
		}
		it.Next()
	}
}

// Descend iterates keys <= pivot in descending order until iter returns false.
// Use pivot="" to start from the last key.
func (tx *WriteTx) Descend(pivot string, iter func(key string, value []byte) bool) {
	it := tx.newIter()
	defer it.Close()
	if pivot == "" {
		it.SeekToLast()
	} else {
		it.seekLE(pivot, false)
	}
	for it.Valid() {
		if !iter(it.Key(), it.iterResolvedValue()) {
			return
		}
		it.Prev()
	}
}

// AscendRange iterates keys in [greaterOrEqual, lessThan) in ascending order.
// Use "" for either bound to leave it open.
func (tx *WriteTx) AscendRange(greaterOrEqual, lessThan string, iter func(key string, value []byte) bool) {
	it := tx.newIter()
	defer it.Close()
	it.Seek(greaterOrEqual)
	for it.Valid() {
		if lessThan != "" && it.Key() >= lessThan {
			return
		}
		if !iter(it.Key(), it.iterResolvedValue()) {
			return
		}
		it.Next()
	}
}

// DescendRange iterates keys in (greaterThan, lessOrEqual] in descending order.
// Use "" for either bound to leave it open.
func (tx *WriteTx) DescendRange(lessOrEqual, greaterThan string, iter func(key string, value []byte) bool) {
	it := tx.newIter()
	defer it.Close()
	if lessOrEqual == "" {
		it.SeekToLast()
	} else {
		it.seekLE(lessOrEqual, false)
	}
	for it.Valid() {
		if greaterThan != "" && it.Key() <= greaterThan {
			return
		}
		if !iter(it.Key(), it.iterResolvedValue()) {
			return
		}
		it.Prev()
	}
}

// ====================== ReadOnlyTx (implements ReadOnlyDB) ======================

var _ ReadOnlyDB = (*ReadOnlyTx)(nil)

type ReadOnlyTx struct{ txBase }

// Get retrieves the value for key. Returns (nil, false) if not found
// or deleted. The returned []byte is a copy, safe to retain.
func (tx *ReadOnlyTx) Get(key string) ([]byte, bool) {
	return tx.db.someLockHeldGet(key)
}

// Find seeks to a key relative to the given key per smod (Exact, GTE, GT, LTE, LT)
// and returns an owned KV copy. exact is true when the returned key equals the query.
func (tx *ReadOnlyTx) Find(smod SearchModifier, key string) (kv *KV, found, exact bool) {
	it := tx.newIter()
	found, exact = findSeekIter(it, smod, key)
	if found {
		kv = findBuildKV(it)
	}
	it.Close()
	return
}

// FindIt is like Find but also returns an iterator positioned at the result.
// The iterator is auto-closed when the transaction ends.
func (tx *ReadOnlyTx) FindIt(smod SearchModifier, key string) (kv *KV, found, exact bool, it *Iter) {
	it = tx.newIter()
	found, exact = findSeekIter(it, smod, key)
	if found {
		kv = findBuildKV(it)
	}
	return
}

// FetchLarge retrieves the full value for a KV. For VLOG-stored
// values it reads from disk; for inline values it returns kv.Value directly.
func (tx *ReadOnlyTx) FetchLarge(kv *KV) ([]byte, error) {
	return tx.db.lockHeldFetchLarge(kv)
}

// NewIter returns a new iterator over the database. It is
// automatically closed when the transaction ends. It is
// legal to Close it sooner if you want to release resources
// early.
func (tx *ReadOnlyTx) NewIter() *Iter {
	return tx.newIter()
}

// Len returns the total number of live keys in the database.
// See also LenBigSmall to get the count partitioned by
// the size class (small inline, large in the VLOG).
func (tx *ReadOnlyTx) Len() int64 {
	return tx.db.liveKeys
}

// LenBigSmall returns live key counts partitioned by size
// class. The returned count big counts the keys in VLOG,
// while small gives the number of keys with inline values.
// See also Len to get the total directly.
func (tx *ReadOnlyTx) LenBigSmall() (big, small int64) {
	return tx.db.liveBigKeys, tx.db.liveSmallKeys
}

// Ascend iterates keys >= pivot in ascending order until iter returns false.
// Use pivot="" to start from the first key.
func (tx *ReadOnlyTx) Ascend(pivot string, iter func(key string, value []byte) bool) {
	it := tx.newIter()
	defer it.Close()
	it.Seek(pivot)
	for it.Valid() {
		if !iter(it.Key(), it.iterResolvedValue()) {
			return
		}
		it.Next()
	}
}

// Descend iterates keys <= pivot in descending order until iter returns false.
// Use pivot="" to start from the last key.
func (tx *ReadOnlyTx) Descend(pivot string, iter func(key string, value []byte) bool) {
	it := tx.newIter()
	defer it.Close()
	if pivot == "" {
		it.SeekToLast()
	} else {
		it.seekLE(pivot, false)
	}
	for it.Valid() {
		if !iter(it.Key(), it.iterResolvedValue()) {
			return
		}
		it.Prev()
	}
}

// AscendRange iterates keys in [greaterOrEqual, lessThan) in ascending order.
// Use "" for either bound to leave it open.
func (tx *ReadOnlyTx) AscendRange(greaterOrEqual, lessThan string, iter func(key string, value []byte) bool) {
	it := tx.newIter()
	defer it.Close()
	it.Seek(greaterOrEqual)
	for it.Valid() {
		if lessThan != "" && it.Key() >= lessThan {
			return
		}
		if !iter(it.Key(), it.iterResolvedValue()) {
			return
		}
		it.Next()
	}
}

// DescendRange iterates keys in (greaterThan, lessOrEqual] in descending order.
// Use "" for either bound to leave it open.
func (tx *ReadOnlyTx) DescendRange(lessOrEqual, greaterThan string, iter func(key string, value []byte) bool) {
	it := tx.newIter()
	defer it.Close()
	if lessOrEqual == "" {
		it.SeekToLast()
	} else {
		it.seekLE(lessOrEqual, false)
	}
	for it.Valid() {
		if greaterThan != "" && it.Key() <= greaterThan {
			return
		}
		if !iter(it.Key(), it.iterResolvedValue()) {
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
// All iterators created within fn via rwDB.NewIter() or rwDB.FindIt()
// are automatically closed when fn returns.
//
// Writes via rwDB.Put/rwDB.Delete are applied immediately to the
// database (no buffering, no Commit needed).
//
// Do NOT call db.Put/db.Get/db.Delete/db.Sync inside fn - use rwDB
// methods instead (deadlock).
func (db *FlexDB) Update(fn func(rw *WriteTx) error) error {
	db.topMutRW.Lock()
	tx := &WriteTx{txBase{db: db}}
	defer func() {
		tx.closeAll()
		db.topMutRW.Unlock()
	}()
	return fn(tx)
}

// View runs fn inside a read-only transaction. The read lock
// (topMutRW.RLock()) is held for the duration of fn, blocking
// writers (including the flush worker) but allowing concurrent readers.
//
// All iterators created within fn via roDB.NewIter() or roDB.FindIt()
// are automatically closed when fn returns.
//
// Do NOT call db.Get inside fn - use roDB methods instead (deadlock).
func (db *FlexDB) View(fn func(ro *ReadOnlyTx) error) error {
	db.topMutRW.RLock()
	tx := &ReadOnlyTx{txBase{db: db}}
	defer func() {
		tx.closeAll()
		db.topMutRW.RUnlock()
	}()
	return fn(tx)
}
