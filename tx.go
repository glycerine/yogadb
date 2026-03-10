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
	Merge(key string, fn func(oldVal []byte, exists bool) (newVal []byte, write bool)) error
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

func (tx *WriteTx) Get(key string) ([]byte, bool) {
	return tx.db.someLockHeldGet(key)
}

func (tx *WriteTx) Put(key string, value []byte) error {
	return tx.db.writeLockHeldPut(key, value)
}

func (tx *WriteTx) Delete(key string) error {
	return tx.db.writeLockHeldPut(key, nil)
}

func (tx *WriteTx) Sync() error {
	return tx.db.writeLockHeldSync()
}

func (tx *WriteTx) Find(smod SearchModifier, key string) (kv *KV, found, exact bool) {
	it := tx.newIter()
	found, exact = findSeekIter(it, smod, key)
	if found {
		kv = findBuildKV(it)
	}
	it.Close()
	return
}

func (tx *WriteTx) FindIt(smod SearchModifier, key string) (kv *KV, found, exact bool, it *Iter) {
	it = tx.newIter()
	found, exact = findSeekIter(it, smod, key)
	if found {
		kv = findBuildKV(it)
	}
	return
}

func (tx *WriteTx) FetchLarge(kv *KV) ([]byte, error) {
	return tx.db.lockHeldFetchLarge(kv)
}

func (tx *WriteTx) NewIter() *Iter {
	return tx.newIter()
}

func (tx *WriteTx) Len() int64 {
	return tx.db.liveKeys
}

func (tx *WriteTx) LenBigSmall() (big, small int64) {
	return tx.db.liveBigKeys, tx.db.liveSmallKeys
}

func (tx *WriteTx) DeleteRange(includeLarge bool, begKey, endKey string, begInclusive, endInclusive bool) (n int64, allGone bool, err error) {
	return tx.db.writeLockHeldDeleteRange(includeLarge, begKey, endKey, begInclusive, endInclusive)
}

func (tx *WriteTx) Clear(includeLarge bool) (allGone bool, err error) {
	return tx.db.writeLockHeldClear(includeLarge)
}

func (tx *WriteTx) Merge(key string, fn func(oldVal []byte, exists bool) (newVal []byte, write bool)) error {
	return tx.db.writeLockHeldMerge(key, fn)
}

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

func (tx *ReadOnlyTx) Get(key string) ([]byte, bool) {
	return tx.db.someLockHeldGet(key)
}

func (tx *ReadOnlyTx) Find(smod SearchModifier, key string) (kv *KV, found, exact bool) {
	it := tx.newIter()
	found, exact = findSeekIter(it, smod, key)
	if found {
		kv = findBuildKV(it)
	}
	it.Close()
	return
}

func (tx *ReadOnlyTx) FindIt(smod SearchModifier, key string) (kv *KV, found, exact bool, it *Iter) {
	it = tx.newIter()
	found, exact = findSeekIter(it, smod, key)
	if found {
		kv = findBuildKV(it)
	}
	return
}

func (tx *ReadOnlyTx) FetchLarge(kv *KV) ([]byte, error) {
	return tx.db.lockHeldFetchLarge(kv)
}

func (tx *ReadOnlyTx) NewIter() *Iter {
	return tx.newIter()
}

func (tx *ReadOnlyTx) Len() int64 {
	return tx.db.liveKeys
}

func (tx *ReadOnlyTx) LenBigSmall() (big, small int64) {
	return tx.db.liveBigKeys, tx.db.liveSmallKeys
}

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
