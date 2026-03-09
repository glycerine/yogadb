package yogadb

import "bytes"

// ReadOnlyDB provides read-only access to the database within a View
// transaction. Methods must not be used after the callback returns.
type ReadOnlyDB interface {
	Get(key []byte) ([]byte, bool)
	Find(smod SearchModifier, key []byte) (kv *KV, found, exact bool)
	FindIt(smod SearchModifier, key []byte) (kv *KV, found, exact bool, it *Iter)
	FetchLarge(kv *KV) ([]byte, error)
	NewIter() *Iter
	Ascend(pivot []byte, iter func(key, value []byte) bool)
	Descend(pivot []byte, iter func(key, value []byte) bool)
	AscendRange(greaterOrEqual, lessThan []byte, iter func(key, value []byte) bool)
	DescendRange(lessOrEqual, greaterThan []byte, iter func(key, value []byte) bool)
	Len() int64
	LenBigSmall() (big, small int64)
}

// WritableDB extends ReadOnlyDB with mutation methods. Passed to
// Update transaction callbacks. Writes are applied immediately
// to the database (no buffering).
type WritableDB interface {
	ReadOnlyDB
	Put(key, value []byte) error
	Delete(key []byte) error
	DeleteRange(includeLarge bool, begKey, endKey []byte, begInclusive, endInclusive bool) (n int64, allGone bool, err error)
	Clear(includeLarge bool) (allGone bool, err error)
	Merge(key []byte, fn func(oldVal []byte, exists bool) (newVal []byte, write bool)) error
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

// ====================== writeTx (implements WritableDB) ======================

type writeTx struct{ txBase }

func (tx *writeTx) Get(key []byte) ([]byte, bool) {
	return tx.db.writeLockHeldGet(key)
}

func (tx *writeTx) Put(key, value []byte) error {
	return tx.db.writeLockHeldPut(key, value)
}

func (tx *writeTx) Delete(key []byte) error {
	return tx.db.writeLockHeldPut(key, nil)
}

func (tx *writeTx) Sync() error {
	return tx.db.writeLockHeldSync()
}

func (tx *writeTx) Find(smod SearchModifier, key []byte) (kv *KV, found, exact bool) {
	it := tx.newIter()
	found, exact = findSeekIter(it, smod, key)
	if found {
		kv = findBuildKV(it)
	}
	it.Close()
	return
}

func (tx *writeTx) FindIt(smod SearchModifier, key []byte) (kv *KV, found, exact bool, it *Iter) {
	it = tx.newIter()
	found, exact = findSeekIter(it, smod, key)
	if found {
		kv = findBuildKV(it)
	}
	return
}

func (tx *writeTx) FetchLarge(kv *KV) ([]byte, error) {
	return tx.db.FetchLarge(kv)
}

func (tx *writeTx) NewIter() *Iter {
	return tx.newIter()
}

func (tx *writeTx) Len() int64 {
	return tx.db.liveKeys
}

func (tx *writeTx) LenBigSmall() (big, small int64) {
	return tx.db.liveBigKeys, tx.db.liveSmallKeys
}

func (tx *writeTx) DeleteRange(includeLarge bool, begKey, endKey []byte, begInclusive, endInclusive bool) (n int64, allGone bool, err error) {
	return tx.db.writeLockHeldDeleteRange(includeLarge, begKey, endKey, begInclusive, endInclusive)
}

func (tx *writeTx) Clear(includeLarge bool) (allGone bool, err error) {
	return tx.db.writeLockHeldClear(includeLarge)
}

func (tx *writeTx) Merge(key []byte, fn func(oldVal []byte, exists bool) (newVal []byte, write bool)) error {
	return tx.db.writeLockHeldMerge(key, fn)
}

func (tx *writeTx) Ascend(pivot []byte, iter func(key, value []byte) bool) {
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

func (tx *writeTx) Descend(pivot []byte, iter func(key, value []byte) bool) {
	it := tx.newIter()
	defer it.Close()
	if pivot == nil {
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

func (tx *writeTx) AscendRange(greaterOrEqual, lessThan []byte, iter func(key, value []byte) bool) {
	it := tx.newIter()
	defer it.Close()
	it.Seek(greaterOrEqual)
	for it.Valid() {
		if lessThan != nil && bytes.Compare(it.Key(), lessThan) >= 0 {
			return
		}
		if !iter(it.Key(), it.iterResolvedValue()) {
			return
		}
		it.Next()
	}
}

func (tx *writeTx) DescendRange(lessOrEqual, greaterThan []byte, iter func(key, value []byte) bool) {
	it := tx.newIter()
	defer it.Close()
	if lessOrEqual == nil {
		it.SeekToLast()
	} else {
		it.seekLE(lessOrEqual, false)
	}
	for it.Valid() {
		if greaterThan != nil && bytes.Compare(it.Key(), greaterThan) <= 0 {
			return
		}
		if !iter(it.Key(), it.iterResolvedValue()) {
			return
		}
		it.Prev()
	}
}

// ====================== readTx (implements ReadOnlyDB) ======================

type readTx struct{ txBase }

func (tx *readTx) Get(key []byte) ([]byte, bool) {
	return tx.db.writeLockHeldGet(key)
}

func (tx *readTx) Find(smod SearchModifier, key []byte) (kv *KV, found, exact bool) {
	it := tx.newIter()
	found, exact = findSeekIter(it, smod, key)
	if found {
		kv = findBuildKV(it)
	}
	it.Close()
	return
}

func (tx *readTx) FindIt(smod SearchModifier, key []byte) (kv *KV, found, exact bool, it *Iter) {
	it = tx.newIter()
	found, exact = findSeekIter(it, smod, key)
	if found {
		kv = findBuildKV(it)
	}
	return
}

func (tx *readTx) FetchLarge(kv *KV) ([]byte, error) {
	return tx.db.FetchLarge(kv)
}

func (tx *readTx) NewIter() *Iter {
	return tx.newIter()
}

func (tx *readTx) Len() int64 {
	return tx.db.liveKeys
}

func (tx *readTx) LenBigSmall() (big, small int64) {
	return tx.db.liveBigKeys, tx.db.liveSmallKeys
}

func (tx *readTx) Ascend(pivot []byte, iter func(key, value []byte) bool) {
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

func (tx *readTx) Descend(pivot []byte, iter func(key, value []byte) bool) {
	it := tx.newIter()
	defer it.Close()
	if pivot == nil {
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

func (tx *readTx) AscendRange(greaterOrEqual, lessThan []byte, iter func(key, value []byte) bool) {
	it := tx.newIter()
	defer it.Close()
	it.Seek(greaterOrEqual)
	for it.Valid() {
		if lessThan != nil && bytes.Compare(it.Key(), lessThan) >= 0 {
			return
		}
		if !iter(it.Key(), it.iterResolvedValue()) {
			return
		}
		it.Next()
	}
}

func (tx *readTx) DescendRange(lessOrEqual, greaterThan []byte, iter func(key, value []byte) bool) {
	it := tx.newIter()
	defer it.Close()
	if lessOrEqual == nil {
		it.SeekToLast()
	} else {
		it.seekLE(lessOrEqual, false)
	}
	for it.Valid() {
		if greaterThan != nil && bytes.Compare(it.Key(), greaterThan) <= 0 {
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
// Do NOT call db.Put/db.Get/db.Delete/db.Sync inside fn — use rwDB
// methods instead (deadlock).
func (db *FlexDB) Update(fn func(rwDB WritableDB) error) error {
	db.topMutRW.Lock()
	tx := writeTx{txBase{db: db}}
	defer func() {
		tx.closeAll()
		db.topMutRW.Unlock()
	}()
	return fn(&tx)
}

// View runs fn inside a read-only transaction. The read lock
// (topMutRW.RLock()) is held for the duration of fn, blocking
// writers (including the flush worker) but allowing concurrent readers.
//
// All iterators created within fn via roDB.NewIter() or roDB.FindIt()
// are automatically closed when fn returns.
//
// Do NOT call db.Get inside fn — use roDB methods instead (deadlock).
func (db *FlexDB) View(fn func(roDB ReadOnlyDB) error) error {
	db.topMutRW.RLock()
	tx := readTx{txBase{db: db}}
	defer func() {
		tx.closeAll()
		db.topMutRW.RUnlock()
	}()
	return fn(&tx)
}
