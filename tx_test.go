package yogadb

import (
	"errors"
	"fmt"
	"sync"
	"testing"
)

func mustTxGet(t *testing.T, tx *WriteTx, key, wantValue string) {
	t.Helper()
	val, ok, _, _, err := tx.Get(key)
	if err != nil {
		t.Fatalf("tx.Get(%q): %v", key, err)
	}
	if !ok {
		t.Fatalf("tx.Get(%q): not found (want %q)", key, wantValue)
	}
	if string(val) != wantValue {
		t.Fatalf("tx.Get(%q) = %q, want %q", key, val, wantValue)
	}
}

func mustTxMiss(t *testing.T, tx *WriteTx, key string) {
	t.Helper()
	val, ok, _, _, err := tx.Get(key)
	if err != nil {
		t.Fatalf("tx.Get(%q): %v", key, err)
	}
	if ok {
		t.Fatalf("tx.Get(%q) = %q, want miss", key, val)
	}
}

func TestTx_UpdateBasic(t *testing.T) {
	db, _ := openTestDB(t, nil)
	mustPut(t, db, "k1", "v1")

	err := db.Update(func(rwDB *WriteTx) error {
		if _, err := rwDB.Put("k2", []byte("v2"), 0); err != nil {
			return err
		}
		if _, err := rwDB.Put("k3", []byte("v3"), 0); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	mustGet(t, db, "k1", "v1")
	mustGet(t, db, "k2", "v2")
	mustGet(t, db, "k3", "v3")
}

func TestTxPutReturnsMemWALBeginErrorBeforeApplyingWrite(t *testing.T) {
	fs, dir := newTestFS(t)
	db, err := OpenFlexDB(dir, &Config{
		FS:                     fs,
		DisableBackgroundFlush: true,
	})
	if err != nil {
		t.Fatalf("OpenFlexDB: %v", err)
	}
	db.AllowReads()

	failFD := &failWriteAtFileForTest{File: db.mt.memWalFD, failWriteAt: true}
	db.mt.memWalFD = failFD
	db.mt.memWalBuf = db.mt.memWalBuf[:memtableWalBufCap-1]

	err = db.Update(func(rwDB *WriteTx) error {
		_, err := rwDB.Put("wal-fail", []byte("value"), 0)
		return err
	})
	if !errors.Is(err, errMemWALWriteForTest) {
		t.Fatalf("Update error = %v, want injected MEMWAL write failure", err)
	}
	if _, ok := db.mt.get("wal-fail"); ok {
		t.Fatal("failed transaction write was applied to memtable")
	}
	db.AllowReads()
	if value, found, _, _, err := db.Get("wal-fail"); err != nil || found {
		t.Fatalf("Get after failed WAL begin = (%q, %v, %v), want not found with nil error", value, found, err)
	}

	failFD.failWriteAt = false
	db.mt.memWalBuf = db.mt.memWalBuf[:0]
	db.Close()
}

func TestTx_GreenMEMWALLazyBeginAfterCommit(t *testing.T) {
	db, _ := openTestDB(t, &Config{DisableBackgroundFlush: true})

	err := db.Update(func(rwDB *WriteTx) error {
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(db.mt.memWalBuf) != 0 {
		t.Fatalf("empty Update left %d buffered MEMWAL bytes, want 0", len(db.mt.memWalBuf))
	}
	if got, err := db.mt.memWalSize(); err != nil || got != memWalHeaderSize {
		t.Fatalf("empty Update MEMWAL size = %d err=%v, want %d nil", got, err, memWalHeaderSize)
	}

	err = db.Update(func(rwDB *WriteTx) error {
		if _, err := rwDB.Put("k", []byte("v"), 0); err != nil {
			return err
		}
		return rwDB.Commit()
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(db.mt.memWalBuf) != 0 {
		t.Fatalf("Update after tx.Commit left %d buffered MEMWAL bytes, want 0", len(db.mt.memWalBuf))
	}
	if got, err := db.mt.memWalSize(); err != nil || got != memWalHeaderSize {
		t.Fatalf("Update after tx.Commit MEMWAL size = %d err=%v, want %d nil", got, err, memWalHeaderSize)
	}
}

func TestTx_UpdateCallbackErrorRollsBackWrites(t *testing.T) {
	db, _ := openTestDB(t, &Config{DisableBackgroundFlush: true})
	mustPut(t, db, "keep", "old")
	mustPut(t, db, "delete-me", "old")

	sentinel := errors.New("rollback sentinel")
	err := db.Update(func(rwDB *WriteTx) error {
		if _, err := rwDB.Put("temp", []byte("value"), 0); err != nil {
			return err
		}
		if _, err := rwDB.Put("keep", []byte("new"), 0); err != nil {
			return err
		}
		if err := rwDB.Delete("delete-me"); err != nil {
			return err
		}

		mustTxGet(t, rwDB, "temp", "value")
		mustTxGet(t, rwDB, "keep", "new")
		mustTxMiss(t, rwDB, "delete-me")
		return sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("Update error = %v, want sentinel", err)
	}

	mustMiss(t, db, "temp")
	mustGet(t, db, "keep", "old")
	mustGet(t, db, "delete-me", "old")
}

func TestBeginUpdateCommitRollbackFirstWins(t *testing.T) {
	db, _ := openTestDB(t, nil)

	rwDB, err := db.BeginUpdate()
	if err != nil {
		t.Fatal(err)
	}
	defer rwDB.Rollback()

	if _, err := rwDB.Put("committed", []byte("yes"), 0); err != nil {
		t.Fatal(err)
	}
	if err := rwDB.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := rwDB.Rollback(); err != nil {
		t.Fatalf("Rollback after Commit = %v, want nil", err)
	}
	if _, err := rwDB.Put("after-commit", []byte("no"), 0); !errors.Is(err, ErrWriteTxClosed) {
		t.Fatalf("Put after Commit error = %v, want ErrWriteTxClosed", err)
	}

	mustGet(t, db, "committed", "yes")
}

func TestBeginUpdateRollbackCommitFirstWins(t *testing.T) {
	db, _ := openTestDB(t, nil)

	rwDB, err := db.BeginUpdate()
	if err != nil {
		t.Fatal(err)
	}
	defer rwDB.Rollback()

	if _, err := rwDB.Put("rolled-back", []byte("no"), 0); err != nil {
		t.Fatal(err)
	}
	if err := rwDB.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err := rwDB.Commit(); err != nil {
		t.Fatalf("Commit after Rollback = %v, want nil", err)
	}
	if _, err := rwDB.Put("after-rollback", []byte("no"), 0); !errors.Is(err, ErrWriteTxClosed) {
		t.Fatalf("Put after Rollback error = %v, want ErrWriteTxClosed", err)
	}

	mustMiss(t, db, "rolled-back")
}

func TestWriteTxClearTrueResetsRollbackBaselineAndContinues(t *testing.T) {
	db, _ := openTestDB(t, &Config{DisableBackgroundFlush: true})
	mustPut(t, db, "a", "1")
	mustPut(t, db, "b", "2")
	if err := db.Sync(); err != nil {
		t.Fatal(err)
	}

	err := db.Update(func(rwDB *WriteTx) error {
		allGone, err := rwDB.Clear(true)
		if err != nil {
			return err
		}
		if !allGone {
			t.Fatal("Clear(true) allGone = false, want true")
		}
		mustTxMiss(t, rwDB, "a")
		mustTxMiss(t, rwDB, "b")
		if _, err := rwDB.Put("after-clear", []byte("x"), 0); err != nil {
			return err
		}
		mustTxGet(t, rwDB, "after-clear", "x")
		return errors.New("rollback after clear")
	})
	if err == nil || err.Error() != "rollback after clear" {
		t.Fatalf("Update error = %v, want rollback after clear", err)
	}

	mustMiss(t, db, "a")
	mustMiss(t, db, "b")
	mustMiss(t, db, "after-clear")
	if got := db.Len(); got != 0 {
		t.Fatalf("Len after rollback from post-Clear(true) tx = %d, want 0", got)
	}
}

func TestWriteTxDeleteRangeAllGoneResetsRollbackBaselineAndContinues(t *testing.T) {
	db, _ := openTestDB(t, &Config{DisableBackgroundFlush: true})
	mustPut(t, db, "a", "1")
	mustPut(t, db, "b", "2")
	mustPut(t, db, "c", "3")
	if err := db.Sync(); err != nil {
		t.Fatal(err)
	}

	err := db.Update(func(rwDB *WriteTx) error {
		n, allGone, err := rwDB.DeleteRange(true, "a", "z", true, true)
		if err != nil {
			return err
		}
		if !allGone {
			t.Fatal("DeleteRange(all keys) allGone = false, want true")
		}
		if n != 0 {
			t.Fatalf("DeleteRange allGone n=%d, want 0", n)
		}
		mustTxMiss(t, rwDB, "a")
		mustTxMiss(t, rwDB, "b")
		mustTxMiss(t, rwDB, "c")
		if _, err := rwDB.Put("after-delete-range", []byte("x"), 0); err != nil {
			return err
		}
		mustTxGet(t, rwDB, "after-delete-range", "x")
		return nil
	})
	if err != nil {
		t.Fatalf("Update: %v", err)
	}

	mustMiss(t, db, "a")
	mustMiss(t, db, "b")
	mustMiss(t, db, "c")
	mustGet(t, db, "after-delete-range", "x")
	if got := db.Len(); got != 1 {
		t.Fatalf("Len after committed post-DeleteRange tx = %d, want 1", got)
	}
}

func TestTx_ViewBasic(t *testing.T) {
	db, _ := openTestDB(t, nil)
	mustPut(t, db, "k1", "v1")
	mustPut(t, db, "k2", "v2")

	err := db.View(func(roDB *ReadOnlyTx) error {
		val, ok, _, _, gerr := roDB.Get("k1")
		panicOn(gerr)
		if !ok || string(val) != "v1" {
			t.Fatalf("View Get(k1) = %q, %v; want v1, true", val, ok)
		}
		val, ok, _, _, gerr = roDB.Get("k2")
		panicOn(gerr)
		if !ok || string(val) != "v2" {
			t.Fatalf("View Get(k2) = %q, %v; want v2, true", val, ok)
		}
		_, ok, _, _, gerr = roDB.Get("k3")
		panicOn(gerr)
		if ok {
			t.Fatal("View Get(k3): expected not found")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestTx_UpdateSeesOwnWrites(t *testing.T) {
	db, _ := openTestDB(t, nil)
	mustPut(t, db, "k1", "v1")

	err := db.Update(func(rwDB *WriteTx) error {
		// Should see pre-existing key.
		val, ok, _, _, gerr := rwDB.Get("k1")
		panicOn(gerr)
		if !ok || string(val) != "v1" {
			t.Fatalf("Get(k1) before write: %q, %v", val, ok)
		}

		// Write a new key.
		if _, err := rwDB.Put("k2", []byte("v2"), 0); err != nil {
			return err
		}

		// Should see the write immediately.
		val, ok, _, _, gerr = rwDB.Get("k2")
		panicOn(gerr)
		if !ok || string(val) != "v2" {
			t.Fatalf("Get(k2) after write: %q, %v", val, ok)
		}

		// Overwrite k1.
		if _, err := rwDB.Put("k1", []byte("v1-updated"), 0); err != nil {
			return err
		}
		val, ok, _, _, gerr = rwDB.Get("k1")
		panicOn(gerr)
		if !ok || string(val) != "v1-updated" {
			t.Fatalf("Get(k1) after overwrite: %q, %v", val, ok)
		}

		// Delete k1.
		if err := rwDB.Delete("k1"); err != nil {
			return err
		}
		_, ok, _, _, gerr = rwDB.Get("k1")
		panicOn(gerr)
		if ok {
			t.Fatal("Get(k1) after delete: expected not found")
		}

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	// k1 should be deleted, k2 should exist.
	mustMiss(t, db, "k1")
	mustGet(t, db, "k2", "v2")
}

func TestTx_ViewCannotMutate(t *testing.T) {
	db, _ := openTestDB(t, nil)
	mustPut(t, db, "k1", "v1")

	// ReadOnlyDB has no Put or Delete methods -- this is a compile-time
	// guarantee. We just verify that Get works inside View.
	err := db.View(func(roDB *ReadOnlyTx) error {
		val, ok, _, _, gerr := roDB.Get("k1")
		panicOn(gerr)
		if !ok || string(val) != "v1" {
			t.Fatalf("View Get(k1) = %q, %v; want v1, true", val, ok)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	// Compile-time check: ReadOnlyDB does not satisfy WritableDB.
	var _ ReadOnlyDB = (*ReadOnlyTx)(nil)
	// The following would fail to compile:
	// var _ WritableDB = (*readTx)(nil)
}

func TestTx_SerializedUpdates(t *testing.T) {
	db, _ := openTestDB(t, nil)
	mustPut(t, db, "counter", "0")

	const n = 10
	var wg sync.WaitGroup
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs[i] = db.Update(func(rwDB *WriteTx) error {
				// Read current counter value.
				val, ok, _, _, gerr := rwDB.Get("counter")
				if gerr != nil {
					return gerr
				}
				if !ok {
					return fmt.Errorf("counter not found in goroutine %d", i)
				}
				// Increment.
				c := 0
				for _, b := range val {
					c = c*10 + int(b-'0')
				}
				c++
				newVal := []byte(fmt.Sprintf("%d", c))
				_, err := rwDB.Put("counter", newVal, 0)
				return err
			})
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("goroutine %d: %v", i, err)
		}
	}

	// Because updates are serialized, counter should be exactly n.
	val, ok, _, _, gerr := db.Get("counter")
	panicOn(gerr)
	if !ok {
		t.Fatal("counter not found after all updates")
	}
	want := fmt.Sprintf("%d", n)
	if string(val) != want {
		t.Fatalf("counter = %q, want %q", val, want)
	}
}

func TestTx_UpdateIterator(t *testing.T) {
	db, _ := openTestDB(t, nil)
	mustPut(t, db, "a", "1")
	mustPut(t, db, "b", "2")
	mustPut(t, db, "c", "3")

	err := db.Update(func(rwDB *WriteTx) error {
		it := rwDB.NewIter()
		defer it.Close()

		var got []string
		it.SeekFirst()
		for it.Valid() {
			got = append(got, string(it.Key()))
			it.Next()
		}
		want := []string{"a", "b", "c"}
		expectKeys(t, "UpdateIterator", got, want)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestTx_ViewIterator(t *testing.T) {
	db, _ := openTestDB(t, nil)
	mustPut(t, db, "x", "10")
	mustPut(t, db, "y", "20")
	mustPut(t, db, "z", "30")

	err := db.View(func(roDB *ReadOnlyTx) error {
		it := roDB.NewIter()
		defer it.Close()

		var got []string
		it.SeekFirst()
		for it.Valid() {
			got = append(got, string(it.Key()))
			it.Next()
		}
		want := []string{"x", "y", "z"}
		expectKeys(t, "ViewIterator", got, want)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestTx_MultipleIteratorsUpdate(t *testing.T) {
	db, _ := openTestDB(t, nil)
	mustPut(t, db, "a", "1")
	mustPut(t, db, "b", "2")
	mustPut(t, db, "c", "3")
	mustPut(t, db, "d", "4")

	err := db.Update(func(rwDB *WriteTx) error {
		it1 := rwDB.NewIter()
		it2 := rwDB.NewIter()
		defer it1.Close()
		defer it2.Close()

		// Forward scan with it1.
		var fwd []string
		it1.SeekFirst()

		// Backward scan with it2.
		var bwd []string
		it2.SeekLast()

		for it1.Valid() || it2.Valid() {
			if it1.Valid() {
				fwd = append(fwd, string(it1.Key()))
				it1.Next()
			}

			if it2.Valid() {
				bwd = append(bwd, string(it2.Key()))
				it2.Prev()
			}
		}

		expectKeys(t, "MultiIter-fwd", fwd, []string{"a", "b", "c", "d"})
		expectKeys(t, "MultiIter-bwd", bwd, []string{"d", "c", "b", "a"})

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestTx_MultipleIteratorsView(t *testing.T) {
	db, _ := openTestDB(t, nil)
	mustPut(t, db, "a", "1")
	mustPut(t, db, "b", "2")
	mustPut(t, db, "c", "3")
	mustPut(t, db, "d", "4")

	err := db.View(func(roDB *ReadOnlyTx) error {
		it1 := roDB.NewIter()
		it2 := roDB.NewIter()
		defer it1.Close()
		defer it2.Close()

		// Forward scan with it1.
		var fwd []string
		it1.SeekFirst()

		// Backward scan with it2.
		var bwd []string
		it2.SeekLast()

		for it1.Valid() || it2.Valid() {
			if it1.Valid() {
				fwd = append(fwd, string(it1.Key()))
				it1.Next()
			}

			if it2.Valid() {
				bwd = append(bwd, string(it2.Key()))
				it2.Prev()
			}
		}

		expectKeys(t, "MultiIter-fwd", fwd, []string{"a", "b", "c", "d"})
		expectKeys(t, "MultiIter-bwd", bwd, []string{"d", "c", "b", "a"})

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestTx_UpdateAscendDescend(t *testing.T) {
	db, _ := openTestDB(t, nil)
	mustPut(t, db, "a", "1")
	mustPut(t, db, "b", "2")
	mustPut(t, db, "c", "3")
	mustPut(t, db, "d", "4")

	err := db.Update(func(rwDB *WriteTx) error {
		// Ascend from "b".
		var asc []string
		rwDB.Ascend("b", func(key string, value []byte, vtyp uint64, hlc HLC) bool {
			asc = append(asc, key)
			return true
		})
		expectKeys(t, "Ascend-from-b", asc, []string{"b", "c", "d"})

		// Descend from "c".
		var desc []string
		rwDB.Descend("c", func(key string, value []byte, vtyp uint64, hlc HLC) bool {
			desc = append(desc, key)
			return true
		})
		expectKeys(t, "Descend-from-c", desc, []string{"c", "b", "a"})

		// AscendRange [b, d).
		var rng []string
		rwDB.AscendRange("b", "d", func(key string, value []byte, vtyp uint64, hlc HLC) bool {
			rng = append(rng, key)
			return true
		})
		expectKeys(t, "AscendRange-b-d", rng, []string{"b", "c"})

		// DescendRange [c, a).
		var drng []string
		rwDB.DescendRange("c", "a", func(key string, value []byte, vtyp uint64, hlc HLC) bool {
			drng = append(drng, key)
			return true
		})
		expectKeys(t, "DescendRange-c-a", drng, []string{"c", "b"})

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestTx_UpdateDeleteRange(t *testing.T) {
	db, _ := openTestDB(t, nil)
	mustPut(t, db, "a", "1")
	mustPut(t, db, "b", "2")
	mustPut(t, db, "c", "3")
	mustPut(t, db, "d", "4")
	mustPut(t, db, "e", "5")

	err := db.Update(func(rwDB *WriteTx) error {
		// Delete range [b, d] inclusive.
		n, _, err := rwDB.DeleteRange(false, "b", "d", true, true)
		if err != nil {
			return err
		}
		if n != 3 {
			t.Fatalf("DeleteRange returned n=%d, want 3", n)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	mustGet(t, db, "a", "1")
	mustMiss(t, db, "b")
	mustMiss(t, db, "c")
	mustMiss(t, db, "d")
	mustGet(t, db, "e", "5")
}

func TestTx_ErrorPropagation(t *testing.T) {
	db, _ := openTestDB(t, nil)

	sentinel := errors.New("test sentinel error")

	// Update should propagate callback error.
	err := db.Update(func(rwDB *WriteTx) error {
		return sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("Update error = %v, want %v", err, sentinel)
	}

	// View should propagate callback error.
	err = db.View(func(roDB *ReadOnlyTx) error {
		return sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("View error = %v, want %v", err, sentinel)
	}
}
