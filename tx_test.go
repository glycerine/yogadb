package yogadb

import (
	"errors"
	"fmt"
	"sync"
	"testing"
)

func TestTx_UpdateBasic(t *testing.T) {
	db, _ := openTestDB(t, nil)
	mustPut(t, db, "k1", "v1")

	err := db.Update(func(rwDB *WriteTx) error {
		if err := rwDB.Put("k2", []byte("v2")); err != nil {
			return err
		}
		if err := rwDB.Put("k3", []byte("v3")); err != nil {
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

func TestTx_ViewBasic(t *testing.T) {
	db, _ := openTestDB(t, nil)
	mustPut(t, db, "k1", "v1")
	mustPut(t, db, "k2", "v2")

	err := db.View(func(roDB *ReadOnlyTx) error {
		val, ok, gerr := roDB.Get("k1")
		panicOn(gerr)
		if !ok || string(val) != "v1" {
			t.Fatalf("View Get(k1) = %q, %v; want v1, true", val, ok)
		}
		val, ok, gerr = roDB.Get("k2")
		panicOn(gerr)
		if !ok || string(val) != "v2" {
			t.Fatalf("View Get(k2) = %q, %v; want v2, true", val, ok)
		}
		_, ok, gerr = roDB.Get("k3")
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
		val, ok, gerr := rwDB.Get("k1")
		panicOn(gerr)
		if !ok || string(val) != "v1" {
			t.Fatalf("Get(k1) before write: %q, %v", val, ok)
		}

		// Write a new key.
		if err := rwDB.Put("k2", []byte("v2")); err != nil {
			return err
		}

		// Should see the write immediately.
		val, ok, gerr = rwDB.Get("k2")
		panicOn(gerr)
		if !ok || string(val) != "v2" {
			t.Fatalf("Get(k2) after write: %q, %v", val, ok)
		}

		// Overwrite k1.
		if err := rwDB.Put("k1", []byte("v1-updated")); err != nil {
			return err
		}
		val, ok, gerr = rwDB.Get("k1")
		panicOn(gerr)
		if !ok || string(val) != "v1-updated" {
			t.Fatalf("Get(k1) after overwrite: %q, %v", val, ok)
		}

		// Delete k1.
		if err := rwDB.Delete("k1"); err != nil {
			return err
		}
		_, ok, gerr = rwDB.Get("k1")
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
		val, ok, gerr := roDB.Get("k1")
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
				val, ok, gerr := rwDB.Get("counter")
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
				return rwDB.Put("counter", newVal)
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
	val, ok, gerr := db.Get("counter")
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
		it.SeekToFirst()
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
		it.SeekToFirst()
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
		it1.SeekToFirst()

		// Backward scan with it2.
		var bwd []string
		it2.SeekToLast()

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
		it1.SeekToFirst()

		// Backward scan with it2.
		var bwd []string
		it2.SeekToLast()

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
		rwDB.Ascend("b", func(key string, value []byte) bool {
			asc = append(asc, key)
			return true
		})
		expectKeys(t, "Ascend-from-b", asc, []string{"b", "c", "d"})

		// Descend from "c".
		var desc []string
		rwDB.Descend("c", func(key string, value []byte) bool {
			desc = append(desc, key)
			return true
		})
		expectKeys(t, "Descend-from-c", desc, []string{"c", "b", "a"})

		// AscendRange [b, d).
		var rng []string
		rwDB.AscendRange("b", "d", func(key string, value []byte) bool {
			rng = append(rng, key)
			return true
		})
		expectKeys(t, "AscendRange-b-d", rng, []string{"b", "c"})

		// DescendRange [c, a).
		var drng []string
		rwDB.DescendRange("c", "a", func(key string, value []byte) bool {
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
