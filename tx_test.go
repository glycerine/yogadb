package yogadb

import (
	"fmt"
	"sync"
	"testing"
)

func TestTx_DoubleCommit(t *testing.T) {
	db, _ := openTestDB(t, nil)
	mustPut(t, db, "k1", "v1")

	err := db.Update(func(tx *Tx) error {
		if err := tx.Put([]byte("k2"), []byte("v2")); err != nil {
			t.Fatal(err)
		}
		err1 := tx.Commit()
		err2 := tx.Commit()
		if err1 != nil {
			t.Fatalf("first Commit: %v", err1)
		}
		if err2 != nil {
			t.Fatalf("second Commit: %v", err2)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	mustGet(t, db, "k2", "v2")
}

func TestTx_DoubleCancel(t *testing.T) {
	db, _ := openTestDB(t, nil)
	mustPut(t, db, "k1", "v1")

	err := db.Update(func(tx *Tx) error {
		if err := tx.Put([]byte("k2"), []byte("v2")); err != nil {
			t.Fatal(err)
		}
		err1 := tx.Cancel()
		err2 := tx.Cancel()
		if err1 != nil {
			t.Fatalf("first Cancel: %v", err1)
		}
		if err2 != nil {
			t.Fatalf("second Cancel: %v", err2)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	mustMiss(t, db, "k2")
}

func TestTx_CancelThenCommit(t *testing.T) {
	db, _ := openTestDB(t, nil)
	mustPut(t, db, "k1", "v1")

	err := db.Update(func(tx *Tx) error {
		if err := tx.Put([]byte("k2"), []byte("v2")); err != nil {
			t.Fatal(err)
		}
		err1 := tx.Cancel()
		err2 := tx.Commit()
		if err1 != nil {
			t.Fatalf("Cancel: %v", err1)
		}
		if err2 != ErrTxDone {
			t.Fatalf("Commit after Cancel: got %v, want ErrTxDone", err2)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	// Writes should NOT have been applied.
	mustMiss(t, db, "k2")
}

func TestTx_CommitThenCancel(t *testing.T) {
	db, _ := openTestDB(t, nil)
	mustPut(t, db, "k1", "v1")

	err := db.Update(func(tx *Tx) error {
		if err := tx.Put([]byte("k2"), []byte("v2")); err != nil {
			t.Fatal(err)
		}
		err1 := tx.Commit()
		err2 := tx.Cancel()
		if err1 != nil {
			t.Fatalf("Commit: %v", err1)
		}
		if err2 != nil {
			t.Fatalf("Cancel after Commit: got %v, want nil", err2)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	// Writes SHOULD have been applied.
	mustGet(t, db, "k2", "v2")
}

func TestTx_UpdateBasic(t *testing.T) {
	db, _ := openTestDB(t, nil)
	mustPut(t, db, "k1", "v1")

	err := db.Update(func(tx *Tx) error {
		if err := tx.Put([]byte("k2"), []byte("v2")); err != nil {
			return err
		}
		if err := tx.Put([]byte("k3"), []byte("v3")); err != nil {
			return err
		}
		return tx.Commit()
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

	err := db.View(func(tx *Tx) error {
		val, ok := tx.Get([]byte("k1"))
		if !ok || string(val) != "v1" {
			t.Fatalf("View Get(k1) = %q, %v; want v1, true", val, ok)
		}
		val, ok = tx.Get([]byte("k2"))
		if !ok || string(val) != "v2" {
			t.Fatalf("View Get(k2) = %q, %v; want v2, true", val, ok)
		}
		_, ok = tx.Get([]byte("k3"))
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

	err := db.Update(func(tx *Tx) error {
		// Should see pre-existing key.
		val, ok := tx.Get([]byte("k1"))
		if !ok || string(val) != "v1" {
			t.Fatalf("Get(k1) before write: %q, %v", val, ok)
		}

		// Write a new key.
		if err := tx.Put([]byte("k2"), []byte("v2")); err != nil {
			return err
		}

		// Should see the buffered write.
		val, ok = tx.Get([]byte("k2"))
		if !ok || string(val) != "v2" {
			t.Fatalf("Get(k2) after buffered write: %q, %v", val, ok)
		}

		// Overwrite k1 in the buffer.
		if err := tx.Put([]byte("k1"), []byte("v1-updated")); err != nil {
			return err
		}
		val, ok = tx.Get([]byte("k1"))
		if !ok || string(val) != "v1-updated" {
			t.Fatalf("Get(k1) after overwrite: %q, %v", val, ok)
		}

		// Delete k1 via buffer.
		if err := tx.Delete([]byte("k1")); err != nil {
			return err
		}
		_, ok = tx.Get([]byte("k1"))
		if ok {
			t.Fatal("Get(k1) after delete: expected not found")
		}

		return tx.Commit()
	})
	if err != nil {
		t.Fatal(err)
	}

	// k1 should be deleted, k2 should exist.
	mustMiss(t, db, "k1")
	mustGet(t, db, "k2", "v2")
}

func TestTx_CancelDiscardsWrites(t *testing.T) {
	db, _ := openTestDB(t, nil)
	mustPut(t, db, "k1", "v1")

	err := db.Update(func(tx *Tx) error {
		if err := tx.Put([]byte("k2"), []byte("v2")); err != nil {
			return err
		}
		if err := tx.Put([]byte("k1"), []byte("OVERWRITTEN")); err != nil {
			return err
		}
		return tx.Cancel()
	})
	if err != nil {
		t.Fatal(err)
	}

	// Original value preserved, new key not written.
	mustGet(t, db, "k1", "v1")
	mustMiss(t, db, "k2")
}

func TestTx_UpdateAutoCancel(t *testing.T) {
	db, _ := openTestDB(t, nil)
	mustPut(t, db, "k1", "v1")

	err := db.Update(func(tx *Tx) error {
		if err := tx.Put([]byte("k2"), []byte("v2")); err != nil {
			return err
		}
		// Return without calling Commit or Cancel.
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	// Writes should be discarded (auto-cancel).
	mustMiss(t, db, "k2")
	mustGet(t, db, "k1", "v1")
}

func TestTx_ViewPutReturnsError(t *testing.T) {
	db, _ := openTestDB(t, nil)
	mustPut(t, db, "k1", "v1")

	err := db.View(func(tx *Tx) error {
		err := tx.Put([]byte("k2"), []byte("v2"))
		if err != ErrTxNotWritable {
			t.Fatalf("Put in View: got %v, want ErrTxNotWritable", err)
		}
		err = tx.Delete([]byte("k1"))
		if err != ErrTxNotWritable {
			t.Fatalf("Delete in View: got %v, want ErrTxNotWritable", err)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
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
			errs[i] = db.Update(func(tx *Tx) error {
				// Read current counter value.
				val, ok := tx.Get([]byte("counter"))
				if !ok {
					t.Error("counter not found")
					return tx.Cancel()
				}
				// Increment.
				c := 0
				for _, b := range val {
					c = c*10 + int(b-'0')
				}
				c++
				newVal := []byte(fmt.Sprintf("%d", c))
				if err := tx.Put([]byte("counter"), newVal); err != nil {
					return err
				}
				return tx.Commit()
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
	val, ok := db.Get([]byte("counter"))
	if !ok {
		t.Fatal("counter not found after all updates")
	}
	want := fmt.Sprintf("%d", n)
	if string(val) != want {
		t.Fatalf("counter = %q, want %q", val, want)
	}
}
