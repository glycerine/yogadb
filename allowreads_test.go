package yogadb

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

func expectBeforeAllowReadsPanic(t *testing.T, fn func()) {
	t.Helper()
	defer func() {
		r := recover()
		if r == nil {
			t.Fatalf("operation before AllowReads() did not panic")
		}
		if got, want := fmt.Sprint(r), "must call db.AllowReads() first"; !strings.HasPrefix(got, want) {
			t.Fatalf("panic = %q, want prefix %q", got, want)
		}
	}()
	fn()
}

func TestAllowReadsRequiredBeforeGet(t *testing.T) {
	dir := t.TempDir()
	db, err := OpenFlexDB(dir, &Config{OmitMemWalFsync: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	expectBeforeAllowReadsPanic(t, func() {
		_, _, _, _, _ = db.Get("missing")
	})
}

func loadOneBulkKeyBeforeAllowReads(t *testing.T, db *FlexDB) {
	t.Helper()
	b := db.NewBatch()
	if err := b.Set("bulk-key", []byte("bulk-value"), 0); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Commit(false); err != nil {
		t.Fatal(err)
	}
	b.Close()
	if db.allowReads.Load() {
		t.Fatal("test setup unexpectedly allowed reads")
	}
	if db.mt.bulk.count == 0 {
		t.Fatal("test setup did not leave initial load in bulk memtable")
	}
}

func assertBulkKeySurvivedRejectedPreAllowReadsOp(t *testing.T, db *FlexDB) {
	t.Helper()
	if db.allowReads.Load() {
		t.Fatal("rejected pre-AllowReads operation unexpectedly allowed reads")
	}
	if db.mt.bulk.count == 0 {
		t.Fatal("rejected pre-AllowReads operation materialized or discarded bulk state")
	}
	db.AllowReads()
	got, found, _, _, err := db.Get("bulk-key")
	if err != nil {
		t.Fatalf("Get after AllowReads: %v", err)
	}
	if !found || string(got) != "bulk-value" {
		t.Fatalf("Get after rejected pre-AllowReads operation = %q, %v; want bulk-value, true", got, found)
	}
}

func TestOnlyBatchLoadAllowedBeforeAllowReads(t *testing.T) {
	tests := []struct {
		name string
		op   func(*testing.T, *FlexDB)
	}{
		{
			name: "Put",
			op: func(t *testing.T, db *FlexDB) {
				_, _ = db.Put("late-key", []byte("late-value"), 0)
			},
		},
		{
			name: "Delete",
			op: func(t *testing.T, db *FlexDB) {
				_ = db.Delete("bulk-key")
			},
		},
		{
			name: "DeleteRange",
			op: func(t *testing.T, db *FlexDB) {
				_, _, _ = db.DeleteRange(true, "bulk-key", "bulk-key", true, true)
			},
		},
		{
			name: "ClearAll",
			op: func(t *testing.T, db *FlexDB) {
				_, _ = db.Clear(true)
			},
		},
		{
			name: "ClearSmall",
			op: func(t *testing.T, db *FlexDB) {
				_, _ = db.Clear(false)
			},
		},
		{
			name: "Merge",
			op: func(t *testing.T, db *FlexDB) {
				_ = db.Merge("bulk-key", func(oldVal []byte, exists bool, oldVtyp uint64) ([]byte, bool, bool, uint64) {
					return nil, false, false, 0
				})
			},
		},
		{
			name: "VacuumVLOG",
			op: func(t *testing.T, db *FlexDB) {
				_, _ = db.VacuumVLOG()
			},
		},
		{
			name: "VacuumKV",
			op: func(t *testing.T, db *FlexDB) {
				_, _ = db.VacuumKV()
			},
		},
		{
			name: "Update",
			op: func(t *testing.T, db *FlexDB) {
				_ = db.Update(func(tx *WriteTx) error {
					_, err := tx.Put("late-key", []byte("late-value"), 0)
					return err
				})
			},
		},
		{
			name: "BeginUpdate",
			op: func(t *testing.T, db *FlexDB) {
				tx, err := db.BeginUpdate()
				if err == nil {
					defer tx.Rollback()
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			db, err := OpenFlexDB(dir, &Config{OmitMemWalFsync: true})
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()

			loadOneBulkKeyBeforeAllowReads(t, db)
			expectBeforeAllowReadsPanic(t, func() {
				tt.op(t, db)
			})
			assertBulkKeySurvivedRejectedPreAllowReadsOp(t, db)
		})
	}
}

func TestAllowReadsMaterializesBulkInitialData(t *testing.T) {
	dir := t.TempDir()
	db, err := OpenFlexDB(dir, &Config{OmitMemWalFsync: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	b := db.NewBatch()
	for i := 0; i < 100; i++ {
		k := fmt.Sprintf("key%03d", i)
		b.Set(k, []byte(k), 0)
	}
	if _, err := b.Commit(false); err != nil {
		t.Fatal(err)
	}

	db.AllowReads()

	if got := db.Len(); got != 100 {
		t.Fatalf("Len() = %d, want 100", got)
	}
	for _, k := range []string{"key000", "key050", "key099"} {
		got, found, _, _, err := db.Get(k)
		if err != nil {
			t.Fatalf("Get(%q): %v", k, err)
		}
		if !found || string(got) != k {
			t.Fatalf("Get(%q) = %q, %v; want %q, true", k, got, found, k)
		}
	}
}

func TestBatchSetBytesAllowedBeforeAllowReads(t *testing.T) {
	dir := t.TempDir()
	db, err := OpenFlexDB(dir, &Config{OmitMemWalFsync: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	key := []byte("bytes-key")
	b := db.NewBatch()
	if err := b.SetBytes(key, key, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Commit(false); err != nil {
		t.Fatal(err)
	}
	b.Close()

	db.AllowReads()
	got, found, _, _, err := db.Get("bytes-key")
	if err != nil {
		t.Fatal(err)
	}
	if !found || string(got) != "bytes-key" {
		t.Fatalf("Get(bytes-key) = %q, %v; want bytes-key, true", got, found)
	}
}

func TestBatchDeleteBeforeAllowReadsPanics(t *testing.T) {
	dir := t.TempDir()
	db, err := OpenFlexDB(dir, &Config{OmitMemWalFsync: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	b := db.NewBatch()
	expectBeforeAllowReadsPanic(t, func() {
		b.Delete("bulk-key")
	})
}

func TestReadOnlyViewsCanOverlapAfterAllowReads(t *testing.T) {
	dir := t.TempDir()
	db, err := OpenFlexDB(dir, &Config{OmitMemWalFsync: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	b := db.NewBatch()
	if err := b.Set("k", []byte("v"), 0); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Commit(false); err != nil {
		t.Fatal(err)
	}
	b.Close()
	db.AllowReads()

	firstEntered := make(chan struct{})
	releaseFirst := make(chan struct{})
	firstDone := make(chan error, 1)
	go func() {
		firstDone <- db.View(func(ro *ReadOnlyTx) error {
			close(firstEntered)
			<-releaseFirst
			return nil
		})
	}()

	<-firstEntered

	secondEntered := make(chan struct{})
	secondDone := make(chan error, 1)
	go func() {
		secondDone <- db.View(func(ro *ReadOnlyTx) error {
			close(secondEntered)
			_, found, _, _, err := ro.Get("k")
			if err != nil {
				return err
			}
			if !found {
				return fmt.Errorf("missing k")
			}
			return nil
		})
	}()

	select {
	case <-secondEntered:
	case <-time.After(200 * time.Millisecond):
		close(releaseFirst)
		t.Fatalf("second View did not overlap first View")
	}

	close(releaseFirst)
	if err := <-firstDone; err != nil {
		t.Fatalf("first View: %v", err)
	}
	if err := <-secondDone; err != nil {
		t.Fatalf("second View: %v", err)
	}
}

func TestConcurrentGetsAfterAllowReadsRaceFree(t *testing.T) {
	dir := t.TempDir()
	db, err := OpenFlexDB(dir, &Config{OmitMemWalFsync: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	b := db.NewBatch()
	for i := 0; i < 128; i++ {
		k := fmt.Sprintf("key%03d", i)
		b.Set(k, []byte(k), 0)
	}
	if _, err := b.Commit(false); err != nil {
		t.Fatal(err)
	}
	if err := db.Sync(); err != nil {
		t.Fatal(err)
	}
	db.AllowReads()

	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 128; i++ {
				k := fmt.Sprintf("key%03d", i)
				got, found, _, _, err := db.Get(k)
				if err != nil {
					t.Errorf("Get(%q): %v", k, err)
					return
				}
				if !found || string(got) != k {
					t.Errorf("Get(%q) = %q, %v; want %q, true", k, got, found, k)
					return
				}
			}
		}()
	}
	wg.Wait()
}
