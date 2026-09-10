package yogadb

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func expectReadBeforeAllowReadsPanic(t *testing.T, fn func()) {
	t.Helper()
	defer func() {
		r := recover()
		if r == nil {
			t.Fatalf("read before AllowReads() did not panic")
		}
		if got, want := fmt.Sprint(r), "must call db.AllowReads() first"; got != want {
			t.Fatalf("panic = %q, want %q", got, want)
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

	expectReadBeforeAllowReadsPanic(t, func() {
		_, _, _, _, _ = db.Get("missing")
	})
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

func TestReadOnlyViewsCanOverlapAfterAllowReads(t *testing.T) {
	dir := t.TempDir()
	db, err := OpenFlexDB(dir, &Config{OmitMemWalFsync: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Put("k", []byte("v"), 0); err != nil {
		t.Fatal(err)
	}
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
