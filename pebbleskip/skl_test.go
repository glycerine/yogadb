package pebbleskip

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
	"testing"
)

func testKey(i int) []byte {
	return []byte(fmt.Sprintf("%05d", i))
}

func testKeyString(i int) string {
	return fmt.Sprintf("%05d", i)
}

func testVal(i int) []byte {
	return []byte(fmt.Sprintf("v%05d", i))
}

func testKV(i int) KV {
	v := testVal(i)
	return KV{Key: testKeyString(i), Value: v, Vptr: VPtr{Length: uint64(len(v))}, Hlc: HLC(i + 1)}
}

func countForward(s *Skiplist) int {
	it := s.NewIter("", "")
	defer it.Close()
	n := 0
	for kv := it.First(); kv != nil; kv = it.Next() {
		n++
	}
	return n
}

func TestBasic(t *testing.T) {
	for _, useInserter := range []bool{false, true} {
		t.Run(fmt.Sprintf("inserter=%t", useInserter), func(t *testing.T) {
			s := New(1<<20, nil)
			add := s.Add
			if useInserter {
				var ins Inserter
				add = func(kv KV) error { return ins.Add(s, kv) }
			}

			for _, i := range []int{3, 1, 2} {
				if err := add(testKV(i)); err != nil {
					t.Fatalf("Add(%d): %v", i, err)
				}
			}
			if err := add(KV{Key: testKeyString(2), Value: []byte("dup")}); !errors.Is(err, ErrRecordExists) {
				t.Fatalf("duplicate Add err = %v, want %v", err, ErrRecordExists)
			}

			for _, i := range []int{1, 2, 3} {
				got, ok := s.Get(testKeyString(i))
				if !ok || !bytes.Equal(got.Value, testVal(i)) {
					t.Fatalf("Get(%d) = %q,%v want %q,true", i, got.Value, ok, testVal(i))
				}
			}
			if got, ok := s.Get("99999"); ok || got.Key != "" {
				t.Fatalf("missing Get = %q,%v want nil,false", got, ok)
			}

			it := s.NewIter("", "")
			defer it.Close()
			for i, kv := 1, it.First(); i <= 3; i, kv = i+1, it.Next() {
				if kv == nil || kv.Key != testKeyString(i) || !bytes.Equal(kv.Value, testVal(i)) {
					t.Fatalf("forward[%d] = %#v", i, kv)
				}
			}
			if kv := it.Next(); kv != nil {
				t.Fatalf("Next after end = %#v, want nil", kv)
			}

			for i, kv := 3, it.Last(); i >= 1; i, kv = i-1, it.Prev() {
				if kv == nil || kv.Key != testKeyString(i) {
					t.Fatalf("reverse[%d] = %#v", i, kv)
				}
			}
			if kv := it.Prev(); kv != nil {
				t.Fatalf("Prev after beginning = %#v, want nil", kv)
			}
		})
	}
}

func TestBoundsAndSeek(t *testing.T) {
	s := New(1<<20, nil)
	for i := 0; i < 10; i++ {
		if err := s.Add(testKV(i)); err != nil {
			t.Fatal(err)
		}
	}

	it := s.NewIter(testKeyString(3), testKeyString(7))
	defer it.Close()
	if kv := it.SeekGE(testKeyString(4)); kv == nil || kv.Key != testKeyString(4) {
		t.Fatalf("SeekGE bounded = %#v", kv)
	}
	var keys []string
	for kv := it.First(); kv != nil; kv = it.Next() {
		keys = append(keys, kv.Key)
	}
	want := []string{"00000", "00001", "00002", "00003", "00004", "00005", "00006"}
	if fmt.Sprint(keys) != fmt.Sprint(want) {
		t.Fatalf("bounded forward keys=%v want %v", keys, want)
	}
	if kv := it.SeekLT(testKeyString(3)); kv != nil {
		t.Fatalf("SeekLT lower bound = %#v, want nil", kv)
	}
}

func TestArenaFull(t *testing.T) {
	s := New(512, nil)
	for i := 0; ; i++ {
		err := s.Add(testKV(i))
		if errors.Is(err, ErrArenaFull) {
			return
		}
		if err != nil {
			t.Fatalf("Add(%d): %v", i, err)
		}
		if i > 1000 {
			t.Fatal("expected arena to fill")
		}
	}
}

func TestInserterRejectsDuplicateCachedNext(t *testing.T) {
	s := New(1<<20, nil)
	if err := s.Add(testKV(100)); err != nil {
		t.Fatal(err)
	}
	if err := s.Add(testKV(120)); err != nil {
		t.Fatal(err)
	}

	prev, next, found := s.findSpliceForLevel(testKeyString(110), 0, s.head)
	if found || prev == s.head || next == s.tail {
		t.Fatalf("test setup failed: prev=%#v next=%#v found=%v", prev, next, found)
	}

	var ins Inserter
	ins.height = 1
	ins.spl[0].init(prev, next)
	s.height.Store(1)

	if err := ins.Add(s, testKV(120)); !errors.Is(err, ErrRecordExists) {
		t.Fatalf("cached-next duplicate Add err = %v, want %v", err, ErrRecordExists)
	}
}

func TestConcurrentAddGetIterate(t *testing.T) {
	const n = 2000
	s := New(8<<20, nil)
	s.EnableTestingDelays(true)

	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if err := s.Add(testKV(i)); err != nil {
				t.Errorf("Add(%d): %v", i, err)
			}
		}(i)
	}
	wg.Wait()

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			got, ok := s.Get(testKeyString(i))
			if !ok || !bytes.Equal(got.Value, testVal(i)) {
				t.Errorf("Get(%d) = %q,%v", i, got.Value, ok)
			}
		}(i)
	}
	wg.Wait()

	if got := countForward(s); got != n {
		t.Fatalf("countForward = %d, want %d", got, n)
	}

	it := s.NewIter("", "")
	defer it.Close()
	var prev []byte
	for kv := it.First(); kv != nil; kv = it.Next() {
		if prev != nil && bytes.Compare(prev, []byte(kv.Key)) >= 0 {
			t.Fatalf("keys out of order: prev=%q cur=%q", prev, kv.Key)
		}
		prev = append(prev[:0], kv.Key...)
	}
}
