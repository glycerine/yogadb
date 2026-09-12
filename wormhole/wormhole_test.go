package wormhole

import (
	"bytes"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"testing"
)

func whKey(i int) string {
	return fmt.Sprintf("k%05d", i)
}

func whVal(i int) []byte {
	return []byte(fmt.Sprintf("v%05d", i))
}

func whKV(i int) KV {
	v := whVal(i)
	return KV{Key: whKey(i), Value: v, Vptr: VPtr{Length: uint64(len(v))}, Hlc: HLC(i + 1)}
}

func collectAsc(m *Map) []string {
	var keys []string
	m.Ascend("", func(kv KV) bool {
		keys = append(keys, kv.Key)
		return true
	})
	return keys
}

func TestBasicPutGetDeleteScan(t *testing.T) {
	m := New(Options{LeafCapacity: 4})
	for _, i := range []int{7, 1, 9, 3, 5, 2, 4, 6, 8, 0} {
		if replaced := m.Put(whKV(i)); replaced {
			t.Fatalf("Put(%d) replaced unexpectedly", i)
		}
	}
	if got := m.Len(); got != 10 {
		t.Fatalf("Len=%d want 10", got)
	}
	if !m.Put(KV{Key: whKey(3), Value: []byte("new"), Vptr: VPtr{Length: 3}, Hlc: 100}) {
		t.Fatal("overwrite did not report replaced")
	}
	if got, ok := m.Get(whKey(3)); !ok || string(got.Value) != "new" {
		t.Fatalf("Get overwritten = %q,%v", got.Value, ok)
	}
	got := collectAsc(m)
	want := []string{"k00000", "k00001", "k00002", "k00003", "k00004", "k00005", "k00006", "k00007", "k00008", "k00009"}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("asc keys=%v want %v", got, want)
	}

	var desc []string
	m.Descend("", func(kv KV) bool {
		desc = append(desc, kv.Key)
		return true
	})
	sort.Sort(sort.Reverse(sort.StringSlice(want)))
	if fmt.Sprint(desc) != fmt.Sprint(want) {
		t.Fatalf("desc keys=%v want %v", desc, want)
	}

	if !m.Delete(whKey(0)) || !m.Delete(whKey(9)) || m.Delete("missing") {
		t.Fatal("delete results not as expected")
	}
	if got := m.Len(); got != 8 {
		t.Fatalf("Len after delete=%d want 8", got)
	}
	if _, ok := m.Get(whKey(0)); ok {
		t.Fatal("deleted key still found")
	}
}

func TestAgainstSortedMapModel(t *testing.T) {
	m := New(Options{LeafCapacity: 8})
	model := map[string][]byte{}
	rng := rand.New(rand.NewSource(1))
	for step := 0; step < 5000; step++ {
		k := whKey(rng.Intn(256))
		switch rng.Intn(3) {
		case 0:
			v := []byte(fmt.Sprintf("step-%d", step))
			m.Put(KV{Key: k, Value: v, Vptr: VPtr{Length: uint64(len(v))}, Hlc: HLC(step + 1)})
			model[k] = append([]byte(nil), v...)
		case 1:
			got, ok := m.Get(k)
			want, wantOK := model[k]
			if ok != wantOK || !bytes.Equal(got.Value, want) {
				t.Fatalf("step %d Get(%q)=%q,%v want %q,%v", step, k, got.Value, ok, want, wantOK)
			}
		case 2:
			got := m.Delete(k)
			_, want := model[k]
			delete(model, k)
			if got != want {
				t.Fatalf("step %d Delete(%q)=%v want %v", step, k, got, want)
			}
		}
		if step%97 == 0 {
			var want []string
			for k := range model {
				want = append(want, k)
			}
			sort.Strings(want)
			got := collectAsc(m)
			if fmt.Sprint(got) != fmt.Sprint(want) {
				t.Fatalf("step %d scan=%v want %v", step, got, want)
			}
		}
	}
}

func TestDeleteEmptyMiddleLeafKeepsSearchCorrect(t *testing.T) {
	m := New(Options{LeafCapacity: 4})
	for i := 0; i < 7; i++ {
		m.Put(whKV(i))
	}

	if !m.Delete(whKey(2)) || !m.Delete(whKey(3)) {
		t.Fatal("expected deletes to remove the middle leaf keys")
	}
	for _, i := range []int{0, 1, 4, 5, 6} {
		got, ok := m.Get(whKey(i))
		if !ok || got.Key != whKey(i) {
			t.Fatalf("Get(%q)=%#v,%v after empty leaf delete", whKey(i), got, ok)
		}
	}
	if _, ok := m.Get(whKey(2)); ok {
		t.Fatal("deleted key k00002 was found")
	}

	got := collectAsc(m)
	want := []string{"k00000", "k00001", "k00004", "k00005", "k00006"}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("asc keys after empty leaf delete=%v want %v", got, want)
	}
}

func TestConcurrentMixedAccess(t *testing.T) {
	m := New(Options{LeafCapacity: 16})
	const writers = 8
	const perWriter = 500

	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				id := w*perWriter + i
				m.Put(whKV(id))
				if i%7 == 0 {
					_, _ = m.Get(whKey(id / 2))
				}
				if i%19 == 0 {
					m.Delete(whKey(id - 3))
				}
			}
		}(w)
	}
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 300; i++ {
				prev := ""
				m.Ascend("", func(kv KV) bool {
					if prev != "" && prev >= kv.Key {
						t.Errorf("scan out of order: %q >= %q", prev, kv.Key)
						return false
					}
					prev = kv.Key
					return true
				})
			}
		}()
	}
	wg.Wait()

	keys := collectAsc(m)
	for i := 1; i < len(keys); i++ {
		if keys[i-1] >= keys[i] {
			t.Fatalf("final keys out of order at %d: %q >= %q", i, keys[i-1], keys[i])
		}
	}
	if int64(len(keys)) != m.Len() {
		t.Fatalf("scan count=%d Len=%d", len(keys), m.Len())
	}
}
