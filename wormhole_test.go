package yogadb

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

func collectAsc(m *wormhole) []string {
	var keys []string
	const x = true
	m.Ascend("", x, func(kv KV) bool {
		keys = append(keys, kv.Key)
		return true
	})
	return keys
}

func TestBasicPutGetDeleteScan(t *testing.T) {
	m := newWormhole(wormConfig{leafCapacity: 4})
	const x = true
	for _, i := range []int{7, 1, 9, 3, 5, 2, 4, 6, 8, 0} {
		if old, replaced := m.Put(whKV(i), x); replaced || !equalKV(old, KV{}) {
			t.Fatalf("Put(%d) replaced unexpectedly", i)
		}
	}
	if got := m.Len(); got != 10 {
		t.Fatalf("Len=%d want 10", got)
	}
	old, replaced := m.Put(KV{Key: whKey(3), Value: []byte("new"), Vptr: VPtr{Length: 3}, Hlc: 100}, x)
	if !replaced {
		t.Fatal("overwrite did not report replaced")
	}
	if !equalKV(old, whKV(3)) {
		t.Fatalf("overwrite old=%#v want %#v", old, whKV(3))
	}
	if got, ok := m.Get(whKey(3), x); !ok || string(got.Value) != "new" {
		t.Fatalf("Get overwritten = %q,%v", got.Value, ok)
	}
	old, replaced = m.Put(KV{Key: whKey(9), Value: []byte("tail"), Vptr: VPtr{Length: 4}, Hlc: 101}, x)
	if !replaced {
		t.Fatal("tail overwrite did not report replaced")
	}
	if !equalKV(old, whKV(9)) {
		t.Fatalf("tail overwrite old=%#v want %#v", old, whKV(9))
	}
	if got, ok := m.Get(whKey(9), x); !ok || string(got.Value) != "tail" {
		t.Fatalf("Get tail overwritten = %q,%v", got.Value, ok)
	}
	got := collectAsc(m)
	want := []string{"k00000", "k00001", "k00002", "k00003", "k00004", "k00005", "k00006", "k00007", "k00008", "k00009"}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("asc keys=%v want %v", got, want)
	}

	var desc []string
	m.Descend("", x, func(kv KV) bool {
		desc = append(desc, kv.Key)
		return true
	})
	sort.Sort(sort.Reverse(sort.StringSlice(want)))
	if fmt.Sprint(desc) != fmt.Sprint(want) {
		t.Fatalf("desc keys=%v want %v", desc, want)
	}

	if !m.Delete(whKey(0), x) || !m.Delete(whKey(9), x) || m.Delete("missing", x) {
		t.Fatal("delete results not as expected")
	}
	if got := m.Len(); got != 8 {
		t.Fatalf("Len after delete=%d want 8", got)
	}
	if _, ok := m.Get(whKey(0), x); ok {
		t.Fatal("deleted key still found")
	}
}

func TestDescendRange(t *testing.T) {
	m := newWormhole(wormConfig{leafCapacity: 4})
	const x = true
	for i := 0; i < 10; i++ {
		m.Put(whKV(i), x)
	}

	tests := []struct {
		lessOrEqual string
		greaterThan string
		want        []string
	}{
		{"", "", []string{"k00009", "k00008", "k00007", "k00006", "k00005", "k00004", "k00003", "k00002", "k00001", "k00000"}},
		{"k00007", "k00003", []string{"k00007", "k00006", "k00005", "k00004"}},
		{"", "k00007", []string{"k00009", "k00008"}},
		{"k00003", "", []string{"k00003", "k00002", "k00001", "k00000"}},
		{"k00005", "k00005", nil},
		{"k00003", "k00007", nil},
	}

	for _, tc := range tests {
		var got []string
		m.DescendRange(tc.lessOrEqual, tc.greaterThan, x, func(kv KV) bool {
			got = append(got, kv.Key)
			return true
		})
		if fmt.Sprint(got) != fmt.Sprint(tc.want) {
			t.Fatalf("DescendRange(%q,%q)=%v want %v", tc.lessOrEqual, tc.greaterThan, got, tc.want)
		}
	}

	var early []string
	m.DescendRange("", "", x, func(kv KV) bool {
		early = append(early, kv.Key)
		return len(early) < 3
	})
	wantEarly := []string{"k00009", "k00008", "k00007"}
	if fmt.Sprint(early) != fmt.Sprint(wantEarly) {
		t.Fatalf("early stop DescendRange=%v want %v", early, wantEarly)
	}
}

func TestAgainstSortedMapModel(t *testing.T) {
	m := newWormhole(wormConfig{leafCapacity: 8})
	model := map[string][]byte{}
	rng := rand.New(rand.NewSource(1))
	const x = true
	for step := 0; step < 5000; step++ {
		k := whKey(rng.Intn(256))
		switch rng.Intn(3) {
		case 0:
			v := []byte(fmt.Sprintf("step-%d", step))
			m.Put(KV{Key: k, Value: v, Vptr: VPtr{Length: uint64(len(v))}, Hlc: HLC(step + 1)}, x)
			model[k] = append([]byte(nil), v...)
		case 1:
			got, ok := m.Get(k, x)
			want, wantOK := model[k]
			if ok != wantOK || !bytes.Equal(got.Value, want) {
				t.Fatalf("step %d Get(%q)=%q,%v want %q,%v", step, k, got.Value, ok, want, wantOK)
			}
		case 2:
			got := m.Delete(k, x)
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
	m := newWormhole(wormConfig{leafCapacity: 4})
	const x = true
	for i := 0; i < 7; i++ {
		m.Put(whKV(i), x)
	}

	if !m.Delete(whKey(2), x) || !m.Delete(whKey(3), x) {
		t.Fatal("expected deletes to remove the middle wormLeaf keys")
	}
	for _, i := range []int{0, 1, 4, 5, 6} {
		got, ok := m.Get(whKey(i), x)
		if !ok || got.Key != whKey(i) {
			t.Fatalf("Get(%q)=%#v,%v after empty wormLeaf delete", whKey(i), got, ok)
		}
	}
	if _, ok := m.Get(whKey(2), x); ok {
		t.Fatal("deleted key k00002 was found")
	}

	got := collectAsc(m)
	want := []string{"k00000", "k00001", "k00004", "k00005", "k00006"}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("asc keys after empty wormLeaf delete=%v want %v", got, want)
	}
}

func TestBuildPointIndexTracksMutation(t *testing.T) {
	m := newWormhole(wormConfig{leafCapacity: 4})
	const x = true
	for i := 0; i < 8; i++ {
		m.Put(whKV(i), x)
	}
	m.BuildPointIndex(x)

	got, ok := m.Get(whKey(3), x)
	if !ok || got.Key != whKey(3) {
		t.Fatalf("indexed Get(%q)=%#v,%v", whKey(3), got, ok)
	}

	old, replaced := m.Put(KV{Key: whKey(3), Value: []byte("after"), Vptr: VPtr{Length: 5}, Hlc: 99}, x)
	if !replaced {
		t.Fatal("overwrite after BuildPointIndex did not report replaced")
	}
	if !equalKV(old, whKV(3)) {
		t.Fatalf("overwrite after BuildPointIndex old=%#v want %#v", old, whKV(3))
	}
	got, ok = m.Get(whKey(3), x)
	if !ok || string(got.Value) != "after" {
		t.Fatalf("Get after indexed overwrite=%#v,%v", got, ok)
	}

	if old, replaced := m.Put(KV{Key: whKey(99), Value: []byte("new"), Vptr: VPtr{Length: 3}, Hlc: 199}, x); replaced || !equalKV(old, KV{}) {
		t.Fatal("new insert after BuildPointIndex reported replaced")
	}
	got, ok = m.Get(whKey(99), x)
	if !ok || string(got.Value) != "new" {
		t.Fatalf("Get after indexed insert=%#v,%v", got, ok)
	}

	if !m.Delete(whKey(3), x) {
		t.Fatal("delete after BuildPointIndex failed")
	}
	if got, ok := m.Get(whKey(3), x); ok {
		t.Fatalf("Get after indexed delete=%#v,true", got)
	}
}

func TestClearReusesStorePages(t *testing.T) {
	m := newWormhole(wormConfig{leafCapacity: 4})
	const x = true
	for i := 0; i < 32; i++ {
		m.Put(whKV(i), x)
	}
	m.BuildPointIndex(x)

	block := m.store.blocks[0].Load()
	if block == nil {
		t.Fatal("expected allocated store block before clear")
	}
	page := block.pages[0].Load()
	if page == nil {
		t.Fatal("expected allocated store page before clear")
	}
	if len(m.leaves) < 2 {
		t.Fatal("test setup expected multiple leaves before clear")
	}

	m.clear()
	if got := m.Len(); got != 0 {
		t.Fatalf("Len after clear=%d want 0", got)
	}
	if got := m.store.next.Load(); got != 0 {
		t.Fatalf("store next after clear=%d want 0", got)
	}
	if got := len(m.leaves); got != 1 {
		t.Fatalf("leaf count after clear=%d want 1", got)
	}
	if m.tail != m.leaves[0] {
		t.Fatal("tail after clear does not point at the sole leaf")
	}
	if got, ok := m.Get(whKey(3), x); ok {
		t.Fatalf("Get after clear=%#v,true", got)
	}
	if got := m.store.blocks[0].Load(); got != block {
		t.Fatal("clear replaced the store block instead of reusing it")
	}
	if got := block.pages[0].Load(); got != page {
		t.Fatal("clear replaced the store page instead of reusing it")
	}
	if kv := page.kvs[0]; kv.Key != "" || kv.Value != nil || kv.Vptr != (VPtr{}) || kv.Hlc != 0 {
		t.Fatalf("clear did not zero old KV slot: %#v", kv)
	}

	if old, replaced := m.Put(whKV(100), x); replaced || !equalKV(old, KV{}) {
		t.Fatal("first Put after clear reported replaced")
	}
	got, ok := m.Get(whKey(100), x)
	if !ok || got.Key != whKey(100) {
		t.Fatalf("Get after reuse=%#v,%v", got, ok)
	}
	if got := m.store.blocks[0].Load(); got != block {
		t.Fatal("store block was not reused after Put")
	}
	if got := block.pages[0].Load(); got != page {
		t.Fatal("store page was not reused after Put")
	}
}

func TestConcurrentMixedAccess(t *testing.T) {
	m := newWormhole(wormConfig{leafCapacity: 16})
	const x = false
	const writers = 8
	const perWriter = 500

	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				id := w*perWriter + i
				m.Put(whKV(id), x)
				if i%7 == 0 {
					_, _ = m.Get(whKey(id/2), x)
				}
				if i%19 == 0 {
					m.Delete(whKey(id-3), x)
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
				m.Ascend("", x, func(kv KV) bool {
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
