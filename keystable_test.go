package yogadb

import (
	"bytes"
	"math/rand"
	"slices"
	"sort"
	"testing"
	"unsafe"
)

func TestKeyStableAddFindAndSortedOrder(t *testing.T) {
	s := newKeyStable(16)
	model := make(map[string]int)
	for _, key := range [][]byte{
		[]byte("delta"),
		[]byte("alpha"),
		[]byte("charlie"),
		[]byte("bravo"),
		nil,
		[]byte("bravo\x00"),
		[]byte("apple"),
	} {
		idx := s.addKey(key)
		model[string(key)] = idx
	}

	want := []string{"", "alpha", "apple", "bravo", "bravo\x00", "charlie", "delta"}
	if got := keyStableSortedKeys(s); !slices.Equal(got, want) {
		t.Fatalf("sorted keys = %#v, want %#v", got, want)
	}
	assertKeyStableMatchesModel(t, s, model)

	stableBefore, keysBefore, sortedBefore := len(s.stable), len(s.keys), len(s.sorted)
	dup := s.addKey([]byte("charlie"))
	if dup != model["charlie"] {
		t.Fatalf("duplicate add returned stable index %d, want %d", dup, model["charlie"])
	}
	if len(s.stable) != stableBefore || len(s.keys) != keysBefore || len(s.sorted) != sortedBefore {
		t.Fatalf("duplicate add changed lengths: stable=%d/%d keys=%d/%d sorted=%d/%d",
			len(s.stable), stableBefore, len(s.keys), keysBefore, len(s.sorted), sortedBefore)
	}
	assertKeyStableMatchesModel(t, s, model)

	for _, tc := range []struct {
		needle string
		where  int
	}{
		{needle: "aardvark", where: 1},
		{needle: "bravo\x01", where: 5},
		{needle: "zz", where: len(want)},
	} {
		where, found := s.findKey([]byte(tc.needle))
		if found || where != tc.where {
			t.Fatalf("findKey(%q) = (%d, %v), want (%d, false)", tc.needle, where, found, tc.where)
		}
	}
}

func TestKeyStableCopiesInputBytes(t *testing.T) {
	s := newKeyStable(1)
	key := []byte("mutable")
	idx := s.addKey(key)
	for i := range key {
		key[i] = 'x'
	}

	if got := string(s.at(idx)); got != "mutable" {
		t.Fatalf("stored key = %q after input mutation, want %q", got, "mutable")
	}
	if _, found := s.findKey([]byte("mutable")); !found {
		t.Fatal("findKey should still find the original copied key")
	}
	if _, found := s.findKey(key); found {
		t.Fatalf("findKey found mutated caller buffer %q", key)
	}
}

func TestKeyStableDeleteAndReAdd(t *testing.T) {
	s := newKeyStable(8)
	model := make(map[string]int)
	for _, key := range []string{"c", "a", "b", "aa"} {
		model[key] = s.addKey([]byte(key))
	}
	assertKeyStableMatchesModel(t, s, model)

	oldA := model["a"]
	if !s.delKey([]byte("a")) {
		t.Fatal("delKey(a) = false, want true")
	}
	delete(model, "a")
	assertKeyStableMatchesModel(t, s, model)
	if s.delKey([]byte("a")) {
		t.Fatal("second delKey(a) = true, want false")
	}
	if got, want := keyStableTombKeys(s), []string{"a"}; !slices.Equal(got, want) {
		t.Fatalf("tomb keys = %#v, want %#v", got, want)
	}

	newA := s.addKey([]byte("a"))
	if newA == oldA {
		t.Fatalf("re-added key reused tombstoned stable index %d", oldA)
	}
	model["a"] = newA
	assertKeyStableMatchesModel(t, s, model)
	if got, want := keyStableSortedKeys(s), []string{"a", "aa", "b", "c"}; !slices.Equal(got, want) {
		t.Fatalf("sorted keys after re-add = %#v, want %#v", got, want)
	}
}

func TestKeyStableTombKeysStaySorted(t *testing.T) {
	s := newKeyStable(8)
	for _, key := range []string{"c", "a", "b", "aa"} {
		s.addKey([]byte(key))
	}
	for _, key := range []string{"c", "a", "b"} {
		if !s.delKey([]byte(key)) {
			t.Fatalf("delKey(%q) = false, want true", key)
		}
	}

	if got, want := keyStableTombKeys(s), []string{"a", "b", "c"}; !slices.Equal(got, want) {
		t.Fatalf("tomb keys = %#v, want %#v", got, want)
	}
	if got, want := keyStableSortedKeys(s), []string{"aa"}; !slices.Equal(got, want) {
		t.Fatalf("active keys = %#v, want %#v", got, want)
	}
}

func TestKeyStableSetGetAndReplaceKV(t *testing.T) {
	s := newKeyStable(4)
	for _, kv := range []KV{
		{Key: "b", Value: []byte("vb"), Vptr: VPtr{Length: 2}, Hlc: 1},
		{Key: "a", Value: []byte("va"), Vptr: VPtr{Length: 2}, Hlc: 2},
	} {
		if old, replaced := s.set(kv); replaced {
			t.Fatalf("set(%q) replaced old KV %#v, want fresh insert", kv.Key, old)
		}
	}

	old, replaced := s.set(KV{Key: "b", Value: []byte("vb2"), Vptr: VPtr{Length: 3, Offset: 99}, Hlc: 3})
	if !replaced {
		t.Fatal("set duplicate did not report replacement")
	}
	if old.Key != "b" || string(old.Value) != "vb" || old.Hlc != 1 {
		t.Fatalf("old KV = %#v, want key b value vb hlc 1", old)
	}
	if s.Len() != 2 {
		t.Fatalf("Len after replacement = %d, want 2", s.Len())
	}

	got, found := s.get("b")
	if !found {
		t.Fatal("get(b) was not found")
	}
	if got.Key != "b" || string(got.Value) != "vb2" || got.Vptr.Offset != 99 || got.Hlc != 3 {
		t.Fatalf("get(b) = %#v, want replacement KV", got)
	}
	if got := keyStableSortedKVKeys(s); !slices.Equal(got, []string{"a", "b"}) {
		t.Fatalf("sorted KV keys = %#v, want %#v", got, []string{"a", "b"})
	}
}

func TestKeyStableSetCopiesKeyAndPreservesValueAlias(t *testing.T) {
	s := newKeyStable(1)
	key := []byte("alias-key")
	keyString := unsafe.String(unsafe.SliceData(key), len(key))
	kv := KV{
		Key:   keyString,
		Value: key,
		Vptr:  VPtr{Length: uint64(len(key))},
		Hlc:   7,
	}
	if !slottedInlineValueAliasesKey(kv) {
		t.Fatal("test setup should have key/value aliasing")
	}

	if _, replaced := s.set(kv); replaced {
		t.Fatal("first set replaced an existing key")
	}
	for i := range key {
		key[i] = 'x'
	}

	got, found := s.get("alias-key")
	if !found {
		t.Fatal("get(alias-key) was not found")
	}
	if got.Key != "alias-key" || string(got.Value) != "alias-key" || got.Hlc != 7 {
		t.Fatalf("arena-owned alias KV = %#v, want key/value alias-key hlc 7", got)
	}
	if !slottedInlineValueAliasesKey(got) {
		t.Fatalf("returned KV should preserve key/value aliasing: %#v", got)
	}
	if _, found := s.get("xxxxxxxxx"); found {
		t.Fatal("mutating caller buffer changed keyStable lookup key")
	}
}

func TestKeyStableClearReusesTable(t *testing.T) {
	s := newKeyStable(4)
	s.addKey([]byte("b"))
	s.addKey([]byte("a"))
	s.delKey([]byte("a"))

	s.clear()
	if len(s.keys) != 0 || len(s.stable) != 0 || len(s.sorted) != 0 || len(s.tomb) != 0 {
		t.Fatalf("clear left lengths keys=%d stable=%d sorted=%d tomb=%d",
			len(s.keys), len(s.stable), len(s.sorted), len(s.tomb))
	}

	idx := s.addKey([]byte("fresh"))
	if idx != 0 {
		t.Fatalf("stable index after clear = %d, want 0", idx)
	}
	if got := keyStableSortedKeys(s); !slices.Equal(got, []string{"fresh"}) {
		t.Fatalf("sorted keys after clear = %#v, want %#v", got, []string{"fresh"})
	}
}

func TestKeyStableSortInterfaceUsesSortedIndexes(t *testing.T) {
	s := newKeyStable(4)
	for _, key := range []string{"b", "a", "c"} {
		s.addKey([]byte(key))
	}

	s.sorted = []int{0, 1, 2}
	sort.Sort(s)
	if got, want := keyStableSortedKeys(s), []string{"a", "b", "c"}; !slices.Equal(got, want) {
		t.Fatalf("sort.Sort(keyStable) sorted keys = %#v, want %#v", got, want)
	}
}

func TestKeyStableRandomOperationsAgreeWithMap(t *testing.T) {
	s := newKeyStable(64)
	model := make(map[string]int)
	rng := rand.New(rand.NewSource(12345))
	universe := [][]byte{
		nil,
		[]byte("a"),
		[]byte("aa"),
		[]byte("a\x00"),
		[]byte{0, 1, 2},
	}
	for i := 0; i < 64; i++ {
		universe = append(universe, randomKeyStableKey(rng))
	}

	for step := 0; step < 500; step++ {
		key := slices.Clone(universe[rng.Intn(len(universe))])
		if rng.Intn(4) == 0 {
			got := s.delKey(key)
			_, want := model[string(key)]
			if got != want {
				t.Fatalf("step %d: delKey(%q) = %v, want %v", step, key, got, want)
			}
			delete(model, string(key))
		} else {
			got := s.addKey(key)
			if want, found := model[string(key)]; found {
				if got != want {
					t.Fatalf("step %d: addKey duplicate %q = %d, want %d", step, key, got, want)
				}
			} else {
				model[string(key)] = got
			}
		}
		assertKeyStableMatchesModel(t, s, model)
	}
}

func keyStableSortedKeys(s *keyStable) []string {
	keys := make([]string, len(s.sorted))
	s.ensureSorted()
	for i, stableIdx := range s.sorted {
		keys[i] = string(s.at(stableIdx))
	}
	return keys
}

func keyStableTombKeys(s *keyStable) []string {
	keys := make([]string, len(s.tomb))
	for i, stableIdx := range s.tomb {
		keys[i] = string(s.at(stableIdx))
	}
	return keys
}

func keyStableSortedKVKeys(s *keyStable) []string {
	keys := make([]string, 0, len(s.sorted))
	s.Scan(func(kv KV) bool {
		keys = append(keys, kv.Key)
		return true
	})
	return keys
}

func assertKeyStableMatchesModel(t *testing.T, s *keyStable, model map[string]int) {
	t.Helper()

	wantKeys := make([]string, 0, len(model))
	for key := range model {
		wantKeys = append(wantKeys, key)
	}
	slices.Sort(wantKeys)

	if got := keyStableSortedKeys(s); !slices.Equal(got, wantKeys) {
		t.Fatalf("sorted keys = %#v, want %#v", got, wantKeys)
	}
	if len(s.sorted) != len(model) {
		t.Fatalf("len(sorted) = %d, want active model size %d", len(s.sorted), len(model))
	}

	for i := 1; i < len(s.sorted); i++ {
		prev := s.at(s.sorted[i-1])
		next := s.at(s.sorted[i])
		if bytes.Compare(prev, next) >= 0 {
			t.Fatalf("sorted invariant broken at %d: %q >= %q", i, prev, next)
		}
	}

	for where, key := range wantKeys {
		gotWhere, found := s.findKey([]byte(key))
		if !found {
			t.Fatalf("findKey(%q) was not found", key)
		}
		if gotWhere != where {
			t.Fatalf("findKey(%q) where = %d, want %d", key, gotWhere, where)
		}
		gotStable := s.sorted[gotWhere]
		if gotStable != model[key] {
			t.Fatalf("findKey(%q) stable index = %d, want %d", key, gotStable, model[key])
		}
	}
}

func randomKeyStableKey(rng *rand.Rand) []byte {
	alphabet := []byte{0, 1, 'a', 'b', 'c', 'd', 0xff}
	key := make([]byte, rng.Intn(5))
	for i := range key {
		key[i] = alphabet[rng.Intn(len(alphabet))]
	}
	return key
}
