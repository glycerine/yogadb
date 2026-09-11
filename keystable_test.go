package yogadb

import (
	"bytes"
	"encoding/binary"
	"math/rand"
	"slices"
	"sort"
	"testing"

	"github.com/cespare/xxhash/v2"
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

func TestKeyStableConstructorsAndCapHints(t *testing.T) {
	s := makeKeyStable(3)
	if s.Len() != 0 {
		t.Fatalf("fresh makeKeyStable Len = %d, want 0", s.Len())
	}
	if cap(s.stable) < 3 || cap(s.sorted) < 3 || cap(s.kvs) < 3 {
		t.Fatalf("makeKeyStable(3) capacities stable=%d sorted=%d kvs=%d, want at least 3",
			cap(s.stable), cap(s.sorted), cap(s.kvs))
	}
	if s.headmap == nil {
		t.Fatal("makeKeyStable should initialize headmap")
	}

	defaultSized := makeKeyStable(0)
	if got, want := cap(defaultSized.stable), 256<<10; got < want {
		t.Fatalf("makeKeyStable(0) stable cap = %d, want at least %d", got, want)
	}

	sp := newKeyStable(2)
	if sp == nil {
		t.Fatal("newKeyStable returned nil")
	}
	if cap(sp.sorted) < 2 {
		t.Fatalf("newKeyStable(2) sorted cap = %d, want at least 2", cap(sp.sorted))
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

func TestKeyStableAppendHelpersAndHashChains(t *testing.T) {
	s := newKeyStable(8)

	s.headmap = nil
	h := xxhash.Sum64String("nil-map")
	nilMapIdx := s.appendKeyString("nil-map", h)
	if got, found := s.findStableByString("nil-map", h); !found || got != nilMapIdx {
		t.Fatalf("appendKeyString with nil headmap find = (%d, %v), want (%d, true)", got, found, nilMapIdx)
	}

	bytesIdx := s.appendKeyBytes([]byte("bytes"), xxhash.Sum64([]byte("bytes")))
	stringIdx := s.appendKeyString("string", xxhash.Sum64String("string"))
	if got := string(s.at(bytesIdx)); got != "bytes" {
		t.Fatalf("appendKeyBytes stored %q, want bytes", got)
	}
	if got := string(s.at(stringIdx)); got != "string" {
		t.Fatalf("appendKeyString stored %q, want string", got)
	}
	if !s.sortedDirty {
		t.Fatal("append helpers should mark sortedDirty")
	}
	h = xxhash.Sum64String("bytes")
	if got, found := s.findStableByBytes([]byte("bytes"), h); !found || got != bytesIdx {
		t.Fatalf("findStableByBytes(bytes) = (%d, %v), want (%d, true)", got, found, bytesIdx)
	}
	h = xxhash.Sum64String("string")
	if got, found := s.findStableByString("string", h); !found || got != stringIdx {
		t.Fatalf("findStableByString(string) = (%d, %v), want (%d, true)", got, found, stringIdx)
	}

	h = xxhash.Sum64String("dup")
	tail := s.appendKeyString("dup", h)
	head := s.appendKeyString("dup", h)
	if got, found := s.findStableByString("dup", h); !found || got != head {
		t.Fatalf("findStableByString(dup) = (%d, %v), want newest head %d", got, found, head)
	}

	s.removeStableFromHeadmap(tail)
	if got, found := s.findStableByString("dup", h); !found || got != head {
		t.Fatalf("after tail removal findStableByString(dup) = (%d, %v), want head %d", got, found, head)
	}

	s.removeStableFromHeadmap(head)
	if _, found := s.findStableByString("dup", h); found {
		t.Fatal("after removing both duplicate chain entries, dup should not be found")
	}
}

func TestKeyStableEnsureHeadmapRebuildSkipsDeletedKeys(t *testing.T) {
	s := newKeyStable(4)
	oldA := s.addKey([]byte("a"))
	bIdx := s.addKey([]byte("b"))
	if !s.delKey([]byte("a")) {
		t.Fatal("delKey(a) = false, want true")
	}

	s.headmap = nil
	s.nextSameHash = s.nextSameHash[:0]
	ha := xxhash.Sum64String("a")
	hb := xxhash.Sum64String("b")
	if got, found := s.findStableByBytes([]byte("a"), ha); found {
		t.Fatalf("rebuilt headmap found deleted key a at stable index %d", got)
	}
	if got, found := s.findStableByString("b", hb); !found || got != bIdx {
		t.Fatalf("rebuilt headmap findStableByString(b) = (%d, %v), want (%d, true)", got, found, bIdx)
	}

	newA := s.addKey([]byte("a"))
	if newA == oldA {
		t.Fatalf("re-added key reused deleted stable index %d after headmap rebuild", oldA)
	}
}

func TestKeyStableCompareStringBytes(t *testing.T) {
	for _, tc := range []struct {
		a string
		b []byte
	}{
		{a: "", b: nil},
		{a: "", b: []byte{0}},
		{a: "\x00", b: []byte{}},
		{a: "abc", b: []byte("abc")},
		{a: "abc", b: []byte("abd")},
		{a: "abd", b: []byte("abc")},
		{a: "abc", b: []byte("abcd")},
		{a: "abcd", b: []byte("abc")},
		{a: "\x00a", b: []byte("\x00b")},
		{a: "\xff", b: []byte("\x00")},
	} {
		got := cmpSign(compareStringBytes(tc.a, tc.b))
		want := cmpSign(bytes.Compare([]byte(tc.a), tc.b))
		if got != want {
			t.Fatalf("compareStringBytes(%q, %#v) sign = %d, want %d", tc.a, tc.b, got, want)
		}
	}
}

func TestKeyStableSetGetAndReplaceKV(t *testing.T) {
	s := newKeyStable(4)
	const x = true
	for _, kv := range []KV{
		{Key: "b", Value: []byte("vb"), Vptr: VPtr{Length: 2}, Hlc: 1},
		{Key: "a", Value: []byte("va"), Vptr: VPtr{Length: 2}, Hlc: 2},
	} {
		if old, replaced := s.set(kv, x); replaced {
			t.Fatalf("set(%q) replaced old KV %#v, want fresh insert", kv.Key, old)
		}
	}

	old, replaced := s.set(KV{Key: "b", Value: []byte("vb2"), Vptr: VPtr{Length: 3, Offset: 99}, Hlc: 3}, x)
	if !replaced {
		t.Fatal("set duplicate did not report replacement")
	}
	if old.Key != "b" || string(old.Value) != "vb" || old.Hlc != 1 {
		t.Fatalf("old KV = %#v, want key b value vb hlc 1", old)
	}
	if s.Len() != 2 {
		t.Fatalf("Len after replacement = %d, want 2", s.Len())
	}

	got, found := s.get("b", x)
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

func TestKeyStableSeekEdges(t *testing.T) {
	s := newKeyStable(4)
	const x = true

	if _, found := s.seekGE("anything", false, x); found {
		t.Fatal("seekGE on empty keyStable found a key")
	}
	if _, found := s.seekLE("anything", false, x); found {
		t.Fatal("seekLE on empty keyStable found a key")
	}

	for _, key := range []string{"b", "d", "f"} {
		s.set(KV{Key: key, Value: []byte("v-" + key), Vptr: VPtr{Length: 3}, Hlc: 1}, x)
	}
	for _, tc := range []struct {
		target string
		strict bool
		want   string
		found  bool
	}{
		{target: "", want: "b", found: true},
		{target: "b", want: "b", found: true},
		{target: "b", strict: true, want: "d", found: true},
		{target: "c", want: "d", found: true},
		{target: "z"},
	} {
		got, found := s.seekGE(tc.target, tc.strict, x)
		if found != tc.found || (found && got.Key != tc.want) {
			t.Fatalf("seekGE(%q, strict=%v) = (%q, %v), want (%q, %v)",
				tc.target, tc.strict, got.Key, found, tc.want, tc.found)
		}
	}
	for _, tc := range []struct {
		target string
		strict bool
		want   string
		found  bool
	}{
		{target: "", want: "f", found: true},
		{target: "", strict: true, want: "f", found: true},
		{target: "f", want: "f", found: true},
		{target: "f", strict: true, want: "d", found: true},
		{target: "e", want: "d", found: true},
		{target: "a"},
		{target: "b", strict: true},
	} {
		got, found := s.seekLE(tc.target, tc.strict, x)
		if found != tc.found || (found && got.Key != tc.want) {
			t.Fatalf("seekLE(%q, strict=%v) = (%q, %v), want (%q, %v)",
				tc.target, tc.strict, got.Key, found, tc.want, tc.found)
		}
	}
}

func TestKeyStableSeekLEEmptySortsDirtyTable(t *testing.T) {
	s := newKeyStable(4)
	const x = true
	for _, key := range []string{"m", "z", "a"} {
		s.set(KV{Key: key, Value: []byte("v-" + key), Vptr: VPtr{Length: 3}, Hlc: 1}, x)
	}
	got, found := s.seekLE("", false, x)
	if !found {
		t.Fatal("seekLE empty on dirty populated table did not find a key")
	}
	if got.Key != "z" {
		t.Fatalf("seekLE empty on dirty table = %q, want z", got.Key)
	}
}

func TestKeyStableIterationMethodsAndEarlyStop(t *testing.T) {
	s := newKeyStable(4)
	const x = true
	for _, key := range []string{"c", "a", "d", "b"} {
		s.set(KV{Key: key, Value: []byte("v-" + key), Vptr: VPtr{Length: 3}, Hlc: 1}, x)
	}

	var scan []string
	s.Scan(x, func(kv KV) bool {
		scan = append(scan, kv.Key)
		return true
	})
	if want := []string{"a", "b", "c", "d"}; !slices.Equal(scan, want) {
		t.Fatalf("Scan keys = %#v, want %#v", scan, want)
	}
	var scanStopped []string
	s.Scan(x, func(kv KV) bool {
		scanStopped = append(scanStopped, kv.Key)
		return len(scanStopped) < 2
	})
	if want := []string{"a", "b"}; !slices.Equal(scanStopped, want) {
		t.Fatalf("Scan early stop keys = %#v, want %#v", scanStopped, want)
	}

	var ascend []string
	s.Ascend(x, KV{Key: "b"}, func(kv KV) bool {
		ascend = append(ascend, kv.Key)
		return len(ascend) < 2
	})
	if want := []string{"b", "c"}; !slices.Equal(ascend, want) {
		t.Fatalf("Ascend early stop keys = %#v, want %#v", ascend, want)
	}

	var descendExact []string
	s.Descend(x, KV{Key: "c"}, func(kv KV) bool {
		descendExact = append(descendExact, kv.Key)
		return true
	})
	if want := []string{"c", "b", "a"}; !slices.Equal(descendExact, want) {
		t.Fatalf("Descend exact keys = %#v, want %#v", descendExact, want)
	}
	var descendStopped []string
	s.Descend(x, KV{Key: "d"}, func(kv KV) bool {
		descendStopped = append(descendStopped, kv.Key)
		return len(descendStopped) < 2
	})
	if want := []string{"d", "c"}; !slices.Equal(descendStopped, want) {
		t.Fatalf("Descend early stop keys = %#v, want %#v", descendStopped, want)
	}

	var descendBetween []string
	s.Descend(x, KV{Key: "bb"}, func(kv KV) bool {
		descendBetween = append(descendBetween, kv.Key)
		return true
	})
	if want := []string{"b", "a"}; !slices.Equal(descendBetween, want) {
		t.Fatalf("Descend between keys = %#v, want %#v", descendBetween, want)
	}

	var descendAll []string
	s.Descend(x, KV{}, func(kv KV) bool {
		descendAll = append(descendAll, kv.Key)
		return true
	})
	if want := []string{"d", "c", "b", "a"}; !slices.Equal(descendAll, want) {
		t.Fatalf("Descend all keys = %#v, want %#v", descendAll, want)
	}
	if len(descendAll) != 4 {
		t.Fatalf("Descend with empty pivot visited %#v, want all 4", descendAll)
	}

	var reverse []string
	s.Reverse(x, func(kv KV) bool {
		reverse = append(reverse, kv.Key)
		return len(reverse) < 3
	})
	if want := []string{"d", "c", "b"}; !slices.Equal(reverse, want) {
		t.Fatalf("Reverse early stop keys = %#v, want %#v", reverse, want)
	}
}

func TestKeyStableClearReusesTable(t *testing.T) {
	s := newKeyStable(4)
	s.addKey([]byte("b"))
	s.addKey([]byte("a"))
	s.delKey([]byte("a"))
	const x = true
	s.clear(x)
	if len(s.keys) != 0 || len(s.stable) != 0 || len(s.sorted) != 0 {
		t.Fatalf("clear left lengths keys=%d stable=%d sorted=%v",
			len(s.keys), len(s.stable), len(s.sorted))
	}

	idx := s.addKey([]byte("fresh"))
	if idx != 0 {
		t.Fatalf("stable index after clear = %d, want 0", idx)
	}
	if got := keyStableSortedKeys(s); !slices.Equal(got, []string{"fresh"}) {
		t.Fatalf("sorted keys after clear = %#v, want %#v", got, []string{"fresh"})
	}
}

func TestKeyStableClearZerosRetainedKVSlots(t *testing.T) {
	s := newKeyStable(2)
	const x = true

	s.set(KV{Key: "a", Value: []byte("value"), Vptr: VPtr{Length: 5, Offset: 9}, Hlc: 11}, x)
	retained := s.kvs[:len(s.kvs)]
	s.clear(x)

	if retained[0].Key != "" || retained[0].Value != nil || retained[0].Vptr != (VPtr{}) || retained[0].Hlc != 0 {
		t.Fatalf("clear retained stale KV slot: %#v", retained[0])
	}
	if len(s.headmap) != 0 {
		t.Fatalf("clear left headmap length %d, want 0", len(s.headmap))
	}
	if s.sortedDirty {
		t.Fatal("clear left sortedDirty=true")
	}
}

func TestKeyStableSortInterfaceUsesSortedIndexes(t *testing.T) {
	s := newKeyStable(4)
	for _, key := range []string{"b", "a", "c"} {
		s.addKey([]byte(key))
	}

	if s.Less(0, 0) {
		t.Fatal("Less(i, i) = true, want false")
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

func FuzzKeyStableInsertDeleteGet(f *testing.F) {
	for _, seed := range [][]byte{
		nil,
		[]byte{0},
		[]byte("set-a-get-a-del-a"),
		[]byte{0, 1, 'a', 2, 1, 'a', 1, 1, 'a'},
		[]byte{0x80, 3, 'k', 'e', 'y', 2, 3, 'k', 'e', 'y'},
		[]byte{1, 0, 0, 0, 2, 0},
	} {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		s := newKeyStable(0)
		model := make(map[string]KV)
		const x = false

		for i, step := 0, 0; i < len(data); step++ {
			op := data[i]
			i++
			if i >= len(data) {
				break
			}
			keyLen := int(data[i] & 0x0f)
			i++
			if i+keyLen > len(data) {
				break
			}
			keyBytes := slices.Clone(data[i : i+keyLen])
			i += keyLen
			key := string(keyBytes)

			switch op % 3 {
			case 0:
				kv := keyStableFuzzKV(step, op, keyBytes)
				old, replaced := s.set(kv, x)
				wantOld, hadOld := model[key]
				if replaced != hadOld {
					t.Fatalf("step %d: set(%q) replaced=%v, want %v", step, keyBytes, replaced, hadOld)
				}
				if hadOld {
					assertKeyStableKVEqual(t, "old replacement", old, wantOld)
				}
				model[key] = kv
			case 1:
				got := s.delKey(keyBytes)
				_, want := model[key]
				if got != want {
					t.Fatalf("step %d: delKey(%q) = %v, want %v", step, keyBytes, got, want)
				}
				delete(model, key)
			case 2:
				got, found := s.get(key, x)
				want, wantFound := model[key]
				if found != wantFound {
					t.Fatalf("step %d: get(%q) found=%v, want %v", step, keyBytes, found, wantFound)
				}
				if wantFound {
					assertKeyStableKVEqual(t, "get", got, want)
				}
			}

			if step%17 == 0 {
				assertKeyStableKVModel(t, s, model)
			}
		}
		assertKeyStableKVModel(t, s, model)
	})
}

func BenchmarkKeyStableSet(b *testing.B) {
	keys := benchmarkKeyStableKeys(1 << 16)
	value := []byte("value")
	s := makeKeyStable(len(keys))
	const x = true

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if i > 0 && i%len(keys) == 0 {
			s.clear(x)
		}
		s.set(KV{
			Key:   keys[i&(len(keys)-1)],
			Value: value,
			Vptr:  VPtr{Length: uint64(len(value))},
			Hlc:   HLC(i + 1),
		}, x)
	}
	keyStableBenchKV, _ = s.get(keys[(b.N-1)&(len(keys)-1)], x)
}

func BenchmarkKeyStableGet(b *testing.B) {
	keys := benchmarkKeyStableKeys(1 << 16)
	value := []byte("value")
	s := makeKeyStable(len(keys))
	x := true
	for i, key := range keys {
		s.set(KV{Key: key, Value: value, Vptr: VPtr{Length: uint64(len(value))}, Hlc: HLC(i + 1)}, x)
	}
	x = false

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		kv, found := s.get(keys[i&(len(keys)-1)], x)
		if !found {
			b.Fatalf("get(%q) was not found", keys[i&(len(keys)-1)])
		}
		keyStableBenchKV = kv
	}
}

func BenchmarkKeyStableEnsureSorted(b *testing.B) {
	keys := benchmarkKeyStableKeys(4096)
	value := []byte("value")
	const x = true

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		s := makeKeyStable(len(keys))
		for j, key := range keys {
			s.set(KV{Key: key, Value: value, Vptr: VPtr{Length: uint64(len(value))}, Hlc: HLC(j + 1)}, x)
		}
		b.StartTimer()
		s.ensureSorted()
		keyStableBenchInt = s.Len()
	}
}

func BenchmarkKeyStableScan(b *testing.B) {
	keys := benchmarkKeyStableKeys(4096)
	value := []byte("value")
	s := makeKeyStable(len(keys))
	const x = true
	for i, key := range keys {
		s.set(KV{Key: key, Value: value, Vptr: VPtr{Length: uint64(len(value))}, Hlc: HLC(i + 1)}, x)
	}
	s.ensureSorted()

	var total int
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Scan(x, func(kv KV) bool {
			total += len(kv.Key) + len(kv.Value)
			return true
		})
	}
	keyStableBenchInt = total
}

func BenchmarkKeyStableAscendOwnedKeys(b *testing.B) {
	keys := benchmarkKeyStableKeys(4096)
	value := []byte("value")
	s := makeKeyStable(len(keys))
	const x = true
	for i, key := range keys {
		s.set(KV{Key: key, Value: value, Vptr: VPtr{Length: uint64(len(value))}, Hlc: HLC(i + 1)}, x)
	}
	s.ensureSorted()

	var total int
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Ascend(x, KV{}, func(kv KV) bool {
			total += len(kv.Key) + len(kv.Value)
			return true
		})
	}
	keyStableBenchInt = total
}

func keyStableSortedKeys(s *keyStable) []string {
	keys := make([]string, len(s.sorted))
	s.ensureSorted()
	for i, stableIdx := range s.sorted {
		keys[i] = string(s.at(stableIdx))
	}
	return keys
}

func keyStableSortedKVKeys(s *keyStable) []string {
	keys := make([]string, 0, len(s.sorted))
	const x = true
	s.Scan(x, func(kv KV) bool {
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

func assertKeyStableKVModel(t *testing.T, s *keyStable, model map[string]KV) {
	t.Helper()
	const x = false

	wantKeys := make([]string, 0, len(model))
	for key := range model {
		wantKeys = append(wantKeys, key)
	}
	slices.Sort(wantKeys)
	if s.Len() != len(model) {
		t.Fatalf("Len = %d, want model size %d", s.Len(), len(model))
	}

	gotKeys := make([]string, 0, len(model))
	s.Scan(x, func(kv KV) bool {
		gotKeys = append(gotKeys, kv.Key)
		want, found := model[kv.Key]
		if !found {
			t.Fatalf("Scan visited unexpected key %q in %#v", kv.Key, kv)
		}
		assertKeyStableKVEqual(t, "Scan", kv, want)
		return true
	})
	if !slices.Equal(gotKeys, wantKeys) {
		t.Fatalf("Scan keys = %#v, want %#v", gotKeys, wantKeys)
	}

	for where, key := range wantKeys {
		got, found := s.get(key, x)
		if !found {
			t.Fatalf("get(%q) was not found", key)
		}
		assertKeyStableKVEqual(t, "get", got, model[key])

		gotWhere, found := s.findKeyString(key)
		if !found || gotWhere != where {
			t.Fatalf("findKeyString(%q) = (%d, %v), want (%d, true)", key, gotWhere, found, where)
		}
	}
}

func assertKeyStableKVEqual(t *testing.T, label string, got, want KV) {
	t.Helper()
	if got.Key != want.Key || !bytes.Equal(got.Value, want.Value) || got.Vptr != want.Vptr || got.Hlc != want.Hlc {
		t.Fatalf("%s KV = %#v, want %#v", label, got, want)
	}
}

func keyStableFuzzKV(step int, op byte, keyBytes []byte) KV {
	key := string(keyBytes)
	value := []byte{byte(step), byte(len(keyBytes)), op}
	if len(keyBytes) > 0 && op&0x80 != 0 {
		value = slices.Clone(keyBytes)
	}

	return KV{
		Key:   key,
		Value: value,
		Vptr:  VPtr{Offset: uint64(op >> 2), Length: uint64(len(value))},
		Hlc:   HLC(step + 1),
	}
}

func benchmarkKeyStableKeys(n int) []string {
	keys := make([]string, n)
	var buf [16]byte
	for i := range keys {
		binary.LittleEndian.PutUint64(buf[:8], uint64(i)*0x9e3779b97f4a7c15)
		binary.LittleEndian.PutUint64(buf[8:], uint64(i))
		keys[i] = string(buf[:])
	}
	return keys
}

func cmpSign(n int) int {
	switch {
	case n < 0:
		return -1
	case n > 0:
		return 1
	default:
		return 0
	}
}

var (
	keyStableBenchKV  KV
	keyStableBenchInt int
)

func randomKeyStableKey(rng *rand.Rand) []byte {
	alphabet := []byte{0, 1, 'a', 'b', 'c', 'd', 0xff}
	key := make([]byte, rng.Intn(5))
	for i := range key {
		key[i] = alphabet[rng.Intn(len(alphabet))]
	}
	return key
}

func TestKeyStable_set_then_set(t *testing.T) {
	s := newKeyStable(0)
	const x = true
	s.set(KV{Key: "a", Value: []byte{1}}, x)
	kv, found0 := s.get("a", x)
	if !found0 {
		t.Fatalf("expected to find key 'a'")
	}
	if kv.Key != "a" {
		t.Fatalf("expected kv to have Key 'a' but has '%v'", kv.Key)
	}
	if 0 != bytes.Compare(kv.Value, []byte{1}) {
		t.Fatalf("expected kv to have Value 1, but got: '%v'", string(kv.Value))
	}
	n := s.Len()
	if n != 1 {
		t.Fatalf("expected len of 1, got %v", n)
	}

	//vv("s = '%s'", s)

	oldKV, replaced := s.set(KV{Key: "a", Value: []byte{2}}, x)

	//vv("after set of key 'a' value:2, we have: s = '%s'", s)

	if !replaced {
		t.Fatalf("expected to replace previous key")
	}
	if oldKV.Key != "a" {
		t.Fatalf("expected oldKV.Key to be 'a', was %v", oldKV.Key)
	}
	if 0 != bytes.Compare(oldKV.Value, []byte{1}) {
		t.Fatalf("expected oldKV to have Value 1, but got: '%v'", string(oldKV.Value))
	}
	kv, found3 := s.get("a", x)

	if !found3 {
		t.Fatalf("expected to find key 'a'")
	}
	if kv.Key != "a" {
		t.Fatalf("expected kv to have Key 'a' but has '%v'", kv.Key)
	}
	if 0 != bytes.Compare(kv.Value, []byte{2}) {
		t.Fatalf("expected kv to have Value 2, but got: '%v'", string(kv.Value))
	}
	n3 := s.Len()
	if n3 != 1 {
		t.Fatalf("expected len of 1, got %v", n3)
	}
	nstable := len(s.stable)
	if nstable != 1 {
		t.Fatalf("expected nstable = 1; got %v", nstable)
	}
}
