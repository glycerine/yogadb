package yogadb

import (
	"bytes"
	"fmt"
	"slices"
	"testing"
)

func TestKVXConversionAndZeroKey(t *testing.T) {
	var zero Key
	if got := keyString(zero); got != "" {
		t.Fatalf("zero key string = %q, want empty", got)
	}
	if got := keyLen(zero); got != 0 {
		t.Fatalf("zero key len = %d, want 0", got)
	}

	kv := KV{Key: "alpha", Value: []byte("value"), Vptr: VPtr{Length: 5, Offset: 9}, Hlc: 11}
	kvx := kvxFromKV(kv)
	if got := keyString(kvx.Key); got != kv.Key {
		t.Fatalf("KVX key = %q, want %q", got, kv.Key)
	}
	round := kvx.kv()
	if round.Key != kv.Key || !bytes.Equal(round.Value, kv.Value) || round.Vptr != kv.Vptr || round.Hlc != kv.Hlc {
		t.Fatalf("KVX round trip = %#v, want %#v", round, kv)
	}
	if kvx.isTombstone() || kvx.HasVPtr() || kvx.Large() {
		t.Fatalf("small inline KVX reported tombstone/large/vptr: %#v", kvx)
	}
	if got := kvx.Vtyp(); got != 9 {
		t.Fatalf("KVX Vtyp = %d, want 9", got)
	}

	var vtyp [8]byte
	putUint64(vtyp[:], 123)
	large := KVX{Key: keyH("big"), Value: vtyp[:], Vptr: VPtr{Length: vlogInlineThreshold + 1}, Hlc: 12}
	if !large.HasVPtr() || !large.Large() {
		t.Fatalf("large KVX did not report large VPtr: %#v", large)
	}
	if got := large.Vtyp(); got != 123 {
		t.Fatalf("large KVX Vtyp = %d, want 123", got)
	}
}

func TestKeyUniqSetGetReplaceDelete(t *testing.T) {
	s := newKeyUniq(2)

	for _, kv := range []KV{
		{Key: "b", Value: []byte("vb"), Vptr: VPtr{Length: 2}, Hlc: 1},
		{Key: "a", Value: []byte("va"), Vptr: VPtr{Length: 2}, Hlc: 2},
		{Key: "c", Value: []byte("vc"), Vptr: VPtr{Length: 2}, Hlc: 3},
	} {
		old, replaced := s.set(kv)
		if replaced {
			t.Fatalf("set(%q) replaced old KVX %#v, want fresh insert", kv.Key, old)
		}
	}
	if got := s.Len(); got != 3 {
		t.Fatalf("Len = %d, want 3", got)
	}
	if got := keyUniqSortedKeys(s); !slices.Equal(got, []string{"a", "b", "c"}) {
		t.Fatalf("sorted keys = %#v", got)
	}

	old, replaced := s.set(KV{Key: "b", Value: []byte("vb2"), Vptr: VPtr{Length: 3, Offset: 99}, Hlc: 4})
	if !replaced {
		t.Fatal("replacement of b reported fresh insert")
	}
	if keyString(old.Key) != "b" || string(old.Value) != "vb" || old.Hlc != 1 {
		t.Fatalf("old replacement KVX = %#v, want old b", old)
	}
	got, found := s.get("b")
	if !found {
		t.Fatal("get(b) did not find replacement")
	}
	if keyString(got.Key) != "b" || string(got.Value) != "vb2" || got.Vptr.Offset != 99 || got.Hlc != 4 {
		t.Fatalf("get(b) = %#v, want replacement", got)
	}

	if !s.delKey([]byte("b")) {
		t.Fatal("delKey(b) returned false")
	}
	if _, found := s.get("b"); found {
		t.Fatal("deleted key b still found")
	}
	if s.delKey([]byte("b")) {
		t.Fatal("second delKey(b) returned true")
	}
	if got := keyUniqSortedKeys(s); !slices.Equal(got, []string{"a", "c"}) {
		t.Fatalf("sorted keys after delete = %#v, want a,c", got)
	}

	s.set(KV{Key: "b", Value: []byte("vb3"), Vptr: VPtr{Length: 3}, Hlc: 5})
	if got := keyUniqSortedKeys(s); !slices.Equal(got, []string{"a", "b", "c"}) {
		t.Fatalf("sorted keys after re-add = %#v, want a,b,c", got)
	}
}

func TestKeyUniqSeekAndIteration(t *testing.T) {
	s := newKeyUniq(4)
	for _, key := range []string{"d", "b", "a", "c"} {
		s.set(KV{Key: key, Value: []byte("v-" + key), Vptr: VPtr{Length: 3}, Hlc: 1})
	}

	for _, tc := range []struct {
		target string
		strict bool
		want   string
		found  bool
	}{
		{"", false, "a", true},
		{"b", false, "b", true},
		{"b", true, "c", true},
		{"bb", false, "c", true},
		{"z", false, "", false},
	} {
		got, found := s.seekGE(tc.target, tc.strict)
		if found != tc.found || (found && keyString(got.Key) != tc.want) {
			t.Fatalf("seekGE(%q, %v) = %q/%v, want %q/%v",
				tc.target, tc.strict, keyString(got.Key), found, tc.want, tc.found)
		}
	}

	for _, tc := range []struct {
		target string
		strict bool
		want   string
		found  bool
	}{
		{"", false, "d", true},
		{"c", false, "c", true},
		{"c", true, "b", true},
		{"cc", false, "c", true},
		{"a", true, "", false},
	} {
		got, found := s.seekLE(tc.target, tc.strict)
		if found != tc.found || (found && keyString(got.Key) != tc.want) {
			t.Fatalf("seekLE(%q, %v) = %q/%v, want %q/%v",
				tc.target, tc.strict, keyString(got.Key), found, tc.want, tc.found)
		}
	}

	var scan []string
	s.Scan(func(kv KVX) bool {
		scan = append(scan, keyString(kv.Key))
		return true
	})
	if !slices.Equal(scan, []string{"a", "b", "c", "d"}) {
		t.Fatalf("Scan = %#v", scan)
	}

	var ascend []string
	s.Ascend(KVX{Key: keyH("b")}, func(kv KVX) bool {
		ascend = append(ascend, keyString(kv.Key))
		return len(ascend) < 2
	})
	if !slices.Equal(ascend, []string{"b", "c"}) {
		t.Fatalf("Ascend from b stopped = %#v, want b,c", ascend)
	}

	var descend []string
	s.Descend(KVX{Key: keyH("c")}, func(kv KVX) bool {
		descend = append(descend, keyString(kv.Key))
		return true
	})
	if !slices.Equal(descend, []string{"c", "b", "a"}) {
		t.Fatalf("Descend from c = %#v, want c,b,a", descend)
	}

	called := false
	s.Descend(KVX{}, func(kv KVX) bool {
		called = true
		return false
	})
	if called {
		t.Fatal("Descend with empty pivot should not call iterator")
	}

	var reverse []string
	s.Reverse(func(kv KVX) bool {
		reverse = append(reverse, keyString(kv.Key))
		return len(reverse) < 3
	})
	if !slices.Equal(reverse, []string{"d", "c", "b"}) {
		t.Fatalf("Reverse stopped = %#v, want d,c,b", reverse)
	}
}

func TestKeyUniqSeekLEEmptySortsDirtyTable(t *testing.T) {
	s := newKeyUniq(4)
	for _, key := range []string{"m", "z", "a"} {
		s.set(KV{Key: key, Value: []byte("v-" + key), Vptr: VPtr{Length: 3}, Hlc: 1})
	}
	got, found := s.seekLE("", false)
	if !found {
		t.Fatal("seekLE empty on dirty populated table did not find a key")
	}
	if keyString(got.Key) != "z" {
		t.Fatalf("seekLE empty on dirty table = %q, want z", keyString(got.Key))
	}
}

func TestKeyUniqHandlesSurviveClearAndReuse(t *testing.T) {
	s := newKeyUniq(1)
	s.set(KV{Key: "arena1", Value: []byte("value1"), Vptr: VPtr{Length: 6}, Hlc: 7})

	got, found := s.get("arena1")
	if !found {
		t.Fatal("get(arena1) did not find inserted key")
	}
	held := got
	heldKey := got.Key
	if keyString(heldKey) != "arena1" {
		t.Fatalf("held key before clear = %q, want arena1", keyString(heldKey))
	}

	s.clear()
	for _, key := range []string{"xxxxxx", "yyyyyy", "zzzzzz"} {
		s.set(KV{Key: key, Value: []byte("value2"), Vptr: VPtr{Length: 6}, Hlc: 8})
	}

	if got := keyString(heldKey); got != "arena1" {
		t.Fatalf("held handle key after clear/reuse = %q, want arena1", got)
	}
	if got := held.kv().Key; got != "arena1" {
		t.Fatalf("held KVX round-trip key after clear/reuse = %q, want arena1", got)
	}
	if string(held.Value) != "value1" {
		t.Fatalf("held value changed after clear/reuse = %q, want value1", held.Value)
	}
}

func TestKeyUniqValueAliasKeyStorage(t *testing.T) {
	s := newKeyUniq(1)
	kv := valueIsKeyKV("alias-key", 7)
	if !slottedInlineValueAliasesKey(kv) {
		t.Fatal("test setup did not create value-is-key KV")
	}
	s.set(kv)

	stableIdx, found := s.findStableByString("alias-key")
	if !found {
		t.Fatal("findStableByString(alias-key) did not find inserted key")
	}
	if !s.valueAliasKey[stableIdx] {
		t.Fatal("keyUniq did not remember value/key alias")
	}
	if s.kvs[stableIdx].Key != "" || s.kvs[stableIdx].Value != nil {
		t.Fatalf("stored KV retained key/value alias storage: %#v", s.kvs[stableIdx])
	}

	got, found := s.get("alias-key")
	if !found {
		t.Fatal("get(alias-key) did not find inserted key")
	}
	if keyString(got.Key) != "alias-key" || string(got.Value) != "alias-key" || got.Hlc != 7 {
		t.Fatalf("get(alias-key) = %#v, want value aliases key contents", got)
	}
	got.Value[0] = 'A'
	if keyString(got.Key) != "alias-key" {
		t.Fatalf("mutating returned alias value changed key to %q", keyString(got.Key))
	}
	gotAgain, found := s.get("alias-key")
	if !found || string(gotAgain.Value) != "alias-key" {
		t.Fatalf("mutating returned alias value changed stored value: %#v found=%v", gotAgain, found)
	}
}

func TestKeyUniqClearZerosRetainedStorage(t *testing.T) {
	s := newKeyUniq(2)
	s.set(KV{Key: "a", Value: []byte("value"), Vptr: VPtr{Length: 5, Offset: 9}, Hlc: 11})
	s.set(KV{Key: "b", Value: []byte("value"), Vptr: VPtr{Length: 5, Offset: 10}, Hlc: 12})
	capStable := cap(s.stable)
	capKVs := cap(s.kvs)

	s.clear()
	if s.Len() != 0 || len(s.tomb) != 0 || len(s.imap) != 0 || s.sortedDirty {
		t.Fatalf("clear left logical state: Len=%d tomb=%d imap=%d dirty=%v",
			s.Len(), len(s.tomb), len(s.imap), s.sortedDirty)
	}
	var zero Key
	for i, h := range s.stable[:capStable] {
		if h != zero {
			t.Fatalf("retained stable handle %d not zeroed: %v", i, keyString(h))
		}
	}
	for i, kv := range s.kvs[:capKVs] {
		if kv.Key != "" || kv.Value != nil || kv.Vptr != (VPtr{}) || kv.Hlc != 0 {
			t.Fatalf("retained KV slot %d not zeroed: %#v", i, kv)
		}
	}
}

func TestMemtableKeyUniqSizeAccountingForAliasReplacement(t *testing.T) {
	m := &memtable{ks: makeKeyUniq(0), empty: true}
	alias := valueIsKeyKV("alias-key", 1)
	wantAliasSize := int64(kvSizeApprox(&alias))

	old, replaced := m.put(alias)
	if replaced {
		t.Fatalf("fresh alias put replaced old KV %#v", old)
	}
	if m.size != wantAliasSize {
		t.Fatalf("memtable size after alias put = %d, want %d", m.size, wantAliasSize)
	}

	replacement := KV{Key: "alias-key", Value: []byte("replacement"), Vptr: VPtr{Length: uint64(len("replacement"))}, Hlc: 2}
	old, replaced = m.put(replacement)
	if !replaced {
		t.Fatal("replacement put reported fresh insert")
	}
	if old.Key != "alias-key" || string(old.Value) != "alias-key" || !slottedInlineValueAliasesKey(old) {
		t.Fatalf("old alias KV = %#v, want compact value-is-key KV", old)
	}
	wantReplacementSize := int64(kvSizeApprox(&replacement))
	if m.size != wantReplacementSize {
		t.Fatalf("memtable size after replacement = %d, want %d", m.size, wantReplacementSize)
	}
}

func FuzzKeyUniqInsertDeleteGet(f *testing.F) {
	for _, seed := range [][]byte{
		[]byte("abc"),
		[]byte("replace-delete-reinsert"),
		{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, ops []byte) {
		if len(ops) > 512 {
			ops = ops[:512]
		}
		s := newKeyUniq(0)
		model := make(map[string]KV)

		for step, op := range ops {
			key := fmt.Sprintf("%c%c", 'a'+rune(op%11), 'a'+rune(step%17))
			switch op % 3 {
			case 0:
				value := []byte{op, byte(step), byte(op ^ byte(step))}
				kv := KV{Key: key, Value: value, Vptr: VPtr{Length: uint64(len(value)), Offset: uint64(op)}, Hlc: HLC(step + 1)}
				if op&0x20 != 0 {
					kv = valueIsKeyKV(key, HLC(step+1))
				}
				s.set(kv)
				model[key] = kv
			case 1:
				s.delKey([]byte(key))
				delete(model, key)
			case 2:
				got, found := s.get(key)
				want, wantFound := model[key]
				if found != wantFound {
					t.Fatalf("step %d get(%q) found=%v, want %v", step, key, found, wantFound)
				}
				if found {
					assertKVXMatchesKV(t, got, want)
				}
			}
		}
		assertKeyUniqMatchesModel(t, s, model)
	})
}

func BenchmarkKeyUniqSet(b *testing.B) {
	keys := keyUniqBenchKeys(1 << 15)
	values := keyUniqBenchValues(len(keys))
	b.ReportAllocs()
	b.ResetTimer()
	s := makeKeyUniq(len(keys))
	for i := 0; i < b.N; i++ {
		key := keys[i&(len(keys)-1)]
		value := values[i&(len(values)-1)]
		s.set(KV{Key: key, Value: value, Vptr: VPtr{Length: uint64(len(value))}, Hlc: HLC(i + 1)})
	}
	keyUniqBenchLen = s.Len()
}

func BenchmarkKeyUniqGet(b *testing.B) {
	keys := keyUniqBenchKeys(1 << 15)
	values := keyUniqBenchValues(len(keys))
	s := makeKeyUniq(len(keys))
	for i, key := range keys {
		value := values[i]
		s.set(KV{Key: key, Value: value, Vptr: VPtr{Length: uint64(len(value))}, Hlc: HLC(i + 1)})
	}
	s.ensureSorted()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		keyUniqBenchKVX, _ = s.get(keys[i&(len(keys)-1)])
	}
}

func BenchmarkKeyUniqScan(b *testing.B) {
	keys := keyUniqBenchKeys(1 << 15)
	values := keyUniqBenchValues(len(keys))
	s := makeKeyUniq(len(keys))
	for i, key := range keys {
		value := values[i]
		s.set(KV{Key: key, Value: value, Vptr: VPtr{Length: uint64(len(value))}, Hlc: HLC(i + 1)})
	}
	s.ensureSorted()

	b.ReportAllocs()
	b.ResetTimer()
	var total int
	for i := 0; i < b.N; i++ {
		total = 0
		s.Scan(func(kv KVX) bool {
			total += keyLen(kv.Key) + len(kv.Value)
			return true
		})
	}
	keyUniqBenchLen = total
}

func keyUniqSortedKeys(s *keyUniq) []string {
	s.ensureSorted()
	out := make([]string, 0, len(s.sorted))
	for _, stableIdx := range s.sorted {
		out = append(out, s.at(stableIdx))
	}
	return out
}

func assertKeyUniqMatchesModel(t *testing.T, s *keyUniq, model map[string]KV) {
	t.Helper()
	var gotKeys []string
	s.Scan(func(kv KVX) bool {
		key := keyString(kv.Key)
		gotKeys = append(gotKeys, key)
		want, found := model[key]
		if !found {
			t.Fatalf("Scan visited unexpected key %q in %#v", key, kv)
		}
		assertKVXMatchesKV(t, kv, want)
		return true
	})
	wantKeys := make([]string, 0, len(model))
	for key := range model {
		wantKeys = append(wantKeys, key)
	}
	slices.Sort(wantKeys)
	if !slices.Equal(gotKeys, wantKeys) {
		t.Fatalf("Scan keys = %#v, want %#v", gotKeys, wantKeys)
	}
}

func assertKVXMatchesKV(t *testing.T, got KVX, want KV) {
	t.Helper()
	if keyString(got.Key) != want.Key || !bytes.Equal(got.Value, want.Value) || got.Vptr != want.Vptr || got.Hlc != want.Hlc {
		t.Fatalf("got KVX=%#v, want KV=%#v", got, want)
	}
}

func keyUniqBenchKeys(n int) []string {
	keys := make([]string, n)
	for i := range keys {
		keys[i] = fmt.Sprintf("key-%08d", i)
	}
	return keys
}

func keyUniqBenchValues(n int) [][]byte {
	values := make([][]byte, n)
	for i := range values {
		values[i] = []byte(fmt.Sprintf("value-%08d", i))
	}
	return values
}

var (
	keyUniqBenchKVX KVX
	keyUniqBenchLen int
)
