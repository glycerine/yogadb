package pebbleskip

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"testing"
	"time"
)

func TestRandomizedAddGetIterCorrectness(t *testing.T) {
	duration := randomizedTestDuration(t, "PEBBLESKIP_RANDOMIZED_SECONDS")
	deadline := time.Now().Add(duration)
	const baseSeed uint64 = 0xbb67ae8584caa73b

	var runs, ops int
	for run := 0; run == 0 || time.Now().Before(deadline); run++ {
		seed := baseSeed + uint64(run)*0x9e3779b97f4a7c15
		rng := rand.New(rand.NewSource(int64(seed)))
		s := New(8<<20, nil)
		model := make(map[string]KV)
		var ins Inserter
		useInserter := rng.Intn(2) == 0

		for step := 0; step < 4096 && time.Now().Before(deadline); step++ {
			ops++
			key := randomizedPebbleKey(rng)
			op := rng.Intn(100)
			switch {
			case op < 45:
				kv := randomizedPebbleKV(rng, key, run*4096+step+1)
				_, existed := model[key]
				var err error
				if useInserter {
					err = ins.Add(s, kv)
				} else {
					err = s.Add(kv)
				}
				if existed {
					if !errors.Is(err, ErrRecordExists) {
						t.Fatalf("seed=0x%x run=%d step=%d Add duplicate %q err=%v want %v", seed, run, step, key, err, ErrRecordExists)
					}
				} else {
					if err != nil {
						t.Fatalf("seed=0x%x run=%d step=%d Add(%q): %v", seed, run, step, key, err)
					}
					model[key] = cloneKV(kv)
				}
			case op < 70:
				assertPebbleGet(t, s, model, key, seed, run, step)
			case op < 85:
				assertPebbleIterAll(t, s, model, seed, run, step)
			default:
				lower := randomizedOptionalPebbleKey(rng)
				upper := randomizedOptionalPebbleKey(rng)
				seek := randomizedPebbleKey(rng)
				assertPebbleSeekBounds(t, s, model, lower, upper, seek, seed, run, step)
			}
			assertPebbleAll(t, s, model, seed, run, step)
		}
		runs++
	}
	t.Logf("pebbleskip randomized correctness duration=%s runs=%d ops=%d", duration, runs, ops)
}

func randomizedTestDuration(t *testing.T, env string) time.Duration {
	t.Helper()
	if raw := os.Getenv(env); raw != "" {
		seconds, err := strconv.ParseFloat(raw, 64)
		if err != nil || seconds <= 0 {
			t.Fatalf("%s=%q is not a positive number of seconds", env, raw)
		}
		return time.Duration(seconds * float64(time.Second))
	}
	return 250 * time.Millisecond
}

func randomizedPebbleKey(rng *rand.Rand) string {
	return testKeyString(rng.Intn(320))
}

func randomizedOptionalPebbleKey(rng *rand.Rand) string {
	if rng.Intn(8) == 0 {
		return ""
	}
	return randomizedPebbleKey(rng)
}

func randomizedPebbleKV(rng *rand.Rand, key string, hlc int) KV {
	value := randomizedValue(rng)
	kv := KV{
		Key:   key,
		Value: value,
		Vptr:  VPtr{Offset: uint64(rng.Intn(11)), Length: uint64(len(value))},
		Hlc:   HLC(hlc),
	}
	if rng.Intn(32) == 0 {
		kv.Value = nil
		kv.Vptr = VPtr{Length: rawVlenTombstone}
	}
	return kv
}

func randomizedValue(rng *rand.Rand) []byte {
	n := rng.Intn(65)
	if rng.Intn(16) == 0 {
		n = 0
	}
	value := make([]byte, n)
	for i := range value {
		value[i] = byte(rng.Intn(256))
	}
	return value
}

func cloneKV(kv KV) KV {
	kv.Value = append([]byte(nil), kv.Value...)
	return kv
}

func equalKV(a, b KV) bool {
	return a.Key == b.Key && bytes.Equal(a.Value, b.Value) && a.Vptr == b.Vptr && a.Hlc == b.Hlc
}

func modelSortedKeys(model map[string]KV) []string {
	keys := make([]string, 0, len(model))
	for key := range model {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func modelKVsForKeys(model map[string]KV, keys []string) []KV {
	kvs := make([]KV, 0, len(keys))
	for _, key := range keys {
		kvs = append(kvs, model[key])
	}
	return kvs
}

func compareKVLists(got, want []KV) string {
	if len(got) != len(want) {
		return fmt.Sprintf("len got=%d want=%d got=%v want=%v", len(got), len(want), kvKeys(got), kvKeys(want))
	}
	for i := range got {
		if !equalKV(got[i], want[i]) {
			return fmt.Sprintf("at %d got=%#v want=%#v", i, got[i], want[i])
		}
	}
	return ""
}

func kvKeys(kvs []KV) []string {
	keys := make([]string, len(kvs))
	for i := range kvs {
		keys[i] = kvs[i].Key
	}
	return keys
}

func assertPebbleAll(t *testing.T, s *Skiplist, model map[string]KV, seed uint64, run, step int) {
	t.Helper()
	for i := 0; i < 320; i++ {
		assertPebbleGet(t, s, model, testKeyString(i), seed, run, step)
	}
	assertPebbleIterAll(t, s, model, seed, run, step)
}

func assertPebbleGet(t *testing.T, s *Skiplist, model map[string]KV, key string, seed uint64, run, step int) {
	t.Helper()
	got, ok := s.Get(key)
	want, wantOK := model[key]
	if ok != wantOK || (ok && !equalKV(got, want)) {
		t.Fatalf("seed=0x%x run=%d step=%d Get(%q)=%#v,%v want %#v,%v", seed, run, step, key, got, ok, want, wantOK)
	}
}

func assertPebbleIterAll(t *testing.T, s *Skiplist, model map[string]KV, seed uint64, run, step int) {
	t.Helper()
	keys := modelSortedKeys(model)
	want := modelKVsForKeys(model, keys)

	it := s.NewIter("", "")
	var got []KV
	for kv := it.First(); kv != nil; kv = it.Next() {
		got = append(got, cloneKV(*kv))
	}
	it.Close()
	if diff := compareKVLists(got, want); diff != "" {
		t.Fatalf("seed=0x%x run=%d step=%d forward iter: %s", seed, run, step, diff)
	}

	it = s.NewIter("", "")
	got = got[:0]
	for kv := it.Last(); kv != nil; kv = it.Prev() {
		got = append(got, cloneKV(*kv))
	}
	it.Close()
	wantReverse := make([]KV, 0, len(want))
	for i := len(want) - 1; i >= 0; i-- {
		wantReverse = append(wantReverse, want[i])
	}
	if diff := compareKVLists(got, wantReverse); diff != "" {
		t.Fatalf("seed=0x%x run=%d step=%d reverse iter: %s", seed, run, step, diff)
	}
}

func assertPebbleSeekBounds(t *testing.T, s *Skiplist, model map[string]KV, lower, upper, seek string, seed uint64, run, step int) {
	t.Helper()
	keys := modelSortedKeys(model)
	it := s.NewIter(lower, upper)
	defer it.Close()

	var wantGE *KV
	for _, key := range keys {
		if key < seek {
			continue
		}
		if upper != "" && key >= upper {
			break
		}
		kv := model[key]
		wantGE = &kv
		break
	}
	gotGE := it.SeekGE(seek)
	if !optionalKVsEqual(gotGE, wantGE) {
		t.Fatalf("seed=0x%x run=%d step=%d SeekGE lower=%q upper=%q seek=%q got=%#v want=%#v", seed, run, step, lower, upper, seek, gotGE, wantGE)
	}

	var wantLT *KV
	for i := len(keys) - 1; i >= 0; i-- {
		key := keys[i]
		if key >= seek {
			continue
		}
		if lower != "" && key < lower {
			break
		}
		kv := model[key]
		wantLT = &kv
		break
	}
	gotLT := it.SeekLT(seek)
	if !optionalKVsEqual(gotLT, wantLT) {
		t.Fatalf("seed=0x%x run=%d step=%d SeekLT lower=%q upper=%q seek=%q got=%#v want=%#v", seed, run, step, lower, upper, seek, gotLT, wantLT)
	}
}

func optionalKVsEqual(got *KV, want *KV) bool {
	if got == nil || want == nil {
		return got == nil && want == nil
	}
	return equalKV(*got, *want)
}
