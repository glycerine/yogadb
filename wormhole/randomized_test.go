package wormhole

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"testing"
	"time"
)

func TestRandomizedSetGetDeleteCorrectness(t *testing.T) {
	duration := randomizedTestDuration(t, "WORMHOLE_RANDOMIZED_SECONDS")
	deadline := time.Now().Add(duration)
	const baseSeed uint64 = 0x6a09e667f3bcc909
	leafCaps := []int{4, 5, 8, 16, 31, 64, 128}

	var runs, ops int
	for run := 0; run == 0 || time.Now().Before(deadline); run++ {
		seed := baseSeed + uint64(run)*0x9e3779b97f4a7c15
		rng := rand.New(rand.NewSource(int64(seed)))
		m := New(Options{LeafCapacity: leafCaps[rng.Intn(len(leafCaps))]})
		if rng.Intn(2) == 0 {
			m.BuildPointIndex()
		}
		model := make(map[string]KV)

		for step := 0; step < 2048 && time.Now().Before(deadline); step++ {
			ops++
			key := randomizedWormholeKey(rng)
			op := rng.Intn(100)
			switch {
			case op < 40:
				value := randomizedValue(rng)
				kv := KV{
					Key:   key,
					Value: value,
					Vptr:  VPtr{Offset: uint64(rng.Intn(11)), Length: uint64(len(value))},
					Hlc:   HLC(run*2048 + step + 1),
				}
				_, existed := model[key]
				replaced := m.Put(kv)
				if replaced != existed {
					t.Fatalf("seed=0x%x run=%d step=%d Put(%q) replaced=%v want %v", seed, run, step, key, replaced, existed)
				}
				model[key] = cloneKV(kv)
			case op < 65:
				assertWormholeGet(t, m, model, key, seed, run, step)
			case op < 85:
				_, existed := model[key]
				deleted := m.Delete(key)
				if deleted != existed {
					t.Fatalf("seed=0x%x run=%d step=%d Delete(%q)=%v want %v", seed, run, step, key, deleted, existed)
				}
				delete(model, key)
			case op < 90:
				start := randomizedOptionalWormholeKey(rng)
				assertWormholeAscend(t, m, model, start, seed, run, step)
			case op < 96:
				start := randomizedOptionalWormholeKey(rng)
				assertWormholeDescend(t, m, model, start, seed, run, step)
			case op < 99:
				start := randomizedOptionalWormholeKey(rng)
				end := randomizedOptionalWormholeKey(rng)
				assertWormholeRange(t, m, model, start, end, seed, run, step)
			default:
				m.BuildPointIndex()
			}
			assertWormholeAll(t, m, model, seed, run, step)
		}
		runs++
	}
	t.Logf("wormhole randomized correctness duration=%s runs=%d ops=%d", duration, runs, ops)
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

func randomizedWormholeKey(rng *rand.Rand) string {
	return whKey(rng.Intn(320))
}

func randomizedOptionalWormholeKey(rng *rand.Rand) string {
	if rng.Intn(8) == 0 {
		return ""
	}
	return randomizedWormholeKey(rng)
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

func assertWormholeGet(t *testing.T, m *Map, model map[string]KV, key string, seed uint64, run, step int) {
	t.Helper()
	got, ok := m.Get(key)
	want, wantOK := model[key]
	if ok != wantOK || (ok && !equalKV(got, want)) {
		t.Fatalf("seed=0x%x run=%d step=%d Get(%q)=%#v,%v want %#v,%v", seed, run, step, key, got, ok, want, wantOK)
	}
}

func assertWormholeAll(t *testing.T, m *Map, model map[string]KV, seed uint64, run, step int) {
	t.Helper()
	if got, want := m.Len(), int64(len(model)); got != want {
		t.Fatalf("seed=0x%x run=%d step=%d Len=%d want %d", seed, run, step, got, want)
	}
	for i := 0; i < 320; i++ {
		assertWormholeGet(t, m, model, whKey(i), seed, run, step)
	}
	assertWormholeAscend(t, m, model, "", seed, run, step)
	assertWormholeDescend(t, m, model, "", seed, run, step)
}

func assertWormholeAscend(t *testing.T, m *Map, model map[string]KV, start string, seed uint64, run, step int) {
	t.Helper()
	var got []KV
	m.Ascend(start, func(kv KV) bool {
		got = append(got, cloneKV(kv))
		return true
	})

	keys := modelSortedKeys(model)
	wantKeys := make([]string, 0, len(keys))
	for _, key := range keys {
		if start == "" || key >= start {
			wantKeys = append(wantKeys, key)
		}
	}
	want := modelKVsForKeys(model, wantKeys)
	if diff := compareKVLists(got, want); diff != "" {
		t.Fatalf("seed=0x%x run=%d step=%d Ascend(%q): %s", seed, run, step, start, diff)
	}
}

func assertWormholeDescend(t *testing.T, m *Map, model map[string]KV, start string, seed uint64, run, step int) {
	t.Helper()
	var got []KV
	m.Descend(start, func(kv KV) bool {
		got = append(got, cloneKV(kv))
		return true
	})

	keys := modelSortedKeys(model)
	wantKeys := make([]string, 0, len(keys))
	for i := len(keys) - 1; i >= 0; i-- {
		key := keys[i]
		if start == "" || key <= start {
			wantKeys = append(wantKeys, key)
		}
	}
	want := modelKVsForKeys(model, wantKeys)
	if diff := compareKVLists(got, want); diff != "" {
		t.Fatalf("seed=0x%x run=%d step=%d Descend(%q): %s", seed, run, step, start, diff)
	}
}

func assertWormholeRange(t *testing.T, m *Map, model map[string]KV, start, end string, seed uint64, run, step int) {
	t.Helper()
	var got []KV
	m.AscendRange(start, end, func(kv KV) bool {
		got = append(got, cloneKV(kv))
		return true
	})

	keys := modelSortedKeys(model)
	wantKeys := make([]string, 0, len(keys))
	for _, key := range keys {
		if start != "" && key < start {
			continue
		}
		if end != "" && key >= end {
			continue
		}
		wantKeys = append(wantKeys, key)
	}
	want := modelKVsForKeys(model, wantKeys)
	if diff := compareKVLists(got, want); diff != "" {
		t.Fatalf("seed=0x%x run=%d step=%d AscendRange(%q,%q): %s", seed, run, step, start, end, diff)
	}
}
