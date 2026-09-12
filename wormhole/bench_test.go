package wormhole

import (
	"encoding/binary"
	"testing"

	"github.com/glycerine/yogadb/pebbleskip"
)

func benchKey(i int) string {
	var buf [10]byte
	buf[0] = 'k'
	for pos := len(buf) - 1; pos > 0; pos-- {
		buf[pos] = byte('0' + i%10)
		i /= 10
	}
	return string(buf[:])
}

func benchValue(i int) []byte {
	var buf [15]byte
	copy(buf[:], "value-")
	for pos := len(buf) - 1; pos >= len("value-"); pos-- {
		buf[pos] = byte('0' + i%10)
		i /= 10
	}
	return buf[:]
}

func benchKV(i int) KV {
	v := benchValue(i)
	return KV{Key: benchKey(i), Value: v, Vptr: VPtr{Length: uint64(len(v))}, Hlc: HLC(i + 1)}
}

func benchSkipKV(i int) pebbleskip.KV {
	v := benchValue(i)
	return pebbleskip.KV{Key: benchKey(i), Value: v, Vptr: pebbleskip.VPtr{Length: uint64(len(v))}, Hlc: pebbleskip.HLC(i + 1)}
}

func benchKVs(n int, base int) []KV {
	kvs := make([]KV, n)
	for i := range kvs {
		kvs[i] = benchKV(base + i)
	}
	return kvs
}

func benchSkipKVs(n int, base int) []pebbleskip.KV {
	kvs := make([]pebbleskip.KV, n)
	for i := range kvs {
		kvs[i] = benchSkipKV(base + i)
	}
	return kvs
}

func benchReadKeys(mask int) []string {
	keys := make([]string, mask+1)
	for i := range keys {
		keys[i] = benchKey(i & mask)
	}
	return keys
}

func BenchmarkWormholePut(b *testing.B) {
	m := New(Options{})
	kvs := benchKVs(b.N, 0)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.Put(kvs[i])
	}
}

func BenchmarkPebbleSkipPut(b *testing.B) {
	arenaBytes := b.N*256 + (1 << 20)
	s := pebbleskip.New(arenaBytes, nil)
	kvs := benchSkipKVs(b.N, 0)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := s.Add(kvs[i]); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkWormholeAscendingScan(b *testing.B) {
	const n = 65536
	m := New(Options{})
	for i := 0; i < n; i++ {
		m.Put(benchKV(i))
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		count := 0
		m.Ascend("", func(KV) bool {
			count++
			return true
		})
		if count != n {
			b.Fatalf("count=%d want %d", count, n)
		}
	}
}

func BenchmarkPebbleSkipAscendingScan(b *testing.B) {
	const n = 65536
	s := pebbleskip.New(n*256+(1<<20), nil)
	for i := 0; i < n; i++ {
		if err := s.Add(benchSkipKV(i)); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		count := 0
		it := s.NewIter("", "")
		for kv := it.First(); kv != nil; kv = it.Next() {
			count++
		}
		it.Close()
		if count != n {
			b.Fatalf("count=%d want %d", count, n)
		}
	}
}

func BenchmarkWormhole_Mixed_ReadsWrites(b *testing.B) {
	m := New(Options{})
	initial := benchKVs(4096, 0)
	for i := 0; i < 4096; i++ {
		m.Put(initial[i])
	}
	writes := benchKVs((b.N+3)/4, 4096)
	readKeys := benchReadKeys(4095)
	b.ReportAllocs()
	b.ResetTimer()
	writeIdx := 0
	for i := 0; i < b.N; i++ {
		if i%4 == 0 {
			m.Put(writes[writeIdx])
			writeIdx++
		} else {
			_, _ = m.Get(readKeys[i&4095])
		}
	}
}

func BenchmarkPebbleSkip_Mixed_ReadsWrites(b *testing.B) {
	s := pebbleskip.New(b.N*128+(2<<20), nil)
	initial := benchSkipKVs(4096, 0)
	for i := 0; i < 4096; i++ {
		if err := s.Add(initial[i]); err != nil {
			b.Fatal(err)
		}
	}
	writes := benchSkipKVs((b.N+3)/4, 4096)
	readKeys := benchReadKeys(4095)
	b.ReportAllocs()
	b.ResetTimer()
	writeIdx := 0
	for i := 0; i < b.N; i++ {
		if i%4 == 0 {
			if err := s.Add(writes[writeIdx]); err != nil {
				b.Fatal(err)
			}
			writeIdx++
		} else {
			_, _ = s.Get(readKeys[i&4095])
		}
	}
}

// ported from keystable benchmark for apples-to-apples

var (
	keyStableBenchKV KV
)

func BenchmarkWormholeGet(b *testing.B) {
	keys := benchmarkKeyStableKeys(1 << 16)
	value := []byte("value")
	//s := makeKeyStable(len(keys))

	s := New(Options{})

	for i, key := range keys {
		s.Put(KV{Key: key, Value: value, Vptr: VPtr{Length: uint64(len(value))}, Hlc: HLC(i + 1)})
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		kv, found := s.Get(keys[i&(len(keys)-1)])
		if !found {
			b.Fatalf("get(%q) was not found", keys[i&(len(keys)-1)])
		}
		keyStableBenchKV = kv
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
