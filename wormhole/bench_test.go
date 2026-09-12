package wormhole

import (
	"fmt"
	"testing"

	"github.com/glycerine/yogadb/pebbleskip"
)

func benchKey(i int) string {
	return fmt.Sprintf("k%09d", i)
}

func benchValue(i int) []byte {
	return []byte(fmt.Sprintf("value-%09d", i))
}

func BenchmarkPutWormhole(b *testing.B) {
	m := New(Options{})
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.Put(benchKey(i), benchValue(i))
	}
}

func BenchmarkPutPebbleSkip(b *testing.B) {
	arenaBytes := b.N*256 + (1 << 20)
	s := pebbleskip.New(arenaBytes, nil)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := s.Add([]byte(benchKey(i)), benchValue(i)); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkScanWormhole(b *testing.B) {
	const n = 65536
	m := New(Options{})
	for i := 0; i < n; i++ {
		m.Put(benchKey(i), benchValue(i))
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

func BenchmarkScanPebbleSkip(b *testing.B) {
	const n = 65536
	s := pebbleskip.New(n*256+(1<<20), nil)
	for i := 0; i < n; i++ {
		if err := s.Add([]byte(benchKey(i)), benchValue(i)); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		count := 0
		it := s.NewIter(nil, nil)
		for kv := it.First(); kv != nil; kv = it.Next() {
			count++
		}
		it.Close()
		if count != n {
			b.Fatalf("count=%d want %d", count, n)
		}
	}
}

func BenchmarkMixedWormhole(b *testing.B) {
	m := New(Options{})
	for i := 0; i < 4096; i++ {
		m.Put(benchKey(i), benchValue(i))
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if i%4 == 0 {
			m.Put(benchKey(4096+i), benchValue(i))
		} else {
			_, _ = m.Get(benchKey(i & 4095))
		}
	}
}

func BenchmarkMixedPebbleSkip(b *testing.B) {
	s := pebbleskip.New(b.N*128+(2<<20), nil)
	for i := 0; i < 4096; i++ {
		if err := s.Add([]byte(benchKey(i)), benchValue(i)); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if i%4 == 0 {
			if err := s.Add([]byte(benchKey(4096+i)), benchValue(i)); err != nil {
				b.Fatal(err)
			}
		} else {
			_, _ = s.Get([]byte(benchKey(i & 4095)))
		}
	}
}
