package yogadb

import (
	"encoding/binary"
	"testing"

	"github.com/glycerine/yogadb/pebbleskip"
	tbtree "github.com/tidwall/btree"
)

func wormBenchKey(i int) string {
	var buf [10]byte
	buf[0] = 'k'
	for pos := len(buf) - 1; pos > 0; pos-- {
		buf[pos] = byte('0' + i%10)
		i /= 10
	}
	return string(buf[:])
}

func wormBenchValue(i int) []byte {
	var buf [15]byte
	copy(buf[:], "value-")
	for pos := len(buf) - 1; pos >= len("value-"); pos-- {
		buf[pos] = byte('0' + i%10)
		i /= 10
	}
	return buf[:]
}

func wormBenchKV(i int) KV {
	v := wormBenchValue(i)
	return KV{Key: wormBenchKey(i), Value: v, Vptr: VPtr{Length: uint64(len(v))}, Hlc: HLC(i + 1)}
}

func wormBenchSkipKV(i int) pebbleskip.KV {
	v := wormBenchValue(i)
	return pebbleskip.KV{Key: wormBenchKey(i), Value: v, Vptr: pebbleskip.VPtr{Length: uint64(len(v))}, Hlc: pebbleskip.HLC(i + 1)}
}

func wormBenchKVs(n int, base int) []KV {
	kvs := make([]KV, n)
	for i := range kvs {
		kvs[i] = wormBenchKV(base + i)
	}
	return kvs
}

func wormBenchSkipKVs(n int, base int) []pebbleskip.KV {
	kvs := make([]pebbleskip.KV, n)
	for i := range kvs {
		kvs[i] = wormBenchSkipKV(base + i)
	}
	return kvs
}

func wormBenchReadKeys(mask int) []string {
	keys := make([]string, mask+1)
	for i := range keys {
		keys[i] = wormBenchKey(i & mask)
	}
	return keys
}

func newWormTidwallBtree() *tbtree.BTreeG[KV] {
	return tbtree.NewBTreeGOptions[KV](func(a, b KV) bool { return a.Key < b.Key }, tbtree.Options{Degree: 32})
}

func BenchmarkWormholePut(b *testing.B) {
	m := newWormhole(wormConfig{})
	kvs := wormBenchKVs(b.N, 0)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.Put(kvs[i])
	}
}

func BenchmarkTidwallBtreePut(b *testing.B) {
	tree := newWormTidwallBtree()
	kvs := wormBenchKVs(b.N, 0)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tree.Set(kvs[i])
	}
}

func BenchmarkPebbleSkipPut(b *testing.B) {
	arenaBytes := b.N*256 + (1 << 20)
	s := pebbleskip.New(arenaBytes, nil)
	kvs := wormBenchSkipKVs(b.N, 0)
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
	m := newWormhole(wormConfig{})
	for i := 0; i < n; i++ {
		m.Put(wormBenchKV(i))
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

func BenchmarkTidwallBtreeAscendingScan(b *testing.B) {
	const n = 65536
	tree := newWormTidwallBtree()
	for i := 0; i < n; i++ {
		tree.Set(wormBenchKV(i))
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		count := 0
		tree.Scan(func(KV) bool {
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
		if err := s.Add(wormBenchSkipKV(i)); err != nil {
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
	m := newWormhole(wormConfig{})
	initial := wormBenchKVs(4096, 0)
	for i := 0; i < 4096; i++ {
		m.Put(initial[i])
	}
	writes := wormBenchKVs((b.N+3)/4, 4096)
	readKeys := wormBenchReadKeys(4095)
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

func BenchmarkTidwallBtree_Mixed_ReadsWrites(b *testing.B) {
	tree := newWormTidwallBtree()
	initial := wormBenchKVs(4096, 0)
	for i := 0; i < 4096; i++ {
		tree.Set(initial[i])
	}
	writes := wormBenchKVs((b.N+3)/4, 4096)
	readKeys := wormBenchReadKeys(4095)
	b.ReportAllocs()
	b.ResetTimer()
	writeIdx := 0
	for i := 0; i < b.N; i++ {
		if i%4 == 0 {
			tree.Set(writes[writeIdx])
			writeIdx++
		} else {
			_, _ = tree.Get(KV{Key: readKeys[i&4095]})
		}
	}
}

func BenchmarkPebbleSkip_Mixed_ReadsWrites(b *testing.B) {
	s := pebbleskip.New(b.N*128+(2<<20), nil)
	initial := wormBenchSkipKVs(4096, 0)
	for i := 0; i < 4096; i++ {
		if err := s.Add(initial[i]); err != nil {
			b.Fatal(err)
		}
	}
	writes := wormBenchSkipKVs((b.N+3)/4, 4096)
	readKeys := wormBenchReadKeys(4095)
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

func BenchmarkTidwallBtreeGet(b *testing.B) {
	keys := wormBenchmarkKeyStableKeys(1 << 16)
	value := []byte("value")
	tree := newWormTidwallBtree()

	for i, key := range keys {
		tree.Set(KV{Key: key, Value: value, Vptr: VPtr{Length: uint64(len(value))}, Hlc: HLC(i + 1)})
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		kv, found := tree.Get(KV{Key: keys[i&(len(keys)-1)]})
		if !found {
			b.Fatalf("get(%q) was not found", keys[i&(len(keys)-1)])
		}
		wormholeBenchKV = kv
	}
}

func BenchmarkTidwallBtreeGetOrdered(b *testing.B) {
	keys := wormBenchmarkKeyStableKeys(1 << 16)
	value := []byte("value")
	tree := newWormTidwallBtree()

	for i, key := range keys {
		tree.Set(KV{Key: key, Value: value, Vptr: VPtr{Length: uint64(len(value))}, Hlc: HLC(i + 1)})
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		kv, found := tree.Get(KV{Key: keys[i&(len(keys)-1)]})
		if !found {
			b.Fatalf("get(%q) was not found", keys[i&(len(keys)-1)])
		}
		wormholeBenchKV = kv
	}
}

// ported from keystable benchmark for apples-to-apples

var (
	wormholeBenchKV KV
)

func BenchmarkWormholeGet(b *testing.B) {
	keys := wormBenchmarkKeyStableKeys(1 << 16)
	value := []byte("value")
	s := newWormhole(wormConfig{})

	for i, key := range keys {
		s.Put(KV{Key: key, Value: value, Vptr: VPtr{Length: uint64(len(value))}, Hlc: HLC(i + 1)})
	}
	s.BuildPointIndex()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		kv, found := s.Get(keys[i&(len(keys)-1)])
		if !found {
			b.Fatalf("get(%q) was not found", keys[i&(len(keys)-1)])
		}
		wormholeBenchKV = kv
	}
}

func BenchmarkWormholeGetOrdered(b *testing.B) {
	keys := wormBenchmarkKeyStableKeys(1 << 16)
	value := []byte("value")
	s := newWormhole(wormConfig{})

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
		wormholeBenchKV = kv
	}
}

func wormBenchmarkKeyStableKeys(n int) []string {
	keys := make([]string, n)
	var buf [16]byte
	for i := range keys {
		binary.LittleEndian.PutUint64(buf[:8], uint64(i)*0x9e3779b97f4a7c15)
		binary.LittleEndian.PutUint64(buf[8:], uint64(i))
		keys[i] = string(buf[:])
	}
	return keys
}
