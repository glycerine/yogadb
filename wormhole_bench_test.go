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
	const x = true
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.Put(kvs[i], x)
	}
}

func BenchmarkKeyStablePut(b *testing.B) {
	m := makeKeyStable(b.N)
	kvs := wormBenchKVs(b.N, 0)
	const x = true
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.set(kvs[i], x)
	}
}

func BenchmarkUartPut(b *testing.B) {
	m := newUartMemtable()
	kvs := wormBenchKVs(b.N, 0)
	const x = true
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.Put(kvs[i], x)
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
	const x = true
	m := newWormhole(wormConfig{})
	for i := 0; i < n; i++ {
		m.Put(wormBenchKV(i), x)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		count := 0
		m.Ascend("", x, func(KV) bool {
			count++
			return true
		})
		if count != n {
			b.Fatalf("count=%d want %d", count, n)
		}
	}
}

func BenchmarkKeyStableAscendingScan(b *testing.B) {
	const n = 65536
	const x = true
	m := makeKeyStable(n)
	for i := 0; i < n; i++ {
		m.set(wormBenchKV(i), x)
	}
	m.ensureSorted()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		count := 0
		m.Scan(x, func(KV) bool {
			count++
			return true
		})
		if count != n {
			b.Fatalf("count=%d want %d", count, n)
		}
	}
}

func BenchmarkUartAscendingScan(b *testing.B) {
	const n = 65536
	const x = true
	m := newUartMemtable()
	for i := 0; i < n; i++ {
		m.Put(wormBenchKV(i), x)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		count := 0
		m.Scan(x, func(KV) bool {
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
	const x = true
	initial := wormBenchKVs(4096, 0)
	for i := 0; i < 4096; i++ {
		m.Put(initial[i], x)
	}
	writes := wormBenchKVs((b.N+3)/4, 4096)
	readKeys := wormBenchReadKeys(4095)
	b.ReportAllocs()
	b.ResetTimer()
	writeIdx := 0
	for i := 0; i < b.N; i++ {
		if i%4 == 0 {
			m.Put(writes[writeIdx], x)
			writeIdx++
		} else {
			_, _ = m.Get(readKeys[i&4095], x)
		}
	}
}

func BenchmarkUart_Mixed_ReadsWrites(b *testing.B) {
	m := newUartMemtable()
	const x = true
	initial := wormBenchKVs(4096, 0)
	for i := 0; i < 4096; i++ {
		m.Put(initial[i], x)
	}
	writes := wormBenchKVs((b.N+3)/4, 4096)
	readKeys := wormBenchReadKeys(4095)
	b.ReportAllocs()
	b.ResetTimer()
	writeIdx := 0
	for i := 0; i < b.N; i++ {
		if i%4 == 0 {
			m.Put(writes[writeIdx], x)
			writeIdx++
		} else {
			_, _ = m.Get(readKeys[i&4095], x)
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
	const x = true

	for i, key := range keys {
		s.Put(KV{Key: key, Value: value, Vptr: VPtr{Length: uint64(len(value))}, Hlc: HLC(i + 1)}, x)
	}
	// this is cheating versus the mixed!
	//s.BuildPointIndex(x)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		kv, found := s.Get(keys[i&(len(keys)-1)], x)
		if !found {
			b.Fatalf("get(%q) was not found", keys[i&(len(keys)-1)])
		}
		wormholeBenchKV = kv
	}
}

func BenchmarkUartGet(b *testing.B) {
	keys := wormBenchmarkKeyStableKeys(1 << 16)
	value := []byte("value")
	s := newUartMemtable()
	const x = true

	for i, key := range keys {
		s.Put(KV{Key: key, Value: value, Vptr: VPtr{Length: uint64(len(value))}, Hlc: HLC(i + 1)}, x)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		kv, found := s.Get(keys[i&(len(keys)-1)], x)
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
	const x = true

	for i, key := range keys {
		s.Put(KV{Key: key, Value: value, Vptr: VPtr{Length: uint64(len(value))}, Hlc: HLC(i + 1)}, x)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		kv, found := s.Get(keys[i&(len(keys)-1)], x)
		if !found {
			b.Fatalf("get(%q) was not found", keys[i&(len(keys)-1)])
		}
		wormholeBenchKV = kv
	}
}

func BenchmarkKeyStableGetOrdered(b *testing.B) {
	keys := wormBenchmarkKeyStableKeys(1 << 16)
	value := []byte("value")
	s := makeKeyStable(len(keys))
	const x = true

	for i, key := range keys {
		s.set(KV{Key: key, Value: value, Vptr: VPtr{Length: uint64(len(value))}, Hlc: HLC(i + 1)}, x)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		kv, found := s.get(keys[i&(len(keys)-1)], x)
		if !found {
			b.Fatalf("get(%q) was not found", keys[i&(len(keys)-1)])
		}
		wormholeBenchKV = kv
	}
}

func BenchmarkUartGetOrdered(b *testing.B) {
	keys := wormBenchmarkKeyStableKeys(1 << 16)
	value := []byte("value")
	s := newUartMemtable()
	const x = true

	for i, key := range keys {
		s.Put(KV{Key: key, Value: value, Vptr: VPtr{Length: uint64(len(value))}, Hlc: HLC(i + 1)}, x)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		kv, found := s.Get(keys[i&(len(keys)-1)], x)
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

/*

go test -v -run=xxx -bench BenchmarkWormhole_Mixed_ReadsWrites
goos: linux
goarch: amd64
pkg: github.com/glycerine/yogadb
cpu: AMD Ryzen Threadripper 3960X 24-Core Processor
BenchmarkWormhole_Mixed_ReadsWrites
BenchmarkWormhole_Mixed_ReadsWrites-48    	 8725756	       126.8 ns/op	      23 B/op	       0 allocs/op
PASS
ok  	github.com/glycerine/yogadb	1.921s

*/
// if in keystable we do no copying and just use the KV.Key to sort on:
/*
go test -v -run=xxx -bench BenchmarkKeyStable_Mixed_ReadsWrites
goos: linux
goarch: amd64
pkg: github.com/glycerine/yogadb
cpu: AMD Ryzen Threadripper 3960X 24-Core Processor
BenchmarkKeyStable_Mixed_ReadsWrites
BenchmarkKeyStable_Mixed_ReadsWrites-48    	 7793846	       142.7 ns/op	     126 B/op	       0 allocs/op
PASS
ok  	github.com/glycerine/yogadb	2.724s

AND
point reads get faster than before on keystable:

  The interesting bit: point reads improved nicely:

  BenchmarkKeyStableSet  ~60-62 ns/op
  BenchmarkKeyStableGet  ~46-47 ns/op

*/
/*
direct map[string]int slot pos; not kept because:

much slower:
go test -v -run=xxx -bench BenchmarkKeyStable_Mixed_ReadsWrites
goos: linux
goarch: amd64
pkg: github.com/glycerine/yogadb
cpu: AMD Ryzen Threadripper 3960X 24-Core Processor
BenchmarkKeyStable_Mixed_ReadsWrites
BenchmarkKeyStable_Mixed_ReadsWrites-48    	 6932964	       196.2 ns/op	     122 B/op	       0 allocs/op
PASS
ok  	github.com/glycerine/yogadb	1.844s

a bit faster:

go test -v -run=xxx -bench BenchmarkKeyStableGet
goos: linux
goarch: amd64
pkg: github.com/glycerine/yogadb
cpu: AMD Ryzen Threadripper 3960X 24-Core Processor
BenchmarkKeyStableGet
BenchmarkKeyStableGet-48    	28141448	        41.36 ns/op	       0 B/op	       0 allocs/op
PASS
ok  	github.com/glycerine/yogadb	2.179s

keystable 50% empty self managed hash chain:

go test -v -run After_Bulk
=== RUN   Test_Writes_Occuring_After_Bulk_Load_YogaDB

afterbulk_test.go:51 [pid 1258529] 2026-09-13 10:33:43.334344869 +0000 UTC using cfg.MemtableKind = keystable

afterbulk_test.go:143 [pid 1258529] 2026-09-13 10:33:53.779767471 +0000 UTC end new writes: HeapAlloc = 2_439_396_248 (diff: 318_830_496);  HeapInuse = 2_575_753_216 (diff: 307_593_216)

afterbulk_test.go:154 [pid 1258529] 2026-09-13 10:33:53.932466484 +0000 UTC after bulkload terminated with AllowReads: yogadb insert 573974.9372770304 writes/sec


afterbulk_test.go:167 [pid 1258529] 2026-09-13 10:33:56.338486805 +0000 UTC good: all 4000000 keys were distinct. len(vals) = 2000000; len(vals2) = 2000000

afterbulk_test.go:202 [pid 1258529] 2026-09-13 10:34:08.197679813 +0000 UTC good: verified all 4000000 keys
--- PASS: Test_Writes_Occuring_After_Bulk_Load_YogaDB (29.40s)
=== RUN   Test_Replacement_After_Bulk_Load_YogaDB

afterbulk_test.go:222 [pid 1258529] 2026-09-13 10:34:12.731856475 +0000 UTC using cfg.MemtableKind = keystable

afterbulk_test.go:309 [pid 1258529] 2026-09-13 10:34:39.429924517 +0000 UTC end replacements: HeapAlloc = 1_655_057_512 (diff: 311_764_920);  HeapInuse = 1_782_521_856 (diff: 303_063_040)

afterbulk_test.go:319 [pid 1258529] 2026-09-13 10:34:39.525206243 +0000 UTC after bulkload terminated with AllowReads: yogadb replacements: 93741.72542961405 writes/sec

*/
