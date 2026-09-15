package yogadb

import (
	"sync"
	"unsafe"

	"github.com/glycerine/uart"
)

/*
Implemented `MemtableUart` and wired it through the real memtable path.

Changed:
- Added `MemtableUart` enum/string/validation in [db.go](/Users/jaten/go/src/github.com/glycerine/yogadb/db.go:1107).
- Added `uart` dispatch to [memtable.go](/Users/jaten/go/src/github.com/glycerine/yogadb/memtable.go:16).
- Replaced the stub with a YogaDB `KV` adapter in [uart.go](/Users/jaten/go/src/github.com/glycerine/yogadb/uart.go:10). For `x=true`, it temporarily sets `uart.Tree.SkipLocking`; for shared paths it uses wrapper locking and leaves uart’s locking intact where appropriate.
- Added `MemtableUart` coverage to [memtable_config_test.go](/Users/jaten/go/src/github.com/glycerine/yogadb/memtable_config_test.go:9).
- Added `uart` benchmarks beside the wormhole set, plus missing aligned keyStable benches, in [wormhole_bench_test.go](/Users/jaten/go/src/github.com/glycerine/yogadb/wormhole_bench_test.go:69).
- Added `uart` to `BenchmarkMemtableInitialLoadThenOrderedScan` in [keystable_test.go](/Users/jaten/go/src/github.com/glycerine/yogadb/keystable_test.go:943).

Verification:
- `go test -run 'TestConfigMemtableKind' -count=1`
- `go test -run '^$' -count=0`

Benchmark command used:
`go test -run '^$' -bench 'Benchmark(Wormhole|KeyStable|Uart)(Put|AscendingScan|_Mixed_ReadsWrites|Get|GetOrdered)$|BenchmarkMemtableInitialLoadThenOrderedScan/(keystable|wormhole|uart)_' -benchtime=1s -count=3 -timeout=30m`

Medians on this machine, darwin/amd64 Intel i7-1068NG7:

| Workload            | keyStable   | wormhole    | uart       |
|---------------------|-------------|-------------|------------|
| Put                 | 125 ns/op   | 101 ns/op   | 502 ns/op  |
| Mixed reads/writes  | 138 ns/op   | 66.8 ns/op  | 227 ns/op  |
| Point get ordered   | 54.7 ns/op  | 232.5 ns/op | 56.5 ns/op |
| Ascending scan, 65K | 0.527 ms/op | 0.514 ms/op | 1.39 ms/op |
| Build+scan, 4K      | 1.04 ms/op  | 1.21 ms/op  | 4.47 ms/op |
| Build+scan, 65K     | 31.7 ms/op  | 26.4 ms/op  | 156.5 ms/op|

Takeaway: `uart` is already excellent for point reads, essentially tied with keyStable and about 4x faster than wormhole without its point index. The current YogaDB wrapper is write-heavy, though: `uart` put is about 4-5x slower than wormhole/keyStable here and allocates `176 B/op, 1 alloc/op`, mainly from storing `KV` through `any` and doing find-before-insert to recover the old value. For this first pass, wormhole still owns mixed workloads; keyStable remains the best all-around write/read baseline; `uart` looks promising if we can reduce value boxing and add a more direct replacement API or bulk path.
*/

type uartMemtable struct {
	mu sync.RWMutex
	t  *uart.Tree
}

func newUartMemtable() *uartMemtable {
	return &uartMemtable{t: uart.NewArtTree()}
}

func uartKey(s string) uart.Key {
	if len(s) == 0 {
		return nil
	}
	return unsafe.Slice(unsafe.StringData(s), len(s))
}

func uartKV(v any) (KV, bool) {
	switch x := v.(type) {
	case KV:
		return x, true
	case *uart.Leaf:
		return uartKV(x.Value)
	default:
		return KV{}, false
	}
}

func uartLeafKV(lf *uart.Leaf) (KV, bool) {
	if lf == nil {
		return KV{}, false
	}
	return uartKV(lf.Value)
}

func (m *uartMemtable) Len() int {
	if m == nil || m.t == nil {
		return 0
	}
	m.mu.RLock()
	n := m.t.Size()
	m.mu.RUnlock()
	return n
}

func (m *uartMemtable) clear(x bool) {
	if x {
		m.t = uart.NewArtTree()
		return
	}
	m.mu.Lock()
	m.t = uart.NewArtTree()
	m.mu.Unlock()
}

func (m *uartMemtable) Put(kv KV, x bool) (old KV, replaced bool) {
	if x {
		t := m.t
		t.SkipLocking = true
		defer func() { t.SkipLocking = false }()
		if val, _, found := t.FindExact(uartKey(kv.Key)); found {
			old, _ = uartKV(val)
			replaced = true
		}
		updated := t.Insert(uartKey(kv.Key), kv)
		if updated != replaced {
			panicf("uart memtable replacement mismatch for key %q: updated=%v replaced=%v", kv.Key, updated, replaced)
		}
		return old, replaced
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	t := m.t
	t.SkipLocking = true
	defer func() { t.SkipLocking = false }()
	if val, _, found := t.FindExact(uartKey(kv.Key)); found {
		old, _ = uartKV(val)
		replaced = true
	}
	updated := t.Insert(uartKey(kv.Key), kv)
	if updated != replaced {
		panicf("uart memtable replacement mismatch for key %q: updated=%v replaced=%v", kv.Key, updated, replaced)
	}
	return old, replaced
}

func (m *uartMemtable) Get(key string, x bool) (KV, bool) {
	if x {
		t := m.t
		t.SkipLocking = true
		defer func() { t.SkipLocking = false }()
		val, _, found := t.FindExact(uartKey(key))
		if !found {
			return KV{}, false
		}
		return uartKV(val)
	}

	m.mu.RLock()
	defer m.mu.RUnlock()
	val, _, found := m.t.FindExact(uartKey(key))
	if !found {
		return KV{}, false
	}
	return uartKV(val)
}

func (m *uartMemtable) Ascend(start string, x bool, fn func(KV) bool) {
	if x {
		t := m.t
		t.SkipLocking = true
		defer func() { t.SkipLocking = false }()
		uartAscend(t, start, fn)
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	uartAscend(m.t, start, fn)
}

func uartAscend(t *uart.Tree, start string, fn func(KV) bool) {
	it := t.Iter(uartKey(start), nil)
	for it.Next() {
		kv, ok := uartLeafKV(it.Leaf())
		if !ok || !fn(kv) {
			return
		}
	}
}

func (m *uartMemtable) Scan(x bool, fn func(KV) bool) {
	m.Ascend("", x, fn)
}

func (m *uartMemtable) Reverse(x bool, fn func(KV) bool) {
	m.Descend("", x, fn)
}

func (m *uartMemtable) Descend(start string, x bool, fn func(KV) bool) {
	if x {
		t := m.t
		t.SkipLocking = true
		defer func() { t.SkipLocking = false }()
		uartDescend(t, start, fn)
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	uartDescend(m.t, start, fn)
}

func uartDescend(t *uart.Tree, start string, fn func(KV) bool) {
	it := t.RevIter(nil, uartKey(start))
	for it.Next() {
		kv, ok := uartLeafKV(it.Leaf())
		if !ok || !fn(kv) {
			return
		}
	}
}

func (m *uartMemtable) SeekGE(target string, strict bool, x bool) (KV, bool) {
	if x {
		t := m.t
		t.SkipLocking = true
		defer func() { t.SkipLocking = false }()
		return uartSeekGE(t, target, strict)
	}

	m.mu.RLock()
	defer m.mu.RUnlock()
	return uartSeekGE(m.t, target, strict)
}

func uartSeekGE(t *uart.Tree, target string, strict bool) (KV, bool) {
	var lf *uart.Leaf
	var found bool
	if strict {
		lf, _, found = t.Find(uart.GT, uartKey(target))
	} else {
		lf, _, found = t.Find(uart.GTE, uartKey(target))
	}
	if !found {
		return KV{}, false
	}
	return uartLeafKV(lf)
}

func (m *uartMemtable) SeekLE(target string, strict bool, x bool) (KV, bool) {
	if x {
		t := m.t
		t.SkipLocking = true
		defer func() { t.SkipLocking = false }()
		return uartSeekLE(t, target, strict)
	}

	m.mu.RLock()
	defer m.mu.RUnlock()
	return uartSeekLE(m.t, target, strict)
}

func uartSeekLE(t *uart.Tree, target string, strict bool) (KV, bool) {
	var lf *uart.Leaf
	var found bool
	if target == "" {
		lf, _, found = t.Find(uart.LTE, nil)
	} else if strict {
		lf, _, found = t.Find(uart.LT, uartKey(target))
	} else {
		lf, _, found = t.Find(uart.LTE, uartKey(target))
	}
	if !found {
		return KV{}, false
	}
	return uartLeafKV(lf)
}
