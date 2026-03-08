package yogadb

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/dgraph-io/badger/v3/skl"
	"github.com/dgraph-io/badger/v3/y"
	"os"
	"time"

	tbtree "github.com/tidwall/btree"

	googbtree "github.com/google/btree"

	"github.com/glycerine/uart"

	"github.com/puzpuzpuz/xsync/v3"

	"math/rand"
	"sync"
	"testing"
	//"time"

	rb "github.com/glycerine/rbtree"
)

const testBenchSeed = 1

// Benchmark
func loadTestFile(path string) [][]byte {
	file, err := os.Open(path)
	if err != nil {
		panic("Couldn't open " + path)
	}
	defer file.Close()

	var words [][]byte
	reader := bufio.NewReader(file)
	for {
		if line, err := reader.ReadBytes(byte('\n')); err != nil {
			break
		} else {
			if len(line) > 0 {
				words = append(words, line[:len(line)-1])
			}
		}
	}
	return words
}

func newValue(v int) []byte {
	return []byte(fmt.Sprintf("%05d", v))
}

func randomKey(rng *rand.Rand, b []byte) []byte {
	key := rng.Uint32()
	key2 := rng.Uint32()
	binary.LittleEndian.PutUint32(b, key)
	binary.LittleEndian.PutUint32(b[4:], key2)
	return b
}

func randomKey2(rng *rand.Rand) []byte {
	b := make([]byte, 8)
	key := rng.Uint32()
	key2 := rng.Uint32()
	binary.LittleEndian.PutUint32(b, key)
	binary.LittleEndian.PutUint32(b[4:], key2)
	return b
}

// Standard test. Some fraction is read. Some fraction is write. Writes have
// to go through mutex lock.
func BenchmarkSklReadWrite(b *testing.B) {
	value := newValue(123)
	for i := 0; i <= 10; i++ {
		readFrac := float32(i) / 10.0
		b.Run(fmt.Sprintf("frac_%d", i), func(b *testing.B) {
			l := skl.NewSkiplist(int64((b.N + 1) * skl.MaxNodeSize))
			defer l.DecrRef()
			b.ResetTimer()
			var count int
			b.RunParallel(func(pb *testing.PB) {
				rng := rand.New(rand.NewSource(testBenchSeed))
				var rkey [8]byte
				for pb.Next() {
					rk := randomKey(rng, rkey[:])

					if rng.Float32() < readFrac {
						v := l.Get(rk)
						if v.Value != nil {
							count++
						}
					} else {
						l.Put(rk, y.ValueStruct{Value: value, Meta: 0, UserMeta: 0})
					}
				}
			})
		})
	}
}

// Standard test. Some fraction is read. Some fraction is write. Writes have
// to go through mutex lock.
func BenchmarkArtReadWrite(b *testing.B) {
	value := newValue(123)
	for i := 0; i <= 10; i++ {
		readFrac := float32(i) / 10.0
		b.Run(fmt.Sprintf("frac_%d", i), func(b *testing.B) {
			l := uart.NewArtTree()
			b.ResetTimer()
			//var count int
			b.RunParallel(func(pb *testing.PB) {
				rng := rand.New(rand.NewSource(testBenchSeed))
				var rkey [8]byte
				for pb.Next() {
					rk := randomKey(rng, rkey[:])

					if rng.Float32() < readFrac {
						l.FindExact(rk)
					} else {
						l.Insert(rk, value)
					}
				}
			})
		})
	}
}

/*
// makes no difference for Art, the allocating
// the key inside the FindExact()/Insert() calls.
// Bizarrely it makes a 2x difference for the Ctrie.
func BenchmarkArtReadWrite2(b *testing.B) {
	value := newValue(123)
	for i := 0; i <= 10; i++ {
		readFrac := float32(i) / 10.0
		b.Run(fmt.Sprintf("frac_%d", i), func(b *testing.B) {
			l := NewArtTree()
			b.ResetTimer()

			b.RunParallel(func(pb *testing.PB) {
				rng := rand.New(rand.NewSource(testBenchSeed))
				for pb.Next() {

					if rng.Float32() < readFrac {
						l.FindExact(randomKey2(rng))
					} else {
						l.Insert(randomKey2(rng), value)
					}
				}
			})
		})
	}
}
*/

func BenchmarkArtLinuxPaths(b *testing.B) {

	paths := loadTestFile("assets/linux.txt")
	n := len(paths)
	_ = n

	//for i := 0; i <= 1; i++ {
	for i := 0; i <= 10; i++ {
		readFrac := float32(i) / 10.0
		_ = readFrac
		b.Run(fmt.Sprintf("frac_%d", i), func(b *testing.B) {
			l := uart.NewArtTree()
			b.ResetTimer()
			//var count int
			b.RunParallel(func(pb *testing.PB) {
				rng := rand.New(rand.NewSource(testBenchSeed))
				for pb.Next() {
					for k := range paths {
						if rng.Float32() < readFrac {
							//l.FindExact(randomKey(rng))
							l.FindExact(paths[k])
							//l.Remove(paths[k])
						} else {
							//l.Insert(randomKey(rng), value)
							l.Insert(paths[k], paths[k])
						}
					}
				}
			})
		})
	}
}

// Standard test. Some fraction is read. Some fraction is write. Writes have
// to go through mutex lock.
func BenchmarkReadWrite_map_RWMutex_wrapped(b *testing.B) {
	value := newValue(123)
	for i := 0; i <= 10; i++ {
		readFrac := float32(i) / 10.0
		b.Run(fmt.Sprintf("frac_%d", i), func(b *testing.B) {
			m := make(map[string][]byte)
			var mutex sync.RWMutex
			b.ResetTimer()
			var count int
			b.RunParallel(func(pb *testing.PB) {
				rng := rand.New(rand.NewSource(testBenchSeed))
				var rkey [8]byte
				for pb.Next() {
					rk := randomKey(rng, rkey[:])
					if rng.Float32() < readFrac {
						mutex.RLock()
						_, ok := m[string(rk)]
						mutex.RUnlock()
						if ok {
							count++
						}
					} else {
						mutex.Lock()
						m[string(rk)] = value
						mutex.Unlock()
					}
				}
			})
		})
	}
}

// bah. will crash the tester if run in parallel.
// so don't run in parallel.
func BenchmarkReadWrite_Map_NoMutex_NoParallel(b *testing.B) {
	value := newValue(123)
	for i := 0; i <= 10; i++ {
		readFrac := float32(i) / 10.0
		b.Run(fmt.Sprintf("frac_%d", i), func(b *testing.B) {
			m := make(map[string][]byte)
			b.ResetTimer()
			var count int

			rng := rand.New(rand.NewSource(testBenchSeed))
			var rkey [8]byte

			for range b.N {
				rk := randomKey(rng, rkey[:])
				if rng.Float32() < readFrac {
					_, ok := m[string(rk)]
					if ok {
						count++
					}
				} else {
					m[string(rk)] = value
				}
			}
		})
	}
}

// Hmm... turns out the locking makes little (in a single
// threaded scenario, but of course!)
//
// without locking: (100% writes 1st, 100% reads 2nd) single goroutine
// BenchmarkArtReadWrite_NoLocking_NoParallel/frac_0-8         	 1750966	       697.6 ns/op	     192 B/op	       4 allocs/op
// BenchmarkArtReadWrite_NoLocking_NoParallel/frac_10-8        	86214415	        13.32 ns/op	       0 B/op	       0 allocs/op
//
// with locking (single goroutine)
// BenchmarkArtReadWrite_NoLocking_NoParallel/frac_0-8         	 1652438	       704.1 ns/op	     194 B/op	       4 allocs/op
// BenchmarkArtReadWrite_NoLocking_NoParallel/frac_10-8        	53834798	        20.88 ns/op	       0 B/op	       0 allocs/op
//

func BenchmarkArtReadWrite_NoLocking_NoParallel(b *testing.B) {
	value := newValue(123)
	for i := 0; i <= 10; i++ {
		readFrac := float32(i) / 10.0
		b.Run(fmt.Sprintf("frac_%d", i), func(b *testing.B) {
			l := uart.NewArtTree()
			l.SkipLocking = true
			b.ResetTimer()

			rng := rand.New(rand.NewSource(testBenchSeed))
			var rkey [8]byte

			for range b.N {
				rk := randomKey(rng, rkey[:])
				if rng.Float32() < readFrac {
					l.FindExact(rk)
				} else {
					l.Insert(rk, value)
				}
			}
		})
	}
}

type kvs struct {
	key string
	val string
}

func newRBtree() *rb.Tree {
	rbtree := rb.NewTree(func(a, b rb.Item) int {
		av := a.(*kvs).key
		bv := b.(*kvs).key
		if av > bv {
			return -1
		}
		if av < bv {
			return 1
		}
		return 0
	})
	return rbtree
}

func BenchmarkReadWrite_RedBlackTree(b *testing.B) {

	tree := newRBtree()

	//value := newValue(123)
	for i := 0; i <= 10; i++ {
		readFrac := float32(i) / 10.0
		b.Run(fmt.Sprintf("frac_%d", i), func(b *testing.B) {

			b.ResetTimer()
			var count int

			rng := rand.New(rand.NewSource(testBenchSeed))
			var rkey [8]byte

			for range b.N {
				v := randomKey(rng, rkey[:])
				str := string(v)
				if rng.Float32() < readFrac {
					query := &kvs{
						key: str,
					}
					it := tree.FindGE(query)
					ok := !it.Limit()
					if ok {
						count++
					}
				} else {
					pay := &kvs{
						key: str,
						val: str,
					}
					tree.Insert(pay)
					//m[string(randomKey(rng))] = value
				}
			}
		})
		//vv("count = %v", count)
		//_ = count
	}
}

//func LeafLess(a, b *Leaf) bool {
//	return bytes.Compare(a.Key, b.Key) < 0
//}

// 100% writes: 964 ns/op at degree 30.
// 100% writes: 1842 ns/op at degree 3000.
// degree 30:
/*
BenchmarkReadWrite_GoogleBtree/read_frac_0-8         	 5468764	       216.3 ns/op
BenchmarkReadWrite_GoogleBtree/read_frac_1
BenchmarkReadWrite_GoogleBtree/read_frac_1-8         	 5615397	       207.9 ns/op
BenchmarkReadWrite_GoogleBtree/read_frac_2
BenchmarkReadWrite_GoogleBtree/read_frac_2-8         	 5063714	       209.4 ns/op
BenchmarkReadWrite_GoogleBtree/read_frac_3
BenchmarkReadWrite_GoogleBtree/read_frac_3-8         	 5534418	       205.9 ns/op
BenchmarkReadWrite_GoogleBtree/read_frac_4
BenchmarkReadWrite_GoogleBtree/read_frac_4-8         	 5513121	       210.0 ns/op
BenchmarkReadWrite_GoogleBtree/read_frac_5
BenchmarkReadWrite_GoogleBtree/read_frac_5-8         	 5585946	       203.6 ns/op
BenchmarkReadWrite_GoogleBtree/read_frac_6
BenchmarkReadWrite_GoogleBtree/read_frac_6-8         	 5893560	       210.0 ns/op
BenchmarkReadWrite_GoogleBtree/read_frac_7
BenchmarkReadWrite_GoogleBtree/read_frac_7-8         	 5606530	       207.6 ns/op
BenchmarkReadWrite_GoogleBtree/read_frac_8
BenchmarkReadWrite_GoogleBtree/read_frac_8-8         	 5667998	       196.1 ns/op
BenchmarkReadWrite_GoogleBtree/read_frac_9
BenchmarkReadWrite_GoogleBtree/read_frac_9-8         	 6190226	       195.3 ns/op
BenchmarkReadWrite_GoogleBtree/read_frac_10
BenchmarkReadWrite_GoogleBtree/read_frac_10-8        	214434543	         5.783 ns/op
*/
func BenchmarkReadWrite_GoogleBtree(b *testing.B) {

	// 30 was fastest with all writes that I saw: 876 ns/op
	// 3000 is fastest with no contention for reading and writing.
	//degree := 3000
	//degree := 30 // 964-1144 ns/op all writes
	//degree := 10 // 1155 ns/op all writes degree 10.
	degree := 30 // 964 ns/op all writes degree 50

	for i := 0; i <= 10; i++ {
		readFrac := float32(i) / 10.0
		b.Run(fmt.Sprintf("read_frac_%d", i), func(b *testing.B) {
			//b.Run(fmt.Sprintf("read_frac_100_pct"), func(b *testing.B) {

			//tree := googbtree.NewG[string](degree, googbtree.Less[string]())
			tree := googbtree.NewG[[]byte](degree,
				func(a, b []byte) bool {
					return bytes.Compare(a, b) < 0
				})

			var seed [32]byte
			prng := newPRNG(seed)

			// pre-gen random keys
			nkey := 10_000
			keys := make([][]byte, nkey)
			var cid string
			for i := range keys {
				cid = prng.NewCallID()
				keys[i] = []byte(cid)
			}
			rng := rand.New(rand.NewSource(testBenchSeed))
			_ = rng

			b.ResetTimer()

			for i := range b.N {
				if rng.Float32() < readFrac {
					tree.Get(keys[i%nkey])
				} else {
					tree.ReplaceOrInsert(keys[i%nkey])
				}
			}
		})
	}
	//vv("count = %v", count)
	//_ = count
	//}
}

// googbtree

func Benchmark_ParallelReadWrite_googbtree(b *testing.B) {

	degree := 30
	btree := googbtree.NewG[string](degree, googbtree.Less[string]())
	_ = btree

	//var ptr atomic.Pointer

	//value := newValue(123)
	for i := 0; i <= 10; i++ {
		readFrac := float32(i) / 10.0
		b.Run(fmt.Sprintf("frac_%d", i), func(b *testing.B) {

			rbmut := xsync.NewRBMutex()
			//var rwmut sync.RWMutex
			b.ResetTimer()
			var count int
			b.RunParallel(func(pb *testing.PB) {
				rng := rand.New(rand.NewSource(testBenchSeed))
				for pb.Next() {
					if rng.Float32() < readFrac {

						tok := rbmut.RLock()
						//rwmut.RLock()
						_, ok := btree.Get(string(randomKey2(rng)))
						//ok = true
						//rwmut.RUnlock()
						rbmut.RUnlock(tok)
						//_, ok := m.Load(string(randomKey2(rng)))
						if ok {
							count++
						}
					} else {
						rbmut.Lock()
						btree.ReplaceOrInsert(string(randomKey2(rng)))
						rbmut.Unlock()
						//m.Store(string(randomKey2(rng)), value)
					}
				}
			})
		})
	}
}

// Standard test. Some fraction is read. Some fraction is write. Writes have
// to go through mutex lock.
func BenchmarkReadWriteSyncMap(b *testing.B) {
	value := newValue(123)
	for i := 0; i <= 10; i++ {
		readFrac := float32(i) / 10.0
		b.Run(fmt.Sprintf("frac_%d", i), func(b *testing.B) {
			var m sync.Map
			b.ResetTimer()
			var count int
			b.RunParallel(func(pb *testing.PB) {
				rng := rand.New(rand.NewSource(testBenchSeed))
				for pb.Next() {
					if rng.Float32() < readFrac {
						_, ok := m.Load(string(randomKey2(rng)))
						if ok {
							count++
						}
					} else {
						m.Store(string(randomKey2(rng)), value)
					}
				}
			})
		})
	}
}

// github.com/tidwall/btree

func BenchmarkTidwallBtreeConcurrentInsert(b *testing.B) {
	l := tbtree.NewGenericOptions[[]byte](func(a, b []byte) bool {
		return bytes.Compare(a, b) < 0
	}, tbtree.Options{NoLocks: false})
	b.ResetTimer()
	//var count int
	b.RunParallel(func(pb *testing.PB) {
		rng := rand.New(rand.NewSource(time.Now().UnixNano()))
		var rkey [8]byte
		for pb.Next() {
			rk := randomKey(rng, rkey[:])
			l.Set(rk)
		}
	})
}

// 100% writes 983-1006 ns/op, degree 32.
// at degree 50, more reads => faster, of course. 100% reads = 20.20 ns/op. 100 writes: 984ns/op.
// node 32
/*
BenchmarkReadWriteTidwallBtree/read_frac_0
BenchmarkReadWriteTidwallBtree/read_frac_0-8         	 5224664	       214.6 ns/op
BenchmarkReadWriteTidwallBtree/read_frac_1
BenchmarkReadWriteTidwallBtree/read_frac_1-8         	 5348961	       210.5 ns/op
BenchmarkReadWriteTidwallBtree/read_frac_2
BenchmarkReadWriteTidwallBtree/read_frac_2-8         	 5403148	       225.4 ns/op
BenchmarkReadWriteTidwallBtree/read_frac_3
BenchmarkReadWriteTidwallBtree/read_frac_3-8         	 5299521	       218.3 ns/op
BenchmarkReadWriteTidwallBtree/read_frac_4
BenchmarkReadWriteTidwallBtree/read_frac_4-8         	 5189113	       221.6 ns/op
BenchmarkReadWriteTidwallBtree/read_frac_5
BenchmarkReadWriteTidwallBtree/read_frac_5-8         	 5164780	       218.0 ns/op
BenchmarkReadWriteTidwallBtree/read_frac_6
BenchmarkReadWriteTidwallBtree/read_frac_6-8         	 5268568	       216.1 ns/op
BenchmarkReadWriteTidwallBtree/read_frac_7
BenchmarkReadWriteTidwallBtree/read_frac_7-8         	 5405991	       214.8 ns/op
BenchmarkReadWriteTidwallBtree/read_frac_8
BenchmarkReadWriteTidwallBtree/read_frac_8-8         	 5461636	       214.7 ns/op
BenchmarkReadWriteTidwallBtree/read_frac_9
BenchmarkReadWriteTidwallBtree/read_frac_9-8         	 5681372	       214.1 ns/op
BenchmarkReadWriteTidwallBtree/read_frac_10
BenchmarkReadWriteTidwallBtree/read_frac_10-8        	58361557	        19.93 ns/op
*/
func BenchmarkReadWriteTidwallBtree(b *testing.B) {
	//value := newValue(123)
	for i := 0; i <= 10; i++ {
		readFrac := float32(i) / 10.0
		//	_ = readFrac
		b.Run(fmt.Sprintf("read_frac_%d", i), func(b *testing.B) {
			//b.Run(fmt.Sprintf("read_frac_100_pct"), func(b *testing.B) {

			//l := tbtree.NewGenericOptions[[]byte](func(a, b []byte) bool {
			l := tbtree.NewBTreeGOptions[[]byte](func(a, b []byte) bool {
				return bytes.Compare(a, b) < 0
			}, tbtree.Options{
				NoLocks: false,
				Degree:  32,
				//Degree: 50,
			})

			var seed [32]byte
			prng := newPRNG(seed)

			// pre-gen random keys
			nkey := 10_000
			keys := make([][]byte, nkey)
			var cid string
			for i := range nkey {
				cid = prng.NewCallID()
				keys[i] = []byte(cid)
			}
			rng := rand.New(rand.NewSource(testBenchSeed))
			_ = rng

			b.ResetTimer()

			for i := range b.N {
				if rng.Float32() < readFrac {
					l.Get(keys[i%nkey])
				} else {
					l.Set(keys[i%nkey])
				}
			}
		})
	}
}

func BenchmarkReadWrite_parallel_TidwallBtree(b *testing.B) {
	//value := newValue(123)
	for i := 0; i <= 10; i++ {
		readFrac := float32(i) / 10.0
		b.Run(fmt.Sprintf("frac_%d", i), func(b *testing.B) {

			//l := tbtree.NewGenericOptions[[]byte](func(a, b []byte) bool {
			l := tbtree.NewBTreeGOptions[[]byte](func(a, b []byte) bool {
				return bytes.Compare(a, b) < 0
			}, tbtree.Options{NoLocks: false})

			b.ResetTimer()

			b.RunParallel(func(pb *testing.PB) {
				rng := rand.New(rand.NewSource(testBenchSeed))
				for pb.Next() {
					if rng.Float32() < readFrac {
						l.Get(randomKey2(rng))
					} else {
						l.Set(randomKey2(rng))
					}
				}
			})
		})
	}
}

func BenchmarkReadWriteYogaDB(b *testing.B) {
	value := newValue(123)
	for i := 0; i <= 10; i++ {
		readFrac := float32(i) / 10.0
		b.Run(fmt.Sprintf("frac_%d", i), func(b *testing.B) {

			dir := b.TempDir()
			db, err := OpenFlexDB(dir, nil)
			if err != nil {
				b.Fatalf("OpenFlexDB: %v", err)
			}
			b.Cleanup(func() {
				//alwaysPrintf("%v end of test, YogaDB DirSize='%v'", b.Name(), MustDirSize(dir))
				db.Close()
			})

			b.ResetTimer()

			b.RunParallel(func(pb *testing.PB) {
				rng := rand.New(rand.NewSource(testBenchSeed))
				for pb.Next() {
					if rng.Float32() < readFrac {
						db.Get(randomKey2(rng))
					} else {
						db.Put(randomKey2(rng), value)
					}
				}
			})
		})
	}
}
