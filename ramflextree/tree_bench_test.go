package ramflextree

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"

	googbtree "github.com/google/btree"
	"github.com/glycerine/uart"
	"github.com/puzpuzpuz/xsync/v3"
	tbtree "github.com/tidwall/btree"
)

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

func BenchmarkReadWriteYogaDB(b *testing.B) {
	value := newValue(123)
	for i := 0; i <= 10; i++ {
		readFrac := float32(i) / 10.0
		b.Run(fmt.Sprintf("frac_%d", i), func(b *testing.B) {

			db, err := OpenFlexDB("", nil)
			if err != nil {
				b.Fatalf("OpenFlexDB: %v", err)
			}
			b.Cleanup(func() {
				db.Close()
			})

			b.ResetTimer()

			b.RunParallel(func(pb *testing.PB) {
				rng := rand.New(rand.NewSource(testBenchSeed))
				for pb.Next() {
					if rng.Float32() < readFrac {
						db.Get(string(randomKey2(rng)))
					} else {
						db.Put(string(randomKey2(rng)), value)
					}
				}
			})
		})
	}
}

func BenchmarkTidwallBtreeConcurrentInsert(b *testing.B) {
	l := tbtree.NewGenericOptions[[]byte](func(a, b []byte) bool {
		return bytes.Compare(a, b) < 0
	}, tbtree.Options{NoLocks: false})
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		rng := rand.New(rand.NewSource(time.Now().UnixNano()))
		var rkey [8]byte
		for pb.Next() {
			rk := randomKey(rng, rkey[:])
			l.Set(rk)
		}
	})
}

func BenchmarkReadWriteTidwallBtree(b *testing.B) {
	for i := 0; i <= 10; i++ {
		readFrac := float32(i) / 10.0
		b.Run(fmt.Sprintf("read_frac_%d", i), func(b *testing.B) {

			l := tbtree.NewBTreeGOptions[[]byte](func(a, b []byte) bool {
				return bytes.Compare(a, b) < 0
			}, tbtree.Options{
				NoLocks: false,
				Degree:  32,
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
	for i := 0; i <= 10; i++ {
		readFrac := float32(i) / 10.0
		b.Run(fmt.Sprintf("frac_%d", i), func(b *testing.B) {

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

// ====================== Art tree benchmarks ======================

func BenchmarkArtReadWrite(b *testing.B) {
	value := newValue(123)
	for i := 0; i <= 10; i++ {
		readFrac := float32(i) / 10.0
		b.Run(fmt.Sprintf("frac_%d", i), func(b *testing.B) {
			l := uart.NewArtTree()
			b.ResetTimer()
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

func BenchmarkArtLinuxPaths(b *testing.B) {

	paths := loadTestFile("../assets/linux.txt")

	for i := 0; i <= 10; i++ {
		readFrac := float32(i) / 10.0
		b.Run(fmt.Sprintf("frac_%d", i), func(b *testing.B) {
			l := uart.NewArtTree()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				rng := rand.New(rand.NewSource(testBenchSeed))
				for pb.Next() {
					for k := range paths {
						if rng.Float32() < readFrac {
							l.FindExact(paths[k])
						} else {
							l.Insert(paths[k], paths[k])
						}
					}
				}
			})
		})
	}
}

// ====================== Google B-tree benchmarks ======================

func BenchmarkReadWrite_GoogleBtree(b *testing.B) {
	degree := 30

	for i := 0; i <= 10; i++ {
		readFrac := float32(i) / 10.0
		b.Run(fmt.Sprintf("read_frac_%d", i), func(b *testing.B) {

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
}

func Benchmark_ParallelReadWrite_googbtree(b *testing.B) {
	degree := 30
	btree := googbtree.NewG[string](degree, googbtree.Less[string]())

	for i := 0; i <= 10; i++ {
		readFrac := float32(i) / 10.0
		b.Run(fmt.Sprintf("frac_%d", i), func(b *testing.B) {

			rbmut := xsync.NewRBMutex()
			b.ResetTimer()
			var count int
			b.RunParallel(func(pb *testing.PB) {
				rng := rand.New(rand.NewSource(testBenchSeed))
				for pb.Next() {
					if rng.Float32() < readFrac {
						tok := rbmut.RLock()
						_, ok := btree.Get(string(randomKey2(rng)))
						rbmut.RUnlock(tok)
						if ok {
							count++
						}
					} else {
						rbmut.Lock()
						btree.ReplaceOrInsert(string(randomKey2(rng)))
						rbmut.Unlock()
					}
				}
			})
		})
	}
}

// ====================== SyncMap benchmark ======================

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

// ====================== RWMutex-map benchmarks ======================

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
