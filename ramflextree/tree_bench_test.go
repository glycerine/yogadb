package ramflextree

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand"
	"testing"
	"time"

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
