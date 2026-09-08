package yogadb

import (
	"fmt"
	"os"
	"testing"

	"github.com/glycerine/greenpack/msgp"
)

func makeTestGreenMEMWAL_KV(j int) (g *GreenMEMWAL_KV) {
	g = &GreenMEMWAL_KV{
		WalRecordType: int32(1 + j),
		VptrLength:    uint64(2 + j),
		VptrOffset:    uint64(3 + j),
		Hlc:           int64(4 + j),
		Key:           "five_key",
		InlineVal:     []byte("five_value"),
		CRC32c:        [4]byte{byte(6 + j), byte(5 + j), byte(4 + j), byte(3 + j)},
	}
	return
}

func Test222TestLoadSave_of_GreenMEMWAL_KV(t *testing.T) {

	N := 10
	fn := "test.greenwal_kv.222.msgp"
	f, err := os.Create(fn)
	panicOn(err)
	defer os.Remove(fn)
	w := msgp.NewWriter(f)

	g := make([]*GreenMEMWAL_KV, N)
	for i := range N {
		g[i] = makeTestGreenMEMWAL_KV(i)
		err = g[i].Save(w)
		panicOn(err)
	}

	w.Flush()
	f.Close()

	f2, err := os.Open(fn)
	panicOn(err)
	defer f.Close()

	g2 := make([]*GreenMEMWAL_KV, N)
	r := msgp.NewReader(f2)
	for i := range N {
		var nr int
		var err error
		g2[i], nr, err = LoadMEMWAL(r)
		panicOn(err)
		_ = nr
		//vv("nr = %v", nr)

		if !g2[i].Equal(g[i]) {
			fmt.Printf("\n wrote to disk:        g[%v] = %#v\n", i, g[i])
			fmt.Printf("\n from disk read back: g2[%v] = %#v\n", i, g2[i])
			panicf("not equal: g2 != g at i = %v", i)
		}
	}
}
