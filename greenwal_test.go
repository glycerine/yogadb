package yogadb

import (
	"fmt"
	"os"
	"testing"

	"github.com/glycerine/greenpack/msgp"
)

func makeTestGreenMEMWAL_KV() (g *GreenMEMWAL_KV) {
	g = &GreenMEMWAL_KV{
		WalRecordType: 1,
		VptrLength:    2,
		VptrOffset:    3,
		Hlc:           4,
		Key:           "five_key",
		InlineVal:     []byte("five_value"),
		CRC32c:        [4]byte{6, 5, 4, 3},
	}
	return
}

func Test222TestLoadSave_of_GreenMEMWAL_KV(t *testing.T) {

	g := makeTestGreenMEMWAL_KV()

	fn := "test.greenwal_kv.222.msgp"
	f, err := os.Create(fn)
	panicOn(err)
	defer os.Remove(fn)
	w := msgp.NewWriter(f)

	err = g.Save(w)
	panicOn(err)

	w.Flush()
	f.Close()

	f2, err := os.Open(fn)
	panicOn(err)
	defer f.Close()

	r := msgp.NewReader(f2)
	g2, nr, err := LoadMEMWAL(r)
	panicOn(err)
	vv("nr = %v", nr)

	fmt.Printf("\n g  = %#v\n", g)
	fmt.Printf("\n g2 = %#v\n", g2)

	if !g.Equal(g2) {
		panicf("not equal: g != g2")
	}
}
