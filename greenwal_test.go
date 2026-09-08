package yogadb

import (
	"bytes"
	"fmt"
	"os"
	"testing"

	"github.com/glycerine/greenpack/msgp"
)

func num2str(j int) string {
	switch j {
	case 0:
		return "zero"
	case 1:
		return "one"
	case 2:
		return "two"
	case 3:
		return "three"
	case 4:
		return "four"
	case 5:
		return "five"
	case 6:
		return "six"
	case 7:
		return "seven"
	case 8:
		return "eight"
	case 9:
		return "nine"
	case 10:
		return "ten"
	}
	panicf("%v too large, not supported by num2str()", j)
	return ""
}

func makeTestGreenMEMWAL_KV(j int) (g *GreenMEMWAL_KV) {
	str := num2str(j)
	g = &GreenMEMWAL_KV{
		WalRecordType: int32(1 + j),
		VptrLength:    uint64(2 + j),
		VptrOffset:    uint64(3 + j),
		Hlc:           int64(4 + j),
		Key:           str + "_key",
		InlineVal:     []byte(str + "_value"),
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

	var tot int
	g2 := make([]*GreenMEMWAL_KV, N)
	r := msgp.NewReader(f2)
	for i := range N {
		var nr int
		var err error
		g2[i], nr, err = LoadMEMWAL(r)
		panicOn(err)
		tot += nr
		//vv("nr = %v", nr)

		//fmt.Printf("\n wrote to disk:        g[%v] = %#v\n", i, g[i])
		//fmt.Printf("\n from disk read back: g2[%v] = %#v\n", i, g2[i])

		if !g2[i].Equal(g[i]) {
			fmt.Printf("\n wrote to disk:        g[%v] = %#v\n", i, g[i])
			fmt.Printf("\n from disk read back: g2[%v] = %#v\n", i, g2[i])
			panicf("not equal: g2 != g at i = %v", i)
		}
	}
	//vv("total bytes read: %v in %v records", tot, N)
}

func TestGreenMEMWAL_SaveToSliceRoundTrip(t *testing.T) {
	const N = 10

	var buf []byte
	g := make([]*GreenMEMWAL_KV, N)
	for i := range N {
		g[i] = makeTestGreenMEMWAL_KV(i)
		b, err := g[i].SaveToSlice()
		panicOn(err)
		buf = append(buf, b...)
	}

	r := msgp.NewReader(bytes.NewReader(buf))
	for i := range N {
		g2, _, err := LoadMEMWAL(r)
		panicOn(err)
		if !g2.Equal(g[i]) {
			fmt.Printf("\n wrote to slice:       g[%v] = %#v\n", i, g[i])
			fmt.Printf("\n from slice read back: g2 = %#v\n", g2)
			panicf("not equal: g2 != g at i = %v", i)
		}
	}
}
