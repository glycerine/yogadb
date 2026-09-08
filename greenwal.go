package yogadb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"

	"github.com/glycerine/greenpack/msgp"
)

//go:generate greenpack

// GreenMEMWAL_KV is a greenpack version of KV128 which allows typed BEGIN_TX
// and COMMIT_TX records in the MEMWAL too.
type GreenMEMWAL_KV struct {
	WalRecordType int32   `zid:"0"`
	VptrLength    uint64  `zid:"1"`
	VptrOffset    uint64  `zid:"2"`
	Hlc           int64   `zid:"3"`
	Key           string  `zid:"4"`
	InlineVal     []byte  `zid:"5"`
	CRC32c        [4]byte `zid:"6"`
}

func (a *GreenMEMWAL_KV) Equal(b *GreenMEMWAL_KV) bool {
	if a.WalRecordType != b.WalRecordType {
		return false
	}
	if a.VptrLength != b.VptrLength {
		return false
	}
	if a.VptrOffset != b.VptrOffset {
		return false
	}
	if a.Hlc != b.Hlc {
		return false
	}
	if a.Key != b.Key {
		return false
	}
	if 0 != bytes.Compare(a.InlineVal, b.InlineVal) {
		return false
	}
	if 0 != bytes.Compare(a.CRC32c[:], b.CRC32c[:]) {
		return false
	}
	return true
}

// A simple wrapper header on all msgpack messages; has the length and the bytes.
// Allows us length delimited messages; with length knowledge up front.
type ByteSlice []byte

// the msgpack codes for binary slices of various sizes. Up to 32 bit integer len.
const (
	bin8  uint8 = 0xc4
	bin16 uint8 = 0xc5
	bin32 uint8 = 0xc6
)

type UnframeError int

const (
	NotEnoughBytes UnframeError = -1
	NotBinarySlice UnframeError = -2
)

func (e UnframeError) Error() string {
	switch e {
	case NotEnoughBytes:
		return "unframeBinMsgpack() error: NotEnoughBytes"
	case NotBinarySlice:
		return "unframeBinMsgpack() error: NotBinarySlice: could not find 0xC4, 0xC5, 0xC6 in start of binary msgpack"
	default:
		return "UnknownUnframeError"
	}
}

// unframeBinMsgpack() works on just the minimal 2-5 bytes peek ahead
// needed to see how much to read next.
//
// ninside returns the number of bytes inside/that follow the 2-5 byte
// binary msgpack header. The header frames the internal msgpack serialized
// object. ntotal returns the total number of bytes including the
// header bytes. The header is always a bin8/bin16/bin32 msgpack object
// itself, and so is 2-5 bytes extra, not counting the internal byte
// slice that makes up the internal msgp object. So there are two
// msgpack decoding steps to get a golang object back.
func unframeBinMsgpack(p []byte) (ntotal int, ninside int, nheader int, err error) {

	if len(p) == 0 {
		err = NotEnoughBytes
		return
	}
	switch p[0] {
	case bin8:
		if len(p) < 2 {
			err = NotEnoughBytes
			return
		}
		ninside = int(p[1])
		nheader = 2
		ntotal = ninside + nheader
	case bin16:
		if len(p) < 3 {
			err = NotEnoughBytes
			return
		}
		ninside = int(binary.BigEndian.Uint16(p[1:3]))
		nheader = 3
		ntotal = ninside + nheader
	case bin32:
		if len(p) < 5 {
			err = NotEnoughBytes
			return
		}
		ninside = int(binary.BigEndian.Uint32(p[1:5]))
		nheader = 5
		ntotal = ninside + nheader
	default:
		fmt.Printf("p bytes = '%#v'\n", p[:5])
		fmt.Printf("p bytes = '%#v'/as string='%v'\n", p, string(p))
		err = NotBinarySlice
		panic(err)
	}
	return
}

// read and de-serialize a GreenMEMWAL_KV struct from the byte stream r.
func LoadMEMWAL(r *msgp.Reader) (g *GreenMEMWAL_KV, numread int, err error) {

	// peek ahead first, so we can avoid
	// moving the read point ahead if there
	// are insufficient bytes

	// try to get at least 5 bytes, but
	// settle for 2 since that is possible.
	var by []byte

	var i int
	for i = 5; i >= 2; i-- {
		by, err = r.R.Peek(i)
		if err == nil {
			break
		}
		if err == io.EOF {
			// try shorter
			continue
		}
		return nil, 0, err
	}
	if err == io.EOF {
		return nil, 0, err
	}
	if err != nil {
		return nil, 0, fmt.Errorf("LoadMEMWAL() error trying to r.R.Peek() for bytes: '%s'/%T", err, err)
	}

	ntotal, ninside, nheader, err := unframeBinMsgpack(by)

	if err != nil {
		return nil, 0, fmt.Errorf("LoadMEMWAL() error on UnframeBinMsgPack(): '%s'", err)
	}

	var tmp []byte
	tmp, err = r.R.Peek(ntotal)
	if err != nil {
		return nil, 0, fmt.Errorf("LoadMEMWAL() error on Peek() call for ntotal(%v) bytes: '%s'/%T (only got, len(tmp)=%v; i = %v, ninside=%v, nheader=%v)", ntotal, err, err, len(tmp), i, ninside, nheader)
	}

	g = &GreenMEMWAL_KV{}
	//_, err = g.UnmarshalMsg(bs2)
	_, err = g.UnmarshalMsg(tmp[nheader:])
	if err != nil {
		return nil, ntotal, fmt.Errorf("LoadMEMWAL() error on GreenMemWalKV.UnmarshalMsg(): '%s'; bs2='%#v'; string(bs2)='%v' (len: %v); partly decoded GreenMEMWAL_KV: '%#v'", err, tmp, string(tmp), len(tmp), g)
	}
	_, err = r.R.Skip(ntotal)
	panicOn(err)

	// read the crc32c checksum. should take up 10 bytes: 2 description + 8 payload.
	var bs2 ByteSlice
	err = bs2.DecodeMsg(r)
	if err != nil {
		return nil, ntotal, fmt.Errorf("LoadMEMWAL() crc32c read error on ByteSlice(by).DecodeMsg(): '%s'", err)
	}

	got := crc32.Checksum(tmp[nheader:], crc32cTable)
	want := binary.LittleEndian.Uint32(bs2[:4])
	if got != want {
		return nil, ntotal + 10, fmt.Errorf("crc32c checksum failed! got=%v; want=%v", got, want)
	}
	return g, ntotal + 10, nil
}

// save g to w.
func (g *GreenMEMWAL_KV) Save(w *msgp.Writer) error {
	b, err := g.MarshalMsg(nil)
	if err != nil {
		return err
	}
	var crcBuf [8]byte = [8]byte{'1', '2', '3', '4', '=', '=', '=', '\n'}
	binary.LittleEndian.PutUint32(crcBuf[:4], crc32.Checksum(b, crc32cTable))
	err = ByteSlice(b).EncodeMsg(w)
	if err != nil {
		return err
	}
	return ByteSlice(crcBuf[:]).EncodeMsg(w)
}

// Save g as a framed msgpack message (where first few bytes are a []byte encoded
// to tell us the size of the rest of the bytes that follow. Those following
// bytes consist themselves of a msgpack serialized GreenMEMWAL_KV.
func (tk *GreenMEMWAL_KV) SaveToSlice() ([]byte, error) {

	b, err := tk.MarshalMsg(nil)
	if err != nil {
		return nil, fmt.Errorf("GreenMEMWAL_KV.SaveToSlice() error on MarshalMsg: '%s'", err)
	}
	return ByteSlice(b).MarshalMsg(nil)
}
