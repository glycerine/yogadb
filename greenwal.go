package yogadb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"unsafe"

	"github.com/glycerine/greenpack/msgp"
)

//go:generate greenpack

// GreenMEMWAL_KV is a greenpack version of KV128 which allows typed BEGIN_TX
// and COMMIT_TX records in the MEMWAL too.
type GreenMEMWAL_KV struct {
	WalRecordType int32  `zid:"0"`
	VptrLength    uint64 `zid:"1"`
	VptrOffset    uint64 `zid:"2"`
	Hlc           int64  `zid:"3"`
	Key           string `zid:"4"`
	InlineVal     []byte `zid:"5"`
}

const (
	MEMWAL_KV                        int32 = 0
	MEMWAL_BEGIN_TXN                 int32 = 1
	MEMWAL_COMMIT_TXN                int32 = 2
	MEMWAL_BATCH_KV                  int32 = 3
	MEMWAL_BATCH_KV_HLC              int32 = 4
	MEMWAL_BATCH_KV_HLC_VALUE_IS_KEY int32 = 5
)

const compactMEMWALMagic = "\x00YMW1"

func (g *GreenMEMWAL_KV) fillFromKV(kv *KV) {
	g.WalRecordType = MEMWAL_KV
	g.VptrLength = kv.Vptr.Length
	g.VptrOffset = kv.Vptr.Offset
	g.Hlc = int64(kv.Hlc)
	g.Key = kv.Key
	if len(kv.Value) == 0 {
		g.InlineVal = nil
	} else {
		g.InlineVal = append(g.InlineVal[:0], kv.Value...)
	}
}

func (g *GreenMEMWAL_KV) appendCompactPayload(b []byte) []byte {
	b = append(b, compactMEMWALMagic...)
	b = binary.AppendVarint(b, int64(g.WalRecordType))
	if g.WalRecordType != MEMWAL_KV {
		return b
	}
	b = binary.AppendUvarint(b, g.VptrLength)
	b = binary.AppendUvarint(b, g.VptrOffset)
	b = binary.AppendVarint(b, g.Hlc)
	b = binary.AppendUvarint(b, uint64(len(g.Key)))
	b = append(b, g.Key...)
	b = binary.AppendUvarint(b, uint64(len(g.InlineVal)))
	b = append(b, g.InlineVal...)
	return b
}

func (g *GreenMEMWAL_KV) compactPayloadSize() int {
	size := len(compactMEMWALMagic) + binary.MaxVarintLen64
	if g.WalRecordType != MEMWAL_KV {
		return size
	}
	size += binary.MaxVarintLen64 // VptrLength
	size += binary.MaxVarintLen64 // VptrOffset
	size += binary.MaxVarintLen64 // Hlc
	size += binary.MaxVarintLen64 + len(g.Key)
	size += binary.MaxVarintLen64 + len(g.InlineVal)
	return size
}

func compactPayloadToGreenMEMWAL(payload []byte, g *GreenMEMWAL_KV) (bool, error) {
	if !bytes.HasPrefix(payload, []byte(compactMEMWALMagic)) {
		return false, nil
	}
	payload = payload[len(compactMEMWALMagic):]
	recTyp, n := binary.Varint(payload)
	if n <= 0 {
		return true, fmt.Errorf("compact MEMWAL record type decode failed")
	}
	payload = payload[n:]
	g.WalRecordType = int32(recTyp)
	g.VptrLength = 0
	g.VptrOffset = 0
	g.Hlc = 0
	g.Key = ""
	g.InlineVal = g.InlineVal[:0]
	if g.WalRecordType == MEMWAL_BATCH_KV || g.WalRecordType == MEMWAL_BATCH_KV_HLC || g.WalRecordType == MEMWAL_BATCH_KV_HLC_VALUE_IS_KEY {
		g.InlineVal = append(g.InlineVal[:0], payload...)
		return true, nil
	}
	if g.WalRecordType != MEMWAL_KV {
		if len(payload) != 0 {
			return true, fmt.Errorf("compact MEMWAL control record has %d trailing bytes", len(payload))
		}
		return true, nil
	}
	var u uint64
	u, n = binary.Uvarint(payload)
	if n <= 0 {
		return true, fmt.Errorf("compact MEMWAL VptrLength decode failed")
	}
	g.VptrLength = u
	payload = payload[n:]
	u, n = binary.Uvarint(payload)
	if n <= 0 {
		return true, fmt.Errorf("compact MEMWAL VptrOffset decode failed")
	}
	g.VptrOffset = u
	payload = payload[n:]
	var hlc int64
	hlc, n = binary.Varint(payload)
	if n <= 0 {
		return true, fmt.Errorf("compact MEMWAL Hlc decode failed")
	}
	g.Hlc = hlc
	payload = payload[n:]
	u, n = binary.Uvarint(payload)
	if n <= 0 {
		return true, fmt.Errorf("compact MEMWAL key length decode failed")
	}
	payload = payload[n:]
	if u > uint64(len(payload)) {
		return true, fmt.Errorf("compact MEMWAL key length %d exceeds remaining %d", u, len(payload))
	}
	g.Key = string(payload[:u])
	payload = payload[u:]
	u, n = binary.Uvarint(payload)
	if n <= 0 {
		return true, fmt.Errorf("compact MEMWAL value length decode failed")
	}
	payload = payload[n:]
	if u > uint64(len(payload)) {
		return true, fmt.Errorf("compact MEMWAL value length %d exceeds remaining %d", u, len(payload))
	}
	g.InlineVal = append(g.InlineVal[:0], payload[:u]...)
	payload = payload[u:]
	if len(payload) != 0 {
		return true, fmt.Errorf("compact MEMWAL KV has %d trailing bytes", len(payload))
	}
	return true, nil
}

func appendCompactBatchPayload(b []byte, kvs []KV) []byte {
	if len(kvs) > 0 {
		hlc := kvs[0].Hlc
		allSameHLC := true
		for i := 1; i < len(kvs); i++ {
			if kvs[i].Hlc != hlc {
				allSameHLC = false
				break
			}
		}
		if allSameHLC {
			return appendCompactBatchHLCPayload(b, kvs, hlc)
		}
	}
	b = append(b, compactMEMWALMagic...)
	b = binary.AppendVarint(b, int64(MEMWAL_BATCH_KV))
	b = binary.AppendUvarint(b, uint64(len(kvs)))
	for i := range kvs {
		kv := &kvs[i]
		b = binary.AppendUvarint(b, kv.Vptr.Length)
		b = binary.AppendUvarint(b, kv.Vptr.Offset)
		b = binary.AppendVarint(b, int64(kv.Hlc))
		b = binary.AppendUvarint(b, uint64(len(kv.Key)))
		b = append(b, kv.Key...)
		b = binary.AppendUvarint(b, uint64(len(kv.Value)))
		b = append(b, kv.Value...)
	}
	return b
}

func appendCompactBatchHLCPayload(b []byte, kvs []KV, hlc HLC) []byte {
	b = append(b, compactMEMWALMagic...)
	b = binary.AppendVarint(b, int64(MEMWAL_BATCH_KV_HLC))
	b = binary.AppendUvarint(b, uint64(len(kvs)))
	b = binary.AppendVarint(b, int64(hlc))
	for i := range kvs {
		kv := &kvs[i]
		b = binary.AppendUvarint(b, kv.Vptr.Length)
		b = binary.AppendUvarint(b, kv.Vptr.Offset)
		b = binary.AppendUvarint(b, uint64(len(kv.Key)))
		b = append(b, kv.Key...)
		b = binary.AppendUvarint(b, uint64(len(kv.Value)))
		b = append(b, kv.Value...)
	}
	return b
}

func compactBatchHLCValueIsKey(kvs []KV) bool {
	for i := range kvs {
		kv := &kvs[i]
		if len(kv.Key) == 0 || len(kv.Value) != len(kv.Key) || kv.Vptr.Length != uint64(len(kv.Key)) {
			return false
		}
		keyBytes := unsafe.Slice(unsafe.StringData(kv.Key), len(kv.Key))
		if !bytes.Equal(keyBytes, kv.Value) {
			return false
		}
	}
	return len(kvs) > 0
}

func compactBatchHLCValueIsKeyPayloadSize(kvs []KV, hlc HLC) int {
	size := len(compactMEMWALMagic) + varintLen64(int64(MEMWAL_BATCH_KV_HLC_VALUE_IS_KEY)) + uvarintLen64(uint64(len(kvs))) + varintLen64(int64(hlc))
	for i := range kvs {
		kv := &kvs[i]
		size += uvarintLen64(kv.Vptr.Length)
		size += uvarintLen64(kv.Vptr.Offset)
		size += uvarintLen64(uint64(len(kv.Key))) + len(kv.Key)
	}
	return size
}

func appendCompactBatchHLCValueIsKeyPayload(b []byte, kvs []KV, hlc HLC) []byte {
	b = append(b, compactMEMWALMagic...)
	b = binary.AppendVarint(b, int64(MEMWAL_BATCH_KV_HLC_VALUE_IS_KEY))
	b = binary.AppendUvarint(b, uint64(len(kvs)))
	b = binary.AppendVarint(b, int64(hlc))
	for i := range kvs {
		kv := &kvs[i]
		b = binary.AppendUvarint(b, kv.Vptr.Length)
		b = binary.AppendUvarint(b, kv.Vptr.Offset)
		b = binary.AppendUvarint(b, uint64(len(kv.Key)))
		b = append(b, kv.Key...)
	}
	return b
}

func compactBatchPayloadSize(kvs []KV) int {
	size := len(compactMEMWALMagic) + binary.MaxVarintLen64 + binary.MaxVarintLen64
	if len(kvs) > 0 {
		hlc := kvs[0].Hlc
		allSameHLC := true
		for i := 1; i < len(kvs); i++ {
			if kvs[i].Hlc != hlc {
				allSameHLC = false
				break
			}
		}
		if allSameHLC {
			size += binary.MaxVarintLen64
			for i := range kvs {
				kv := &kvs[i]
				size += binary.MaxVarintLen64 // VptrLength
				size += binary.MaxVarintLen64 // VptrOffset
				size += binary.MaxVarintLen64 + len(kv.Key)
				size += binary.MaxVarintLen64 + len(kv.Value)
			}
			return size
		}
	}
	for i := range kvs {
		kv := &kvs[i]
		size += binary.MaxVarintLen64 // VptrLength
		size += binary.MaxVarintLen64 // VptrOffset
		size += binary.MaxVarintLen64 // Hlc
		size += binary.MaxVarintLen64 + len(kv.Key)
		size += binary.MaxVarintLen64 + len(kv.Value)
	}
	return size
}

func compactBatchHLCPayloadSize(kvs []KV, hlc HLC) int {
	size := len(compactMEMWALMagic) + varintLen64(int64(MEMWAL_BATCH_KV_HLC)) + uvarintLen64(uint64(len(kvs))) + varintLen64(int64(hlc))
	for i := range kvs {
		kv := &kvs[i]
		size += uvarintLen64(kv.Vptr.Length)
		size += uvarintLen64(kv.Vptr.Offset)
		size += uvarintLen64(uint64(len(kv.Key))) + len(kv.Key)
		size += uvarintLen64(uint64(len(kv.Value))) + len(kv.Value)
	}
	return size
}

func compactBatchHLCPayloadToKVs(payload []byte, out []KV) ([]KV, error) {
	count, n := binary.Uvarint(payload)
	if n <= 0 {
		return out, fmt.Errorf("compact MEMWAL batch HLC count decode failed")
	}
	payload = payload[n:]
	hlc, n := binary.Varint(payload)
	if n <= 0 {
		return out, fmt.Errorf("compact MEMWAL batch HLC decode failed")
	}
	payload = payload[n:]
	if count > uint64(int(^uint(0)>>1)) {
		return out, fmt.Errorf("compact MEMWAL batch HLC count overflows int: %d", count)
	}
	if cap(out)-len(out) < int(count) {
		newOut := make([]KV, len(out), len(out)+int(count))
		copy(newOut, out)
		out = newOut
	}
	for i := 0; i < int(count); i++ {
		var kv KV
		u, n := binary.Uvarint(payload)
		if n <= 0 {
			return out, fmt.Errorf("compact MEMWAL batch HLC[%d] VptrLength decode failed", i)
		}
		kv.Vptr.Length = u
		payload = payload[n:]
		u, n = binary.Uvarint(payload)
		if n <= 0 {
			return out, fmt.Errorf("compact MEMWAL batch HLC[%d] VptrOffset decode failed", i)
		}
		kv.Vptr.Offset = u
		payload = payload[n:]
		kv.Hlc = HLC(hlc)
		u, n = binary.Uvarint(payload)
		if n <= 0 {
			return out, fmt.Errorf("compact MEMWAL batch HLC[%d] key length decode failed", i)
		}
		payload = payload[n:]
		if u > uint64(len(payload)) {
			return out, fmt.Errorf("compact MEMWAL batch HLC[%d] key length %d exceeds remaining %d", i, u, len(payload))
		}
		kv.Key = string(payload[:u])
		payload = payload[u:]
		u, n = binary.Uvarint(payload)
		if n <= 0 {
			return out, fmt.Errorf("compact MEMWAL batch HLC[%d] value length decode failed", i)
		}
		payload = payload[n:]
		if u > uint64(len(payload)) {
			return out, fmt.Errorf("compact MEMWAL batch HLC[%d] value length %d exceeds remaining %d", i, u, len(payload))
		}
		kv.Value = append(kv.Value[:0], payload[:u]...)
		payload = payload[u:]
		out = append(out, kv)
	}
	if len(payload) != 0 {
		return out, fmt.Errorf("compact MEMWAL batch HLC has %d trailing bytes", len(payload))
	}
	return out, nil
}

func compactBatchHLCValueIsKeyPayloadToKVs(payload []byte, out []KV) ([]KV, error) {
	count, n := binary.Uvarint(payload)
	if n <= 0 {
		return out, fmt.Errorf("compact MEMWAL batch HLC value-is-key count decode failed")
	}
	payload = payload[n:]
	hlc, n := binary.Varint(payload)
	if n <= 0 {
		return out, fmt.Errorf("compact MEMWAL batch HLC value-is-key HLC decode failed")
	}
	payload = payload[n:]
	if count > uint64(int(^uint(0)>>1)) {
		return out, fmt.Errorf("compact MEMWAL batch HLC value-is-key count overflows int: %d", count)
	}
	if cap(out)-len(out) < int(count) {
		newOut := make([]KV, len(out), len(out)+int(count))
		copy(newOut, out)
		out = newOut
	}
	for i := 0; i < int(count); i++ {
		var kv KV
		u, n := binary.Uvarint(payload)
		if n <= 0 {
			return out, fmt.Errorf("compact MEMWAL batch HLC value-is-key[%d] VptrLength decode failed", i)
		}
		kv.Vptr.Length = u
		payload = payload[n:]
		u, n = binary.Uvarint(payload)
		if n <= 0 {
			return out, fmt.Errorf("compact MEMWAL batch HLC value-is-key[%d] VptrOffset decode failed", i)
		}
		kv.Vptr.Offset = u
		payload = payload[n:]
		kv.Hlc = HLC(hlc)
		u, n = binary.Uvarint(payload)
		if n <= 0 {
			return out, fmt.Errorf("compact MEMWAL batch HLC value-is-key[%d] key length decode failed", i)
		}
		payload = payload[n:]
		if u > uint64(len(payload)) {
			return out, fmt.Errorf("compact MEMWAL batch HLC value-is-key[%d] key length %d exceeds remaining %d", i, u, len(payload))
		}
		kv.Key = string(payload[:u])
		payload = payload[u:]
		kv.Value = append(kv.Value[:0], kv.Key...)
		out = append(out, kv)
	}
	if len(payload) != 0 {
		return out, fmt.Errorf("compact MEMWAL batch HLC value-is-key has %d trailing bytes", len(payload))
	}
	return out, nil
}

func compactBatchPayloadToKVs(payload []byte, out []KV) ([]KV, error) {
	count, n := binary.Uvarint(payload)
	if n <= 0 {
		return out, fmt.Errorf("compact MEMWAL batch count decode failed")
	}
	payload = payload[n:]
	if count > uint64(int(^uint(0)>>1)) {
		return out, fmt.Errorf("compact MEMWAL batch count overflows int: %d", count)
	}
	if cap(out)-len(out) < int(count) {
		newOut := make([]KV, len(out), len(out)+int(count))
		copy(newOut, out)
		out = newOut
	}
	for i := 0; i < int(count); i++ {
		var kv KV
		u, n := binary.Uvarint(payload)
		if n <= 0 {
			return out, fmt.Errorf("compact MEMWAL batch[%d] VptrLength decode failed", i)
		}
		kv.Vptr.Length = u
		payload = payload[n:]
		u, n = binary.Uvarint(payload)
		if n <= 0 {
			return out, fmt.Errorf("compact MEMWAL batch[%d] VptrOffset decode failed", i)
		}
		kv.Vptr.Offset = u
		payload = payload[n:]
		hlc, n := binary.Varint(payload)
		if n <= 0 {
			return out, fmt.Errorf("compact MEMWAL batch[%d] Hlc decode failed", i)
		}
		kv.Hlc = HLC(hlc)
		payload = payload[n:]
		u, n = binary.Uvarint(payload)
		if n <= 0 {
			return out, fmt.Errorf("compact MEMWAL batch[%d] key length decode failed", i)
		}
		payload = payload[n:]
		if u > uint64(len(payload)) {
			return out, fmt.Errorf("compact MEMWAL batch[%d] key length %d exceeds remaining %d", i, u, len(payload))
		}
		kv.Key = string(payload[:u])
		payload = payload[u:]
		u, n = binary.Uvarint(payload)
		if n <= 0 {
			return out, fmt.Errorf("compact MEMWAL batch[%d] value length decode failed", i)
		}
		payload = payload[n:]
		if u > uint64(len(payload)) {
			return out, fmt.Errorf("compact MEMWAL batch[%d] value length %d exceeds remaining %d", i, u, len(payload))
		}
		kv.Value = append(kv.Value[:0], payload[:u]...)
		payload = payload[u:]
		out = append(out, kv)
	}
	if len(payload) != 0 {
		return out, fmt.Errorf("compact MEMWAL batch has %d trailing bytes", len(payload))
	}
	return out, nil
}

func (g *GreenMEMWAL_KV) toKV(kv *KV) {
	kv.Key = g.Key
	kv.Vptr.Length = g.VptrLength
	kv.Vptr.Offset = g.VptrOffset
	kv.Hlc = HLC(g.Hlc)
	kv.Value = g.InlineVal // now owned by the kv.
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
		err = NotBinarySlice
	}
	return
}

// read and de-serialize a GreenMEMWAL_KV struct from the byte stream r.
func LoadMEMWAL(r *msgp.Reader) (g *GreenMEMWAL_KV, numread int, err error) {
	var payload ByteSlice
	err = payload.DecodeMsg(r)
	if err != nil {
		return nil, 0, fmt.Errorf("LoadMEMWAL() payload read error on ByteSlice.DecodeMsg(): %w", err)
	}
	numread += msgpackBinFrameSize(len(payload))

	g = &GreenMEMWAL_KV{}
	var compact bool
	compact, err = compactPayloadToGreenMEMWAL(payload, g)
	if err != nil {
		return nil, numread, fmt.Errorf("LoadMEMWAL() error on compact MEMWAL decode: %w", err)
	}
	if !compact {
		_, err = g.UnmarshalMsg(payload)
		if err != nil {
			return nil, numread, fmt.Errorf("LoadMEMWAL() error on GreenMEMWAL_KV.UnmarshalMsg(): %w", err)
		}
	}

	// read the crc32c checksum. should take up 10 bytes: 2 description + 8 payload.
	var bs2 ByteSlice
	err = bs2.DecodeMsg(r)
	if err != nil {
		return nil, numread, fmt.Errorf("LoadMEMWAL() crc32c read error on ByteSlice.DecodeMsg(): %w", err)
	}
	numread += msgpackBinFrameSize(len(bs2))
	if len(bs2) < 4 {
		return nil, numread, fmt.Errorf("LoadMEMWAL() crc32c frame too short: got %d bytes, want at least 4", len(bs2))
	}

	got := crc32.Checksum(payload, crc32cTable)
	want := binary.LittleEndian.Uint32(bs2[:4])
	if got != want {
		return nil, numread, fmt.Errorf("crc32c checksum failed! got=%v; want=%v", got, want)
	}
	return g, numread, nil
}

func msgpackBinFrameSize(n int) int {
	switch {
	case n <= 0xff:
		return 2 + n
	case n <= 0xffff:
		return 3 + n
	default:
		return 5 + n
	}
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
		return nil, fmt.Errorf("GreenMEMWAL_KV.SaveToSlice() error on MarshalMsg: %w", err)
	}
	var crcBuf [8]byte = [8]byte{'1', '2', '3', '4', '=', '=', '=', '\n'}
	binary.LittleEndian.PutUint32(crcBuf[:4], crc32.Checksum(b, crc32cTable))
	out, err := ByteSlice(b).MarshalMsg(nil)
	if err != nil {
		return nil, fmt.Errorf("GreenMEMWAL_KV.SaveToSlice() error framing payload: %w", err)
	}
	out, err = ByteSlice(crcBuf[:]).MarshalMsg(out)
	if err != nil {
		return nil, fmt.Errorf("GreenMEMWAL_KV.SaveToSlice() error framing crc32c: %w", err)
	}
	return out, nil
}
