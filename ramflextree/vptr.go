package ramflextree

import "encoding/binary"

const (
	// VPtr size in kv128 encoding: 8-byte offset + 8-byte length
	vptrSize = 16
)

// VPtr is kept for API compatibility. In ramflextree all values are
// inline, so VPtr is only used as a tombstone sentinel (Length==1).
type VPtr struct {
	Offset uint64
	Length uint64
}

func (vp VPtr) encode(buf []byte) {
	binary.LittleEndian.PutUint64(buf[0:8], vp.Offset)
	binary.LittleEndian.PutUint64(buf[8:16], vp.Length)
}

func decodeVPtr(buf []byte) VPtr {
	return VPtr{
		Offset: binary.LittleEndian.Uint64(buf[0:8]),
		Length: binary.LittleEndian.Uint64(buf[8:16]),
	}
}
