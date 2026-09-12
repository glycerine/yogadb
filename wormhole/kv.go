package wormhole

import "encoding/binary"

const (
	vlogInlineThreshold = 64
	rawVlenTombstone    = ^uint64(0)
)

type HLC int64

type VPtr struct {
	Offset uint64
	Length uint64
}

// KV mirrors yogadb.KV so this package can be evaluated as a memtable
// container without importing the root package and creating an import cycle.
type KV struct {
	Key   string
	Value []byte
	Vptr  VPtr
	Hlc   HLC
}

func (kv *KV) HasVPtr() bool {
	return kv.Vptr.Length > vlogInlineThreshold && kv.Vptr.Length < rawVlenTombstone
}

func (kv *KV) Vtyp() uint64 {
	if kv.Vptr.Length == rawVlenTombstone {
		return 0
	}
	if kv.Vptr.Length > vlogInlineThreshold {
		if len(kv.Value) != 8 {
			return 0
		}
		return binary.LittleEndian.Uint64(kv.Value)
	}
	return kv.Vptr.Offset
}
