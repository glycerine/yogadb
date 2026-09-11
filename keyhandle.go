package yogadb

import (
	"fmt"
	"unique"
	"unsafe"
)

type Key = unique.Handle[string]

func keyH(key string) Key {
	return unique.Make(key)
}

func keyString(key Key) string {
	var zero Key
	if key == zero {
		return ""
	}
	return key.Value()
}

func keyLen(key Key) int {
	return len(keyString(key))
}

// KVX is the externally safe sibling of KV: its key is an interned handle
// rather than a possibly borrowed string. Holding a KVX holds the canonical key
// live until the KVX itself is dropped.
type KVX struct {
	Key   Key
	Value []byte
	Vptr  VPtr
	Hlc   HLC

	valueAliasKey bool
}

func kvxFromKV(kv KV) KVX {
	return KVX{
		Key:           keyH(kv.Key),
		Value:         kv.Value,
		Vptr:          kv.Vptr,
		Hlc:           kv.Hlc,
		valueAliasKey: slottedInlineValueAliasesKey(kv),
	}
}

func (kvx KVX) kv() KV {
	key := keyString(kvx.Key)
	if kvx.valueAliasKey {
		var value []byte
		if len(key) > 0 {
			value = unsafe.Slice(unsafe.StringData(key), len(key))
		}
		return KV{
			Key:   key,
			Value: value,
			Vptr:  kvx.Vptr,
			Hlc:   kvx.Hlc,
		}
	}
	return KV{
		Key:   key,
		Value: kvx.Value,
		Vptr:  kvx.Vptr,
		Hlc:   kvx.Hlc,
	}
}

func (kvx *KVX) HasVPtr() bool {
	return kvx.Vptr.Length > vlogInlineThreshold && kvx.Vptr.Length < rawVlenTombstone
}

func (kvx *KVX) Vtyp() uint64 {
	if kvx.Vptr.Length > vlogInlineThreshold {
		if len(kvx.Value) != 8 {
			return 0
		}
		return getUint64(kvx.Value)
	}
	return kvx.Vptr.Offset
}

func (kvx *KVX) isTombstone() bool {
	return kvx.Vptr.Length == rawVlenTombstone
}

func (kvx *KVX) Large() bool {
	x := kvx.Vptr.Length
	return vlogInlineThreshold < x && x < rawVlenTombstone
}

func kvxSizeApprox(kvx *KVX) int {
	size := 24 + keyLen(kvx.Key) + len(kvx.Value)
	if kvx.valueAliasKey {
		size -= len(kvx.Value)
	}
	return size
}

func (kvx *KVX) String() (r string) {
	r = "&KVX{\n"
	r += fmt.Sprintf("    Key: %v,\n", keyString(kvx.Key))
	r += fmt.Sprintf("  Value: %v,\n", string(kvx.Value))
	r += fmt.Sprintf("   Vptr: %v,\n", kvx.Vptr)
	r += fmt.Sprintf("HasVPtr: %v,\n", kvx.HasVPtr())
	r += fmt.Sprintf("    Hlc: %v,\n", kvx.Hlc.String())
	r += "}\n"
	return
}
