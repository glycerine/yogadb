package yogadb

//go:generate greenpack

// GreenKV is a greenpack version of KV128 which allows typed BEGIN_TX
// and COMMIT_TX records in the MEMWAL too.
type GreenMemWalKV struct {
	WalRecordType int32   `zid:"0"`
	CRC32c        [4]byte `zid:"1"`
	VptrLength    uint64  `zid:"2"`
	VptrOffset    uint64  `zid:"3"`
	Hlc           int64   `zid:"4"`
	Key           string  `zid:"5"`
	InlineVal     []byte  `zid:"6"`
}
