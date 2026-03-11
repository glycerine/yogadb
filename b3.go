package yogadb

import (
	cristalbase64 "github.com/cristalhq/base64"
	"github.com/glycerine/blake3"
)

// blake3checksum32 returns the first 32 bytes of
// the blake3 cryptographic 64-byte hash.
func blake3checksum32(by []byte) []byte {
	h := blake3.New(64, nil)
	h.Write(by)
	sum := h.Sum(nil)
	return sum[:32]
}

func blake3OfBytes33string(by []byte) string {
	h := blake3.New(64, nil)
	h.Write(by)
	sum := h.Sum(nil)
	return "blake3.33B-" + cristalbase64.URLEncoding.EncodeToString(sum[:33])
}
