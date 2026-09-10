package yogadb

import (
	cristalbase64 "github.com/cristalhq/base64"
	"github.com/glycerine/blake3"
)

// blake3checksum32 returns the 32-byte blake3 digest.
func blake3checksum32(by []byte) [32]byte {
	return blake3.Sum256(by)
}

func blake3OfBytes33string(by []byte) string {
	h := blake3.New(64, nil)
	h.Write(by)
	sum := h.Sum(nil)
	return "blake3.33B-" + cristalbase64.URLEncoding.EncodeToString(sum[:33])
}
