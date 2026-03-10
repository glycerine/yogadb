package main

// keygen.go — Key generation matching C's kv_refill_hex64_klen.
//
// The C benchmark generates keys by converting a uint64 to a 16-character
// lowercase hex string, then padding with '!' if klen > 16.

import (
	"encoding/hex"
	"strings"
)

// hexKey generates a key matching C's kv_refill_hex64_klen(kv, n, klen, NULL, 0).
// The first 16 bytes are the hex representation of n (big-endian, lowercase).
// If klen > 16, the remaining bytes are '!'.
func hexKey(n uint64, klen int) string {
	var buf [8]byte
	buf[0] = byte(n >> 56)
	buf[1] = byte(n >> 48)
	buf[2] = byte(n >> 40)
	buf[3] = byte(n >> 32)
	buf[4] = byte(n >> 24)
	buf[5] = byte(n >> 16)
	buf[6] = byte(n >> 8)
	buf[7] = byte(n)
	h := hex.EncodeToString(buf[:])
	if klen <= 16 {
		return h
	}
	return h + strings.Repeat("!", klen-16)
}

// hexKeyBuf writes a key into buf, returning the slice. buf must be >= klen.
// This avoids allocation for hot paths.
func hexKeyBuf(buf []byte, n uint64, klen int) []byte {
	const hextable = "0123456789abcdef"
	// Write 16 hex chars (big-endian u64)
	for i := 0; i < 8; i++ {
		b := byte(n >> (56 - uint(i)*8))
		buf[i*2] = hextable[b>>4]
		buf[i*2+1] = hextable[b&0x0f]
	}
	for i := 16; i < klen; i++ {
		buf[i] = '!'
	}
	return buf[:klen]
}

// makeValue creates a value of the given length filled with a repeating pattern.
func makeValue(vlen int) []byte {
	v := make([]byte, vlen)
	for i := range v {
		v[i] = byte('A' + (i % 26))
	}
	return v
}
