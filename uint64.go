package yogadb

func putUint64(b []byte, i uint64) {
	if len(b) < 8 {
		panicf("putUint64 error: fill-in []byte target must be at least 8 bytes. we saw %v", len(b))
	}
	_ = b[7] // early bounds check to guarantee safety of writes below
	b[0] = byte(i >> 56)
	b[1] = byte(i >> 48)
	b[2] = byte(i >> 40)
	b[3] = byte(i >> 32)
	b[4] = byte(i >> 24)
	b[5] = byte(i >> 16)
	b[6] = byte(i >> 8)
	b[7] = byte(i)
}

func getUint64(b []byte) uint64 {
	if len(b) < 8 {
		return 0
	}
	_ = b[7] // bounds check hint to compiler; see golang.org/issue/14808
	return (uint64(b[0]) << 56) | (uint64(b[1]) << 48) |
		(uint64(b[2]) << 40) | (uint64(b[3]) << 32) |
		(uint64(b[4]) << 24) | (uint64(b[5]) << 16) |
		(uint64(b[6]) << 8) | (uint64(b[7]))
}
