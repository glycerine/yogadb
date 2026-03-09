package yogadb

import "unsafe"

// prefetchLine issues a hardware prefetch hint to bring the cache line
// containing addr into L1 cache. This is a no-op if addr is nil.
// Implemented in assembly (prefetch_amd64.s, prefetch_arm64.s).
//
//go:nosplit
func prefetchLine(addr unsafe.Pointer)
