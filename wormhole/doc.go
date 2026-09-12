// Package wormhole contains a Go port/evaluation vehicle for the Wormhole
// ordered-map shape used by the C reference in study/flexspace/c/wh.c.
//
// The C implementation combines fixed-size sorted leaves, prefix/hash metadata,
// per-leaf locks, and QSBR memory reclamation. This Go version keeps the
// leaf-oriented concurrency model and split-on-overflow behavior, but uses a
// Go slice anchor index protected by a structural RWMutex instead of the C
// hash-prefix metadata table. It is intended for memtable experiments where
// scans and writes are interleaved more heavily than keyStable handles well.
//
// Put takes ownership of the supplied KV payload. Callers must not modify
// kv.Value after insertion. This matches YogaDB's memtable path, where the DB
// API has already made any required defensive user-value copy before building
// the KV.
//
// Scan callbacks receive values that alias immutable map storage and are only
// promised valid for the duration of the callback. Callbacks must not re-enter
// the same Map.
package wormhole
