package yogadb

// vlog.go - Append-only value log for large-value separation (WiscKey-style).
//
// Values larger than vlogInlineThreshold are stored in the VLOG file instead of
// inline in FlexSpace. This reduces write amplification: large values are written
// once to VLOG and never rewritten by FlexSpace GC, compaction, or interval rewrites.
//
// On-disk layout:
//   {db_dir}/VLOG - append-only file of value entries
//
// Each VLOG entry (56-byte header + N-byte value):
//   [4 bytes]  hdrCRC  - CRC32C of bytes 4..56 (HLC + length + valCRC + blake3)
//   [8 bytes]  HLC timestamp (big-endian int64 - preserves sort order in byte scans)
//   [8 bytes]  value length (little-endian uint64 - supports values up to int64 max)
//   [4 bytes]  valCRC  - CRC32C of the N value bytes that follow
//   [32 bytes] blake3  - Blake3 cryptographic checksum of the value bytes
//   [N bytes]  raw value bytes
//
// The blake3 checksum enables cheap dedup: before appending a new VLOG entry,
// we can read just the 56-byte header of the old entry and compare blake3
// checksums. If they match (and lengths match), the value is unchanged and
// we can skip the VLOG write entirely, reusing the old VPtr.
//
// IMPORTANT: HLC STALENESS IN VLOG HEADERS
//
// When dedup reuses an old VPtr, the HLC stored in the VLOG entry header
// becomes stale — it reflects the original write, not the latest overwrite.
// This is intentional and correct. The AUTHORITATIVE HLC for any key-value
// pair lives in the KV128 encoding in FLEXSPACE.KV128_BLOCKS (and in the
// memtable/WAL before flush). The VLOG header HLC is NOT authoritative.
// It is only consulted by VacuumVLOG when rewriting entries, at which point
// the KV128's HLC should be used instead. This design avoids bloating the
// VLOG with duplicate value bytes just because the HLC changed.
//
// The two-CRC design allows validating the header (HLC + length + blake3)
// without reading the value bytes.
//
// The KV encoding in FlexSpace stores a "value pointer" (VPtr) instead of inline
// value bytes when the value is in VLOG. VPtr is 16 bytes: 8-byte offset + 8-byte length.

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	///"os"
	"sync"
	"sync/atomic"

	"github.com/glycerine/vfs"
)

const (
	// Values <= than this threshold remain inline with their key in FlexSpace.
	// Values larger than this threshold are stored in the VLOG file.
	// 64 bytes balances scan performance (small values inline) vs write
	// amplification (large values written once to VLOG, never rewritten).
	vlogInlineThreshold = 64

	// VLOG entry header: 4-byte hdrCRC + 8-byte HLC + 8-byte length + 4-byte valCRC + 32-byte blake3 = 56 bytes
	vlogEntryHeaderSize = 56

	// VPtr size in kv128 encoding: 8-byte offset + 8-byte length
	vptrSize = 16
)

// VPtr is a pointer to a value stored in the VLOG file.
type VPtr struct {
	Offset uint64 // byte offset in VLOG file
	Length uint64 // value length in bytes
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

// valueLog is an append-only log for storing large values.
type valueLog struct {
	vfs  vfs.FS
	fd   vfs.File
	mu   sync.Mutex // protects writes (appends are serialized)
	tail int64      // next write offset (also = file size)

	// Write-byte counter (accessed atomically)
	VLOGBytesWritten int64
}

// openValueLog opens or creates the VLOG file.
func openValueLog(path string, fs vfs.FS) (*valueLog, error) {
	//fd, err := fs.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	fd, err := fs.OpenReadWrite(path, vfs.WriteCategoryUnspecified)

	if err != nil {
		return nil, fmt.Errorf("vlog: open %s: %w", path, err)
	}
	fi, err := fd.Stat()
	if err != nil {
		fd.Close()
		return nil, fmt.Errorf("vlog: stat %s: %w", path, err)
	}
	return &valueLog{
		vfs:  fs,
		fd:   fd,
		tail: fi.Size(),
	}, nil
}

// append writes a value to the VLOG and returns a VPtr.
// Does NOT fsync - caller must call sync() when durability is needed.
// Thread-safe (serialized by mu).
func (vl *valueLog) append(value []byte, hlc HLC) (VPtr, error) {
	vl.mu.Lock()
	defer vl.mu.Unlock()
	return vl.appendLocked(value, hlc)
}

// appendLocked appends a value while mu is already held.
// On-disk format: [4B hdrCRC][8B HLC][8B length][4B valCRC][32B blake3][NB value]
// hdrCRC covers bytes 4..56 (HLC + length + valCRC + blake3).
func (vl *valueLog) appendLocked(value []byte, hlc HLC) (VPtr, error) {
	return vl.appendLockedWithHash(value, hlc, nil)
}

// appendLockedWithHash is like appendLocked but accepts a pre-computed blake3
// hash to avoid recomputing it when the caller already has it (e.g., dedup path).
func (vl *valueLog) appendLockedWithHash(value []byte, hlc HLC, b3 []byte) (VPtr, error) {
	vlen := uint64(len(value))
	entrySize := vlogEntryHeaderSize + int(vlen)

	// Build entry: [hdrCRC][HLC][length][valCRC][blake3][value...]
	buf := make([]byte, entrySize)
	binary.BigEndian.PutUint64(buf[4:12], uint64(hlc))
	binary.LittleEndian.PutUint64(buf[12:20], vlen)
	copy(buf[vlogEntryHeaderSize:], value)
	// valCRC covers the value bytes.
	valCRC := crc32.Checksum(buf[vlogEntryHeaderSize:], crc32cTable)
	binary.LittleEndian.PutUint32(buf[20:24], valCRC)
	// blake3 checksum of value bytes.
	if b3 == nil {
		b3 = blake3checksum32(value)
	}
	copy(buf[24:56], b3)
	// hdrCRC covers bytes 4..56 (HLC + length + valCRC + blake3).
	hdrCRC := crc32.Checksum(buf[4:56], crc32cTable)
	binary.LittleEndian.PutUint32(buf[0:4], hdrCRC)

	offset := vl.tail
	_, err := vl.fd.WriteAt(buf, offset)
	if err != nil {
		return VPtr{}, fmt.Errorf("vlog: write at %d: %w", offset, err)
	}
	atomic.AddInt64(&vl.VLOGBytesWritten, int64(entrySize))
	vl.tail = offset + int64(entrySize)

	return VPtr{
		Offset: uint64(offset),
		Length: vlen,
	}, nil
}

// appendAndSync writes a value to the VLOG, fsyncs, and returns a VPtr.
// This ensures the value is durable before the caller writes the VPtr to WAL.
func (vl *valueLog) appendAndSync(value []byte, hlc HLC, skipSync bool) (VPtr, error) {
	vl.mu.Lock()
	defer vl.mu.Unlock()
	vp, err := vl.appendLocked(value, hlc)
	if err != nil {
		return vp, err
	}
	if !skipSync {
		if err := vl.fd.Sync(); err != nil {
			return vp, fmt.Errorf("vlog: sync: %w", err)
		}
	}
	return vp, nil
}

// appendDedupAndSync checks if the new value matches the old VLOG entry at
// oldVP (by comparing blake3 checksums of the 56-byte headers). If the value
// is unchanged, it returns the old VPtr without writing anything. Otherwise
// it appends the new value normally.
//
// The caller provides the pre-computed blake3 of the new value to avoid
// computing it twice (once here, once in appendLocked).
//
// See "HLC STALENESS IN VLOG HEADERS" comment at the top of this file:
// when dedup reuses an old VPtr, the VLOG header HLC becomes stale.
// The authoritative HLC lives in KV128_BLOCKS / memtable / WAL.
func (vl *valueLog) appendDedupAndSync(value []byte, hlc HLC, oldVP VPtr, skipSync bool) (VPtr, bool, error) {
	newB3 := blake3checksum32(value)

	vl.mu.Lock()
	defer vl.mu.Unlock()

	// Check if old entry has the same blake3. Length match is implied by
	// the caller only calling us when oldVP.Length == len(value).
	if oldVP.Length > 0 {
		oldB3, err := vl.readBlake3(oldVP)
		if err == nil && equal32(newB3, oldB3[:]) {
			// Value unchanged — reuse old VPtr, skip VLOG write.
			return oldVP, true, nil
		}
	}

	// Value changed (or old entry unreadable) — append new entry.
	vp, err := vl.appendLockedWithHash(value, hlc, newB3)
	if err != nil {
		return vp, false, err
	}
	if !skipSync {
		if err := vl.fd.Sync(); err != nil {
			return vp, false, fmt.Errorf("vlog: sync: %w", err)
		}
	}
	return vp, false, nil
}

// appendBatchDedupAndSync is like appendBatchAndSync but skips VLOG writes
// for values whose blake3 matches the existing entry at oldVPs[i].
// An oldVP with Length==0 means "no previous entry" (always append).
// Returns one VPtr per value and the count of dedup hits.
func (vl *valueLog) appendBatchDedupAndSync(values [][]byte, hlcs []HLC, oldVPs []VPtr, skipSync bool) ([]VPtr, int, error) {
	// Pre-compute blake3 checksums for all new values (outside the lock).
	b3s := make([][]byte, len(values))
	for i, v := range values {
		b3s[i] = blake3checksum32(v)
	}

	vl.mu.Lock()
	defer vl.mu.Unlock()

	ptrs := make([]VPtr, len(values))
	dedupHits := 0
	wrote := false

	for i, v := range values {
		// Try dedup if we have an old VPtr with matching length.
		if oldVPs[i].Length == uint64(len(v)) && oldVPs[i].Length > 0 {
			oldB3, err := vl.readBlake3(oldVPs[i])
			if err == nil && equal32(b3s[i], oldB3[:]) {
				ptrs[i] = oldVPs[i]
				dedupHits++
				continue
			}
		}
		// Append new entry.
		vp, err := vl.appendLockedWithHash(v, hlcs[i], b3s[i])
		if err != nil {
			return nil, dedupHits, err
		}
		ptrs[i] = vp
		wrote = true
	}
	if wrote && !skipSync {
		if err := vl.fd.Sync(); err != nil {
			return nil, dedupHits, fmt.Errorf("vlog: batch sync: %w", err)
		}
	}
	return ptrs, dedupHits, nil
}

// appendBatchAndSync writes multiple values to the VLOG with a single fsync.
// Returns one VPtr per value. Thread-safe.
func (vl *valueLog) appendBatchAndSync(values [][]byte, hlcs []HLC, skipSync bool) ([]VPtr, error) {
	vl.mu.Lock()
	defer vl.mu.Unlock()

	ptrs := make([]VPtr, len(values))
	for i, v := range values {
		vp, err := vl.appendLocked(v, hlcs[i])
		if err != nil {
			return nil, err
		}
		ptrs[i] = vp
	}
	if !skipSync {
		if err := vl.fd.Sync(); err != nil {
			return nil, fmt.Errorf("vlog: batch sync: %w", err)
		}
	}
	return ptrs, nil
}

// equal32 compares a 32-byte slice with a 32-byte array without allocating.
func equal32(a []byte, b []byte) bool {
	_ = a[31]
	_ = b[31]
	for i := 0; i < 32; i++ {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// read reads a value from the VLOG at the given VPtr.
// Thread-safe (uses pread).
func (vl *valueLog) read(vp VPtr) ([]byte, error) {
	entrySize := vlogEntryHeaderSize + int(vp.Length)
	buf := make([]byte, entrySize)

	_, err := vl.fd.ReadAt(buf, int64(vp.Offset))
	if err != nil {
		return nil, fmt.Errorf("vlog: read at offset %d len %d: %w", vp.Offset, vp.Length, err)
	}

	// Verify length field.
	storedLen := binary.LittleEndian.Uint64(buf[12:20])
	if storedLen != vp.Length {
		return nil, fmt.Errorf("vlog: length mismatch at offset %d: stored %d, expected %d", vp.Offset, storedLen, vp.Length)
	}
	// Verify hdrCRC (covers bytes 4..56: HLC + length + valCRC + blake3).
	storedHdrCRC := binary.LittleEndian.Uint32(buf[0:4])
	computedHdrCRC := crc32.Checksum(buf[4:56], crc32cTable)
	if computedHdrCRC != storedHdrCRC {
		return nil, fmt.Errorf("vlog: header CRC mismatch at offset %d: stored %08x, computed %08x", vp.Offset, storedHdrCRC, computedHdrCRC)
	}
	// Verify valCRC (covers the N value bytes at buf[56:]).
	storedValCRC := binary.LittleEndian.Uint32(buf[20:24])
	computedValCRC := crc32.Checksum(buf[vlogEntryHeaderSize:], crc32cTable)
	if computedValCRC != storedValCRC {
		return nil, fmt.Errorf("vlog: value CRC mismatch at offset %d: stored %08x, computed %08x", vp.Offset, storedValCRC, computedValCRC)
	}

	value := buf[vlogEntryHeaderSize:]
	return value, nil
}

// readBlake3 reads just the 56-byte header of a VLOG entry and returns the
// 32-byte blake3 checksum without reading the value bytes. This is used for
// dedup checks: if the blake3 of the new value matches, we can skip the write.
func (vl *valueLog) readBlake3(vp VPtr) ([32]byte, error) {
	var hdr [vlogEntryHeaderSize]byte
	var b3 [32]byte

	_, err := vl.fd.ReadAt(hdr[:], int64(vp.Offset))
	if err != nil {
		return b3, fmt.Errorf("vlog: readBlake3 at offset %d: %w", vp.Offset, err)
	}

	// Verify hdrCRC before trusting the blake3 bytes.
	storedHdrCRC := binary.LittleEndian.Uint32(hdr[0:4])
	computedHdrCRC := crc32.Checksum(hdr[4:56], crc32cTable)
	if computedHdrCRC != storedHdrCRC {
		return b3, fmt.Errorf("vlog: readBlake3 hdrCRC mismatch at offset %d", vp.Offset)
	}

	copy(b3[:], hdr[24:56])
	return b3, nil
}

// sync fsyncs the VLOG file.
func (vl *valueLog) sync() error {
	return vl.fd.Sync()
}

// close closes the VLOG file.
func (vl *valueLog) close() (onDiskFootprintBytes int64, err error) {
	onDiskFootprintBytes = mustStatFileSize(vl.fd)
	err = vl.fd.Close()
	return
}

// size returns the current VLOG file size.
func (vl *valueLog) size() int64 {
	return vl.tail
}

// reopen closes the current fd and reopens at the given path.
// Used by VacuumVLOG after renaming the new VLOG file into place.
func (vl *valueLog) reopen(path string) error {
	vl.mu.Lock()
	defer vl.mu.Unlock()
	vl.fd.Close()
	//fd, err := vl.vfs.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	fd, err := vl.vfs.OpenReadWrite(path, vfs.WriteCategoryUnspecified)

	if err != nil {
		return err
	}
	fi, err := fd.Stat()
	if err != nil {
		fd.Close()
		return err
	}
	vl.fd = fd
	vl.tail = fi.Size()
	return nil
}
