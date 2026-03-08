package yogadb

// vlog.go — Append-only value log for large-value separation (WiscKey-style).
//
// Values larger than vlogInlineThreshold are stored in the VLOG file instead of
// inline in FlexSpace. This reduces write amplification: large values are written
// once to VLOG and never rewritten by FlexSpace GC, compaction, or interval rewrites.
//
// On-disk layout:
//   {db_dir}/VLOG — append-only file of value entries
//
// Each VLOG entry (24-byte header + N-byte value):
//   [4 bytes] hdrCRC  — CRC32C of bytes 4..24 (HLC + length + valCRC)
//   [8 bytes] HLC timestamp (big-endian int64 — preserves sort order in byte scans)
//   [8 bytes] value length (little-endian uint64 — supports values up to int64 max)
//   [4 bytes] valCRC  — CRC32C of the N value bytes that follow
//   [N bytes] raw value bytes
//
// The two-CRC design allows validating the header (HLC + length) without reading
// the value bytes. This is useful for TTL expiry checks and vacuum filtering
// where only the timestamp needs inspection.
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

	// VLOG entry header: 4-byte hdrCRC + 8-byte HLC + 8-byte length + 4-byte valCRC = 24 bytes
	vlogEntryHeaderSize = 24

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
// Does NOT fsync — caller must call sync() when durability is needed.
// Thread-safe (serialized by mu).
func (vl *valueLog) append(value []byte, hlc HLC) (VPtr, error) {
	vl.mu.Lock()
	defer vl.mu.Unlock()
	return vl.appendLocked(value, hlc)
}

// appendLocked appends a value while mu is already held.
// On-disk format: [4-byte hdrCRC][8-byte HLC BE][8-byte value length N][4-byte valCRC][N-byte value]
// hdrCRC covers bytes 4..24 (HLC + length + valCRC). valCRC covers the N value bytes.
func (vl *valueLog) appendLocked(value []byte, hlc HLC) (VPtr, error) {
	vlen := uint64(len(value))
	entrySize := vlogEntryHeaderSize + int(vlen)

	// Build entry: [hdrCRC][HLC][length][valCRC][value...]
	buf := make([]byte, entrySize)
	binary.BigEndian.PutUint64(buf[4:12], uint64(hlc))
	binary.LittleEndian.PutUint64(buf[12:20], vlen)
	copy(buf[24:], value)
	// valCRC covers the value bytes.
	valCRC := crc32.Checksum(buf[24:], crc32cTable)
	binary.LittleEndian.PutUint32(buf[20:24], valCRC)
	// hdrCRC covers HLC + length + valCRC (bytes 4..24).
	hdrCRC := crc32.Checksum(buf[4:24], crc32cTable)
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
func (vl *valueLog) appendAndSync(value []byte, hlc HLC) (VPtr, error) {
	vl.mu.Lock()
	defer vl.mu.Unlock()
	vp, err := vl.appendLocked(value, hlc)
	if err != nil {
		return vp, err
	}
	if err := vl.fd.Sync(); err != nil {
		return vp, fmt.Errorf("vlog: sync: %w", err)
	}
	return vp, nil
}

// appendBatchAndSync writes multiple values to the VLOG with a single fsync.
// Returns one VPtr per value. Thread-safe.
func (vl *valueLog) appendBatchAndSync(values [][]byte, hlcs []HLC) ([]VPtr, error) {
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
	if err := vl.fd.Sync(); err != nil {
		return nil, fmt.Errorf("vlog: batch sync: %w", err)
	}
	return ptrs, nil
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
	// Verify hdrCRC (covers HLC + length + valCRC — bytes 4..24).
	storedHdrCRC := binary.LittleEndian.Uint32(buf[0:4])
	computedHdrCRC := crc32.Checksum(buf[4:24], crc32cTable)
	if computedHdrCRC != storedHdrCRC {
		return nil, fmt.Errorf("vlog: header CRC mismatch at offset %d: stored %08x, computed %08x", vp.Offset, storedHdrCRC, computedHdrCRC)
	}
	// Verify valCRC (covers the N value bytes at buf[24:]).
	storedValCRC := binary.LittleEndian.Uint32(buf[20:24])
	computedValCRC := crc32.Checksum(buf[24:], crc32cTable)
	if computedValCRC != storedValCRC {
		return nil, fmt.Errorf("vlog: value CRC mismatch at offset %d: stored %08x, computed %08x", vp.Offset, storedValCRC, computedValCRC)
	}

	value := buf[24:]
	return value, nil
}

// sync fsyncs the VLOG file.
func (vl *valueLog) sync() error {
	return vl.fd.Sync()
}

// close closes the VLOG file.
func (vl *valueLog) close() error {
	return vl.fd.Close()
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
