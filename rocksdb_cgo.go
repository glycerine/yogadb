//go:build linux && cgo && rocksdb

package yogadb

/*
#cgo CFLAGS: -I${SRCDIR}/../../facebook/rocksdb/include
#cgo LDFLAGS: -L${SRCDIR}/../../facebook/rocksdb -lrocksdb -l:liblz4.so.1 -lstdc++ -lm -lpthread -lrt -ldl -lsnappy -lz -lbz2 -lzstd -luring
#include <stdlib.h>
#include "rocksdb/c.h"
*/
import "C"

import (
	"fmt"
	"unsafe"
)

type benchRocksDB struct {
	db *C.rocksdb_t
}

type benchRocksDBWriteOptions struct {
	opts *C.rocksdb_writeoptions_t
}

func rocksDBCheckErr(err *C.char) {
	if err == nil {
		return
	}
	defer C.rocksdb_free(unsafe.Pointer(err))
	panic(fmt.Sprintf("rocksdb: %s", C.GoString(err)))
}

func openBenchRocksDB(dir string) *benchRocksDB {
	opts := C.rocksdb_options_create()
	defer C.rocksdb_options_destroy(opts)
	C.rocksdb_options_set_create_if_missing(opts, 1)

	cdir := C.CString(dir)
	defer C.free(unsafe.Pointer(cdir))

	var err *C.char
	db := C.rocksdb_open(opts, cdir, &err)
	rocksDBCheckErr(err)
	return &benchRocksDB{db: db}
}

func closeBenchRocksDB(db *benchRocksDB) {
	if db != nil && db.db != nil {
		C.rocksdb_close(db.db)
		db.db = nil
	}
}

func newBenchRocksDBWriteOptions(sync bool) *benchRocksDBWriteOptions {
	opts := C.rocksdb_writeoptions_create()
	if sync {
		C.rocksdb_writeoptions_set_sync(opts, 1)
	} else {
		C.rocksdb_writeoptions_set_sync(opts, 0)
	}
	return &benchRocksDBWriteOptions{opts: opts}
}

func closeBenchRocksDBWriteOptions(opts *benchRocksDBWriteOptions) {
	if opts != nil && opts.opts != nil {
		C.rocksdb_writeoptions_destroy(opts.opts)
		opts.opts = nil
	}
}

func rocksDBPutBatch(db *benchRocksDB, writeOpts *benchRocksDBWriteOptions, keys [][]byte, start, end int) {
	batch := C.rocksdb_writebatch_create()
	defer C.rocksdb_writebatch_destroy(batch)
	for _, k := range keys[start:end] {
		kptr := (*C.char)(unsafe.Pointer(unsafe.SliceData(k)))
		C.rocksdb_writebatch_put(batch, kptr, C.size_t(len(k)), kptr, C.size_t(len(k)))
	}
	var err *C.char
	C.rocksdb_write(db.db, writeOpts.opts, batch, &err)
	rocksDBCheckErr(err)
}

func flushBenchRocksDB(db *benchRocksDB) {
	flushOpts := C.rocksdb_flushoptions_create()
	defer C.rocksdb_flushoptions_destroy(flushOpts)
	C.rocksdb_flushoptions_set_wait(flushOpts, 1)

	var err *C.char
	C.rocksdb_flush(db.db, flushOpts, &err)
	rocksDBCheckErr(err)
}

func countBenchRocksDB(db *benchRocksDB) int {
	readOpts := C.rocksdb_readoptions_create()
	defer C.rocksdb_readoptions_destroy(readOpts)

	iter := C.rocksdb_create_iterator(db.db, readOpts)
	defer C.rocksdb_iter_destroy(iter)

	count := 0
	for C.rocksdb_iter_seek_to_first(iter); C.rocksdb_iter_valid(iter) != 0; C.rocksdb_iter_next(iter) {
		count++
	}
	var err *C.char
	C.rocksdb_iter_get_error(iter, &err)
	rocksDBCheckErr(err)
	return count
}
