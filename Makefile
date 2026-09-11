
.PHONY: all fuzz

all: 
	go install
	cd cmd/ymerge_into && go install
	cd cmd/yload && go install
	cd cmd/yview && go install
	cd cmd/yvac && go install
	cd cmd/ydiff && go install
	cd load_yogadb && go install
	cd cmd/yogabench && go install

fuzz:
	#rm -rf ~/anchorfuzz/
	#go test -c -fuzz=FuzzAnchorTreeDrift -tags memfs # for gdb.
	#go test -tags memfs -fuzz FuzzAnchorTreeDrift -fuzztime 30m -run=xxx -timeout 35m
	go test -fuzz FuzzYogaDBAPI -fuzztime 20hm -run=xxx -timeout 20h -tags memfs || true
	go test -fuzz KeyStable -fuzztime=5m -run=xxx -tags memfs || true
	go test -fuzz FuzzBulkLoadReloadBeforeAllowReads -fuzztime 5m -run=xxx -tags memfs || true
	go test -fuzz FuzzBulkLoadBeforeAllowReads -fuzztime 5m -run=xxx -tags memfs || true
	go test -fuzz FuzzAnchorTreeDrift -fuzztime 5m -run=xxx -tags memfs || true
	go test -fuzz FuzzFlexTree -fuzztime 5m -run=xxx -tags memfs || true
	go test -fuzz FuzzBruteForce -fuzztime 5m -run=xxx -tags memfs || true
	go test -fuzz FuzzIntervalCache_Dedup -fuzztime 5m -run=xxx -tags memfs || true
	go test -fuzz FuzzIntervalCache_FindKey -fuzztime 5m -run=xxx -tags memfs || true
	go test -fuzz FuzzIntervalCache_Mutations -fuzztime 5m -run=xxx -tags memfs || true
	go test -fuzz FuzzSparseIndexTree -fuzztime 5m -run=xxx -tags memfs || true
	go test -fuzz FuzzFlexSpace -fuzztime 30m -run=xxx -timeout 35m -tags memfs || true
	go test -fuzz FuzzRecoveryFlexSpace -fuzztime 30m -run=xxx -timeout 35m -tags memfs || true
	go test -fuzz FuzzFlexDBVtypRoundTrip -fuzztime 1m -run=xxx -timeout 1m -tags memfs || true

rocks:
	# benchmark versus RocksDB and CockroachDB/Pebble. Requires rocksdb source installed locally on linux.
	# note that the benchmem allocations will be off for rocksdb since most are in opqaque C/C++.
	CGO_ENABLED=1 go test -tags rocksdb -run '^$$' \
	-bench 'Benchmark_LoadOnly_RocksDB|Benchmark_Iter_RocksDB_Ascend' \
	-benchtime=10x -count=3
	go test -v -run=xxx -bench=Iter

