
.PHONY: all fuzz

all: 
	go install
	cd cmd/yload && go install
	cd cmd/yview && go install
	cd cmd/yvac && go install
	cd cmd/ydiff && go install
	cd load_yogadb && go install
	cd cmd/yogabench && go install

fuzz:
	#go test -tags memfs -fuzz FuzzAnchorTreeDrift -fuzztime 30m -run=xxx -timeout 35m
	go test -tags memfs -fuzz FuzzFlexSpace -fuzztime 30m -run=xxx -timeout 35m
	go test -tags memfs -fuzz FuzzRecoveryFlexSpace -fuzztime 30m -run=xxx -timeout 35m
	go test -fuzz FuzzFlexTree -fuzztime 5m -run=xxx -tags memfs || true
	go test -fuzz FuzzBruteForce -fuzztime 5m -run=xxx -tags memfs || true
	go test -fuzz FuzzIntervalCache_Dedup -fuzztime 5m -run=xxx -tags memfs || true
	go test -fuzz FuzzIntervalCache_FindKey -fuzztime 5m -run=xxx -tags memfs || true
	go test -fuzz FuzzIntervalCache_Mutations -fuzztime 5m -run=xxx -tags memfs || true
	go test -fuzz FuzzSparseIndexTree -fuzztime 5m -run=xxx -tags memfs || true

