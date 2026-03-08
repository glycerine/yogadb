
.PHONY: all fuzz

all: 
	go install

fuzz:
	go test -fuzz FuzzFlexTree -fuzztime 5m -run=xxx || true
	go test -fuzz FuzzBruteForce -fuzztime 5m -run=xxx || true
	go test -fuzz FuzzIntervalCache_Dedup -fuzztime 5m -run=xxx || true
	go test -fuzz FuzzIntervalCache_FindKey -fuzztime 5m -run=xxx || true
	go test -fuzz FuzzIntervalCache_Mutations -fuzztime 5m -run=xxx || true
	go test -fuzz FuzzSparseIndexTree -fuzztime 5m -run=xxx || true
