.PHONY:all

all:
	rm -rf bench_pebble.db
	rm -f load_pebble
	go build
	./load_pebble ~/all
	du -sh bench_pebble.db
