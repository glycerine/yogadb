.PHONY:all

all:
	rm -rf bench.db
	rm -f load_yogadb
	go build
	./load_yogadb ~/all
	du -sh bench.db
