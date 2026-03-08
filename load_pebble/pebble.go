package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/cockroachdb/pebble/v2"
)

var newline = []byte("\n")
var colon = []byte(":")

func main() {
	dbPath := "bench_pebble.db"
	panicOn(os.MkdirAll(dbPath, 0777))
	db, err := pebble.Open(dbPath, &pebble.Options{})
	if err != nil {
		log.Fatal(err)
	}
	var t0 time.Time

	//defer os.RemoveAll(dbPath)
	defer func() {

		elapsed := time.Since(t0)
		fmt.Printf("Pebble ingestion finished in: %s\n", elapsed)
		fmt.Println("--------------------------------------------------")

		// Extract and print the internal metrics
		metrics := db.Metrics()
		levelMetrics := metrics.Total()
		// Pebble tracks WA precisely at the Total level
		wa := levelMetrics.WriteAmp()

		fmt.Printf("Total Bytes Ingested (Logical):  %d bytes\n", levelMetrics.TableBytesIn)
		fmt.Printf("Total Bytes Flushed (Physical):  %d bytes\n", levelMetrics.TableBytesFlushed)
		fmt.Printf("Total Bytes Compacted:           %d bytes\n", levelMetrics.TableBytesCompacted)
		fmt.Printf("Total Write Amplification (WA):  %.2f\n", wa)

		db.Close()
	}()

	var dataPath string
	if len(os.Args) == 1 {
		justShowAll(db, dbPath)
		return
	}

	if len(os.Args) > 1 {
		dataPath = os.Args[1]
	}
	if !fileExists(dataPath) {
		panicf("could not locate dataPath '%v'", dataPath)
	}

	file, err := os.Open(dataPath)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)

	// Configuration for batching
	const maxBatchCount = 10000
	batch := db.NewBatch()
	currentBatchCount := 0

	const dupKeyToVal = true
	//const dupKeyToVal = false
	if dupKeyToVal {
		vv("note: duplicating key as value too to see VLOG fill. This will double our logical write!")
	}
	fmt.Fprintf(os.Stderr, "Starting Batched Pebble ingestion to dbPath: %v ...\n", dbPath)
	t0 = time.Now()

	optNoSync := &pebble.WriteOptions{Sync: false}

	fmt.Println("Starting Batched Pebble ingestion...")

	for scanner.Scan() {
		key := scanner.Bytes()

		// Add the key to the batch.
		// nil is passed for WriteOptions because options are applied at Commit.

		if dupKeyToVal {
			// add key as value too, so that we can see our VLOG get used.
			err = batch.Set(key, key, nil)
		} else {
			//err = batch.Set(key, []byte{})
			err = batch.Set(key, nil, nil)
		}
		panicOn(err)

		currentBatchCount++

		// When the batch hits our threshold, commit it to Pebble and reset.
		if currentBatchCount >= maxBatchCount {
			err := db.Apply(batch, optNoSync) // nil => default => does fsync: &WriteOptions{Sync:true})
			//if err := batch.Commit(pebble.NoSync); err != nil {
			//log.Fatal(err)
			//}
			panicOn(err)
			batch.Close() // Free the memory used by the old batch
			batch = db.NewBatch()
			currentBatchCount = 0
		}
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

	// Commit any remaining keys that didn't fill up the final batch
	if currentBatchCount > 0 {
		err := db.Apply(batch, optNoSync) // nil => default => does fsync: &WriteOptions{Sync:true})
		panicOn(err)
		//if err := batch.Commit(pebble.NoSync); err != nil {
		//	log.Fatal(err)
		//}
		batch.Close()
	}

	db.Flush()

	// If you want to see the entire LSM-tree breakdown (levels, tables, sizes):
	// fmt.Println(metrics.String())
}

/*
func justShowAll(db *pebble.DB, dbPath string) {
	saw := 0
	//o := &pebble.IterOptions{}
	iter, err := db.NewIter(nil)
	panicOn(err)
	for iter.First(); iter.Valid(); iter.Next() {
		key := iter.Key()
		value := iter.Value()
		os.Stdout.Write(key)
		if len(value) > 0 {
			os.Stdout.Write(colon)
			os.Stdout.Write(value)
		}
		os.Stdout.Write(newline)
		saw++
	}
	os.Stdout.Sync()
	fmt.Fprintf(os.Stderr, "Pebble saw %v presently in the DB ('%v').\n", saw, dbPath)
}
*/

func justShowAll(db *pebble.DB, dbPath string) {
	saw := 0
	buf := make([]byte, 0, 4<<20)

	//o := &pebble.IterOptions{}
	iter, err := db.NewIter(nil)
	panicOn(err)
	for iter.First(); iter.Valid(); iter.Next() {
		key := iter.Key()
		value := iter.Value()
		need := 2 + len(key) + len(value)
		if len(buf)+need <= cap(buf) {
			// fine. write below.
		} else {
			os.Stdout.Write(buf)
			buf = buf[:0]
		}
		buf = append(buf, key...)
		if len(value) > 0 {
			buf = append(buf, colon...)
			buf = append(buf, value...)
		}
		buf = append(buf, newline...)

		saw++
	}
	if len(buf) > 0 {
		os.Stdout.Write(buf)
	}
	os.Stdout.Sync()
	fmt.Fprintf(os.Stderr, "Pebble saw %v presently in the DB ('%v').\n", saw, dbPath)
}

/*
jaten@Js-MacBook-Pro ~/yogadb/pebble_load (master) $ ./pebble_load ~/trash/all

2026/03/03 05:16:35 Found 0 WALs
Starting Batched Pebble ingestion...
Pebble ingestion finished in: 3.957508772s
--------------------------------------------------
Total Bytes Ingested (Logical):  12717377 bytes
Total Bytes Flushed (Physical):  20190503 bytes
Total Bytes Compacted:           4233123 bytes
Total Write Amplification (WA):  1.92

*/
