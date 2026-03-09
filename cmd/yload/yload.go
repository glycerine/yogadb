package main

import (
	"bufio"
	"fmt"
	"os"
	"time"

	"github.com/glycerine/yogadb"
)

var newline = []byte("\n")
var colon = []byte(":::")

const cmd = "yload"

func main() {

	if len(os.Args) != 3 {
		fmt.Fprintf(os.Stderr, "%v use: yload <file> <dbPath>. Please provide path arguments.\n", cmd)
		os.Exit(1)
	}
	dbPath := os.Args[2]
	if !dirExists(dbPath) {
		fmt.Fprintf(os.Stderr, "%v error: path to database does not exist: '%v'\n", cmd, dbPath)
		os.Exit(1)
	}
	dataPath := os.Args[1]
	if !fileExists(dataPath) {
		fmt.Fprintf(os.Stderr, "%v error: path to file to load does not exist: '%v'\n", cmd, dataPath)
		os.Exit(1)
	}

	cfg := &yogadb.Config{
		OmitFlexSpaceOpsRedoLog: true,
		OmitMemWalFsync:         true,
	}
	db, err := yogadb.OpenFlexDB(dbPath, cfg)
	panicOn(err)

	t0 := time.Now()

	var seen int
	writing := true

	defer func() {
		if false {
			// VacuumVLOG will generate garbage into KV128_BLOCKS, so
			// do it first.
			if true {
				fmt.Fprintf(os.Stderr, "starting db.VacuumVLOG()...\n")
				stats, err := db.VacuumVLOG()
				fmt.Fprintf(os.Stderr, "db.VacuumVLOG() gave err='%v'; stats = '%v'\n", err, stats)
			}

			//stats := &yogadb.VacuumKVStats{
			//	BytesReclaimed: 1, // start the loop off with at least one vacuum
			//}
			//for stats.BytesReclaimed > 0 {
			if true {
				fmt.Fprintf(os.Stderr, "starting db.VacuumKV()...\n")
				stats, err := db.VacuumKV()
				fmt.Fprintf(os.Stderr, "db.VacuumKV() gave err='%v'; stats = '%v'\n", err, stats)
				panicOn(err)
			}
			//}
			if false {
				fmt.Fprintf(os.Stderr, "starting db.VacuumVLOG()...\n")
				stats, err := db.VacuumVLOG()
				fmt.Fprintf(os.Stderr, "db.VacuumVLOG() gave err='%v'; stats = '%v'\n", err, stats)
			}
		}

		if writing {

			if false {
				fmt.Fprintf(os.Stderr, "starting end-of-session db.VacuumKV()...\n")
				stats, err := db.VacuumKV()
				fmt.Fprintf(os.Stderr, "end of end-of-session db.VacuumKV() gave err='%v'; stats = '%v'\n", err, stats)
				panicOn(err)
			}

			t1 := time.Now()
			elapsed := t1.Sub(t0)
			fmt.Fprintf(os.Stderr, "yload of %v pairs finished in: %s ; from '%v'\n", seen, elapsed, dataPath)
			fmt.Fprintf(os.Stderr, "--------------------------------------------------\n")

		}
		finMetrics := db.Close()
		fmt.Fprintf(os.Stderr, "finMetrics = %v\n", finMetrics)
	}()
	//defer os.RemoveAll(dbPath)

	file, err := os.Open(dataPath)
	panicOn(err)
	defer file.Close()

	scanner := bufio.NewScanner(file)

	// Configuration for batching
	const maxBatchCount = 1000
	batch := db.NewBatch()
	defer batch.Close()
	currentBatchCount := 0

	//const dupKeyToVal = true
	const dupKeyToVal = false
	if dupKeyToVal {
		vv("note: duplicating key as value too to see VLOG fill. This will double our logical write!")
	}
	fmt.Fprintf(os.Stderr, "Starting Batched YogaDB ingestion to dbPath: %v ...\n", dbPath)
	t0 = time.Now()

	if false {
		fmt.Fprintf(os.Stderr, "%v DOING START TIME db.VacuumKV()...\n", ts())
		stats, err := db.VacuumKV()
		fmt.Fprintf(os.Stderr, "%v END OF START TIME db.VacuumKV() gave err='%v'; stats = '%v'\n", ts(), err, stats)
		panicOn(err)
	}

	for scanner.Scan() {
		key := scanner.Bytes()

		// Add the key to the batch.
		if dupKeyToVal {
			// add key as value too, so that we can see our VLOG get used.
			err = batch.Set(key, key)
		} else {
			//err = batch.Set(key, []byte{})
			err = batch.Set(key, nil)
		}
		panicOn(err)
		seen++
		currentBatchCount++

		// When the batch hits our threshold, commit (and that automatically resets the batch)
		if currentBatchCount >= maxBatchCount {
			if _, err := batch.Commit(false); err != nil {
				panic(err)
			}
			currentBatchCount = 0
		}
	}

	if err := scanner.Err(); err != nil {
		panic(err)
	}

	// Commit any remaining keys that didn't fill up the final batch
	if currentBatchCount > 0 {
		if _, err := batch.Commit(false); err != nil {
			panic(err)
		}
	}
	// needed? Yes, if Commit(false) above! one fdatasync at the end:
	//vv("load_yogadb about to call db.Sync()")
	db.Sync()
	//vv("load_yogadb back from db.Sync()")

	if false {
		// write back out, to check completeness (no data is lost) and sorted-ness (proper order).
		t1 := time.Now()
		saw := 0
		db.View(func(roDB yogadb.ReadOnlyDB) error {
			roDB.Ascend(nil, func(key, value []byte) bool {
				os.Stdout.Write(key)
				if len(value) > 0 {
					os.Stdout.Write(colon)
					os.Stdout.Write(value)
				}
				os.Stdout.Write(newline)
				saw++
				return true
			})
			return nil
		})
		os.Stdout.Sync()
		fmt.Fprintf(os.Stderr, "YogaDB saw %v, output in sorted order took: %v\n", saw, time.Since(t1))
	}
}

func justShowAll(db *yogadb.FlexDB, dbPath string) {
	saw := 0
	buf := make([]byte, 0, 4<<20)
	db.View(func(roDB yogadb.ReadOnlyDB) error {
		roDB.Ascend(nil, func(key, value []byte) bool {
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
			return true
		})
		return nil
	})
	if len(buf) > 0 {
		os.Stdout.Write(buf)
	}
	os.Stdout.Sync()
	fmt.Fprintf(os.Stderr, "YogaDB saw %v key-value pair(s) in database '%v'.\n", saw, dbPath)
}
