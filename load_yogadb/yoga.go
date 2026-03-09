package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/glycerine/yogadb"
)

var newline = []byte("\n")
var colon = []byte(":")

func main() {
	dbPath := "bench.db"

	cfg := &yogadb.Config{
		//OmitFlexSpaceOpsRedoLog: true,
		OmitMemWalFsync: true,
	}
	db, err := yogadb.OpenFlexDB(dbPath, cfg)
	fmt.Fprintf(os.Stderr, "using REDO.LOG: %v\n", !cfg.OmitFlexSpaceOpsRedoLog)
	if err != nil {
		log.Fatal(err)
	}

	writing := true
	var t0 time.Time
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
			fmt.Fprintf(os.Stderr, "YogaDB ingestion finished in: %s\n", elapsed)
			fmt.Fprintf(os.Stderr, "--------------------------------------------------\n")

		}
		finMetrics := db.Close()
		fmt.Fprintf(os.Stderr, "finMetrics = %v\n", finMetrics)
	}()
	//defer os.RemoveAll(dbPath)

	if len(os.Args) == 1 {
		writing = false
		justShowAll(db, dbPath)
		return

		// just vacuumKV for now, no display of contents.
		if true {
			fmt.Fprintf(os.Stderr, "%v DOING VIEW-ONLY END TIME db.VacuumKV()...\n", ts())
			stats, err := db.VacuumKV()
			fmt.Fprintf(os.Stderr, "%v END OF VIEW-ONLY END TIME db.VacuumKV() gave err='%v'; stats = '%v'\n", ts(), err, stats)
			panicOn(err) // panic: vacuumkv: read poff=37749056 len=320: EOF
		}

		return
	}

	var dataPath string
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
	const maxBatchCount = 1000
	batch := db.NewBatch()
	defer batch.Close()
	currentBatchCount := 0

	const dupKeyToVal = true
	//const dupKeyToVal = false
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

		currentBatchCount++

		// When the batch hits our threshold, commit (and that automatically resets the batch)
		if currentBatchCount >= maxBatchCount {
			if _, err := batch.Commit(false); err != nil {
				log.Fatal(err)
			}
			currentBatchCount = 0
		}
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

	// Commit any remaining keys that didn't fill up the final batch
	if currentBatchCount > 0 {
		if _, err := batch.Commit(false); err != nil {
			log.Fatal(err)
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
	fmt.Fprintf(os.Stderr, "YogaDB saw %v presently in the DB ('%v').\n", saw, dbPath)
}

/*
jaten@jbook ~/yogadb/yogadb_load (master) $ ./yogadb_load ~/trash/all
Starting Batched YogaDB ingestion to dbPath: bench.db ...
YogaDB ingestion finished in: 704.943999ms
--------------------------------------------------

Metrics{
(just this) Session: true,
   DataBytesWritten: 15752655,
    WALBytesWritten: 15755410,
    LogBytesWritten: 13909512,
   TreeBytesWritten: 0,
   VLOGBytesWritten: 0,
LogicalBytesWritten: 11408066,
  TotalBytesWritten: 45417577,
           WriteAmp: 3.981,
}
*/
