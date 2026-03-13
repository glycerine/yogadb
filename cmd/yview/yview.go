package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	cristalbase64 "github.com/cristalhq/base64"
	"github.com/glycerine/blake3"
	"github.com/glycerine/yogadb"
)

var newline = []byte("\n")
var colonarrow = []byte(" ::: ")

const cmd = "yview"

type YviewConfig struct {
	KeysOnly  bool
	StatsOnly bool
	ShowHlc   bool
}

func (c *YviewConfig) SetFlags(fs *flag.FlagSet) {
	fs.BoolVar(&c.KeysOnly, "keys", false, "show keys only")
	fs.BoolVar(&c.StatsOnly, "stats", false, "show stats only")
	fs.BoolVar(&c.ShowHlc, "hlc", false, "show hlc timestamps")
}
func (c *YviewConfig) FinishConfig(fs *flag.FlagSet) (err error) {
	return nil
}

func main() {

	cmdCfg := YviewConfig{}
	fs := flag.NewFlagSet("yview", flag.ExitOnError)
	cmdCfg.SetFlags(fs)
	fs.Parse(os.Args[1:])
	err := cmdCfg.FinishConfig(fs)

	left := fs.Args()
	if len(left) != 1 {
		fmt.Fprintf(os.Stderr, "%v error: provide path to database to dump.\n", cmd)
		os.Exit(1)
	}
	dbPath := left[0]
	if !dirExists(dbPath) {
		fmt.Fprintf(os.Stderr, "%v error: path to database does not exist: '%v'\n", cmd, dbPath)
		os.Exit(1)
	}

	cfg := &yogadb.Config{
		OmitFlexSpaceOpsRedoLog: true,
		OmitMemWalFsync:         true,
	}
	db, err := yogadb.OpenFlexDB(dbPath, cfg)
	panicOn(err)

	if cmdCfg.StatsOnly {
		errs := db.CheckIntegrity()
		for i, err := range errs {
			fmt.Printf("[integrity error %02d]: %v\n", i, err)
		}
		if len(errs) == 0 {
			fmt.Printf("no integrity check errors found.\n")
		}
		metrics := db.Close()
		fmt.Printf("closing metrics= \n%v\n", metrics)
		return
	}

	t0 := time.Now()

	// skip db.Close to avoid stalling on fsyncing the files again.
	if false {
		defer func() {
			finMetrics := db.Close()
			fmt.Fprintf(os.Stderr, "time to dump: %v, finMetrics = %v\n", time.Since(t0), finMetrics)
		}()
	}

	cmdCfg.justShowAll(db, dbPath)
}

func (c *YviewConfig) justShowAll(db *yogadb.FlexDB, dbPath string) {
	saw := 0
	var keyb, valb int64
	buf := make([]byte, 0, 4<<20)
	db.View(func(roDB *yogadb.ReadOnlyTx) error {
		it := roDB.NewIter()

		for it.SeekToFirst(); it.Valid(); it.Next() {
			kv := it.KV()
			if kv != nil {
				value, err := it.FetchV()
				panicOn(err)
				key := kv.Key

				keyb += int64(len(kv.Key))
				valb += int64(len(value))
				need := 2 + len(key) + len(value)
				if len(buf)+need <= cap(buf) {
					// fine. write below.
				} else {
					os.Stdout.Write(buf)
					buf = buf[:0]
				}

				if c.ShowHlc {
					buf = append(buf, []byte(fmt.Sprintf("%v [%v] ", nice9(time.Unix(0, int64(kv.Hlc))), kv.Hlc))...)
				}

				buf = append(buf, key...)
				if c.KeysOnly {
					b3 := blake3OfBytes(value)
					buf = append(buf, []byte(fmt.Sprintf(" => val len %v b3: %v", len(value), b3))...)
				} else {
					if len(value) > 0 {
						buf = append(buf, colonarrow...)
						buf = append(buf, value...)
					}
				}
				buf = append(buf, newline...)

				saw++
			}
		}
		roDB.Ascend("", func(key string, value []byte) bool {
			keyb += int64(len(key))
			valb += int64(len(value))
			need := 2 + len(key) + len(value)
			if len(buf)+need <= cap(buf) {
				// fine. write below.
			} else {
				os.Stdout.Write(buf)
				buf = buf[:0]
			}
			buf = append(buf, key...)
			if c.KeysOnly {
				b3 := blake3OfBytes(value)
				buf = append(buf, []byte(fmt.Sprintf(" => val len %v b3: %v", len(value), b3))...)
			} else {
				if len(value) > 0 {
					buf = append(buf, colonarrow...)
					buf = append(buf, value...)
				}
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
	fmt.Fprintf(os.Stderr, "YogaDB saw %v key-value pair(s) in database '%v'. key bytes: %v; value bytes: %v; total key+value bytes: %v\n", saw, dbPath, keyb, valb, keyb+valb)
}

func blake3OfBytes(by []byte) string {
	h := blake3.New(64, nil)
	h.Write(by)
	sum := h.Sum(nil)
	return "blake3.33B-" + cristalbase64.URLEncoding.EncodeToString(sum[:33])
}
