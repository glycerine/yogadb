package yogadb

import (
	"fmt"
	"testing"
)

func TestFlexDB_MinimalFail(t *testing.T) {
	return
	// Does the issue require deletes?
	t.Run("no_delete", func(t *testing.T) {
		fs, dir := newTestFS(t)
		db := openTestDBAt(fs, t, dir, nil)
		for i := 0; i < 60; i++ {
			mustPut(t, db, fmt.Sprintf("key%05d", i), fmt.Sprintf("val%05d", i))
		}
		db.Sync()
		for i := 0; i < 60; i += 2 {
			mustPut(t, db, fmt.Sprintf("key%05d", i), fmt.Sprintf("new%05d", i))
		}
		db.Sync()
		db.Close()
		db2 := openTestDBAt(fs, t, dir, nil)
		defer db2.Close()
		_ = db2
	})
	t.Run("with_delete", func(t *testing.T) {
		fs, dir := newTestFS(t)
		db := openTestDBAt(fs, t, dir, nil)
		for i := 0; i < 60; i++ {
			mustPut(t, db, fmt.Sprintf("key%05d", i), fmt.Sprintf("val%05d", i))
		}
		db.Sync()
		for i := 0; i < 60; i += 5 {
			mustDelete(t, db, fmt.Sprintf("key%05d", i))
		}
		db.Sync()
		db.Close()
		db2 := openTestDBAt(fs, t, dir, nil)
		defer db2.Close()
		_ = db2
	})
}
