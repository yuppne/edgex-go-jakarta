package application

import (
	"fmt"

	"github.com/linxGnu/grocksdb"
)

func ExampleConnectRocksDB() (string, error) {

	bbto := grocksdb.NewDefaultBlockBasedTableOptions()
	bbto.SetBlockCache(grocksdb.NewLRUCache(3 << 30))

	opts := grocksdb.NewDefaultOptions()
	opts.SetBlockBasedTableFactory(bbto)
	opts.SetCreateIfMissing(true)

	ro := grocksdb.NewDefaultReadOptions()

	wo := grocksdb.NewDefaultWriteOptions()

	db, _ := grocksdb.OpenDb(opts, "/home")

	// if ro and wo are not used again, be sure to Close them.

	fmt.Println("Before PUT data: ")
	_ = db.Put(wo, []byte("yubin"), []byte("ZZANG"))
	fmt.Println("After PUT data: ")
	value, _ := db.Get(ro, []byte("yubin"))
	defer value.Free()

	fmt.Println("After GET data: ", string(value.Data()))
	_ = db.Delete(wo, []byte("yubin"))

	return string(value.Data()), nil

}
