package application

import (
	"fmt"
	"log"

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
	errString := "I am not happy to open data"

	db, err := grocksdb.OpenDb(opts, "/")
	if err != nil {
		log.Println(errString)
		log.Println(err)
		return errString, err
	}
	defer db.Close()
	// if ro and wo are not used again, be sure to Close them.

	fmt.Println("Before PUT data: ")
	errString2 := "I am not happy with PUT data"
	err = db.Put(wo, []byte("yubin"), []byte("0620"))
	if err != nil {
		log.Println(err, errString2)
		return errString2, err
	}

	errString3 := "I am not happy with GET data"
	fmt.Println("After PUT data: ")
	value, err := db.Get(ro, []byte("yubin"))
	if err != nil {
		log.Println(err, errString3)
		return errString3, err
	}
	defer value.Free()

	fmt.Println("After GET data: ", string(value.Data()))
	errString4 := "I am not happy with DELETE data"
	err = db.Delete(wo, []byte("yubin"))
	if err != nil {
		log.Println(err, errString4)
		return errString4, err
	}
	return string(value.Data()), err

}
