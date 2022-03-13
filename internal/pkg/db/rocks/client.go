/*******************************************************************************
 * Copyright 2018 Redis Labs Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *******************************************************************************/
//package redis
//
//import (
//	"fmt"
//	"github.com/yuppne/edgex-go-jakarta/internal/pkg/db"
//	"github.com/edgexfoundry/go-mod-core-contracts/v2/clients/logger"
//	"github.com/gomodule/redigo/redis"
//	"os"
//	"sync"
//	"time"
//)

package gorocksdb

// #include <stdlib.h>
// #include "rocksdb/c.h"
import "C"
import (
	"errors"
	// "github.com/edgexfoundry/go-mod-core-contracts/v2/clients/logger"
	// "github.com/yuppne/edgex-go-jakarta/internal/pkg/db"
	_ "github.com/yuppne/gorocksdb"
	"sync"
	"unsafe"
)

var currClient *Client // a singleton so Readings can be de-referenced
var once sync.Once

// DB is a reusable handle to a RocksDB database on disk, created by Open.
type Client struct {
	c    *C.rocksdb_t
	name string
	opts *C.Options
}

//type Client struct {
//	database      *gorocksdb.DB
//	loggingClient logger.LoggingClient
//}

type CoreDataClient struct {
	*Client
}

// OpenDb opens a database with the specified options.
func OpenDb(opts *C.Options, name string) (*Client, error) {
	var (
		cErr  *C.char
		cName = C.CString(name)
	)
	defer C.free(unsafe.Pointer(cName))

	db := C.rocksdb_open(opts.c, cName, &cErr)

	if cErr != nil {
		defer C.rocksdb_free(unsafe.Pointer(cErr))
		return nil, errors.New(C.GoString(cErr))
	}

	currClient = &Client{
		name: name,
		c:    db,
		opts: opts,
	}

	return currClient, nil
}

// Close closes the database.
func (db *Client) Close() {
	C.rocksdb_close(db.c)
}
