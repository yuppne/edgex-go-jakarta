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

package grocksdb

// #include <stdlib.h>
// #include "rocksdb/c.h"
import "C"
import (
	"github.com/edgexfoundry/go-mod-core-contracts/v2/clients/logger"
	"github.com/linxGnu/grocksdb"
	"github.com/yuppne/edgex-go-jakarta/internal/pkg/db"

	"sync"
)

var currClient *Client // a singleton so Readings can be de-referenced
var once sync.Once

type Client struct {
	Database      *grocksdb.DB
	loggingClient logger.LoggingClient
}

// NewClient OpenDb opens a database with the specified options.
func NewClient(config db.Configuration, lc logger.LoggingClient) (*Client, error) {
	once.Do(func() {
		bbto := grocksdb.NewDefaultBlockBasedTableOptions()
		bbto.SetBlockCache(grocksdb.NewLRUCache(3 << 30))

		opts := grocksdb.NewDefaultOptions()
		opts.SetBlockBasedTableFactory(bbto)
		opts.SetCreateIfMissing(true)

		db, _ := grocksdb.OpenDb(opts, "/path/to/db")
		defer db.Close()

		currClient = &Client{
			Database:      db,
			loggingClient: lc,
		}
	})

	return currClient, nil
}
