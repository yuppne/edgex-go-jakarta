// Package rocksdb /*******************************************************************************
package rocksdb

import (
	"github.com/edgexfoundry/go-mod-core-contracts/v2/clients/logger"
	"github.com/linxGnu/grocksdb"
	"log"
	"sync"

	"github.com/edgexfoundry/edgex-go/internal/pkg/db"
)

// src/edgex-go/internal/pkg/db/rocksdb/client.go

var currClient *Client // a singleton so Readings can be de-referenced
var once sync.Once

// Client represents a Redis client
type Client struct {
	Database      *grocksdb.DB
	loggingClient logger.LoggingClient
}

// Return a pointer to the Redis client
func NewClient(config db.Configuration, lc logger.LoggingClient) (*Client, error) {
	once.Do(func() {
		database := currClient.OpenDB()

		currClient = &Client{
			Database:      database,
			loggingClient: lc,
		}
	})

	return currClient, nil
}

// OpenDB connects to Rocksdb
func (c *Client) OpenDB() *grocksdb.DB {

	bbto := grocksdb.NewDefaultBlockBasedTableOptions()
	bbto.SetBlockCache(grocksdb.NewLRUCache(3 << 30))

	opts := grocksdb.NewDefaultOptions()
	opts.SetBlockBasedTableFactory(bbto)
	opts.SetCreateIfMissing(true)

	// MEMO: where to create folder?
	Rocksdb, err := grocksdb.OpenDb(opts, "/test") // rdb: rocksdb

	if err != nil {
		errString := "unopened deviceprofile DB"
		log.Println(errString)
	}
	defer c.CloseSession()

	return Rocksdb
}

// TODO 1. Redis 2. Rocks -> close 다르게 하기

// CloseSession closes the connections to Redis
func (c *Client) CloseSession() {
	c.Database.Close()
	currClient = nil
	once = sync.Once{}
}
