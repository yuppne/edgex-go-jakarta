//
// Copyright (C) 2021 IOTech Ltd
//
// SPDX-License-Identifier: Apache-2.0

package grocksdb

import (
	"encoding/json"
	"fmt"
	"github.com/linxGnu/grocksdb"
	rocksClient "github.com/yuppne/edgex-go-jakarta/internal/pkg/db/rocks"

	pkgCommon "github.com/yuppne/edgex-go-jakarta/internal/pkg/common"

	"github.com/edgexfoundry/go-mod-core-contracts/v2/common"
	"github.com/edgexfoundry/go-mod-core-contracts/v2/errors"
	"github.com/edgexfoundry/go-mod-core-contracts/v2/models"
)

const (
	IntervalCollection     = "ss|iv"
	IntervalCollectionName = IntervalCollection + DBKeySeparator + common.Name
)

// intervalStoredKey return the interval's stored key which combines the collection name and object id
func intervalStoredKey(id string) string {
	return CreateKey(IntervalCollection, id)
}

// sendAddIntervalCmd sends redis command for adding interval
func sendAddIntervalCmd(conn rocksClient.Client, storedKey string, interval models.Interval) errors.EdgeX {
	m, err := json.Marshal(interval)
	if err != nil {
		return errors.NewCommonEdgeX(errors.KindContractInvalid, "unable to JSON marshal interval for Redis persistence", err)
	}

	wo := grocksdb.NewDefaultWriteOptions()
	db := conn.Database
	err = db.Put(wo, []byte(storedKey), m)

	//_ = conn.Send(SET, storedKey, m)
	//_ = conn.Send(ZADD, IntervalCollection, interval.Modified, storedKey)
	//_ = conn.Send(HSET, IntervalCollectionName, interval.Name, storedKey)
	return nil
}

// addInterval adds a new interval into DB
func addInterval(conn rocksClient.Client, interval models.Interval) (models.Interval, errors.EdgeX) {
	exists, edgeXerr := objectIdExists(conn, intervalStoredKey(interval.Id))
	if edgeXerr != nil {
		return interval, errors.NewCommonEdgeXWrapper(edgeXerr)
	} else if exists {
		return interval, errors.NewCommonEdgeX(errors.KindDuplicateName, fmt.Sprintf("interval id %s already exists", interval.Id), edgeXerr)
	}

	exists, edgeXerr = objectNameExists(conn, IntervalCollectionName, interval.Name)
	if edgeXerr != nil {
		return interval, errors.NewCommonEdgeXWrapper(edgeXerr)
	} else if exists {
		return interval, errors.NewCommonEdgeX(errors.KindDuplicateName, fmt.Sprintf("interval name %s already exists", interval.Name), edgeXerr)
	}

	ts := pkgCommon.MakeTimestamp()
	if interval.Created == 0 {
		interval.Created = ts
	}
	interval.Modified = ts

	storedKey := intervalStoredKey(interval.Id)
	//_ = conn.Send(MULTI)
	edgeXerr = sendAddIntervalCmd(conn, storedKey, interval)
	if edgeXerr != nil {
		return interval, errors.NewCommonEdgeXWrapper(edgeXerr)
	}
	//_, err := conn.Do(EXEC)
	//if err != nil {
	//	edgeXerr = errors.NewCommonEdgeX(errors.KindDatabaseError, "interval creation failed", err)
	//}

	return interval, edgeXerr
}

// intervalByName query interval by name from DB
func intervalByName(conn rocksClient.Client, name string) (interval models.Interval, edgeXerr errors.EdgeX) {
	edgeXerr = getObjectByHash(conn, IntervalCollectionName, name, &interval)
	if edgeXerr != nil {
		return interval, errors.NewCommonEdgeXWrapper(edgeXerr)
	}
	return
}

// intervalById query interval by id from DB
func intervalById(conn rocksClient.Client, id string) (interval models.Interval, edgeXerr errors.EdgeX) {
	edgeXerr = getObjectById(conn, intervalStoredKey(id), &interval)
	if edgeXerr != nil {
		return interval, errors.NewCommonEdgeXWrapper(edgeXerr)
	}
	return
}

// allIntervals queries intervals by offset and limit
func allIntervals(conn rocksClient.Client, offset, limit int) (intervals []models.Interval, edgeXerr errors.EdgeX) {
	objects, edgeXerr := getObjectsByRevRange(conn, IntervalCollection, offset, limit)
	if edgeXerr != nil {
		return intervals, errors.NewCommonEdgeXWrapper(edgeXerr)
	}

	intervals = make([]models.Interval, len(objects))
	for i, o := range objects {
		s := models.Interval{}
		err := json.Unmarshal(o, &s)
		if err != nil {
			return []models.Interval{}, errors.NewCommonEdgeX(errors.KindDatabaseError, "interval format parsing failed from the database", err)
		}
		intervals[i] = s
	}
	return intervals, nil
}

// sendDeleteIntervalCmd sends redis command for deleting interval
func sendDeleteIntervalCmd(conn rocksClient.Client, storedKey string, interval models.Interval) {

	wo := grocksdb.NewDefaultWriteOptions()
	db := conn.Database
	err := db.Delete(wo, []byte(storedKey))
	//_ = conn.Send(DEL, storedKey)
	//_ = conn.Send(ZREM, IntervalCollection, storedKey)
	//_ = conn.Send(HDEL, IntervalCollectionName, interval.Name)
	if err != nil {
		return
	}
}

// deleteIntervalByName deletes the interval by name
func deleteIntervalByName(conn rocksClient.Client, name string) errors.EdgeX {
	interval, edgeXerr := intervalByName(conn, name)
	if edgeXerr != nil {
		return errors.NewCommonEdgeXWrapper(edgeXerr)
	}
	storedKey := intervalStoredKey(interval.Id)
	//_ = conn.Send(MULTI)
	sendDeleteIntervalCmd(conn, storedKey, interval)
	//_, err := conn.Do(EXEC)
	//if err != nil {
	//	return errors.NewCommonEdgeX(errors.KindDatabaseError, "interval deletion failed", err)
	//}
	return nil
}

// updateInterval updates a interval
func updateInterval(conn rocksClient.Client, interval models.Interval) errors.EdgeX {
	oldInterval, edgeXerr := intervalByName(conn, interval.Name)
	if edgeXerr != nil {
		return errors.NewCommonEdgeXWrapper(edgeXerr)
	}

	interval.Modified = pkgCommon.MakeTimestamp()
	storedKey := intervalStoredKey(interval.Id)
	//_ = conn.Send(MULTI)
	sendDeleteIntervalCmd(conn, storedKey, oldInterval)
	edgeXerr = sendAddIntervalCmd(conn, storedKey, interval)
	if edgeXerr != nil {
		return errors.NewCommonEdgeXWrapper(edgeXerr)
	}
	//_, err := conn.Do(EXEC)
	//if err != nil {
	//	return errors.NewCommonEdgeX(errors.KindDatabaseError, "interval update failed", err)
	//}

	return nil
}
