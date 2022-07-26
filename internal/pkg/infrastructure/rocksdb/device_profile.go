//
// Copyright (C) 2020-2021 IOTech Ltd
//
// SPDX-License-Identifier: Apache-2.0

package rocksdb

import (
	"encoding/json"
	"fmt"
	"log"

	pkgCommon "github.com/edgexfoundry/edgex-go/internal/pkg/common"

	"github.com/edgexfoundry/go-mod-core-contracts/v2/common"
	"github.com/edgexfoundry/go-mod-core-contracts/v2/errors"
	"github.com/edgexfoundry/go-mod-core-contracts/v2/models"

	"github.com/gomodule/redigo/redis"
	"github.com/linxGnu/grocksdb"
)

var wo = grocksdb.NewDefaultWriteOptions()

const (
	DeviceProfileCollection             = "md|dp"                                                        // metadata|devcieprofile
	DeviceProfileCollectionName         = DeviceProfileCollection + DBKeySeparator + common.Name         // metadata|devcieprofile:name
	DeviceProfileCollectionLabel        = DeviceProfileCollection + DBKeySeparator + common.Label        // metadata|devcieprofile:label
	DeviceProfileCollectionModel        = DeviceProfileCollection + DBKeySeparator + common.Model        // metadata|devcieprofile:model
	DeviceProfileCollectionManufacturer = DeviceProfileCollection + DBKeySeparator + common.Manufacturer // metadata|devcieprofile:manufacturer
)

// deviceProfileStoredKey return the device profile's stored key which combines the collection name and object id
func deviceProfileStoredKey(id string) string {
	return CreateKey(DeviceProfileCollection, id)
}

// deviceProfileNameExists whether the device profile exists by name
func deviceProfileNameExists(conn *grocksdb.DB, name string) (bool, errors.EdgeX) {
	exists, err := objectNameExists(conn, DeviceProfileCollectionName, name)
	if err != nil {
		return false, errors.NewCommonEdgeXWrapper(err)
	}
	return exists, nil
}

// deviceProfileIdExists checks whether the device profile exists by id
func deviceProfileIdExists(conn *grocksdb.DB, id string) (bool, errors.EdgeX) {
	exists, err := objectIdExists(conn, deviceProfileStoredKey(id))
	if err != nil {
		return false, errors.NewCommonEdgeXWrapper(err)
	}
	return exists, nil
}

// sendAddDeviceProfileCmd send redis command for adding device profile
// 장치 프로필을 추가하기 위한 redis 명령을 보냅니다.
func sendAddDeviceProfileCmd(conn *grocksdb.DB, storedKey string, dp models.DeviceProfile) errors.EdgeX {
	m, err := json.Marshal(dp)
	if err != nil {
		return errors.NewCommonEdgeX(errors.KindContractInvalid, "unable to JSON marshal device profile for Redis persistence", err)
	}
	//_ = conn.Send(SET, storedKey, m)

	//bbto := grocksdb.NewDefaultBlockBasedTableOptions()
	//bbto.SetBlockCache(grocksdb.NewLRUCache(3 << 30))
	//
	//opts := grocksdb.NewDefaultOptions()
	//opts.SetBlockBasedTableFactory(bbto)
	//opts.SetCreateIfMissing(true)
	//
	//db, err := grocksdb.OpenDb(opts, "/")
	//errString := "I am not happy to open deviceprofile DB"
	//if err != nil {
	//	log.Println(errString)
	//	log.Println(err)
	//	return nil
	//}
	//defer db.Close()

	wo := grocksdb.NewDefaultWriteOptions()
	err = conn.Put(wo, []byte(storedKey), m)
	errString2 := "I am not happy with PUT deviceprofile"
	if err != nil {
		log.Println(err, errString2)
		return nil
	}

	//ro := grocksdb.NewDefaultReadOptions()
	//value, err := db.Get(ro, []byte(storedKey))
	//errString3 := "I am not happy with GET deviceprofile"
	//if err != nil {
	//	log.Println(err, errString3)
	//	return nil
	//}
	//defer value.Free()
	//
	//fmt.Println("After GET deviceprofile(string(value.Data()): ", string(value.Data()))
	//fmt.Println("After GET deviceprofile(m): ", m)
	//fmt.Println("After GET deviceprofile(dp.Name): ", dp.Name)

	//_ = conn.Send(ZADD, DeviceProfileCollection, 0, storedKey) // (key, score(int), member)

	//_ = conn.Send(HSET, DeviceProfileCollectionName, dp.Name, storedKey) // (key, field, value)
	err = conn.Put(wo, []byte(DeviceProfileCollectionName+DBKeySeparator+dp.Name), []byte(storedKey))

	//_ = conn.Send(ZADD, CreateKey(DeviceProfileCollectionManufacturer, dp.Manufacturer), dp.Modified, storedKey)
	err = conn.Put(wo, []byte(CreateKey(DeviceProfileCollectionManufacturer, dp.Manufacturer)+DBKeySeparator+string(dp.Modified)), []byte(storedKey))

	//_ = conn.Send(ZADD, CreateKey(DeviceProfileCollectionModel, dp.Model), dp.Modified, storedKey)
	err = conn.Put(wo, []byte(CreateKey(DeviceProfileCollectionModel, dp.Model)+DBKeySeparator+string(dp.Modified)), []byte(storedKey))

	for _, label := range dp.Labels {
		//_ = conn.Send(ZADD, CreateKey(DeviceProfileCollectionLabel, label), dp.Modified, storedKey)
		err = conn.Put(wo, []byte(CreateKey(DeviceProfileCollectionLabel, label)+DBKeySeparator+string(dp.Modified)), []byte(storedKey))
	}

	return nil
}

// addDeviceProfile adds a device profile to DB
// Use sendAddDeviceProfileCmd
func addDeviceProfile(conn *grocksdb.DB, dp models.DeviceProfile) (models.DeviceProfile, errors.EdgeX) {
	// query device profile name and id to avoid the conflict
	exists, edgeXerr := deviceProfileIdExists(conn, dp.Id)
	if edgeXerr != nil {
		return dp, errors.NewCommonEdgeXWrapper(edgeXerr)
	} else if exists {
		return dp, errors.NewCommonEdgeX(errors.KindDuplicateName, fmt.Sprintf("device profile id %s exists", dp.Id), edgeXerr)
	}

	exists, edgeXerr = deviceProfileNameExists(conn, dp.Name)
	if edgeXerr != nil {
		return dp, errors.NewCommonEdgeXWrapper(edgeXerr)
	} else if exists {
		return dp, errors.NewCommonEdgeX(errors.KindDuplicateName, fmt.Sprintf("device profile name %s exists", dp.Name), edgeXerr)
	}

	ts := pkgCommon.MakeTimestamp()
	// For Redis DB, the PUT or PATCH operation will removes the old object and add the modified one,
	// so the Created is not zero value and we shouldn't set the timestamp again.
	if dp.Created == 0 {
		dp.Created = ts
	}
	dp.Modified = ts

	storedKey := deviceProfileStoredKey(dp.Id)
	//_ = conn.Send(MULTI) // 이건 atomic하게 락잡아주는거 같은거임 MULTI/EXEC

	edgeXerr = sendAddDeviceProfileCmd(conn, storedKey, dp)
	if edgeXerr != nil {
		return dp, errors.NewCommonEdgeXWrapper(edgeXerr)
	}
	//_, err := conn.Do(EXEC)
	if err != nil {
		edgeXerr = errors.NewCommonEdgeX(errors.KindDatabaseError, "device profile creation failed", err)
	}

	return dp, edgeXerr
}

// deviceProfileById query device profile by id from DB
func deviceProfileById(conn *grocksdb.DB, id string) (deviceProfile models.DeviceProfile, edgeXerr errors.EdgeX) {
	edgeXerr = getObjectById(conn, deviceProfileStoredKey(id), &deviceProfile)
	if edgeXerr != nil {
		return deviceProfile, errors.NewCommonEdgeXWrapper(edgeXerr)
	}
	return
}

// deviceProfileByName query device profile by name from DB
func deviceProfileByName(conn *grocksdb.DB, name string) (deviceProfile models.DeviceProfile, edgeXerr errors.EdgeX) {
	edgeXerr = getObjectByHash(conn, DeviceProfileCollectionName, name, &deviceProfile)
	if edgeXerr != nil {
		return deviceProfile, errors.NewCommonEdgeXWrapper(edgeXerr)
	}
	return
}

// sendDeleteDeviceProfileCmd send redis command for deleting device profile
func sendDeleteDeviceProfileCmd(conn *grocksdb.DB, storedKey string, dp models.DeviceProfile) {
	_ = conn.Send(DEL, storedKey)
	err := conn.Delete(wo, []byte(storedKey))
	errString2 := "undeleted deviceprofile"
	if err != nil {
		log.Println(err, errString2)
	}
	_ = conn.Send(ZREM, DeviceProfileCollection, storedKey)
	_ = conn.Send(HDEL, DeviceProfileCollectionName, dp.Name)
	_ = conn.Send(ZREM, CreateKey(DeviceProfileCollectionManufacturer, dp.Manufacturer), storedKey)
	_ = conn.Send(ZREM, CreateKey(DeviceProfileCollectionModel, dp.Model), storedKey)
	for _, label := range dp.Labels {
		_ = conn.Send(ZREM, CreateKey(DeviceProfileCollectionLabel, label), storedKey)
	}
}

func deleteDeviceProfile(conn *grocksdb.DB, dp models.DeviceProfile) errors.EdgeX {
	storedKey := deviceProfileStoredKey(dp.Id)

	sendDeleteDeviceProfileCmd(conn, storedKey, dp)

	if err != nil {
		return errors.NewCommonEdgeX(errors.KindDatabaseError, "device profile deletion failed", err)
	}
	return nil
}

// updateDeviceProfile updates a device profile to DB
func updateDeviceProfile(conn *grocksdb.DB, dp models.DeviceProfile) (edgeXerr errors.EdgeX) {
	var oldDeviceProfile models.DeviceProfile
	oldDeviceProfile, edgeXerr = deviceProfileById(conn, dp.Id)
	if edgeXerr == nil {
		if dp.Name != oldDeviceProfile.Name {
			return errors.NewCommonEdgeX(errors.KindContractInvalid, fmt.Sprintf("device profile name '%s' not match the exsting '%s' ", dp.Name, oldDeviceProfile.Name), nil)
		}
	} else {
		oldDeviceProfile, edgeXerr = deviceProfileByName(conn, dp.Name)
		if edgeXerr != nil {
			return errors.NewCommonEdgeXWrapper(edgeXerr)
		}
	}

	dp.Id = oldDeviceProfile.Id
	dp.Created = oldDeviceProfile.Created
	dp.Modified = pkgCommon.MakeTimestamp()

	storedKey := deviceProfileStoredKey(dp.Id)

	sendDeleteDeviceProfileCmd(conn, storedKey, oldDeviceProfile)
	edgeXerr = sendAddDeviceProfileCmd(conn, storedKey, dp)
	if edgeXerr != nil {
		return errors.NewCommonEdgeXWrapper(edgeXerr)
	}

	if err != nil {
		return errors.NewCommonEdgeX(errors.KindDatabaseError, "device profile update failed", err)
	}

	return nil
}

// deleteDeviceProfileById deletes the device profile by id
func deleteDeviceProfileById(conn *grocksdb.DB, id string) errors.EdgeX {
	deviceProfile, err := deviceProfileById(conn, id)
	if err != nil {
		return errors.NewCommonEdgeXWrapper(err)
	}
	err = deleteDeviceProfile(conn, deviceProfile)
	if err != nil {
		return errors.NewCommonEdgeXWrapper(err)
	}
	return nil
}

// deleteDeviceProfileByName deletes the device profile by name
func deleteDeviceProfileByName(conn *grocksdb.DB, name string) errors.EdgeX {
	deviceProfile, err := deviceProfileByName(conn, name)
	if err != nil {
		return errors.NewCommonEdgeXWrapper(err)
	}
	err = deleteDeviceProfile(conn, deviceProfile)
	if err != nil {
		return errors.NewCommonEdgeXWrapper(err)
	}
	return nil
}

// deviceProfilesByLabels query device profile with offset and limit
func deviceProfilesByLabels(conn *grocksdb.DB, offset int, limit int, labels []string) (deviceProfiles []models.DeviceProfile, edgeXerr errors.EdgeX) {
	objects, edgeXerr := getObjectsByLabelsAndSomeRange(conn, ZREVRANGE, DeviceProfileCollection, labels, offset, limit)
	if edgeXerr != nil {
		return deviceProfiles, errors.NewCommonEdgeXWrapper(edgeXerr)
	}

	deviceProfiles = make([]models.DeviceProfile, len(objects))
	for i, in := range objects {
		dp := models.DeviceProfile{}
		err := json.Unmarshal(in, &dp)
		if err != nil {
			return []models.DeviceProfile{}, errors.NewCommonEdgeX(errors.KindDatabaseError, "device profile format parsing failed from the database", err)
		}
		deviceProfiles[i] = dp
	}
	return deviceProfiles, nil
}

// deviceProfilesByModel query device profiles by offset, limit and model
func deviceProfilesByModel(conn *grocksdb.DB, offset int, limit int, model string) (deviceProfiles []models.DeviceProfile, edgeXerr errors.EdgeX) {
	objects, err := getObjectsByRevRange(conn, CreateKey(DeviceProfileCollectionModel, model), offset, limit)
	if err != nil {
		return deviceProfiles, errors.NewCommonEdgeXWrapper(err)
	}

	deviceProfiles = make([]models.DeviceProfile, len(objects))
	for i, in := range objects {
		dp := models.DeviceProfile{}
		err := json.Unmarshal(in, &dp)
		if err != nil {
			return deviceProfiles, errors.NewCommonEdgeX(errors.KindContractInvalid, "device profile parsing failed", err)
		}
		deviceProfiles[i] = dp
	}
	return deviceProfiles, nil
}

// deviceProfilesByManufacturer query device profiles by offset, limit and manufacturer
func deviceProfilesByManufacturer(conn *grocksdb.DB, offset int, limit int, manufacturer string) (deviceProfiles []models.DeviceProfile, edgeXerr errors.EdgeX) {
	objects, err := getObjectsByRevRange(conn, CreateKey(DeviceProfileCollectionManufacturer, manufacturer), offset, limit)
	if err != nil {
		return deviceProfiles, errors.NewCommonEdgeXWrapper(err)
	}

	deviceProfiles = make([]models.DeviceProfile, len(objects))
	for i, in := range objects {
		dp := models.DeviceProfile{}
		err := json.Unmarshal(in, &dp)
		if err != nil {
			return deviceProfiles, errors.NewCommonEdgeX(errors.KindContractInvalid, "device profile parsing failed", err)
		}
		deviceProfiles[i] = dp
	}
	return deviceProfiles, nil
}

// deviceProfilesByManufacturerAndModel query device profiles by offset, limit, manufacturer and model
func deviceProfilesByManufacturerAndModel(conn *grocksdb.DB, offset int, limit int, manufacturer string, model string) (deviceProfiles []models.DeviceProfile, totalCount uint32, edgeXerr errors.EdgeX) {
	if limit == 0 {
		return
	}
	end := offset + limit - 1
	if limit == -1 { //-1 limit means that clients want to retrieve all remaining records after offset from DB, so specifying -1 for end
		end = limit
	}

	idsSlice := make([][]string, 2)
	// query ids by manufacturer
	idsWithManufacturer, err := redis.Strings(conn.Do(ZREVRANGE, CreateKey(DeviceProfileCollectionManufacturer, manufacturer), 0, -1))
	if err != nil {
		return nil, totalCount, errors.NewCommonEdgeX(errors.KindDatabaseError, fmt.Sprintf("query object ids by manufacturer %s from database failed", manufacturer), err)
	}
	idsSlice[0] = idsWithManufacturer
	// query ids by model
	idsWithModel, err := redis.Strings(conn.Do(ZREVRANGE, CreateKey(DeviceProfileCollectionModel, model), 0, -1))
	if err != nil {
		return nil, totalCount, errors.NewCommonEdgeX(errors.KindDatabaseError, fmt.Sprintf("query object ids by model %s from database failed", manufacturer), err)
	}
	idsSlice[1] = idsWithModel

	//find common Ids among two-dimension Ids slice
	commonIds := pkgCommon.FindCommonStrings(idsSlice...)
	totalCount = uint32(len(commonIds))
	if offset > len(commonIds) {
		return nil, totalCount, errors.NewCommonEdgeX(errors.KindRangeNotSatisfiable, fmt.Sprintf("query objects bounds out of range. length:%v", len(commonIds)), nil)
	}
	if end >= len(commonIds) || end == -1 {
		commonIds = commonIds[offset:]
	} else { // as end index in golang re-slice is exclusive, increment the end index to ensure the end could be inclusive
		commonIds = commonIds[offset : end+1]
	}

	objects, edgeXerr := getObjectsByIds(conn, pkgCommon.ConvertStringsToInterfaces(commonIds))
	if edgeXerr != nil {
		return deviceProfiles, totalCount, errors.NewCommonEdgeXWrapper(edgeXerr)
	}

	deviceProfiles = make([]models.DeviceProfile, len(objects))
	for i, in := range objects {
		dp := models.DeviceProfile{}
		err := json.Unmarshal(in, &dp)
		if err != nil {
			return deviceProfiles, totalCount, errors.NewCommonEdgeX(errors.KindContractInvalid, "device profile parsing failed", err)
		}
		deviceProfiles[i] = dp
	}
	return deviceProfiles, totalCount, nil
}
