//
// Copyright (C) 2020-2021 IOTech Ltd
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

	"github.com/gomodule/redigo/redis"
)

const (
	DeviceProfileCollection             = "md|dp"
	DeviceProfileCollectionName         = DeviceProfileCollection + DBKeySeparator + common.Name         // md|dp:name
	DeviceProfileCollectionLabel        = DeviceProfileCollection + DBKeySeparator + common.Label        // md|dp:Label
	DeviceProfileCollectionModel        = DeviceProfileCollection + DBKeySeparator + common.Model        // md|dp:Model
	DeviceProfileCollectionManufacturer = DeviceProfileCollection + DBKeySeparator + common.Manufacturer // md|dp:Manufacturer
)

// deviceProfileStoredKey return the device profile's stored key which combines the collection name and object id
func deviceProfileStoredKey(id string) string {
	return CreateKey(DeviceProfileCollection, id)
}

// deviceProfileNameExists whether the device profile exists by name
func deviceProfileNameExists(conn rocksClient.Client, name string) (bool, errors.EdgeX) {
	exists, err := objectNameExists(conn, DeviceProfileCollectionName, name)
	if err != nil {
		return false, errors.NewCommonEdgeXWrapper(err)
	}
	return exists, nil
}

// deviceProfileIdExists checks whether the device profile exists by id
func deviceProfileIdExists(conn rocksClient.Client, id string) (bool, errors.EdgeX) {
	exists, err := objectIdExists(conn, deviceProfileStoredKey(id))
	if err != nil {
		return false, errors.NewCommonEdgeXWrapper(err)
	}
	return exists, nil
}

// sendAddDeviceProfileCmd send redis command for adding device profile
// 생성한 키를 갖고 넣어주는거같은데.
func sendAddDeviceProfileCmd(conn rocksClient.Client, storedKey string, dp models.DeviceProfile) errors.EdgeX {
	m, err := json.Marshal(dp)
	if err != nil {
		return errors.NewCommonEdgeX(errors.KindContractInvalid, "unable to JSON marshal device profile for Redis persistence", err)
	}

	wo := grocksdb.NewDefaultWriteOptions()
	db := conn.Database

	// _ = conn.Send(SET, storedKey, m)
	_ = db.Put(wo, []byte(storedKey), m)
	//_ = conn.Send(ZADD, DeviceProfileCollection, 0, storedKey)
	_ = db.Put(wo, []byte(DeviceCollection), []byte(storedKey))
	//_ = conn.Send(HSET, DeviceProfileCollectionName, dp.Name, storedKey)
	_ = db.Put(wo, []byte(DeviceCollectionName), []byte(dp.Name))
	_ = db.Put(wo, []byte(dp.Name), []byte(storedKey))
	//_ = conn.Send(ZADD, CreateKey(DeviceProfileCollectionManufacturer, dp.Manufacturer), dp.Modified, storedKey)
	_ = db.Put(wo, []byte(CreateKey(DeviceProfileCollectionManufacturer)), []byte(storedKey))
	//_ = conn.Send(ZADD, CreateKey(DeviceProfileCollectionModel, dp.Model), dp.Modified, storedKey)
	_ = db.Put(wo, []byte(CreateKey(DeviceProfileCollectionModel, dp.Model)), []byte(storedKey))
	for _, label := range dp.Labels {
		// _ = conn.Send(ZADD, CreateKey(DeviceProfileCollectionLabel, label), dp.Modified, storedKey)
		_ = db.Put(wo, []byte(CreateKey(DeviceProfileCollectionLabel, label)), []byte(storedKey))
	}
	return nil
}

// addDeviceProfile adds a device profile to DB
func addDeviceProfile(conn rocksClient.Client, dp models.DeviceProfile) (models.DeviceProfile, errors.EdgeX) {
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

	// db 키 생성
	storedKey := deviceProfileStoredKey(dp.Id)
	// transactionDB로 해야하나? -> answer is no
	// _ = conn.Send(MULTI)

	// 디바이스 프로파일 추가하기 위해서 redis 명령어 보내. return null이 정상
	edgeXerr = sendAddDeviceProfileCmd(conn, storedKey, dp)
	if edgeXerr != nil {
		return dp, errors.NewCommonEdgeXWrapper(edgeXerr)
	}
	//_, err := conn.Do(EXEC)
	//if err != nil {
	//	edgeXerr = errors.NewCommonEdgeX(errors.KindDatabaseError, "device profile creation failed", err)
	//}

	return dp, edgeXerr
}

// deviceProfileById query device profile by id from DB
func deviceProfileById(conn rocksClient.Client, id string) (deviceProfile models.DeviceProfile, edgeXerr errors.EdgeX) {
	edgeXerr = getObjectById(conn, deviceProfileStoredKey(id), &deviceProfile)
	if edgeXerr != nil {
		return deviceProfile, errors.NewCommonEdgeXWrapper(edgeXerr)
	}
	return
}

// deviceProfileByName query device profile by name from DB
func deviceProfileByName(conn rocksClient.Client, name string) (deviceProfile models.DeviceProfile, edgeXerr errors.EdgeX) {
	edgeXerr = getObjectByHash(conn, DeviceProfileCollectionName, name, &deviceProfile)
	if edgeXerr != nil {
		return deviceProfile, errors.NewCommonEdgeXWrapper(edgeXerr)
	}
	return
}

// sendDeleteDeviceProfileCmd send redis command for deleting device profile
func sendDeleteDeviceProfileCmd(conn rocksClient.Client, storedKey string, dp models.DeviceProfile) {
	wo := grocksdb.NewDefaultWriteOptions()
	db := conn.Database

	//_ = conn.Send(DEL, storedKey)
	_ = db.Delete(wo, []byte(storedKey))

	//삭제하는거
	//_ = conn.Send(ZREM, DeviceProfileCollection, storedKey)
	_ = db.Delete(wo, []byte(DeviceProfileCollection))
	//_ = conn.Send(HDEL, DeviceProfileCollectionName, dp.Name)
	_ = db.Delete(wo, []byte(DeviceProfileCollectionName))
	_ = db.Delete(wo, []byte(dp.Name))
	//_ = conn.Send(ZREM, CreateKey(DeviceProfileCollectionManufacturer, dp.Manufacturer), storedKey)
	_ = db.Delete(wo, []byte(CreateKey(DeviceProfileCollectionManufacturer, dp.Manufacturer)))
	//_ = conn.Send(ZREM, CreateKey(DeviceProfileCollectionModel, dp.Model), storedKey)
	_ = db.Delete(wo, []byte(CreateKey(DeviceProfileCollectionModel, dp.Model)))
	for _, label := range dp.Labels {
		//_ = conn.Send(ZREM, CreateKey(DeviceProfileCollectionLabel, label), storedKey)
		_ = db.Delete(wo, []byte(CreateKey(DeviceProfileCollectionLabel, label)))
	}

}

func deleteDeviceProfile(conn rocksClient.Client, dp models.DeviceProfile) errors.EdgeX {
	storedKey := deviceProfileStoredKey(dp.Id)
	//_ = conn.Send(MULTI)
	sendDeleteDeviceProfileCmd(conn, storedKey, dp)
	//_, err := conn.Do(EXEC)
	//if err != nil {
	//	return errors.NewCommonEdgeX(errors.KindDatabaseError, "device profile deletion failed", err)
	//}
	return nil
}

// updateDeviceProfile updates a device profile to DB
func updateDeviceProfile(conn rocksClient.Client, dp models.DeviceProfile) (edgeXerr errors.EdgeX) {
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
	//_ = conn.Send(MULTI)
	sendDeleteDeviceProfileCmd(conn, storedKey, oldDeviceProfile)
	edgeXerr = sendAddDeviceProfileCmd(conn, storedKey, dp)
	if edgeXerr != nil {
		return errors.NewCommonEdgeXWrapper(edgeXerr)
	}
	//_, err := conn.Do(EXEC)
	//if err != nil {
	//	return errors.NewCommonEdgeX(errors.KindDatabaseError, "device profile update failed", err)
	//}

	return nil
}

// deleteDeviceProfileById deletes the device profile by id
func deleteDeviceProfileById(conn rocksClient.Client, id string) errors.EdgeX {
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
func deleteDeviceProfileByName(conn rocksClient.Client, name string) errors.EdgeX {
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
func deviceProfilesByLabels(conn rocksClient.Client, offset int, limit int, labels []string) (deviceProfiles []models.DeviceProfile, edgeXerr errors.EdgeX) {
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
func deviceProfilesByModel(conn rocksClient.Client, offset int, limit int, model string) (deviceProfiles []models.DeviceProfile, edgeXerr errors.EdgeX) {
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
func deviceProfilesByManufacturer(conn rocksClient.Client, offset int, limit int, manufacturer string) (deviceProfiles []models.DeviceProfile, edgeXerr errors.EdgeX) {
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
func deviceProfilesByManufacturerAndModel(conn rocksClient.Client, offset int, limit int, manufacturer string, model string) (deviceProfiles []models.DeviceProfile, totalCount uint32, edgeXerr errors.EdgeX) {
	if limit == 0 {
		return
	}
	end := offset + limit - 1
	if limit == -1 { //-1 limit means that clients want to retrieve all remaining records after offset from DB, so specifying -1 for end
		end = limit
	}

	idsSlice := make([][]string, 2)
	// query ids by manufacturer
	// 0 -1 : 전체 조회
	// grocksdb: For bulk reads, use an Iterator.
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
