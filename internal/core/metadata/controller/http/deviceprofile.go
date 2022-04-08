//
// Copyright (C) 2021 IOTech Ltd
//
// SPDX-License-Identifier: Apache-2.0

package http

import (
	"fmt"
	rocksexample "github.com/yuppne/edgex-go-jakarta/internal/pkg/infrastructure/rocks"
	"math"
	"net/http"

	"github.com/yuppne/edgex-go-jakarta/internal/core/metadata/application"
	metadataContainer "github.com/yuppne/edgex-go-jakarta/internal/core/metadata/container"
	"github.com/yuppne/edgex-go-jakarta/internal/io"
	"github.com/yuppne/edgex-go-jakarta/internal/pkg"
	"github.com/yuppne/edgex-go-jakarta/internal/pkg/correlation"
	"github.com/yuppne/edgex-go-jakarta/internal/pkg/utils"

	"github.com/edgexfoundry/go-mod-bootstrap/v2/bootstrap/container"
	"github.com/edgexfoundry/go-mod-bootstrap/v2/di"
	"github.com/edgexfoundry/go-mod-core-contracts/v2/common"
	"github.com/edgexfoundry/go-mod-core-contracts/v2/dtos"
	commonDTO "github.com/edgexfoundry/go-mod-core-contracts/v2/dtos/common"
	requestDTO "github.com/edgexfoundry/go-mod-core-contracts/v2/dtos/requests"
	responseDTO "github.com/edgexfoundry/go-mod-core-contracts/v2/dtos/responses"
	"github.com/edgexfoundry/go-mod-core-contracts/v2/errors"

	"github.com/gorilla/mux"
)

const yamlFileName = "file"

type DeviceProfileController struct {
	jsonDtoReader io.DtoReader
	yamlDtoReader io.DtoReader
	dic           *di.Container
}

// NewDeviceProfileController creates and initializes an DeviceProfileController
func NewDeviceProfileController(dic *di.Container) *DeviceProfileController {
	return &DeviceProfileController{
		jsonDtoReader: io.NewJsonDtoReader(),
		yamlDtoReader: io.NewYamlDtoReader(),
		dic:           dic,
	}
}

func (dc *DeviceProfileController) AddDeviceProfile(w http.ResponseWriter, r *http.Request) {
	if r.Body != nil {
		defer func() { _ = r.Body.Close() }()
	}

	lc := container.LoggingClientFrom(dc.dic.Get)

	ctx := r.Context()
	correlationId := correlation.FromContext(ctx)

	var reqDTOs []requestDTO.DeviceProfileRequest
	err := dc.jsonDtoReader.Read(r.Body, &reqDTOs)
	if err != nil {
		utils.WriteErrorResponse(w, ctx, lc, err, "")
		return
	}
	deviceProfiles := requestDTO.DeviceProfileReqToDeviceProfileModels(reqDTOs)

	var addResponses []interface{}
	for i, d := range deviceProfiles {
		var addDeviceProfileResponse interface{}
		reqId := reqDTOs[i].RequestId
		newId, err := application.AddDeviceProfile(d, ctx, dc.dic)
		if err != nil {
			lc.Error(err.Error(), common.CorrelationHeader, correlationId)
			lc.Debug(err.DebugMessages(), common.CorrelationHeader, correlationId)
			addDeviceProfileResponse = commonDTO.NewBaseResponse(
				reqId,
				err.Message(),
				err.Code())
		} else {
			addDeviceProfileResponse = commonDTO.NewBaseWithIdResponse(
				reqId,
				"",
				http.StatusCreated,
				newId)
		}
		addResponses = append(addResponses, addDeviceProfileResponse)
	}

	utils.WriteHttpHeader(w, ctx, http.StatusMultiStatus)
	pkg.Encode(addResponses, w, lc)
}

func (dc *DeviceProfileController) UpdateDeviceProfile(w http.ResponseWriter, r *http.Request) {
	if r.Body != nil {
		defer func() { _ = r.Body.Close() }()
	}

	lc := container.LoggingClientFrom(dc.dic.Get)

	ctx := r.Context()
	correlationId := correlation.FromContext(ctx)

	var reqDTOs []requestDTO.DeviceProfileRequest
	err := dc.jsonDtoReader.Read(r.Body, &reqDTOs)
	if err != nil {
		utils.WriteErrorResponse(w, ctx, lc, err, "")
		return
	}
	deviceProfiles := requestDTO.DeviceProfileReqToDeviceProfileModels(reqDTOs)

	var responses []interface{}
	for i, d := range deviceProfiles {
		var response interface{}
		reqId := reqDTOs[i].RequestId
		err := application.UpdateDeviceProfile(d, ctx, dc.dic)
		if err != nil {
			lc.Error(err.Error(), common.CorrelationHeader, correlationId)
			lc.Debug(err.DebugMessages(), common.CorrelationHeader, correlationId)
			response = commonDTO.NewBaseResponse(
				reqId,
				err.Message(),
				err.Code())
		} else {
			response = commonDTO.NewBaseResponse(
				reqId,
				"",
				http.StatusOK)
		}
		responses = append(responses, response)
	}

	utils.WriteHttpHeader(w, ctx, http.StatusMultiStatus)
	pkg.Encode(responses, w, lc)
}

func (dc *DeviceProfileController) AddDeviceProfileByYaml(w http.ResponseWriter, r *http.Request) {
	if r.Body != nil {
		defer func() { _ = r.Body.Close() }()
	}

	lc := container.LoggingClientFrom(dc.dic.Get)
	ctx := r.Context()

	file, _, fileErr := r.FormFile(yamlFileName)
	if fileErr == http.ErrMissingFile {
		utils.WriteErrorResponse(w, ctx, lc, errors.NewCommonEdgeX(errors.KindContractInvalid, "missing yaml file", nil), "")
		return
	} else if fileErr != nil {
		utils.WriteErrorResponse(w, ctx, lc, errors.NewCommonEdgeX(errors.KindServerError, fileErr.Error(), nil), "")
		return
	}

	var deviceProfileDTO dtos.DeviceProfile
	err := dc.yamlDtoReader.Read(file, &deviceProfileDTO)
	if err != nil {
		utils.WriteErrorResponse(w, ctx, lc, err, "")
		return
	}
	deviceProfile := dtos.ToDeviceProfileModel(deviceProfileDTO)

	newId, err := application.AddDeviceProfile(deviceProfile, ctx, dc.dic)
	if err != nil {
		utils.WriteErrorResponse(w, ctx, lc, err, "")
		return
	}

	// **************************** MY CODE ***************************
	value, err2 := rocksexample.ExampleConnectRocksDB()
	if err2 != nil {
		fmt.Println(value)
		utils.WriteErrorResponse(w, ctx, lc, errors.NewCommonEdgeX(errors.KindYubinError, "rocksdb not working", nil), "")
		return
	}
	// *************************** MY CODE ****************************

	response := commonDTO.NewBaseWithIdResponse("", "", http.StatusCreated, newId)
	utils.WriteHttpHeader(w, ctx, http.StatusCreated)
	// Encode and send the resp body as JSON format
	pkg.Encode(response, w, lc)

}

func (dc *DeviceProfileController) UpdateDeviceProfileByYaml(w http.ResponseWriter, r *http.Request) {
	if r.Body != nil {
		defer func() { _ = r.Body.Close() }()
	}

	lc := container.LoggingClientFrom(dc.dic.Get)
	ctx := r.Context()

	file, _, fileErr := r.FormFile(yamlFileName)
	if fileErr == http.ErrMissingFile {
		utils.WriteErrorResponse(w, ctx, lc, errors.NewCommonEdgeX(errors.KindContractInvalid, "missing yaml file", nil), "")
		return
	} else if fileErr != nil {
		utils.WriteErrorResponse(w, ctx, lc, errors.NewCommonEdgeX(errors.KindServerError, fileErr.Error(), nil), "")
		return
	}

	var deviceProfileDTO dtos.DeviceProfile
	err := dc.yamlDtoReader.Read(file, &deviceProfileDTO)
	if err != nil {
		utils.WriteErrorResponse(w, ctx, lc, err, "")
		return
	}

	deviceProfile := dtos.ToDeviceProfileModel(deviceProfileDTO)
	err = application.UpdateDeviceProfile(deviceProfile, ctx, dc.dic)
	if err != nil {
		utils.WriteErrorResponse(w, ctx, lc, err, "")
		return
	}

	response := commonDTO.NewBaseResponse("", "", http.StatusOK)
	utils.WriteHttpHeader(w, ctx, http.StatusOK)
	pkg.Encode(response, w, lc)
}

func (dc *DeviceProfileController) DeviceProfileByName(w http.ResponseWriter, r *http.Request) {
	lc := container.LoggingClientFrom(dc.dic.Get)
	ctx := r.Context()

	// URL parameters
	vars := mux.Vars(r)
	name := vars[common.Name]

	deviceProfile, err := application.DeviceProfileByName(name, ctx, dc.dic)
	if err != nil {
		utils.WriteErrorResponse(w, ctx, lc, err, "")
		return
	}

	response := responseDTO.NewDeviceProfileResponse("", "", http.StatusOK, deviceProfile)
	utils.WriteHttpHeader(w, ctx, http.StatusOK)
	pkg.Encode(response, w, lc) // encode and send out the response
}

func (dc *DeviceProfileController) DeleteDeviceProfileByName(w http.ResponseWriter, r *http.Request) {
	lc := container.LoggingClientFrom(dc.dic.Get)
	ctx := r.Context()

	// URL parameters
	vars := mux.Vars(r)
	name := vars[common.Name]

	err := application.DeleteDeviceProfileByName(name, ctx, dc.dic)
	if err != nil {
		utils.WriteErrorResponse(w, ctx, lc, err, "")
		return
	}

	response := commonDTO.NewBaseResponse("", "", http.StatusOK)
	utils.WriteHttpHeader(w, ctx, http.StatusOK)
	pkg.Encode(response, w, lc)
}

func (dc *DeviceProfileController) AllDeviceProfiles(w http.ResponseWriter, r *http.Request) {
	lc := container.LoggingClientFrom(dc.dic.Get)
	ctx := r.Context()
	config := metadataContainer.ConfigurationFrom(dc.dic.Get)

	// parse URL query string for offset, limit, and labels
	offset, limit, labels, err := utils.ParseGetAllObjectsRequestQueryString(r, 0, math.MaxInt32, -1, config.Service.MaxResultCount)
	if err != nil {
		utils.WriteErrorResponse(w, ctx, lc, err, "")
		return
	}
	deviceProfiles, totalCount, err := application.AllDeviceProfiles(offset, limit, labels, dc.dic)
	if err != nil {
		utils.WriteErrorResponse(w, ctx, lc, err, "")
		return
	}

	response := responseDTO.NewMultiDeviceProfilesResponse("", "", http.StatusOK, totalCount, deviceProfiles)
	utils.WriteHttpHeader(w, ctx, http.StatusOK)
	pkg.Encode(response, w, lc)
}

func (dc *DeviceProfileController) DeviceProfilesByModel(w http.ResponseWriter, r *http.Request) {
	lc := container.LoggingClientFrom(dc.dic.Get)
	ctx := r.Context()
	config := metadataContainer.ConfigurationFrom(dc.dic.Get)

	vars := mux.Vars(r)
	model := vars[common.Model]

	// parse URL query string for offset, limit
	offset, limit, _, err := utils.ParseGetAllObjectsRequestQueryString(r, 0, math.MaxInt32, -1, config.Service.MaxResultCount)
	if err != nil {
		utils.WriteErrorResponse(w, ctx, lc, err, "")
		return
	}
	deviceProfiles, totalCount, err := application.DeviceProfilesByModel(offset, limit, model, dc.dic)
	if err != nil {
		utils.WriteErrorResponse(w, ctx, lc, err, "")
		return
	}

	response := responseDTO.NewMultiDeviceProfilesResponse("", "", http.StatusOK, totalCount, deviceProfiles)
	utils.WriteHttpHeader(w, ctx, http.StatusOK)
	pkg.Encode(response, w, lc)
}

func (dc *DeviceProfileController) DeviceProfilesByManufacturer(w http.ResponseWriter, r *http.Request) {
	lc := container.LoggingClientFrom(dc.dic.Get)
	ctx := r.Context()
	config := metadataContainer.ConfigurationFrom(dc.dic.Get)

	vars := mux.Vars(r)
	manufacturer := vars[common.Manufacturer]

	// parse URL query string for offset, limit
	offset, limit, _, err := utils.ParseGetAllObjectsRequestQueryString(r, 0, math.MaxInt32, -1, config.Service.MaxResultCount)
	if err != nil {
		utils.WriteErrorResponse(w, ctx, lc, err, "")
		return
	}
	deviceProfiles, totalCount, err := application.DeviceProfilesByManufacturer(offset, limit, manufacturer, dc.dic)
	if err != nil {
		utils.WriteErrorResponse(w, ctx, lc, err, "")
		return
	}

	response := responseDTO.NewMultiDeviceProfilesResponse("", "", http.StatusOK, totalCount, deviceProfiles)
	utils.WriteHttpHeader(w, ctx, http.StatusOK)
	pkg.Encode(response, w, lc)
}

func (dc *DeviceProfileController) DeviceProfilesByManufacturerAndModel(w http.ResponseWriter, r *http.Request) {
	lc := container.LoggingClientFrom(dc.dic.Get)
	ctx := r.Context()
	config := metadataContainer.ConfigurationFrom(dc.dic.Get)

	vars := mux.Vars(r)
	manufacturer := vars[common.Manufacturer]
	model := vars[common.Model]

	// parse URL query string for offset, limit
	offset, limit, _, err := utils.ParseGetAllObjectsRequestQueryString(r, 0, math.MaxInt32, -1, config.Service.MaxResultCount)
	if err != nil {
		utils.WriteErrorResponse(w, ctx, lc, err, "")
		return
	}
	deviceProfiles, totalCount, err := application.DeviceProfilesByManufacturerAndModel(offset, limit, manufacturer, model, dc.dic)
	if err != nil {
		utils.WriteErrorResponse(w, ctx, lc, err, "")
		return
	}

	response := responseDTO.NewMultiDeviceProfilesResponse("", "", http.StatusOK, totalCount, deviceProfiles)
	utils.WriteHttpHeader(w, ctx, http.StatusOK)
	pkg.Encode(response, w, lc)
}
