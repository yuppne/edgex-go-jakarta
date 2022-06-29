//
// Copyright (C) 2020 IOTech Ltd
//
// SPDX-License-Identifier: Apache-2.0

package grocksdb

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCreateKey(t *testing.T) {
	result := CreateKey(EventsCollectionDeviceName, "TestDeviceName")
	expected := EventsCollectionDeviceName + DBKeySeparator + "TestDeviceName"
	assert.Equal(t, expected, result)
}