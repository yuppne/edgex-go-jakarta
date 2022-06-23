/*******************************************************************************
 * Copyright 2018 Dell Inc.
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
package main

import (
	"context"
	"fmt"
	"github.com/edgexfoundry/edgex-go/internal/core/metadata"
	"github.com/edgexfoundry/edgex-go/internal/core/metadata/application"
	"log"

	"github.com/gorilla/mux"
)

func main() {
	// **************************** MY CODE ***************************
	value, err2 := application.ExampleConnectRocksDB()
	if err2 != nil {
		fmt.Println("Connrocks Code Error:")
		fmt.Println(err2)
	}
	str1 := "@@@ Core-Metadata @@@\n"
	value2 := str1 + value
	log.Println(value)
	fmt.Println("**********************************************************\n")
	fmt.Println(value2)
	fmt.Println("**********************************************************\n")

	ctx, cancel := context.WithCancel(context.Background())
	metadata.Main(ctx, cancel, mux.NewRouter())
}
