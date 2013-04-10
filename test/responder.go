package main

import (
	"fmt"
	"log"
	"net/http"

	"github.com/snej/go-blip"
)

const verbosity = 0

// This program acts as a listener equivalent to the Objective-C one in MYNetwork's
// BLIPWebSocketTest.m.

func main() {
	context := blip.NewContext()
	context.HandlerForProfile["BLIPTest/EchoData"] = dispatchEcho
	context.LogMessages = verbosity > 1
	context.LogFrames = verbosity > 2

	http.Handle("/test", context.HTTPHandler())
	err := http.ListenAndServe(":12345", nil)
	if err != nil {
		panic("ListenAndServe: " + err.Error())
	}
}

func dispatchEcho(request *blip.Message) {
	body, err := request.Body()
	if err != nil {
		log.Printf("ERROR reading body of %s: %s", request, err)
		return
	}
	for i, b := range body {
		if b != byte(i%256) {
			panic(fmt.Sprintf("Incorrect body: %x", body))
		}
	}
	if request.Properties["Content-Type"] != "application/octet-stream" {
		panic(fmt.Sprintf("Incorrect properties: %#x", request.Properties))
	}
	if response := request.Response(); response != nil {
		response.SetBody(body)
		response.Properties["Content-Type"] = request.Properties["Content-Type"]
	}
}

//  Copyright (c) 2013 Jens Alfke.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.
