package cmd

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"runtime"

	"github.com/couchbase/go-blip"
	"github.com/spf13/cobra"
)

const (
	verbosity  = 0
	kInterface = ":12345"

	// The application protocol id of the BLIP websocket subprotocol used by go-blip examples
	BlipExampleAppProtocolId = "GoBlipExample"
)

// This program acts as a listener equivalent to the Objective-C one in MYNetwork's
// BLIPWebSocketTest.m.

func init() {
	RootCmd.AddCommand(responderCmd)
}

var responderCmd = &cobra.Command{
	Use:   "responder",
	Short: "Respond to blip requests",
	Long:  `Respond to blip requests`,
	Run: func(cmd *cobra.Command, args []string) {
		responder()
	},
}

func responder() {
	maxProcs := runtime.NumCPU()
	runtime.GOMAXPROCS(maxProcs)
	log.Printf("Set GOMAXPROCS to %d", maxProcs)

	context, err := blip.NewContext(BlipExampleAppProtocolId)
	if err != nil {
		panic(err)
	}
	context.HandlerForProfile["BLIPTest/EchoData"] = dispatchEcho
	context.LogMessages = verbosity > 1
	context.LogFrames = verbosity > 2

	mux := blip.AddHTTPHandler(context)
	mux.HandleFunc("/test", httpEcho)

	http.Handle("/test", context.HTTPHandler())
	log.Printf("Listening on %s/test", kInterface)
	err = http.ListenAndServe(kInterface, nil)
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

func httpEcho(r http.ResponseWriter, request *http.Request) {
	body, err := ioutil.ReadAll(request.Body)
	log.Printf("Got HTTP %s %s (%d bytes)", request.Method, request.RequestURI, len(body))
	if err != nil {
		log.Printf("ERROR reading body of %s: %s", request, err)
		return
	}
	if len(body) == 0 {
		panic("Empty body!")
	}
	for i, b := range body {
		if b != byte(i%256) {
			panic(fmt.Sprintf("Incorrect body: %x", body))
		}
	}
	if request.Header.Get("Content-Type") != "application/octet-stream" {
		panic(fmt.Sprintf("Incorrect headers: %#v", request.Header))
	}

	r.Header().Add("Content-Type", "application/octet-stream")
	r.WriteHeader(201)
	r.Write(body)
}

//  Copyright (c) 2013 Jens Alfke.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.
