package cmd

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"os"
	"runtime"
	"runtime/pprof"
	"time"

	"github.com/couchbase/go-blip"
	"github.com/spf13/cobra"
)

// This program acts as a sender equivalent to the Objective-C one in MYNetwork's
// BLIPWebSocketTest.m.

var receivedCount int
var totalBytesSent uint64


func init() {
	RootCmd.AddCommand(httpCmd)
}

var httpCmd = &cobra.Command{
	Use:   "httpexample",
	Short: "Use blip with http",
	Long:  `Use blip with http`,
	Run: func(cmd *cobra.Command, args []string) {
		httpexample()
	},
}

func httpexample() {
	maxProcs := runtime.NumCPU()
	runtime.GOMAXPROCS(maxProcs)
	log.Printf("Set GOMAXPROCS to %d", maxProcs)

	context := blip.NewContext()
	context.MaxSendQueueCount = kMaxSendQueueCount
	context.LogMessages = verbosity > 1
	context.LogFrames = verbosity > 2
	sender, err := context.Dial("ws://localhost:12345/test", "http://localhost")
	if err != nil {
		panic("Error opening WebSocket: " + err.Error())
	}

	httpClient := blip.NewHTTPClient(sender)

	if profilingHeap {
		log.Printf("Writing profile to file heap.pprof")
		f, err := os.Create("heap.pprof")
		if err != nil {
			log.Fatal(err)
		}
		defer pprof.WriteHeapProfile(f)
	} else if profilingCPU {
		log.Printf("Writing profile to file cpu.pprof")
		f, err := os.Create("cpu.pprof")
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	var startTime = time.Now()

	for sentCount < kNumToSend {
		sentCount++
		go sendRequest(httpClient)
		time.Sleep(time.Duration(kMessageInterval * float64(time.Second)))
	}

	log.Printf("Waiting for responses...")
	for {
		mutex.Lock()
		rcvd := receivedCount
		mutex.Unlock()
		if rcvd == sentCount {
			break
		}
	}

	elapsed := float64(time.Now().Sub(startTime)) / float64(time.Second)
	log.Printf("Sent & received %d HTTP messages, %d Mbytes in %.3f sec (%f megabytes/sec)",
		sentCount, totalBytesSent/1000000, elapsed, float64(totalBytesSent)/elapsed/1.0e6)
}

func sendRequest(client *http.Client) {
	requestBody := make([]byte, rand.Intn(kMaxBodySize))
	for i := 0; i < len(requestBody); i++ {
		requestBody[i] = byte(i % 256)
	}
	req, _ := http.NewRequest("GET", "/test", bytes.NewReader(requestBody))
	req.Header.Add("Content-Type", "application/octet-stream")
	response, err := client.Do(req)
	if err != nil {
		panic(fmt.Sprintf("HTTP error: %v", err))
	}
	//log.Printf("Got response: %v", response)

	if response.StatusCode != 201 {
		log.Printf("ERROR: Response status is %d not 201", response.StatusCode)
		return
	}
	responseBody, err := ioutil.ReadAll(response.Body)
	if err != nil {
		log.Printf("ERROR reading body of %s: %s", response, err)
		return
	}
	if len(responseBody) != len(requestBody) {
		panic(fmt.Sprintf("Mismatched length in response body of %v (got %d, expected %d)",
			response, len(responseBody), len(requestBody)))
	}
	for i := len(responseBody) - 1; i >= 0; i-- {
		if responseBody[i] != requestBody[i] {
			panic("Mismatched data in response body")
		}
	}

	mutex.Lock()
	totalBytesSent += uint64(len(requestBody))
	receivedCount++
	mutex.Unlock()
}

//  Copyright (c) 2013 Jens Alfke.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.
