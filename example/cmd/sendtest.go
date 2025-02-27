/*
Copyright 2017-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package cmd

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"runtime"
	"runtime/pprof"
	"sync"
	"time"

	"github.com/couchbase/go-blip"
	"github.com/spf13/cobra"
)

// This program acts as a sender equivalent to the Objective-C one in MYNetwork's
// BLIPWebSocketTest.m.

const kMessageInterval float64 = 0.000 // Seconds to wait after sending each message
const kNumToSend = 10000               // Number of messages to send
const kMaxBodySize = 100000            // Max body size of each message
const kPercentCompressed = 0           // percentage of messages that are sent compressed
const kMaxSendQueueCount = 100
const kMaxPending = 10000

const profilingHeap = false
const profilingCPU = false

var pendingResponses map[blip.MessageNumber]bool
var pendingCount, sentCount int
var mutex sync.Mutex

func init() {
	RootCmd.AddCommand(sendCmd)
}

var sendCmd = &cobra.Command{
	Use:   "sender",
	Short: "Send blip requests",
	Long:  `Send blip requests`,
	Run: func(cmd *cobra.Command, args []string) {
		sender()
	},
}

func sender() {
	maxProcs := runtime.NumCPU()
	runtime.GOMAXPROCS(maxProcs)
	log.Printf("Set GOMAXPROCS to %d", maxProcs)

	context, err := blip.NewContext(blip.ContextOptions{ProtocolIds: []string{BlipExampleAppProtocolId}})
	if err != nil {
		panic(err)
	}
	context.MaxSendQueueCount = kMaxSendQueueCount
	context.LogMessages = verbosity > 1
	context.LogFrames = verbosity > 2
	sender, err := context.Dial("ws://localhost:12345/test")
	if err != nil {
		panic("Error opening WebSocket: " + err.Error())
	}

	pendingResponses = map[blip.MessageNumber]bool{}
	var totalBytesSent uint64 = 0
	var startTime = time.Now()

	log.Printf("Sending %d messages...", kNumToSend)

	if profilingHeap {
		log.Printf("Writing profile to file heap.pprof")
		f, err := os.Create("heap.pprof")
		if err != nil {
			log.Fatal(err)
		}
		defer func() {
			err := pprof.WriteHeapProfile(f)
			if err != nil {
				log.Fatalf("Error writing heap profile: %s", err)
			}
		}()
	} else if profilingCPU {
		log.Printf("Writing profile to file cpu.pprof")
		f, err := os.Create("cpu.pprof")
		if err != nil {
			log.Fatal(err)
		}
		err = pprof.StartCPUProfile(f)
		if err != nil {
			log.Fatalf("Error starting CPU profile: %s", err)
		}
		defer pprof.StopCPUProfile()
	}

	for sentCount < kNumToSend {
		request := blip.NewRequest()
		request.SetProfile("BLIPTest/EchoData")
		request.Properties["Content-Type"] = "application/octet-stream"
		request.SetCompressed(rand.Intn(100) < kPercentCompressed)
		body := make([]byte, rand.Intn(kMaxBodySize))
		for i := 0; i < len(body); i++ {
			body[i] = byte(i % 256)
		}
		request.SetBody(body)
		sender.Send(request)
		totalBytesSent += uint64(len(body))
		pending := addPending(request)

		go awaitResponse(request)

		if sentCount%1000 == 0 {
			log.Printf("Sent %d messages (backlog = %d)", sentCount, getPendingCount())
		}

		//incomingRequests, incomingResponses, outgoingRequests, outgoingResponses := sender.Backlog()
		//log.Printf("Pending: %d ... %d / %d incoming ... %d / %d outgoing",
		//	pendingCount, incomingRequests, incomingResponses, outgoingRequests, outgoingResponses)

		if pending > kMaxPending {
			log.Printf("$$$$$$ Too much backlog! Hang on a sec $$$$$$\n\n")
			time.Sleep(100 * time.Millisecond)
		} else {
			time.Sleep(time.Duration(kMessageInterval * float64(time.Second)))
		}
	}

	log.Printf("\n\nNow waiting for the remaining %d responses...\n\n", getPendingCount())

	for getPendingCount() > 0 {
		time.Sleep(10 * time.Millisecond)
	}

	log.Printf("Closing...")
	sender.Close()

	elapsed := float64(time.Now().Sub(startTime)) / float64(time.Second)
	log.Printf("Sent & received %d Mbytes in %.3f sec (%f megabytes/sec)",
		totalBytesSent/1000000, elapsed, float64(totalBytesSent)/elapsed/1.0e6)
}

func awaitResponse(request *blip.Message) {
	response := request.Response()
	removePending(response)

	if response.SerialNumber() != request.SerialNumber() {
		panic("Mismatched serial numbers")
	}
	body, err := response.Body()
	if err != nil {
		log.Printf("ERROR reading body of %s: %s", response, err)
		return
	}
	requestBody, _ := request.Body()
	if len(body) != len(requestBody) {
		panic(fmt.Sprintf("Mismatched length in response body of %v (got %d, expected %d)",
			response, len(body), len(requestBody)))
	}
	for i := len(body) - 1; i >= 0; i-- {
		if body[i] != requestBody[i] {
			panic("Mismatched data in response body")
		}
	}
}

func addPending(request *blip.Message) int {
	mutex.Lock()
	defer mutex.Unlock()
	pendingResponses[request.SerialNumber()] = true
	pendingCount++
	sentCount++
	if verbosity > 0 {
		body, _ := request.Body()
		log.Printf(">>> Sending request: %s %#v +%dbytes (%d pending)",
			request, request.Properties, len(body), pendingCount)
	}
	return pendingCount
}

func removePending(response *blip.Message) int {
	mutex.Lock()
	defer mutex.Unlock()
	if !pendingResponses[response.SerialNumber()] {
		panic("Wasn't expecting this response (response not pending)")
	}
	delete(pendingResponses, response.SerialNumber())
	pendingCount--
	if verbosity > 0 {
		body, _ := response.Body()
		log.Printf("<<< Response arrived: %s %#v +%dbytes (%d pending)",
			response, response.Properties, len(body), pendingCount)
	}
	return pendingCount
}

func getPendingCount() int {
	mutex.Lock()
	defer mutex.Unlock()
	return pendingCount
}
