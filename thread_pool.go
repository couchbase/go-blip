/*
Copyright 2023-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package blip

import (
	"log"
	"runtime"
	"runtime/debug"
)

// Runs functions asynchronously using a fixed-size pool of goroutines.
// Intended for use by Dispatchers and Handlers, though it's not limited to that.
type ThreadPool struct {
	Concurrency  int                   // The number of goroutines. 0 means GOMAXPROCS
	PanicHandler func(err interface{}) // Called if a function panics

	concurrency int           // Actual concurrency
	channel     chan<- func() // Queue of calls to be dispatched
	terminator  chan bool     // Goroutines stop when this is closed
}

// Starts a ThreadPool.
func (p *ThreadPool) Start() {
	p.concurrency = p.Concurrency
	if p.concurrency <= 0 {
		p.concurrency = runtime.GOMAXPROCS(0)
	}
	ch := make(chan func(), 1000) //TODO: What real limit?
	p.channel = ch
	p.terminator = make(chan bool)

	handlerLoop := func(i int) {
		// Each goroutine in the pool runs this loop, calling functions from the channel
		// until it either reads a nil function, or something happens to the terminator.
		for {
			select {
			case fn := <-ch:
				if fn != nil {
					p.perform(i, fn)
				} else {
					return
				}
			case <-p.terminator:
				return
			}
		}
	}

	for i := 0; i < p.concurrency; i++ {
		go handlerLoop(i + 1)
	}
}

// Stops a ThreadPool after it completes all currently running or queued tasks.
func (p *ThreadPool) Stop() {
	if p.channel != nil {
		for i := 0; i < p.concurrency; i++ {
			p.channel <- nil // each nil stops one goroutine
		}
	}
}

// Stops a ThreadPool ASAP. Currently running tasks will complete; queued tasks will be dropped.
func (p *ThreadPool) StopImmediately() {
	if p.terminator != nil {
		close(p.terminator)
	}
}

// Schedules the function to be called on ThreadPool's next available goroutine.
func (p *ThreadPool) Go(fn func()) {
	if p.channel == nil {
		panic("ThreadPool has not been started")
	} else if fn == nil {
		panic("Invalid nil function")
	}
	p.channel <- fn
}

// Given a SynchronousHandler function, returns an AsyncHandler function that will call the
// wrapped handler on one of the ThreadPool's goroutines.
func (p *ThreadPool) WrapSynchronousHandler(handler SynchronousHandler) AsyncHandler {
	return func(request *Message, onComplete RequestCompletedCallback) {
		p.Go(func() {
			defer onComplete()
			handler(request)
		})
	}
}

// Given an AsyncHandler function, returns an AsyncHandler function that will call the
// wrapped handler on one of the ThreadPool's goroutines.
func (p *ThreadPool) WrapAsyncHandler(handler AsyncHandler) AsyncHandler {
	return func(request *Message, onComplete RequestCompletedCallback) {
		p.Go(func() {
			handler(request, onComplete)
		})
	}
}

func (p *ThreadPool) perform(i int, fn func()) {
	defer func() {
		if panicked := recover(); panicked != nil {
			if p.PanicHandler != nil {
				p.PanicHandler(panicked)
			} else {
				log.Printf("PANIC in ThreadPool[%d] function: %v\n%s", i, panicked, debug.Stack())
			}
		}
	}()

	fn()
}
