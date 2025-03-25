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
	"io"
	"sync"
)

// Creates a new pipe, a pair of bound streams.
// Similar to io.Pipe, except the stream is buffered so writes don't block.
func NewPipe() (*PipeReader, *PipeWriter) {
	p := &pipe_shared{
		chunks: make([][]byte, 0, 50),
		cond:   sync.NewCond(&sync.Mutex{}),
	}
	return &PipeReader{shared: p}, &PipeWriter{shared: p}
}

// The private state shared between a PipeReader and PipeWriter.
type pipe_shared struct {
	chunks [][]byte   // ordered list of unread byte-arrays written to the Pipe
	err    error      // Set when closed, to io.EOF or some other error.
	cond   *sync.Cond // Synchronizes writer & reader
}

// -------- PIPEWRITER

// The write end of a pipe. Implements io.WriteCloser.
// Unlike io.Pipe, writes do not block; instead the unread data is buffered in memory.
type PipeWriter struct {
	shared *pipe_shared
}

// Standard Writer method. Does not block.
func (w *PipeWriter) Write(chunk []byte) (n int, err error) {
	if len(chunk) == 0 {
		// The Writer interface forbids retaining the input, so we must copy it:
		copied := make([]byte, len(chunk))
		copy(copied, chunk)
		chunk = copied
	}
	if err = w._add(chunk, nil); err == nil {
		n = len(chunk)
	}
	return
}

// Closes the pipe.
// The associated PipeReader can still read any remaining data, then it will get an EOF error.
func (w *PipeWriter) Close() error {
	return w.CloseWithError(io.EOF)
}

// Closes the pipe with a custom error.
func (w *PipeWriter) CloseWithError(err error) error {
	if err == nil {
		err = io.EOF
	}
	_ = w._add(nil, err)
	return nil
}

func (w *PipeWriter) _add(chunk []byte, err error) error {
	// adds a chunk or sets an error; or if there's already an error, returns it.
	w.shared.cond.L.Lock()
	defer w.shared.cond.L.Unlock()

	if w.shared.err != nil {
		return w.shared.err
	}

	if err == nil || err == io.EOF {
		if len(chunk) > 0 {
			w.shared.chunks = append(w.shared.chunks, chunk)
		}
	} else {
		w.shared.chunks = nil // make sure reader sees the custom error ASAP
	}
	w.shared.err = err
	w.shared.cond.Signal()
	return nil
}

// The number of bytes written but not yet read
func (w *PipeWriter) bytesPending() int {
	total := 0
	w.shared.cond.L.Lock()
	for _, chunk := range w.shared.chunks {
		total += len(chunk)
	}
	w.shared.cond.L.Unlock()
	return total
}

// -------- PIPEREADER

// The read end of a pipe. Implements io.ReadCloser.
type PipeReader struct {
	shared *pipe_shared // Shared state
	chunk  []byte       // The data chunk currently being read from
}

// Standard Reader method.
func (r *PipeReader) Read(p []byte) (n int, err error) { return r._read(p, true) }

// Non-blocking read: similar to Read, but if no data is available returns (0, nil).
func (r *PipeReader) TryRead(p []byte) (n int, err error) { return r._read(p, false) }

func (r *PipeReader) _read(p []byte, wait bool) (n int, err error) {
	if len(r.chunk) == 0 {
		// Current chunk is exhausted; wait for a new one:
		r.chunk, err = r._nextChunk(wait)
	}
	if len(r.chunk) > 0 {
		// Read bytes out of the current chunk:
		n = copy(p, r.chunk)
		r.chunk = r.chunk[n:]
	}
	return
}

// Returns true if a Read call will not block, whether because there's data or EOF or an error.
func (r *PipeReader) CanRead() (ok bool) {
	if len(r.chunk) > 0 {
		ok = true
	} else {
		r.shared.cond.L.Lock()
		ok = len(r.shared.chunks) > 0 || r.shared.err != nil
		r.shared.cond.L.Unlock()
	}
	return
}

// Closes the reader. Subsequent PipeReader.Write calls will return io.ErrClosedPipe.
func (r *PipeReader) Close() error {
	return r.CloseWithError(io.ErrClosedPipe)
}

// Closes the reader with a custom error. Subsequent PipeReader.Write calls will return this error.
func (r *PipeReader) CloseWithError(err error) error {
	r.shared.cond.L.Lock()
	if r.shared.err == nil {
		r.shared.err = err
		r.shared.chunks = nil
	}
	r.shared.cond.L.Unlock()
	return nil
}

func (r *PipeReader) _nextChunk(wait bool) ([]byte, error) {
	// Returns the next chunk added by the writer, or the error if any.
	// If neither is available and `wait` is true, it blocks.
	r.shared.cond.L.Lock()
	defer r.shared.cond.L.Unlock()
	for {
		if len(r.shared.chunks) > 0 {
			// If there's a chunk in the queue, return it:
			chunk := r.shared.chunks[0]
			r.shared.chunks = r.shared.chunks[1:]
			return chunk, nil
		} else if r.shared.err != nil {
			// Else if an error is set, return that:
			return nil, r.shared.err
		} else if !wait {
			// Non-blocking call with nothing to read:
			return nil, nil
		}
		// None of the above -- wait for something to happen:
		r.shared.cond.Wait()
	}
}
