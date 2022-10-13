/*
Copyright 2018-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package blip

import (
	"bytes"
	"fmt"
	"hash"
	"hash/crc32"
	"io"
	"sync"

	// CBG-1712: std compress/flate has some sort of issue
	// handling the compressed stream of BLIP data that this
	// library does not
	"github.com/klauspost/compress/flate"
)

// The standard trailer appended by 'deflate' when flushing its output. BLIP (like many protocols)
// suppresses this in the transmitted data. The compressor removes the last 4 bytes ofoutput,
// and the decompressor appends the trailer to its input.
const deflateTrailer = "\x00\x00\xff\xff"
const deflateTrailerLength = 4

//////// COMPRESSOR:

// The 'deflate' compression level to use when compressing messages, where 0 means no compression,
// 1 means fastest (least) compression, and 9 means best (slowest) compression. Default is 6.
var CompressionLevel = 6

// A 'deflate' compressor for BLIP messages.
type compressor struct {
	checksum hash.Hash32   // Running checksum of pre-compressed data
	dst      *bytes.Buffer // The stream compressed output is written to
	z        *flate.Writer // The 'deflate' context
	enabled  bool          // Should data be compressed?
}

func newCompressor(writer *bytes.Buffer) *compressor {
	if z, err := flate.NewWriter(writer, CompressionLevel); err != nil {
		panic(fmt.Sprintf("BLIP: flate.NewWriter failed: %v", err))
	} else {
		return &compressor{
			checksum: crc32.NewIEEE(),
			dst:      writer,
			z:        z,
			enabled:  true,
		}
	}
}

func (c *compressor) reset(writer *bytes.Buffer) {
	c.checksum = crc32.NewIEEE()
	c.dst = writer
	c.z.Reset(writer)
	c.enabled = true
}

func (c *compressor) enableCompression(enable bool) {
	c.enabled = enable
}

func (c *compressor) write(data []byte) (n int, err error) {
	if c.enabled {
		n, err = c.z.Write(data)
		if err == nil {
			c.z.Flush()
			// Remove the '00 00 FF FF' trailer from the deflated block:
			// if !bytes.HasSuffix(c.dst.Bytes(), []byte(deflateTrailer)) {
			//     panic(fmt.Sprintf("Unexpected end of compressed data: %x", c.dst.Bytes()))
			// }
			c.dst.Truncate(c.dst.Len() - deflateTrailerLength)
		}
	} else {
		n, err = c.dst.Write(data)
	}
	_, _ = c.checksum.Write(data[0:n]) // Update checksum (no error possible)
	return n, err
}

func (c *compressor) getChecksum() uint32 {
	return c.checksum.Sum32()
}

//////// DECOMPRESSOR:

// Should be larger than the max output z.Read() can return at a time
// (see comment in readAll)
const kDecompressorBufferSize = 8 * 1024

// A 'deflate' decompressor for BLIP messages.
type decompressor struct {
	logContext LogContext
	checksum   hash.Hash32   // Running checksum of pre-compressed data
	src        *bytes.Buffer // The stream compressed input is read from
	z          io.ReadCloser // The 'deflate' decompression context
	buffer     []byte        // Temporary buffer for decompressed data
	outputBuf  bytes.Buffer  // Temporary buffer used by ReadAll
}

func newDecompressor(logContext LogContext) *decompressor {
	buffer := bytes.NewBuffer(make([]byte, 0, kBigFrameSize))
	return &decompressor{
		logContext: logContext,
		checksum:   crc32.NewIEEE(),
		src:        buffer,
		z:          flate.NewReader(buffer),
		buffer:     make([]byte, kDecompressorBufferSize),
	}
}

func (d *decompressor) reset(logContext LogContext) {
	d.logContext = logContext
	d.checksum = crc32.NewIEEE()
	d.src.Reset()
	d.z.(flate.Resetter).Reset(d.src, nil)
}

func (d *decompressor) passthrough(input []byte, checksum *uint32) ([]byte, error) {
	_, _ = d.checksum.Write(input) // Update checksum (no error possible)
	if checksum != nil {
		if curChecksum := d.getChecksum(); curChecksum != *checksum {
			return nil, fmt.Errorf("Invalid checksum %x; should be %x", curChecksum, *checksum)
		}
	}
	return input, nil
}

func (d *decompressor) decompress(input []byte, checksum uint32) ([]byte, error) {
	// Decompressing (inflating) all the available input data is made difficult by Go's implemen-
	// tation, which operates on an input stream. If the Reader ever tries to read past the end of
	// available input it will get an EOF from the Buffer, which it treats as an error condition,
	// causing it to drop the input and stop working. So we have to detect when the Reader has
	// read all of the input and written it to the output, and go no further.
	// After several tries, I've settled on this approach: (1) read data from the flate reader
	// until the input is [almost] consumed; (2) compare the current checksum to the expected one
	// and stop if they match. I say "[almost]" is because the flate reader might leave a few bytes
	// of input unread even though it's produced all the output: the unread data consists of the
	// block trailer (deflateTrailer) plus a single byte before it. These 5 bytes get left behind
	// in the source stream for next time; they won't affect the next output, but the flate reader
	// has to process them or its internal bookkeeping will get thrown off and it'll fail.
	// --Jens, 1/2018

	d.src.Write(input)
	if d.src.Len() == 0 {
		// Empty input
		return []byte{}, nil
	}

	// Restore the deflate trailer that was stripped by the compressor:
	d.src.Write([]byte(deflateTrailer))

	d.outputBuf.Reset()
	// Decompress until the checksum matches and there are only a few bytes of input left:
	for d.src.Len() > deflateTrailerLength+2 || d.getChecksum() != checksum {
		n, err := d.z.Read(d.buffer)
		if err != nil {
			d.logContext.log("ERROR decompressing frame: inputLen=%d, remaining=%d, output=%d, error=%v\n",
				len(input), d.src.Len(), d.outputBuf.Len(), err)
			return nil, err
		} else if n == 0 {
			// Nothing more to read; since checksum didn't match (above), fail:
			return nil, fmt.Errorf("Invalid checksum %x; should be %x", d.getChecksum(), checksum)
		}
		_, _ = d.checksum.Write(d.buffer[0:n]) // Update checksum (no error possible)

		//fmt.Printf("Decompressed %d bytes; %d remaining\n", n, d.src.Len())
		if _, err = d.outputBuf.Write(d.buffer[:n]); err != nil {
			return nil, err
		}
	}

	result := d.outputBuf.Bytes()
	d.outputBuf.Reset()
	return result, nil
}

func (d *decompressor) getChecksum() uint32 {
	return d.checksum.Sum32()
}

//////// CODEC CACHE:

var compressorCache sync.Pool
var decompressorCache sync.Pool

// Gets a compressor from the pool, or creates a new one if the pool is empty:
func getCompressor(writer *bytes.Buffer) *compressor {
	if c, ok := compressorCache.Get().(*compressor); ok {
		c.reset(writer)
		return c
	} else {
		return newCompressor(writer)
	}
}

// Closes a compressor and returns it to the pool:
func returnCompressor(c *compressor) {
	c.z.Close()
	compressorCache.Put(c)
}

// Gets a decompressor from the pool, or creates a new one if the pool is empty:
func getDecompressor(logContext LogContext) *decompressor {
	if d, ok := decompressorCache.Get().(*decompressor); ok {
		d.reset(logContext)
		return d
	} else {
		return newDecompressor(logContext)
	}
}

// Closes a decompressor and returns it to the pool:
func returnDecompressor(d *decompressor) {
	d.z.Close()
	decompressorCache.Put(d)
}
