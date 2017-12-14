package blip

import (
	"bytes"
	"compress/flate"
	"fmt"
	"hash"
	"hash/crc32"
	"io"
	"sync"
)

//////// COMPRESSOR:

// The 'deflate' compression level to use when compressing messages, where 0 means no compression,
// 1 means fastest (least) compression, and 9 means best (slowest) compression. Default is 6.
var CompressionLevel = 6

// A 'deflate' compression context for BLIP messages.
type compressor struct {
	checksum hash.Hash32   // Running checksum of pre-compressed data
	dst      io.Writer     // The stream compressed output is written to
	z        *flate.Writer // The 'deflate' context
	enabled  bool          // Should data be compressed?
}

func newCompressor(writer io.Writer) *compressor {
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

func (c *compressor) reset(writer io.Writer) {
	c.checksum = crc32.NewIEEE()
	c.z.Reset(writer)
	c.enabled = true
}

func (c *compressor) enableCompression(enable bool) {
	c.enabled = enable
}

func (c *compressor) write(data []byte) (n int, err error) {
	if c.enabled {
		n, err = c.z.Write(data)
		c.z.Flush()
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
const kDecompressorBufferSize = 99999

// A 'deflate' decompression context for BLIP messages.
type decompressor struct {
	checksum  hash.Hash32   // Running checksum of pre-compressed data
	src       *bytes.Buffer // The stream compressed input is read from
	z         io.ReadCloser // The 'deflate' decompression context
	enabled   bool          // Should data be decompressed?
	buffer    []byte        // Temporary buffer for decompressed data
	outputBuf bytes.Buffer  // Temporary buffer used by ReadAll
}

func newDecompressor(reader *bytes.Buffer) *decompressor {
	return &decompressor{
		checksum: crc32.NewIEEE(),
		src:      reader,
		z:        flate.NewReader(reader),
		buffer:   make([]byte, kDecompressorBufferSize),
		enabled:  true,
	}
}

func (d *decompressor) reset(reader *bytes.Buffer) {
	d.checksum = crc32.NewIEEE()
	d.src = reader
	d.z.(flate.Resetter).Reset(reader, nil)
	d.enabled = true
}

func (d *decompressor) enableCompression(enable bool) {
	d.enabled = enable
}

func (d *decompressor) read(dst []byte) (n int, err error) {
	if d.enabled {
		n, err = d.z.Read(dst)
	} else {
		n, err = d.src.Read(dst)
	}
	_, _ = d.checksum.Write(dst[0:n]) // Update checksum (no error possible)
	return n, err
}

func (d *decompressor) readFull() (n int64, result []byte,  err error) {
	resultBuffer := bytes.Buffer{}

	if d.enabled {
		n, err = io.Copy(&resultBuffer, d.z)
		_ = d.z.Close()
	} else {
		n, err = io.Copy(&resultBuffer, d.src)
		_ = d.z.Close()
	}
	result = resultBuffer.Bytes()
	_, _ = d.checksum.Write(result[0:n]) // Update checksum (no error possible)
	return n, result, err
}

func (d *decompressor) readAll() ([]byte, error) {

	inputLen := d.src.Len()
	if !d.enabled {
		all := make([]byte, inputLen)
		n, err := d.read(all)
		return all[:n], err
	}
	d.outputBuf.Reset()

	// sketch
	_, readFullResult, err := d.readFull()  // only works if exact size.  if smaller, miss data.  bigger, hit EOF
	if err != nil {
		return nil, err
	}

	if _, err = d.outputBuf.Write(readFullResult); err != nil {
		return nil, err
	}

	//

	//for {
	//
	//
	//	n, err := d.read(d.buffer[:])
	//	log.Printf("d.read() returned %d bytes with err: %v", n, err)
	//	if err != nil {
	//		return nil, err
	//	} else if n == 0 {
	//		break
	//	}
	//	if _, err = d.outputBuf.Write(d.buffer[:n]); err != nil {
	//		return nil, err
	//	}
	//	// Keep going as long as we get a full buffer of output, or there's input left to decompress
	//	if n < len(d.buffer) && d.src.Len() == 0 {
	//		break
	//	}
	//}

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
func getCompressor(writer io.Writer) *compressor {
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
func getDecompressor(reader *bytes.Buffer) *decompressor {
	if d, ok := decompressorCache.Get().(*decompressor); ok {
		d.reset(reader)
		return d
	} else {
		return newDecompressor(reader)
	}
}

// Closes a decompressor and returns it to the pool:
func returnDecompressor(d *decompressor) {
	d.z.Close()
	decompressorCache.Put(d)
}

//  Copyright (c) 2013 Jens Alfke. Copyright (c) 2015-2017 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.
