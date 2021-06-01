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
	"math/rand"
	"testing"

	assert "github.com/couchbaselabs/go.assert"
)

var randomData []byte
var rando *rand.Rand

func init() {
	CompressionLevel = 4
	rando = rand.New(rand.NewSource(57439))

	randomData = make([]byte, 65536)
	var b byte
	var step byte = 1
	for i, _ := range randomData {
		if rando.Intn(10) == 0 {
			b = byte(rando.Intn(256))
			step = byte(rando.Intn(4))
		}
		randomData[i] = byte(b & 0xFF)
		b += step
	}
}

func TestCompressDecompressEmpty(t *testing.T) {
	testCompressDecompress(t, []byte{})
}

func TestCompressDecompressSmallText(t *testing.T) {
	testCompressDecompress(t, []byte("hello"))
}

func TestCompressDecompressManySizes(t *testing.T) {
	for s := 0; s < 65536; s += 1024 {
		startSize := s
		endSize := s + 1024
		t.Run(fmt.Sprintf("%d-%d", startSize, endSize-1), func(t *testing.T) {
			t.Parallel()
			for size := startSize; size < endSize; size++ {
				//t.Logf("Compressing %d bytes", size)
				testCompressDecompress(t, compressibleDataOfLength(size))
			}
		})
	}
}

// Make sure that the decompressor returns an error with completely invalid input
func TestDecompressInvalidInput(t *testing.T) {
	ctx := TestLogContext{silent: true}
	decompressor := getDecompressor(&ctx)
	decompressedBytes, err := decompressor.decompress([]byte("junk_input"), 2)
	assert.True(t, err != nil)
	assert.True(t, len(decompressedBytes) == 0)
	assert.True(t, ctx.count > 0)

}

// Make sure that the decompressor returns an error with valid compressed input, but an invalid checksum
func TestDecompressInvalidChecksum(t *testing.T) {
	ctx := TestLogContext{silent: true}

	// Compress some data
	compressedData, checksum := testCompressData(t, []byte("uncompressed"))

	decompressor := getDecompressor(&ctx)
	decompressedBytes, err := decompressor.decompress([]byte(compressedData), checksum*2)
	assert.True(t, err != nil)
	assert.True(t, len(decompressedBytes) == 0)
	assert.True(t, ctx.count > 0)
}

func testCompressData(t *testing.T, dataToCompress []byte) (compressedData []byte, checksum uint32) {

	// Compress some data
	compressedDest := bytes.Buffer{}
	compressor := getCompressor(&compressedDest)
	compressor.enabled = true
	n, err := compressor.write([]byte(dataToCompress))
	if err != nil {
		t.Errorf("Error compressing <%x> of size %d.  Error: %v", dataToCompress, len(dataToCompress), err)
	}
	assert.Equals(t, n, len(dataToCompress))
	compressedData = compressedDest.Bytes()
	checksum = compressor.getChecksum()
	returnCompressor(compressor)

	return compressedData, checksum
}

func testCompressDecompress(t *testing.T, dataToCompress []byte) {
	// Compress some data
	compressedData, checksum := testCompressData(t, dataToCompress)

	// fmt.Printf("Compressed %4d bytes to %4d (%.3f)\n", len(dataToCompress), len(compressedData),
	//     float32(len(compressedData))/float32(len(dataToCompress)))

	// Decompress it
	decompressor := getDecompressor(&TestLogContext{})
	decompressedBytes, err := decompressor.decompress(compressedData, checksum)
	returnDecompressor(decompressor)
	if err != nil {
		t.Errorf("Error decompressing <%x> of size %d.  Error: %v", compressedData, len(compressedData), err)
	}

	// Make sure that it decompresses to the same data
	assert.Equals(t, bytes.Compare(decompressedBytes, dataToCompress), 0)
}

func compressibleDataOfLength(lengthToCompress int) []byte {
	return randomData[0:lengthToCompress]
}
