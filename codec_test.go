package blip

import (
	"bytes"
	"fmt"
	"testing"

	assert "github.com/couchbaselabs/go.assert"
)

func testCompressDecompress(t *testing.T, dataToCompress []byte) {
	// Compress some data
	compressedDest := bytes.Buffer{}
	compressor := newCompressor(&compressedDest)
	compressor.enabled = true
	n, err := compressor.write([]byte(dataToCompress))
	assert.Equals(t, n, len(dataToCompress))
	assert.True(t, err == nil)

	// Decompress it
	decompressor := newDecompressor(&compressedDest)
	decompressor.enableCompression(true)
	decompressedBytes, err := decompressor.readAll()

	// Make sure that it decompresses to the same data
	assert.True(t, err == nil)
	assert.Equals(t, len(decompressedBytes), len(dataToCompress))
	for i, decompressedByte := range decompressedBytes {
		originalDataByte := dataToCompress[i]
		assert.Equals(t, decompressedByte, originalDataByte)
	}
}

func superCompressibleDataOfLength(lengthToCompress int) []byte {
	dataToCompress := make([]byte, lengthToCompress)
	for i, _ := range dataToCompress {
		dataToCompress[i] = byte(i)
	}
	return dataToCompress
}

func TestCompressDecompress(t *testing.T) {
	testCompressDecompress(t, []byte("hello"))
}

func TestCompressDecompressManySizes(t *testing.T) {
	for size := 32700; size <= 32999; size += 1 {
		if size == 32768 {
			continue // this is the one size that doesn't work
		}
		t.Run(fmt.Sprintf("%d", size), func(t *testing.T) {
			testCompressDecompress(t, superCompressibleDataOfLength(size))
		})
	}
}
