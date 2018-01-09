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
	compressor := getCompressor(&compressedDest)
	compressor.enabled = true
	n, err := compressor.write([]byte(dataToCompress))
	assert.Equals(t, n, len(dataToCompress))
	assert.True(t, err == nil)
	compressedData := compressedDest.Bytes()
	checksum := compressor.getChecksum()
	returnCompressor(compressor)

	// Decompress it
	decompressor := getDecompressor()
	decompressedBytes, err := decompressor.decompress(compressedData, true, &checksum)
	returnDecompressor(decompressor)
	assert.True(t, err == nil)

	// Make sure that it decompresses to the same data
	assert.Equals(t, bytes.Compare(decompressedBytes, dataToCompress), 0)
}

func superCompressibleDataOfLength(lengthToCompress int) []byte {
	dataToCompress := make([]byte, lengthToCompress)
	for i, _ := range dataToCompress {
		dataToCompress[i] = byte(i & 0xFF)
	}
	return dataToCompress
}

func TestCompressDecompress(t *testing.T) {
	testCompressDecompress(t, []byte("hello"))
}

func TestCompressDecompressManySizes(t *testing.T) {
	for size := 1; size <= 65535; size += 1 {
		t.Run(fmt.Sprintf("%d", size), func(t *testing.T) {
			testCompressDecompress(t, superCompressibleDataOfLength(size))
		})
	}
}
