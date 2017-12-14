package blip

import (
	"bytes"
	"testing"

	assert "github.com/couchbaselabs/go.assert"
	"log"
)

func TestCompressDecompress(t *testing.T) {

	dataToCompress := "hello"

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

func TestCompressDecompressExploitKnownIssue(t *testing.T) {

	dataToCompress := make([]byte, 32768)
	for i, _ := range dataToCompress {
		dataToCompress[i] = byte(i)
	}

	// Compress some data
	compressedDest := bytes.Buffer{}
	compressor := newCompressor(&compressedDest)
	compressor.enabled = true
	n, err := compressor.write(dataToCompress)
	assert.Equals(t, n, len(dataToCompress))
	assert.True(t, err == nil)

	// Decompress it
	decompressor := newDecompressor(&compressedDest)
	decompressor.enableCompression(true)
	decompressedBytes, err := decompressor.readAll()

	// Make sure that it decompresses to the same data
	if err != nil {
		log.Printf("decompressor.readAll() error: %v", err)
	}
	assert.True(t, err == nil)
	assert.Equals(t, len(decompressedBytes), len(dataToCompress))
	for i, decompressedByte := range decompressedBytes {
		originalDataByte := dataToCompress[i]
		assert.Equals(t, decompressedByte, originalDataByte)
	}


}