package blip

import (
	"bytes"
	"testing"

	"compress/zlib"
	"io"
	"log"

	assert "github.com/couchbaselabs/go.assert"
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
	compressedDestBytes := compressedDest.Bytes()
	log.Printf("compressedDestBytes: %v", compressedDestBytes)
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

func TestZlib(t *testing.T) {

	buff := []byte{120, 156, 202, 72, 205, 201, 201, 215, 81, 40, 207,
		47, 202, 73, 225, 2, 4, 0, 0, 255, 255, 33, 231, 4, 147}
	b := bytes.NewReader(buff)

	r, err := zlib.NewReader(b)
	if err != nil {
		panic(err)
	}

	var result bytes.Buffer
	io.Copy(&result, r)
	log.Printf("result: %s", result.Bytes())

	r.Close()

}




func TestDecompressor(t *testing.T) {

	dataToCompress := "hello"

	// buff := []byte{202, 72, 205, 201, 201, 0, 0, 255, 255}

	buff := []byte{120, 156, 202, 72, 205, 201, 201, 215, 81, 40, 207,
		47, 202, 73, 225, 2, 4, 0, 0, 255, 255}

	compressedDest := bytes.NewBuffer(buff)

	// Decompress it
	compressedDestBytes := compressedDest.Bytes()
	log.Printf("compressedDestBytes: %v", compressedDestBytes)
	decompressor := newDecompressor(compressedDest)
	decompressor.enableCompression(true)
	decompressedBytes, err := decompressor.readAll()

	// Make sure that it decompresses to the same data
	if err != nil {
		log.Printf("err: %v", err)
	}
	assert.True(t, err == nil)
	assert.Equals(t, len(decompressedBytes), len(dataToCompress))
	for i, decompressedByte := range decompressedBytes {
		originalDataByte := dataToCompress[i]
		assert.Equals(t, decompressedByte, originalDataByte)
	}

}

func TestCompressDecompressExploitKnownIssue(t *testing.T) {

	//maxBytesToCompress := kDecompressorBufferSize * 2
	//for i := 0; i <= maxBytesToCompress; i++ {
	//
	//}

	dataToCompress := make([]byte, 32769)
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
