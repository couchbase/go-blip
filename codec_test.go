package blip

import (
	"bytes"
	"fmt"
	"math/rand"
	"testing"

	assert "github.com/couchbaselabs/go.assert"
)

var randomData []byte

func init() {
	CompressionLevel = 4
	rando := rand.New(rand.NewSource(57439))

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

	// fmt.Printf("Compressed %4d bytes to %4d (%.3f)\n", len(dataToCompress), len(compressedData),
	//     float32(len(compressedData))/float32(len(dataToCompress)))

	// Decompress it
	decompressor := getDecompressor()
	decompressedBytes, err := decompressor.decompress(compressedData, checksum)
	returnDecompressor(decompressor)
	assert.True(t, err == nil)

	// Make sure that it decompresses to the same data
	assert.Equals(t, bytes.Compare(decompressedBytes, dataToCompress), 0)
}

func compressibleDataOfLength(lengthToCompress int) []byte {
	return randomData[0:lengthToCompress]
}

func TestCompressDecompress(t *testing.T) {
	testCompressDecompress(t, []byte("hello"))
}

func TestCompressDecompressManySizes(t *testing.T) {
	for size := 1; size <= 65535; size += 1 {
		t.Run(fmt.Sprintf("%d", size), func(t *testing.T) {
			t.Parallel()
			testCompressDecompress(t, compressibleDataOfLength(size))
		})
	}
}
