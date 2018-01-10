package blip

import (
	"bytes"
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

func TestCompressDecompress(t *testing.T) {
	testCompressDecompress(t, []byte("hello"))
}

func TestCompressDecompressManySizes(t *testing.T) {

	// Pick two boundary value sizes to test with
	sizesToTest := []int{1, 65535}

	// And 1000 random sizes to test with
	for i := 0; i < 1000; i++ {
		randomSize := rando.Intn(65535)
		if randomSize > 1 {
			sizesToTest = append(sizesToTest, randomSize)
		}
	}

	// Test compress/decompress loop with all of those sizes
	for _, sizeToTest := range sizesToTest {
		testCompressDecompress(t, compressibleDataOfLength(sizeToTest))
	}

}

// Make sure that the decompressor returns an error with completely invalid input
func TestDecompressInvalidInput(t *testing.T) {

	decompressor := getDecompressor(TestLogContext{})
	decompressedBytes, err := decompressor.decompress([]byte("junk_input"), 2)
	assert.True(t, err != nil)
	assert.True(t, len(decompressedBytes) == 0)

}

// Make sure that the decompressor returns an error with valid compressed input, but an invalid checksum
func TestDecompressInvalidChecksum(t *testing.T) {

	// Compress some data
	compressedData, checksum := testCompressData(t, []byte("uncompressed"))

	decompressor := getDecompressor(TestLogContext{})
	decompressedBytes, err := decompressor.decompress([]byte(compressedData), checksum*2)
	assert.True(t, err != nil)
	assert.True(t, len(decompressedBytes) == 0)

}

func testCompressData(t *testing.T, dataToCompress []byte) (compressedData []byte, checksum uint32) {

	// Compress some data
	compressedDest := bytes.Buffer{}
	compressor := getCompressor(&compressedDest)
	compressor.enabled = true
	n, err := compressor.write([]byte(dataToCompress))
	assert.Equals(t, n, len(dataToCompress))
	assert.True(t, err == nil)
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
	decompressor := getDecompressor(TestLogContext{})
	decompressedBytes, err := decompressor.decompress(compressedData, checksum)
	returnDecompressor(decompressor)
	if err != nil {
		t.Errorf("Compression error trying to compress %s of size: %d.  Error: %v", string(dataToCompress), len(dataToCompress), err)
	}

	// Make sure that it decompresses to the same data
	assert.Equals(t, bytes.Compare(decompressedBytes, dataToCompress), 0)
}

func compressibleDataOfLength(lengthToCompress int) []byte {
	return randomData[0:lengthToCompress]
}
