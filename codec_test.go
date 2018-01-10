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
	for baseSize := 0; baseSize < 65536; baseSize += 256 {
		t.Run(fmt.Sprintf("%d-%d", baseSize, baseSize+255), func(t *testing.T) {
			for sizeLoop := baseSize; sizeLoop < baseSize+256; sizeLoop++ {
				size := sizeLoop // make a copy that doesn't change, to use inside the function
				t.Run(fmt.Sprintf("%d", size), func(t *testing.T) {
					t.Parallel()
					testCompressDecompress(t, compressibleDataOfLength(size))
				})
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
