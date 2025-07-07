package packets

import (
	"crypto/rand"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func getSampleData() []byte {
	buff := make([]byte, 256*1024)
	rand.Read(buff)

	// Clear a couple of areas
	for n := 0; n < 8192; n++ {
		buff[1024+n] = 0
		buff[10000+n] = 0
		buff[57000+n] = 0
	}

	return buff
}

func benchmarkWriteAtCompGeneral(compressionType byte, mb *testing.B) {
	buff := getSampleData()

	mb.ReportAllocs()
	mb.SetBytes(int64(len(buff)))
	mb.ResetTimer()

	originalLength := 0
	compressedLength := 0

	for i := 0; i < mb.N; i++ {
		d, err := EncodeWriteAtComp(compressionType, 0, buff)
		assert.NoError(mb, err)
		compressedLength += len(d)
		originalLength += len(buff)
	}

	ratio := float64(compressedLength) * 100 / float64(originalLength)

	fmt.Printf("Compress %.2f%% Original %d bytes, compressed %d bytes\n", ratio, originalLength, compressedLength)
}

func benchmarkWriteAtDecGeneral(compressionType byte, mb *testing.B) {
	buff := getSampleData()

	encoded := make([][]byte, mb.N)

	for i := 0; i < mb.N; i++ {
		cbuff, err := EncodeWriteAtComp(compressionType, 0, buff)
		assert.NoError(mb, err)
		encoded[i] = cbuff
	}

	mb.ReportAllocs()
	mb.SetBytes(int64(len(buff)))
	mb.ResetTimer()

	for _, cbuff := range encoded {
		_, _, err := DecodeWriteAtComp(cbuff)
		assert.NoError(mb, err)
	}
}

func BenchmarkWriteAtCompRLE(mb *testing.B) {
	benchmarkWriteAtCompGeneral(WriteAtCompRLE, mb)
}

func BenchmarkWriteAtCompGzip(mb *testing.B) {
	benchmarkWriteAtCompGeneral(WriteAtCompGzip, mb)
}

func BenchmarkWriteAtCompZeroes(mb *testing.B) {
	benchmarkWriteAtCompGeneral(WriteAtCompZeroes, mb)
}

func BenchmarkWriteAtDecRLE(mb *testing.B) {
	benchmarkWriteAtDecGeneral(WriteAtCompRLE, mb)
}

func BenchmarkWriteAtDecGzip(mb *testing.B) {
	benchmarkWriteAtDecGeneral(WriteAtCompGzip, mb)
}
func BenchmarkWriteAtDecZeroes(mb *testing.B) {
	benchmarkWriteAtDecGeneral(WriteAtCompZeroes, mb)
}
