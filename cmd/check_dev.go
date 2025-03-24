package main

import (
	"crypto/sha256"
	"os"

	"github.com/loopholelabs/silo/pkg/storage/sources"
	"github.com/loopholelabs/silo/pkg/storage/util"
	"github.com/spf13/cobra"
)

var (
	cmdCheckDev = &cobra.Command{
		Use:   "checkdev",
		Short: "Check device file",
		Long:  ``,
		Run:   runCheckDev,
	}
)

var checkDevFile string
var checkDevOutFile string
var checkDevBlockSize int

func init() {
	rootCmd.AddCommand(cmdCheckDev)
	cmdCheckDev.Flags().StringVarP(&checkDevFile, "input", "i", "memory.bin", "Input file")
	cmdCheckDev.Flags().IntVarP(&checkDevBlockSize, "blocksize", "b", 1024*1024, "Block size")
	cmdCheckDev.Flags().StringVarP(&checkDevOutFile, "output", "o", "memory.bin.out", "Output file")
}

func runCheckDev(_ *cobra.Command, _ []string) {
	stat, err := os.Stat(checkDevFile)
	if err != nil {
		panic(err)
	}
	source, err := sources.NewFileStorage(checkDevFile, stat.Size())
	if err != nil {
		panic(err)
	}

	// Now go through looking at the data...
	buffer := make([]byte, checkDevBlockSize)
	b := 0
	numBlocks := (source.Size() + uint64(checkDevBlockSize) - 1) / uint64(checkDevBlockSize)
	bitmap := util.NewBitfield(int(numBlocks))

	hashData := make([]byte, 0)

	zerohash := make([]byte, sha256.Size)

	for offset := uint64(0); offset < source.Size(); offset += uint64(checkDevBlockSize) {
		n, err := source.ReadAt(buffer, int64(offset))
		if err != nil {
			panic(err)
		}
		// Check if it's all zeroes
		allZeroes := true
		for f := 0; f < n; f++ {
			if buffer[f] != 0 {
				allZeroes = false
				break
			}
		}

		// Generate a hash
		hash := sha256.Sum256(buffer[:n])

		if allZeroes && n == checkDevBlockSize {
			hashData = append(hashData, zerohash...)
		} else {
			hashData = append(hashData, hash[:]...)
		}

		// Add it to the map...
		if !allZeroes {
			bitmap.SetBit(b)
		}
		b++
	}

	// Write the hashes out
	err = os.WriteFile(checkDevOutFile, hashData, 0666)
	if err != nil {
		panic(err)
	}
}
