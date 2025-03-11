package main

import (
	"fmt"
	"os"

	"github.com/loopholelabs/silo/pkg/storage/sources"
	"github.com/spf13/cobra"
)

var (
	cmdCheckOverlay = &cobra.Command{
		Use:   "checkoverlay",
		Short: "Check an overlay",
		Long:  ``,
		Run:   runCheckOverlay,
	}
)

var checkOverlayBase string
var checkOverlayOverlay string
var checkOverlayBlockSize int

func init() {
	rootCmd.AddCommand(cmdCheckOverlay)
	cmdCheckOverlay.Flags().StringVarP(&checkOverlayBase, "base", "b", "base", "Filename of base")
	cmdCheckOverlay.Flags().StringVarP(&checkOverlayOverlay, "overlay", "o", "overlay", "Filename of overlay")
	cmdCheckOverlay.Flags().IntVarP(&checkOverlayBlockSize, "blocksize", "B", 1024*1024, "Block size")

}

func runCheckOverlay(_ *cobra.Command, _ []string) {
	stat, err := os.Stat(checkOverlayBase)
	if err != nil {
		panic(err)
	}
	sizeBase := stat.Size()
	baseProv, err := sources.NewFileStorage(checkOverlayBase, sizeBase)
	if err != nil {
		panic(err)
	}

	overlayProv, err := sources.NewFileStorageSparse(checkOverlayOverlay, uint64(sizeBase), checkOverlayBlockSize)
	if err != nil {
		panic(err)
	}

	totalOverlay := 0
	totalOverlayEqual := 0
	totalBlocks := 0

	for offset := 0; offset < int(sizeBase); offset += checkOverlayBlockSize {
		block := offset / checkOverlayBlockSize
		// Now check the data...
		length := checkOverlayBlockSize
		if offset+length > int(sizeBase) {
			length = int(sizeBase) - offset
		}

		// Now try to read the block...
		bufferOverlay := make([]byte, length)
		bufferBase := make([]byte, length)

		_, overlayErr := overlayProv.ReadAt(bufferOverlay, int64(offset))

		_, err := baseProv.ReadAt(bufferBase, int64(offset))
		if err != nil {
			panic(err)
		}

		if overlayErr == nil {
			dataEqual := true
			for i := 0; i < length; i++ {
				if bufferOverlay[i] != bufferBase[i] {
					dataEqual = false
					break
				}
			}
			if dataEqual {
				fmt.Printf("Block %d overlay_equal\n", block)
				totalOverlayEqual++
			} else {
				fmt.Printf("Block %d overlay\n", block)
				totalOverlay++
			}
		} else {
			fmt.Printf("Block %d no_overlay\n ", block)
		}
		totalBlocks++
	}

	fmt.Printf("TOTALS %d blocks, %d overlay, %d overlay_equal\n", totalBlocks, totalOverlay, totalOverlayEqual)
}
