package main

import (
	"fmt"
	"os"

	"github.com/loopholelabs/silo/pkg/storage"
	"github.com/loopholelabs/silo/pkg/storage/modules"
	"github.com/loopholelabs/silo/pkg/storage/sources"
	"github.com/spf13/cobra"
)

var (
	cmdCreateBinlog = &cobra.Command{
		Use:   "createbinlog",
		Short: "Create a binlog",
		Long:  ``,
		Run:   runCreateBinlog,
	}
)

var createBinlogInput string
var createBinlogOutput string
var createBinlogBlocksize int

func init() {
	rootCmd.AddCommand(cmdCreateBinlog)
	cmdCreateBinlog.Flags().StringVarP(&createBinlogInput, "in", "i", "memory.bin", "Input device file")
	cmdCreateBinlog.Flags().StringVarP(&createBinlogOutput, "out", "o", "binlog.out", "Binlog output")
	cmdCreateBinlog.Flags().IntVarP(&createBinlogBlocksize, "bs", "b", 1024*1024, "Block size")
}

func runCreateBinlog(_ *cobra.Command, _ []string) {

	stat, err := os.Stat(createBinlogInput)
	if err != nil {
		panic(err)
	}

	size := stat.Size()

	source, err := sources.NewFileStorage(createBinlogInput, size)
	if err != nil {
		panic(err)
	}

	dest, err := sources.NewFileStorageCreate(fmt.Sprintf("%s%s", createBinlogInput, ".output"), size)
	if err != nil {
		panic(err)
	}
	destBinlog, err := modules.NewBinLog(dest, createBinlogOutput)
	if err != nil {
		panic(err)
	}

	err = modules.CreateBinlogFromDevice(source, destBinlog, createBinlogBlocksize)
	if err != nil {
		panic(err)
	}

	// Make *sure* the two devices are equal
	eq, err := storage.Equals(source, destBinlog, createBinlogBlocksize)
	if err != nil {
		panic(err)
	}

	if !eq {
		panic("devices are not equal")
	}

	err = destBinlog.Close()
	if err != nil {
		panic(err)
	}

}
