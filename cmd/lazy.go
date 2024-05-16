package main

import (
	"fmt"
	"os"

	"github.com/loopholelabs/silo/pkg/storage/expose/criu"
	"github.com/spf13/cobra"
)

var (
	cmdLazy = &cobra.Command{
		Use:   "lazy",
		Short: "Run lazy pages",
		Long:  `Run lazy pages`,
		Run:   runLazy,
	}
)

func init() {
	rootCmd.AddCommand(cmdLazy)
}

func runLazy(ccmd *cobra.Command, args []string) {
	fmt.Printf("Running lazy page server\n")

	// Create a socket by which the restore process will communicate with us...

	/*
	   Fault for 484544 address 770c5b64c000
	   Fault for 484158 address 705949a88000
	   Fault for 484544 address 7ffcb487c000
	   Fault for 484544 address 770c5b64b000
	*/

	var uf *criu.UserFaultHandler
	var err error

	fault := func(pid uint32, addr uint64, pending []uint64) error {
		fmt.Printf("Fault for %d address %x\n", pid, addr)
		data := make([]byte, os.Getpagesize())
		uf.WriteData(uint64(pid), addr, data)
		return nil
	}

	uf, err = criu.NewUserFaultHandler("lazy-pages.socket", fault)
	if err != nil {
		panic(err)
	}

	err = uf.Handle()
	if err != nil {
		panic(err)
	}

}
