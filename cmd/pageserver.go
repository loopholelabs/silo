package main

import (
	"fmt"
	"net"

	"github.com/loopholelabs/silo/pkg/storage/expose/criu"
	"github.com/loopholelabs/silo/pkg/storage/modules"
	"github.com/loopholelabs/silo/pkg/storage/sources"
	"github.com/spf13/cobra"
)

var PAGE_SIZE = uint32(4096)

var pages_serve_addr string

var (
	cmdPages = &cobra.Command{
		Use:   "pages",
		Short: "Run page server",
		Long:  `Run page server`,
		Run:   runPages,
	}
)

func init() {
	rootCmd.AddCommand(cmdPages)
	cmdPages.Flags().StringVarP(&pages_serve_addr, "addr", "a", ":5170", "Address to serve from")
}

var page_data = make(map[uint64]*modules.MappedStorage)

func runPages(ccmd *cobra.Command, args []string) {
	fmt.Printf("Running page server listening on %s\n", pages_serve_addr)

	listener, err := net.Listen("tcp", pages_serve_addr)
	if err != nil {
		panic(err)
	}

	for {
		con, err := listener.Accept()
		if err != nil {
			panic(err)
		}

		fmt.Printf("Connection from %s\n", con.RemoteAddr().String())

		ps := criu.NewPageServer()

		ps.AddPageData = func(iov *criu.PageServerIOV, buffer []byte) {
			fmt.Printf("AddPageData %s %d\n", iov.String(), len(buffer))
			maps, ok := page_data[iov.DstID()]
			if !ok {
				// Create a new one
				store := sources.NewMemoryStorage(1 * 1024 * 1024 * 1024)
				m := modules.NewMappedStorage(store, int(criu.PAGE_SIZE))
				maps = m
				page_data[iov.DstID()] = maps
			}

			maps.WriteBlocks(iov.Vaddr, buffer)
			fmt.Printf(" Size - %d\n", maps.Size())
		}

		ps.GetPageData = func(iov *criu.PageServerIOV, buffer []byte) {
			fmt.Printf("GetPageData %s %d\n", iov.String(), len(buffer))
			maps, ok := page_data[iov.DstID()]
			if ok {
				maps.ReadBlocks(iov.Vaddr, buffer)
			}
		}

		ps.Open2 = func(iov *criu.PageServerIOV) bool {
			fmt.Printf("Open2 %s\n", iov.String())
			return false
		}

		ps.Parent = func(iov *criu.PageServerIOV) bool {
			_, exists := page_data[iov.DstID()]
			fmt.Printf("Parent %s %t\n", iov.String(), exists)
			return exists
		}

		ps.Close = func(iov *criu.PageServerIOV) {
			fmt.Printf("Close %s\n", iov.String())
		}

		go ps.Handle(con)
	}
}
