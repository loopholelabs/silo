package main

import (
	"fmt"
	"net"

	"github.com/loopholelabs/silo/pkg/storage/expose/criu"
	"github.com/spf13/cobra"
)

var PAGE_SIZE = uint32(4096)

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
	cmdPages.Flags().StringVarP(&pages_serve_addr, "addr", "a", "localhost:5170", "Address to serve from")
}

func runPages(ccmd *cobra.Command, args []string) {
	fmt.Printf("Running page server proxy listening on %s\n", pages_serve_addr)

	listener, err := net.Listen("tcp", pages_serve_addr)
	if err != nil {
		panic(err)
	}

	for {
		con, err := listener.Accept()
		if err != nil {
			panic(err)
		}

		ps := criu.NewPageServer()
		ps.AddPageData = func(iov *criu.PageServerIOV, buffer []byte) {
			fmt.Printf("AddPageData %s %d\n", iov.String(), len(buffer))
		}
		ps.GetPageData = func(iov *criu.PageServerIOV, buffer []byte) {
			fmt.Printf("GetPageData %s %d\n", iov.String(), len(buffer))
		}
		ps.Open2 = func(iov *criu.PageServerIOV) bool {
			fmt.Printf("Open2 %s\n", iov.String())
			return false
		}
		ps.Parent = func(iov *criu.PageServerIOV) bool {
			fmt.Printf("Parent %s\n", iov.String())
			return false
		}
		ps.Close = func(iov *criu.PageServerIOV) {
			fmt.Printf("Close %s\n", iov.String())
		}

		go ps.Handle(con)
	}
}
