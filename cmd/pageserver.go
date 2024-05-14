package main

import (
	"fmt"
	"net"
	"os"

	"github.com/loopholelabs/silo/pkg/storage/expose/criu"
	"github.com/loopholelabs/silo/pkg/storage/modules"
	"github.com/loopholelabs/silo/pkg/storage/sources"
	"github.com/spf13/cobra"
)

var PAGE_SIZE = uint32(4096)

var pages_serve_addr string
var pages_import_map string
var pages_import_pages string
var pages_import_id int

var pages_export_map string
var pages_export_pages string

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
	cmdPages.Flags().StringVarP(&pages_import_map, "importmap", "m", "", "Import map")
	cmdPages.Flags().StringVarP(&pages_import_pages, "importpages", "p", "", "Import pages")
	cmdPages.Flags().IntVarP(&pages_import_id, "importid", "i", 0, "Import pid")

	cmdPages.Flags().StringVarP(&pages_export_map, "exportmap", "e", "", "Export map")
	cmdPages.Flags().StringVarP(&pages_export_pages, "exportpages", "f", "", "Export pages")
}

var page_data = make(map[uint64]*modules.MappedStorage)
var page_flags = make(map[uint64]map[uint64]uint32)

func runPages(ccmd *cobra.Command, args []string) {
	fmt.Printf("Running page server listening on %s\n", pages_serve_addr)

	// Handle userfaults
	init := func(pid uint32, provide criu.ProvideData) error {
		fmt.Printf("Init %d\n", pid)
		return nil
	}

	fault := func(pid uint32, addr uint64, provide criu.ProvideData) error {
		fmt.Printf("Fault for %d address %x\n", pid, addr)
		// Look it up in page_data...
		maps, ok := page_data[uint64(pid)]
		if !ok {
			panic("We do not have such a page for that pid!")
		}
		data := make([]byte, os.Getpagesize())
		err := maps.ReadBlock(addr, data)
		if err == modules.Err_not_found {
			fmt.Printf("WARN: No data found for this block!\n")
		} else if err != nil {
			panic(err)
		}

		provide(addr, data)
		return nil
	}

	uf, err := criu.NewUserFaultHandler("lazy-pages.socket", init, fault)
	if err != nil {
		panic(err)
	}

	// Handle any userfault stuff...
	go func() {
		err = uf.Handle()
		if err != nil {
			panic(err)
		}
	}()

	if pages_import_map != "" {
		// Import some existing data
		cb := func(vaddr uint64, data []byte, flags uint32) {
			fmt.Printf("Page data %x - %d Flags %d\n", vaddr, len(data), flags)
			// Send it to the thing
			maps, ok := page_data[uint64(pages_import_id)]
			if !ok {
				// Create a new one
				store := sources.NewMemoryStorage(1 * 1024 * 1024 * 1024)
				m := modules.NewMappedStorage(store, int(criu.PAGE_SIZE))
				maps = m
				page_data[uint64(pages_import_id)] = maps
				page_flags[uint64(pages_import_id)] = make(map[uint64]uint32)
			}

			// Store the page flags...
			for ptr := 0; ptr < len(data); ptr += int(criu.PAGE_SIZE) {
				page_flags[uint64(pages_import_id)][vaddr+uint64(ptr)] = flags
			}

			maps.WriteBlocks(vaddr, data)
		}
		err := criu.Import_image(pages_import_map, pages_import_pages, cb)
		if err != nil {
			panic(err)
		}
	}

	// Export it
	if pages_export_map != "" {
		// Need to know the PID here...
		maps, ok := page_data[uint64(pages_import_id)]
		if ok {
			m := maps.GetMap()
			pages := make(map[uint64][]byte, 0)
			for id, _ := range m {
				buffer := make([]byte, int(criu.PAGE_SIZE))
				err = maps.ReadBlock(id, buffer)
				if err != nil {
					panic(err)
				}
				pages[id] = buffer
			}
			criu.Export_image(pages_export_map, pages_export_pages, 1, pages, page_flags[uint64(pages_import_id)])
		}
	}

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
				page_flags[iov.DstID()] = make(map[uint64]uint32)
			}

			// Store the page flags...
			for ptr := 0; ptr < len(buffer); ptr += int(criu.PAGE_SIZE) {
				page_flags[iov.DstID()][iov.Vaddr+uint64(ptr)] = iov.Flags()
			}

			maps.WriteBlocks(iov.Vaddr, buffer)
			fmt.Printf(" Size - %d\n", maps.Size())

			// Export it
			if pages_export_map != "" {
				m := maps.GetMap()
				pages := make(map[uint64][]byte, 0)
				for id, _ := range m {
					buffer := make([]byte, int(criu.PAGE_SIZE))
					err = maps.ReadBlock(id, buffer)
					if err != nil {
						panic(err)
					}
					pages[id] = buffer
				}
				criu.Export_image(pages_export_map, pages_export_pages, 1, pages, page_flags[iov.DstID()])
			}

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
