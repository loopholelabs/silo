package main

import (
	"fmt"
	"net"
	"os"
	"path"

	"github.com/loopholelabs/silo/pkg/storage/expose/criu"
	"github.com/loopholelabs/silo/pkg/storage/modules"
	"github.com/loopholelabs/silo/pkg/storage/sources"
	"github.com/spf13/cobra"
)

var PAGE_SIZE = uint32(4096)

var pages_serve_addr string
var pages_import_dir string
var pages_export_dir string
var pages_import_lazy_addr string

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
	cmdPages.Flags().StringVarP(&pages_import_dir, "importdir", "i", "", "Import maps")
	cmdPages.Flags().StringVarP(&pages_import_lazy_addr, "lazyaddr", "l", ":5171", "Address to get lazy pages from")

	cmdPages.Flags().StringVarP(&pages_export_dir, "exportdir", "e", "", "Export maps")
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

	if pages_import_dir != "" {
		lazy_requests := make([]*criu.PageRequest, 0)

		// Import some existing data
		cb := func(pid uint32, vaddr uint64, data []byte, flags uint32) {
			fmt.Printf("Page data %x - %d Flags %d\n", vaddr, len(data), flags)
			// Send it to the thing
			maps, ok := page_data[uint64(pid)]
			if !ok {
				// Create a new one
				store := sources.NewMemoryStorage(1 * 1024 * 1024 * 1024)
				m := modules.NewMappedStorage(store, int(criu.PAGE_SIZE))
				maps = m
				page_data[uint64(pid)] = maps
				page_flags[uint64(pid)] = make(map[uint64]uint32)
			}

			// Store the page flags...
			for ptr := 0; ptr < len(data); ptr += int(criu.PAGE_SIZE) {
				page_flags[uint64(pid)][vaddr+uint64(ptr)] = flags
			}

			if flags&criu.PE_PRESENT == criu.PE_PRESENT {
				maps.WriteBlocks(vaddr, data)
			} else {
				lazy_requests = append(lazy_requests, &criu.PageRequest{
					Pid:   pid,
					Vaddr: vaddr,
					Pages: uint32(len(data)) / criu.PAGE_SIZE,
				})
			}
		}

		err := criu.Import_images(pages_import_dir, cb)
		if err != nil {
			panic(err)
		}

		// Now go get the lazy pages
		if pages_import_lazy_addr != "" {

			client, err := criu.NewPageClient()
			if err != nil {
				panic(err)
			}

			cb := func(pid uint32, addr uint64, data []byte) {
				// Fill it in...
				maps, ok := page_data[uint64(pid)]
				if ok {
					fmt.Printf("Writing from lazy - %d %x\n", pid, addr)
					maps.WriteBlocks(addr, data)
				}
			}

			client.Connect(pages_import_lazy_addr, lazy_requests, cb)
		}
	}

	// Export it

	if pages_export_dir != "" {
		for pid, maps := range page_data {
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

			exp_map := path.Join(pages_export_dir, fmt.Sprintf("pagemap-%d.img", pid))
			criu.Remove_image(exp_map) // Remove it if there's an existing version
			id := criu.Get_next_page_id(pages_export_dir)
			exp_pages := path.Join(pages_export_dir, fmt.Sprintf("pages-%d.img", id))
			fmt.Printf("Export pages to %s %s %d\n", exp_map, exp_pages, id)
			criu.Export_image(exp_map, exp_pages, id, pages, page_flags[pid])
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
			if pages_export_dir != "" {
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
				exp_map := path.Join(pages_export_dir, fmt.Sprintf("pagemap-%d.img", iov.DstID()))
				criu.Remove_image(exp_map) // Remove it if there's an existing version
				id := criu.Get_next_page_id(pages_export_dir)
				exp_pages := path.Join(pages_export_dir, fmt.Sprintf("pages-%d.img", id))
				criu.Export_image(exp_map, exp_pages, id, pages, page_flags[iov.DstID()])
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
