package main

import (
	"fmt"
	"net"
	"path"
	"time"

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

	faults_served := make(map[uint64]bool)
	// TODO: Mutex etc

	var err error
	var uf *criu.UserFaultHandler

	num_faults := 0
	num_syscalls := 0

	fault := func(pid uint32, addr uint64, pending []uint64) error {
		num_faults++
		// Look it up in page_data...
		maps, ok := page_data[uint64(pid)]

		fmt.Printf("Fault for %d address %x, with pending %v\n", pid, addr, pending)

		_, served := faults_served[uint64(pid)]
		if served {
			return nil
		}
		faults_served[uint64(pid)] = true

		if !ok {
			panic("We do not have such a page for that pid!")
		}

		go func() {
			// TRY TO SEND ALL OUR LAZY DATA IN ONE SHOT (atm)...

			all_addresses := maps.GetBlockAddresses()
			addresses := make([]uint64, 0)
			// Just the lazy pages
			for _, a := range all_addresses {
				flag := page_flags[uint64(pid)][a]
				if (flag & criu.PE_LAZY) == criu.PE_LAZY {
					addresses = append(addresses, a)
				}
			}

			max_size := uint64(256 * 1024)

			ranges := maps.GetRegions(addresses, max_size)

			send_pages := make(map[uint64][]byte)

			// Read the data into memory so its ready to send in one shot...
			for a, l := range ranges {
				data := make([]byte, l)
				err := maps.ReadBlocks(a, data)
				if err != nil {
					panic(err)
				}
				send_pages[a] = data
			}

			ctime := time.Now()

			// Send all the data over...
			for addr, data := range send_pages {
				//				hasher := crypto.SHA256.New()
				//				hasher.Write(data)
				//				hash := hasher.Sum(nil)
				//				fmt.Printf("Provide page at %x data is %x\n", addr, hash)
				uf.WriteData(uint64(pid), addr, data)
				num_syscalls++
			}

			// We've sent everything, we can quit now!
			fmt.Printf("All Data Sent %dms faults=%d syscalls=%d\n", time.Since(ctime).Milliseconds(), num_faults, num_syscalls)

			err = uf.ClosePID(uint64(pid))
			fmt.Printf("Close uffd %v\n", err)
			if err != nil {
				panic(err)
			}

			// Allow more faults now if it's being reused...
			delete(faults_served, uint64(pid))
		}()

		return nil
	}

	uf, err = criu.NewUserFaultHandler("lazy-pages.socket", fault)
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
				store := sources.NewMemoryStorage(2 * 1024 * 1024 * 1024)
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
				err := maps.WriteBlocks(vaddr, data)
				if err != nil {
					panic(err)
				}
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

		fmt.Printf("IMPORT COMPLETED\n")
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
			criu.Export_image(exp_map, pages, page_flags[pid])
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
				criu.Export_image(exp_map, pages, page_flags[iov.DstID()])
			}

		}

		ps.GetPageData = func(iov *criu.PageServerIOV, buffer []byte) {
			fmt.Printf("GetPageData %s %d\n", iov.String(), len(buffer))
			maps, ok := page_data[iov.Dst_id]
			if ok {
				maps.ReadBlocks(iov.Vaddr, buffer)
			} else {
				fmt.Printf("Page data not present for ID... Lets fall back on page server %d - %d\n", iov.DstID(), iov.Dst_id)
				maps, ok := page_data[iov.DstID()]
				if ok {
					maps.ReadBlocks(iov.Vaddr, buffer)
				}
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
