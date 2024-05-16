package main

import (
	"fmt"
	"path"

	"github.com/loopholelabs/silo/pkg/storage/expose/criu"
	"github.com/loopholelabs/silo/pkg/storage/modules"
	"github.com/loopholelabs/silo/pkg/storage/sources"
	"github.com/spf13/cobra"
)

var pagemap_import_dir string
var pagemap_export_dir string
var pagemap_remove_lazy bool

var (
	cmdPagemap = &cobra.Command{
		Use:   "pagemap",
		Short: "Do some pagemap",
		Long:  `Do some pagemap`,
		Run:   runPagemap,
	}
)

var pagemap_data = make(map[uint64]*modules.MappedStorage)
var pagemap_flags = make(map[uint64]map[uint64]uint32)

func init() {
	rootCmd.AddCommand(cmdPagemap)
	cmdPagemap.Flags().StringVarP(&pagemap_import_dir, "importdir", "i", "", "Import maps")
	cmdPagemap.Flags().StringVarP(&pagemap_export_dir, "exportdir", "e", "", "Export maps")
	cmdPagemap.Flags().BoolVarP(&pagemap_remove_lazy, "removelazy", "l", false, "Remove lazy pages")
}

func runPagemap(ccmd *cobra.Command, args []string) {
	fmt.Printf("Running pagemap process from %s to %s\n", pagemap_import_dir, pagemap_export_dir)

	// Import some existing data
	cb := func(pid uint32, vaddr uint64, data []byte, flags uint32) {
		fmt.Printf("Page data %x - %d Flags %d\n", vaddr, len(data), flags)
		// Send it to the thing
		maps, ok := pagemap_data[uint64(pid)]
		if !ok {
			// Create a new one
			store := sources.NewMemoryStorage(2 * 1024 * 1024 * 1024)
			m := modules.NewMappedStorage(store, int(criu.PAGE_SIZE))
			maps = m
			pagemap_data[uint64(pid)] = maps
			pagemap_flags[uint64(pid)] = make(map[uint64]uint32)
		}

		// Store the page flags...
		for ptr := 0; ptr < len(data); ptr += int(criu.PAGE_SIZE) {
			pagemap_flags[uint64(pid)][vaddr+uint64(ptr)] = flags
		}

		if flags&criu.PE_PRESENT == criu.PE_PRESENT {
			err := maps.WriteBlocks(vaddr, data)
			if err != nil {
				panic(err)
			}
		}
	}

	err := criu.Import_images(pagemap_import_dir, cb)
	if err != nil {
		panic(err)
	}

	if pagemap_remove_lazy {
		for _, maps := range pagemap_flags {
			for a, f := range maps {
				if (f & criu.PE_LAZY) == criu.PE_LAZY {
					// Clear the PE_PRESENT flag
					maps[a] = maps[a] & ^uint32(criu.PE_PRESENT)
				}
			}
		}
	}

	if pagemap_export_dir != "" {
		for pid, maps := range pagemap_data {
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

			exp_map := path.Join(pagemap_export_dir, fmt.Sprintf("pagemap-%d.img", pid))
			criu.Remove_image(exp_map) // Remove it if there's an existing version
			id := criu.Get_next_page_id(pages_export_dir)
			exp_pages := path.Join(pagemap_export_dir, fmt.Sprintf("pages-%d.img", id))
			fmt.Printf("Export pages to %s %s %d\n", exp_map, exp_pages, id)
			criu.Export_image(exp_map, exp_pages, id, pages, pagemap_flags[pid])
		}
	}
}
