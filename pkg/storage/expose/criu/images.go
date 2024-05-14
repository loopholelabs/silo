package criu

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"slices"

	"google.golang.org/protobuf/proto"

	criuproto "github.com/loopholelabs/silo/pkg/proto"
)

var pagemap_magic = []byte{0x19, 0x43, 0x56, 0x54, 0x25, 0x40, 0x08, 0x56}

func Export_image(mapfile string, pagefile string, id uint32, pages map[uint64][]byte, page_flags map[uint64]uint32) error {
	// First compress the data...
	new_pages := make(map[uint64][]byte)
	addresses := make([]uint64, 0)
	for addr, data := range pages {
		addresses = append(addresses, addr)
		new_pages[addr] = data
	}
	slices.Sort(addresses)

	compressed_pages := make(map[uint64][]byte)
	// Now go through compressing
	for _, a := range addresses {
		p, ok := new_pages[a]
		if ok {
			compressed_pages[a] = p
			// Now try to add on...
			ptr := PAGE_SIZE
			for {
				morea := a + uint64(ptr)
				morep, kk := new_pages[morea]
				if kk && (page_flags[a] == page_flags[morea]) { // Only compress if flags are the same...
					compressed_pages[a] = append(compressed_pages[a], morep...)
					delete(new_pages, morea)
				} else {
					break
				}
				ptr += PAGE_SIZE
			}
		}
	}

	pages = compressed_pages

	data := make([]byte, 0)
	data = append(data, pagemap_magic...)

	// Write a header
	pagemap_header := &criuproto.PagemapHead{
		PagesId: &id,
	}
	pagemap_header_encoded, err := proto.Marshal(pagemap_header)
	if err != nil {
		return err
	}
	data = binary.LittleEndian.AppendUint32(data, uint32(len(pagemap_header_encoded)))
	data = append(data, pagemap_header_encoded...)

	pagedata := make([]byte, 0)

	page_addresses := make([]uint64, 0)
	for addr := range pages {
		page_addresses = append(page_addresses, addr)
	}
	slices.Sort(page_addresses)

	// Now go through entries...
	for _, addr := range page_addresses {
		mapdata := pages[addr]

		inParent := false
		numPages := uint32(len(mapdata) / int(PAGE_SIZE))
		flags := page_flags[addr]

		pagemap_entry := &criuproto.PagemapEntry{
			Vaddr:    &addr,
			NrPages:  &numPages,
			InParent: &inParent,
			Flags:    &flags,
		}
		pagemap_entry_encoded, err := proto.Marshal(pagemap_entry)
		if err != nil {
			return err
		}
		data = binary.LittleEndian.AppendUint32(data, uint32(len(pagemap_entry_encoded)))
		data = append(data, pagemap_entry_encoded...)

		if flags&PE_PRESENT == PE_PRESENT {
			pagedata = append(pagedata, mapdata...)
		}
	}

	err = os.WriteFile(mapfile, data, 0777)
	if err != nil {
		return err
	}

	err = os.WriteFile(pagefile, pagedata, 0777)
	if err != nil {
		return err
	}

	return nil
}

func Import_image(mapfile string, pagefile string, data_cb func(uint64, []byte, uint32)) error {
	data, err := os.ReadFile(mapfile)
	if err != nil {
		return err
	}

	pages, err := os.Open(pagefile)
	if err != nil {
		return err
	}
	defer pages.Close()

	// Check the first 8 bytes are correct
	for i := 0; i < len(pagemap_magic); i++ {
		if pagemap_magic[i] != data[i] {
			return errors.New("Invalid pagemap")
		}
	}

	ptr := 8
	header_len := binary.LittleEndian.Uint32(data[ptr:])
	ptr += 4
	header_data := data[ptr : ptr+int(header_len)]
	ptr += int(header_len)

	pagemap_header := &criuproto.PagemapHead{}

	// Now read the data out...
	err = proto.Unmarshal(header_data, pagemap_header)
	if err != nil {
		return err
	}
	fmt.Printf("PageMap header %v\n", pagemap_header)

	data_ptr := 0
	for {
		if ptr == len(data) {
			break
		}
		entry_len := binary.LittleEndian.Uint32(data[ptr:])
		ptr += 4
		entry_data := data[ptr : ptr+int(entry_len)]
		ptr += int(entry_len)
		pagemap_entry := &criuproto.PagemapEntry{}

		// Now read the data out...
		err = proto.Unmarshal(entry_data, pagemap_entry)
		if err != nil {
			return err
		}

		flags := ""
		if *pagemap_entry.Flags&PE_LAZY == PE_LAZY {
			flags = flags + " LAZY"
		}
		if *pagemap_entry.Flags&PE_PARENT == PE_PARENT {
			flags = flags + " PARENT"
		}
		if *pagemap_entry.Flags&PE_PRESENT == PE_PRESENT {
			flags = flags + " PRESENT"
		}
		fmt.Printf("Pagemap entry Vaddr %x pages %d flags %s\n", *pagemap_entry.Vaddr, *pagemap_entry.NrPages, flags)

		data_len := (int(*pagemap_entry.NrPages) * os.Getpagesize())
		pagedata := make([]byte, data_len)

		if *pagemap_entry.Flags&PE_PRESENT == PE_PRESENT {
			_, err = pages.ReadAt(pagedata, int64(data_ptr))
			if err != nil {
				return err
			}
			data_ptr += data_len
		}

		// TODO: Check flags

		data_cb(*pagemap_entry.Vaddr, pagedata, *pagemap_entry.Flags)

	}

	return nil
}
