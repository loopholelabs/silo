package criu

import (
	"encoding/binary"
	"fmt"
	"os"

	"google.golang.org/protobuf/proto"

	criuproto "github.com/loopholelabs/silo/pkg/proto"
)

func Import_image(mapfile string, pagefile string, data_cb func(uint64, []byte)) error {
	data, err := os.ReadFile(mapfile)
	if err != nil {
		return err
	}

	pages, err := os.Open(pagefile)
	if err != nil {
		return err
	}
	defer pages.Close()

	// TODO: Check the first 8 bytes are correct

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
		//		fmt.Printf("Pagemap entry Vaddr %x pages %d flags %d\n", *pagemap_entry.Vaddr, *pagemap_entry.NrPages, *pagemap_entry.Flags)

		data_len := (int(*pagemap_entry.NrPages) * os.Getpagesize())
		pagedata := make([]byte, data_len)
		_, err = pages.ReadAt(pagedata, int64(data_ptr))
		if err != nil {
			return err
		}

		// TODO: Check flags

		data_cb(*pagemap_entry.Vaddr, pagedata)

		data_ptr += data_len
	}

	return nil
}
