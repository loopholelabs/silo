package criu

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	"google.golang.org/protobuf/proto"

	criuproto "github.com/loopholelabs/silo/pkg/proto"
)

var pagemap_magic = []byte{0x19, 0x43, 0x56, 0x54, 0x25, 0x40, 0x08, 0x56}

/**
 * Export a pagemap / pages image
 *
 */
func Export_image(mapfile string, pages map[uint64][]byte, page_flags map[uint64]uint32) error {

	// Figure out a pages-%d.img file to use...
	pages_dir := filepath.Dir(mapfile)
	id := Get_next_page_id(pages_dir)
	pagefile := path.Join(pages_dir, fmt.Sprintf("pages-%d.img", id))

	// First compress the pages...
	MAX_PAGES := 1024

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
			ptr := os.Getpagesize()
			for {
				morea := a + uint64(ptr)
				morep, kk := new_pages[morea]
				if len(compressed_pages[a]) >= MAX_PAGES*os.Getpagesize() {
					break
				}
				if kk && (page_flags[a] == page_flags[morea]) { // Only compress if flags are the same...
					compressed_pages[a] = append(compressed_pages[a], morep...)
					delete(new_pages, morea)
				} else {
					break
				}
				ptr += os.Getpagesize()
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

	page_addresses := make([]uint64, 0)
	for addr := range pages {
		page_addresses = append(page_addresses, addr)
	}
	slices.Sort(page_addresses)

	pagefilefp, err := os.Create(pagefile)
	if err != nil {
		return err
	}

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
			_, err = pagefilefp.Write(mapdata)
			if err != nil {
				return err
			}
		}
	}

	err = pagefilefp.Close()
	if err != nil {
		return err
	}

	err = os.WriteFile(mapfile, data, 0777)
	if err != nil {
		return err
	}

	return nil
}

/**
 * Import all pagemap / pages-%d.img images in a directory
 *
 */
func Import_images(dir string, cb func(uint32, uint64, []byte, uint32)) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}

	for _, ent := range entries {
		if !ent.IsDir() && strings.HasPrefix(ent.Name(), "pagemap-") && strings.HasSuffix(ent.Name(), ".img") {
			d := ent.Name()
			id, err := strconv.ParseInt(d[8:len(d)-4], 10, 32)
			if err != nil {
				return err
			}
			// Now import the image...
			Import_image(path.Join(dir, ent.Name()), uint32(id), cb)
		}
	}
	return nil
}

/**
 * Find a free pages-%d.img ID in a directory.
 *
 */
func Get_next_page_id(mapdir string) uint32 {
	id := uint32(1)
	for {
		n := path.Join(mapdir, fmt.Sprintf("pages-%d.img", id))
		_, err := os.Stat(n)
		if errors.Is(err, os.ErrNotExist) {
			return id
		}
		id++
	}
}

/**
 * Remove a pagemap file, and the associated pages-%d.img file.
 *
 */
func Remove_image(mapfile string) error {
	data, err := os.ReadFile(mapfile)
	if err != nil {
		return err
	}

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

	// Open the page data
	pagefile := path.Join(filepath.Dir(mapfile), fmt.Sprintf("pages-%d.img", *pagemap_header.PagesId))

	err = os.Remove(pagefile)
	if err != nil {
		return err
	}
	return os.Remove(mapfile)
}

/**
 * Import a pagemap and associated pages-%d.img file.
 *
 */
func Import_image(mapfile string, id uint32, data_cb func(uint32, uint64, []byte, uint32)) error {
	data, err := os.ReadFile(mapfile)
	if err != nil {
		return err
	}

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

	// Open the page data
	pagefile := path.Join(filepath.Dir(mapfile), fmt.Sprintf("pages-%d.img", *pagemap_header.PagesId))

	pages, err := os.Open(pagefile)
	if err != nil {
		return err
	}
	defer pages.Close()

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

		data_len := (int(*pagemap_entry.NrPages) * os.Getpagesize())
		pagedata := make([]byte, data_len)

		if *pagemap_entry.Flags&PE_PRESENT == PE_PRESENT {
			_, err = pages.ReadAt(pagedata, int64(data_ptr))
			if err != nil {
				return err
			}
			data_ptr += data_len
		}
		data_cb(id, *pagemap_entry.Vaddr, pagedata, *pagemap_entry.Flags)
	}

	return nil
}
