package criu

import (
	"fmt"
	"path"
	"sync"
)

// This is a simple in memory store for memory pages

type PageStore struct {
	pages_lock sync.Mutex
	pages      map[uint64]*PageMap
}

type PageMap struct {
	pages_lock sync.Mutex
	pages      map[uint64]*Page
}

type Page struct {
	data []byte
}

func NewPageStore() *PageStore {
	return &PageStore{
		pages: make(map[uint64]*PageMap),
	}
}

func (ps *PageStore) RemoveID(id uint64) {
	ps.pages_lock.Lock()
	defer ps.pages_lock.Unlock()
	delete(ps.pages, id)
}

func (ps *PageStore) AddPageData(iov *PageServerIOV, data []byte) {
	id := iov.DstID()
	ps.pages_lock.Lock()
	pmap, ok := ps.pages[id]
	if !ok {
		pmap = &PageMap{pages: make(map[uint64]*Page)}
		ps.pages[id] = pmap
	}
	ps.pages_lock.Unlock()

	// Now deal with pmap

	pmap.pages_lock.Lock()
	// Split it up into blocks and store...
	vaddr := iov.Vaddr
	for ptr := 0; ptr < len(data); ptr += int(PAGE_SIZE) {
		pmap.pages[vaddr+uint64(ptr)] = &Page{data: data[ptr : ptr+int(PAGE_SIZE)]}
	}
	pmap.pages_lock.Unlock()
}

func (ps *PageStore) GetPageList(pid uint64) []uint64 {
	ps.pages_lock.Lock()
	info := ps.pages[pid]
	ps.pages_lock.Unlock()

	pages := make([]uint64, 0)

	info.pages_lock.Lock()
	for id := range info.pages {
		pages = append(pages, id)
	}
	info.pages_lock.Unlock()

	return pages
}

func (ps *PageStore) RemovePageData(pid uint64, vaddr uint64) {
	ps.pages_lock.Lock()
	info := ps.pages[pid]
	ps.pages_lock.Unlock()

	info.pages_lock.Lock()
	delete(info.pages, vaddr)
	info.pages_lock.Unlock()
}

func (ps *PageStore) IDExists(iov *PageServerIOV) bool {
	id := iov.DstID()
	ps.pages_lock.Lock()
	_, ok := ps.pages[id]
	ps.pages_lock.Unlock()
	return ok
}

func (ps *PageStore) GetPageData(iov *PageServerIOV, data []byte) {
	id := iov.DstID()
	ps.pages_lock.Lock()
	pmap, ok := ps.pages[id]
	if !ok {
		pmap = &PageMap{pages: make(map[uint64]*Page)}
		ps.pages[id] = pmap
	}
	ps.pages_lock.Unlock()

	// Now deal with pmap

	pmap.pages_lock.Lock()
	// Get pages and combine
	vaddr := iov.Vaddr
	for ptr := 0; ptr < len(data); ptr += int(PAGE_SIZE) {
		page := pmap.pages[vaddr+uint64(ptr)]
		copy(data[ptr:], page.data)
	}
	pmap.pages_lock.Unlock()
}

func (ps *PageStore) ShowAll() {
	fmt.Printf("-- Page Store --\n")
	ps.pages_lock.Lock()
	defer ps.pages_lock.Unlock()

	for id, pmap := range ps.pages {
		pmap.pages_lock.Lock()
		for vaddr, page := range pmap.pages {
			fmt.Printf("PAGE ID(%d) VADDR(%x) data %d\n", id, vaddr, len(page.data))
		}
		pmap.pages_lock.Unlock()
	}
}

func (ps *PageStore) Dump(dir string) error {
	for id, pmap := range ps.pages {
		pages := make(map[uint64][]byte)
		flags := make(map[uint64]uint32)
		for vaddr, page := range pmap.pages {
			pages[vaddr] = page.data
			flags[vaddr] = PE_PRESENT
		}
		mapfile := path.Join(dir, fmt.Sprintf("pagemap-%d.img", id))
		err := Export_image(mapfile, pages, flags)
		if err != nil {
			return err
		}
	}
	return nil
}
