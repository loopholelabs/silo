package modules

import (
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/loopholelabs/silo/pkg/storage"
	ext4 "github.com/masahiro331/go-ext4-filesystem/ext4"
)

// TODO: Probably switch to https://github.com/masahiro331/go-ext4-filesystem/tree/main
// since it supports inodes not using extents

// TODO: Use https://github.com/Velocidex/go-journalctl to get at journal files directly

type Ext4Monitor struct {
	storage.ProviderWithEvents
	prov storage.Provider

	filesLock sync.Mutex
	files     map[string]fileData
}

type fileData struct {
	path   string
	info   fs.FileInfo
	blocks []*ext4.FileBlockData
}

// Relay events to embedded StorageProvider
func (i *Ext4Monitor) SendSiloEvent(eventType storage.EventType, eventData storage.EventData) []storage.EventReturnData {
	data := i.ProviderWithEvents.SendSiloEvent(eventType, eventData)
	return append(data, storage.SendSiloEvent(i.prov, eventType, eventData)...)
}

func NewExt4Check(prov storage.Provider) *Ext4Monitor {
	e := &Ext4Monitor{
		prov: prov,
	}

	fmt.Printf("Ext4Check walking fs...\n")

	err := e.walkfs()
	if err != nil {
		fmt.Printf("Ext4Check error %v\n", err)
	}

	for p, fi := range e.files {
		fmt.Printf("# File %s | %d\n", p, fi.info.Size())
		for _, bl := range fi.blocks {
			fmt.Printf("  Block %d / %d\n", bl.Offset, bl.Length)
		}
	}

	return e
}

func (i *Ext4Monitor) walkfs() error {
	filesystem, err := ext4.NewFS(*io.NewSectionReader(i.prov, 0, int64(i.prov.Size())), nil)
	if err != nil {
		return err
	}

	fmt.Printf("Filesystem %v\n", filesystem)

	i.filesLock.Lock()
	defer i.filesLock.Unlock()
	i.files = make(map[string]fileData)

	err = fs.WalkDir(filesystem, "/", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return fmt.Errorf("file walk error: %w", err)
		}
		// We won't worry about directories for now...
		if d.IsDir() {
			return nil
		}

		if strings.HasPrefix(path, "/var/log/journal") {
			blocks := make([]*ext4.FileBlockData, 0)

			fmt.Printf("Open %s\n", path)
			cf, err := filesystem.Open(path)
			if err == nil {
				ccf := cf.(*ext4.File)
				blocks, _ = ccf.GetBlocks()
				cf.Close()
			}

			info, err := d.Info()
			if err != nil {
				return err
			}

			i.files[path] = fileData{
				path:   path,
				info:   info,
				blocks: blocks,
			}

			if strings.HasPrefix(path, "/var/log/journal") {
				filename := filepath.Base(path)
				// Copy the files out...
				dst, err := os.Create(fmt.Sprintf("journal_%s", filename))
				if err == nil {
					fmt.Printf("### Open %s\n", path)
					ff, err := filesystem.Open(path)
					if err == nil {
						io.Copy(dst, ff)
						dst.Close()
						ff.Close()
					} else {
						fmt.Printf("Error %v\n", err)
					}
				} else {
					fmt.Printf("Error %v\n", err)
				}
			}
		}

		return nil
	})

	return err
}

func (i *Ext4Monitor) copyJournal() error {
	filesystem, err := ext4.NewFS(*io.NewSectionReader(i.prov, 0, int64(i.prov.Size())), nil)
	if err != nil {
		return err
	}
	ff, err := filesystem.Open("/etc/machine-id")
	if err != nil {
		return err
	}
	machineID, err := io.ReadAll(ff)
	if err != nil {
		return err
	}
	ff.Close()

	fmt.Printf("MachineID %s\n", machineID)

	dst, err := os.Create(fmt.Sprintf("journal"))
	if err != nil {
		return err
	}
	defer dst.Close()

	ff, err = filesystem.Open(fmt.Sprintf("/var/log/journal/%s/system.journal", machineID))
	if err != nil {
		return err
	}
	defer ff.Close()

	n, err := io.Copy(dst, ff)

	if err == nil {
		fmt.Printf(" # Journal %d bytes\n", n)
	}

	return err
}

func (i *Ext4Monitor) ReadAt(p []byte, off int64) (n int, err error) {
	return i.prov.ReadAt(p, off)
}

func (i *Ext4Monitor) WriteAt(p []byte, off int64) (int, error) {
	n, err := i.prov.WriteAt(p, off)

	// TODO: Figure out what it's writing...

	// e := i.copyJournal()
	// if e != nil {
	//	fmt.Printf("Error copying journal %v\n", e)
	// }

	i.walkfs()

	return n, err
}

func (i *Ext4Monitor) Flush() error {
	return i.prov.Flush()
}

func (i *Ext4Monitor) Size() uint64 {
	return i.prov.Size()
}

func (i *Ext4Monitor) Close() error {
	return i.prov.Close()
}

func (i *Ext4Monitor) CancelWrites(offset int64, length int64) {
	i.prov.CancelWrites(offset, length)
}
