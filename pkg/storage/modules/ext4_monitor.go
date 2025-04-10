package modules

import (
	"fmt"
	"io"
	"io/fs"
	"log"
	"os"

	"github.com/loopholelabs/silo/pkg/storage"
	ext4 "github.com/masahiro331/go-ext4-filesystem/ext4"
)

// TODO: Probably switch to https://github.com/masahiro331/go-ext4-filesystem/tree/main
// since it supports inodes not using extents

// TODO: Use https://github.com/Velocidex/go-journalctl to get at journal files directly

type Ext4Monitor struct {
	storage.ProviderWithEvents
	prov storage.Provider
}

// Relay events to embedded StorageProvider
func (i *Ext4Monitor) SendSiloEvent(eventType storage.EventType, eventData storage.EventData) []storage.EventReturnData {
	data := i.ProviderWithEvents.SendSiloEvent(eventType, eventData)
	return append(data, storage.SendSiloEvent(i.prov, eventType, eventData)...)
}

func NewExt4Check(prov storage.Provider) *Ext4Monitor {

	filesystem, err := ext4.NewFS(*io.NewSectionReader(prov, 0, int64(prov.Size())), nil)
	if err != nil {
		log.Fatal(err)
	}

	fs.WalkDir(filesystem, "/", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return fmt.Errorf("file walk error: %w", err)
		}
		if d.IsDir() {
			return nil
		}

		fmt.Println(path)

		if path == "/test.log" {
			sf, err := filesystem.Open(path)
			if err != nil {
				return err
			}
			data, err := io.ReadAll(sf)
			fmt.Printf("#DATA#\n%s\n", string(data))
		}

		if path == "/usr/lib/os-release" {
			of, _ := os.Create("os-release")
			defer of.Close()

			sf, err := filesystem.Open(path)
			if err != nil {
				return err
			}
			io.Copy(of, sf)
		}
		return nil
	})

	return &Ext4Monitor{
		prov: prov,
	}
}

func (i *Ext4Monitor) ReadAt(p []byte, off int64) (n int, err error) {
	return i.prov.ReadAt(p, off)
}

func (i *Ext4Monitor) WriteAt(p []byte, off int64) (int, error) {
	n, err := i.prov.WriteAt(p, off)
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
