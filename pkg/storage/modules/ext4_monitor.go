package modules

import (
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/davissp14/go-ext4"
	"github.com/loopholelabs/silo/pkg/storage"
)

// TODO: Probably switch to https://github.com/masahiro331/go-ext4-filesystem/tree/main
// since it supports inodes not using extents

// TODO: Use https://github.com/Velocidex/go-journalctl to get at journal files directly

type Ext4Monitor struct {
	storage.ProviderWithEvents
	prov storage.Provider

	superBlock *ext4.Superblock
	bgdl       *ext4.BlockGroupDescriptorList
	ext4lock   sync.Mutex

	// For a test log follower
	testData string
}

// Relay events to embedded StorageProvider
func (i *Ext4Monitor) SendSiloEvent(eventType storage.EventType, eventData storage.EventData) []storage.EventReturnData {
	data := i.ProviderWithEvents.SendSiloEvent(eventType, eventData)
	return append(data, storage.SendSiloEvent(i.prov, eventType, eventData)...)
}

func (i *Ext4Monitor) followFile(prov storage.Provider) error {
	i.ext4lock.Lock()
	defer i.ext4lock.Unlock()

	f := storage.NewReadSeeker(prov)

	inodeNumber := ext4.InodeRootDirectory

	bgd, err := i.bgdl.GetWithAbsoluteInode(inodeNumber)
	if err != nil {
		fmt.Printf("Error reading disk GetWithAbsoluteInode %v\n", err)
	}

	dw, err := ext4.NewDirectoryWalk(f, bgd, inodeNumber)
	if err != nil {
		fmt.Printf("Error reading disk NewDirectoryWalk %v\n", err)
	}

	for {
		fullPath, de, err := dw.Next()
		if err != nil {
			fmt.Printf("Error reading disk Walking DIR %v\n", err)
			break
		}

		fmt.Printf(" # EXT4 Inode %d - %s\n", de.Data().Inode, fullPath)

		if strings.HasPrefix(fullPath, "log") {
			ino := de.Data().Inode
			inode, err := ext4.NewInodeWithReadSeeker(bgd, f, int(ino))
			if err != nil {
				fmt.Printf("Error reading disk %v\n", err)
			}

			en := ext4.NewExtentNavigatorWithReadSeeker(f, inode)
			ir := ext4.NewInodeReader(en)

			fileData, err := io.ReadAll(ir)
			if err != nil {
				fmt.Printf("Error reading disk %v\n", err)
			}

			fmt.Printf(" -EXT4 File \"%s\" has changed Inode %d Size %d\n", fullPath, de.Data().Inode, inode.Size())
			fmt.Printf("%s\n -End data-\n", string(fileData))
		}

	}

	/*
		// Init things...
		superblock := make([]byte, ext4.SuperblockSize)
		prov.ReadAt(superblock, ext4.Superblock0Offset)
		// TODO: Check for error

		sb, err := ext4.NewSuperblockWithReader(bytes.NewReader(superblock))
		if err != nil {
			fmt.Printf("Error in superblock %v\n", err)
		}

		fmt.Printf("Volume name %s\n", sb.VolumeName())
		fmt.Printf("Block size %d\n", sb.BlockSize())
		fmt.Printf("Block count %d\n", sb.BlockCount())

		// Now read bgdl
	*/

	return nil
}

func NewExt4Check(prov storage.Provider) *Ext4Monitor {

	f := storage.NewReadSeeker(prov)
	f.Seek(ext4.Superblock0Offset, io.SeekStart)
	s, err := ext4.NewSuperblockWithReader(f)
	if err != nil {
		fmt.Printf("NewExtCheck Error %v\n", err)
	}

	bgdl, err := ext4.NewBlockGroupDescriptorListWithReadSeeker(f, s)
	if err != nil {
		fmt.Printf("NewExtCheck Error reading disk %v\n", err)
	}

	fmt.Printf("NewExt4Check %s\n", s.VolumeName())

	return &Ext4Monitor{
		prov:       prov,
		superBlock: s,
		bgdl:       bgdl,
	}
}

func (i *Ext4Monitor) ReadAt(p []byte, off int64) (n int, err error) {
	return i.prov.ReadAt(p, off)
}

func (i *Ext4Monitor) WriteAt(p []byte, off int64) (int, error) {
	n, err := i.prov.WriteAt(p, off)

	f := storage.NewReadSeeker(i.prov)

	writeStart := off
	writeEnd := off + int64(len(p))

	i.ext4lock.Lock()

	// If it involved the superblock, update
	if writeStart <= ext4.Superblock0Offset && writeEnd >= ext4.Superblock0Offset+ext4.SuperblockSize {
		f.Seek(ext4.Superblock0Offset, io.SeekStart)
		i.superBlock, err = ext4.NewSuperblockWithReader(f)
		fmt.Printf("#EXT4# Superblock0 updated %v\n", err)
	}

	// If it involved bgdl, update
	initialBlock := uint64(i.superBlock.Data().SFirstDataBlock) + 1
	sbStart := int64(initialBlock * uint64(i.superBlock.BlockSize()))
	sbEnd := sbStart + int64(ext4.BlockGroupDescriptorSize*i.superBlock.BlockGroupCount())

	if (writeStart >= sbStart && writeStart < sbEnd) ||
		(writeEnd >= sbStart && writeEnd < sbEnd) {
		i.bgdl, err = ext4.NewBlockGroupDescriptorListWithReadSeeker(f, i.superBlock)
		if err != nil {
			fmt.Printf("Newbgdl Error reading disk %v\n", err)
		}
		fmt.Printf("#EXT4# BlockGroupDescriptorList updated %v\n", err)
	}
	// Do some analysis on what's being written here...
	fmt.Printf("# WriteAt %d - %d bytes\n", off, len(p))

	i.ext4lock.Unlock()

	// Experimental followFile
	i.followFile(i.prov)

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
