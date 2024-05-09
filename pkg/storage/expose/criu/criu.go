package criu

import (
	"encoding/binary"
	"fmt"
)

var PAGE_SIZE = uint32(4096)

type PageServerIOV struct {
	Cmd        uint32 // CRIU Command
	Page_count uint32
	Vaddr      uint64
	Dst_id     uint64 // contains Type also
}

func Decode(buffer []byte) *PageServerIOV {
	return &PageServerIOV{
		Cmd:        binary.LittleEndian.Uint32(buffer),
		Page_count: binary.LittleEndian.Uint32(buffer[4:]),
		Vaddr:      binary.LittleEndian.Uint64(buffer[8:]),
		Dst_id:     binary.LittleEndian.Uint64(buffer[16:]),
	}
}

func (iov *PageServerIOV) Encode() []byte {
	data := make([]byte, 24)
	binary.LittleEndian.PutUint32(data, iov.Cmd)
	binary.LittleEndian.PutUint32(data[4:], iov.Page_count)
	binary.LittleEndian.PutUint64(data[8:], iov.Vaddr)
	binary.LittleEndian.PutUint64(data[16:], iov.Dst_id)
	return data
}

func (iov *PageServerIOV) Command() uint16 {
	return uint16(iov.Cmd & PS_CMD_MASK)
}

func (iov *PageServerIOV) SetCommand(cmd uint16) {
	flags := iov.Cmd >> PS_CMD_BITS
	iov.Cmd = flags<<PS_CMD_BITS | uint32(cmd)
}

func (iov *PageServerIOV) DstID() uint64 {
	return iov.Dst_id >> PS_TYPE_BITS
}

func (iov *PageServerIOV) String() string {
	cmd := iov.Command()
	cmdString, ok := PS_IOV_COMMANDS[uint16(cmd)]
	if !ok {
		cmdString = fmt.Sprintf("CMD=0x%x", cmd)
	}

	flags := iov.Cmd >> PS_CMD_BITS
	flagsString := ""
	if (flags & PE_LAZY) == PE_LAZY {
		flagsString = flagsString + " LAZY"
	}
	if (flags & PE_PARENT) == PE_PARENT {
		flagsString = flagsString + " PARENT"
	}
	if (flags & PE_PRESENT) == PE_PRESENT {
		flagsString = flagsString + " PRESENT"
	}

	ty := iov.Dst_id & PS_TYPE_MASK
	typeString := fmt.Sprintf("TYPE=0x%x", ty)
	if ty == 10 || ty == 11 || ty == 16 || ty == 17 || ty == PS_TYPE_PID {
		typeString = "FD_PAGEMAP"
	} else if ty == 27 || ty == 28 || ty == 29 || ty == 32 || ty == 33 || ty == 34 || ty == PS_TYPE_SHMEM {
		typeString = "FD_SHMEM_PAGEMAP"
	}

	// Now show the iov data...
	return fmt.Sprintf("CMD(%s) FLAGS(%s) ID(%d) TYPE(%s) VADDR(0x%x)", cmdString, flagsString, iov.DstID(), typeString, iov.Vaddr)
}

const PS_CMD_BITS = 16
const PS_CMD_MASK = ((1 << PS_CMD_BITS) - 1)

const PS_TYPE_BITS = 8
const PS_TYPE_MASK = ((1 << PS_TYPE_BITS) - 1)

// Flags
const PE_PARENT = 1  // Pages are in parent snapshot
const PE_LAZY = 2    // Pages can be lazily restored
const PE_PRESENT = 4 // Pages are present in pages*img

// Commands
const PS_IOV_ADD = 1
const PS_IOV_HOLE = 2
const PS_IOV_OPEN = 3
const PS_IOV_OPEN2 = 4
const PS_IOV_PARENT = 5
const PS_IOV_ADD_F = 6
const PS_IOV_GET = 7
const PS_IOV_CLOSE = 0x1023
const PS_IOV_FORCE_CLOSE = 0x1024

var PS_IOV_COMMANDS = map[uint16]string{
	1:      "ADD",
	2:      "HOLE",
	3:      "OPEN",
	4:      "OPEN2",
	5:      "PARENT",
	6:      "ADD_F",
	7:      "GET",
	0x1023: "CLOSE",
	0x1024: "FORCE_CLOSE",
}

// Type
const PS_TYPE_PID = 1
const PS_TYPE_SHMEM = 2
