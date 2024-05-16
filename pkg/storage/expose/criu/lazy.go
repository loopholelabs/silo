package criu

/*
#include <sys/syscall.h>
#include <fcntl.h>
#include <linux/userfaultfd.h>

struct uffd_pagefault {
	__u64	flags;
	__u64	address;
	__u32 ptid;
};
*/
import "C"
import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"syscall"
	"unsafe"

	"golang.org/x/sys/unix"
)

var (
	ErrUnexpectedEventType = errors.New("unexpected event type")
)

const LAZY_PAGES_RESTORE_FINISHED = 0x52535446 /* ReSTore Finished */
const UFFDIO_COPY = 3223890435                 // From <linux/userfaultfd.h>
const UFFDIO_COPY_MODE_DONTWAKE = 1            // From <linux/userfaultfd.h>

type UFFD uintptr

type UserFaultHandler struct {
	listener       *net.UnixListener
	uffds          map[uint64]UFFD
	pending_faults map[uint64]map[uint64]bool
	uffds_lock     sync.Mutex
	DONTWAKE       bool
	faultHandler   func(uint32, uint64, []uint64) error
}

func NewUserFaultHandler(socket string, faultHandler func(uint32, uint64, []uint64) error) (*UserFaultHandler, error) {
	// If it exists, remove the socket
	os.Remove(socket)

	addr, err := net.ResolveUnixAddr("unix", socket)
	if err != nil {
		return nil, err
	}

	l, err := net.ListenUnix("unix", addr)
	if err != nil {
		return nil, err
	}

	return &UserFaultHandler{
		listener:       l,
		faultHandler:   faultHandler,
		uffds:          make(map[uint64]UFFD),
		pending_faults: make(map[uint64]map[uint64]bool, 0),
		DONTWAKE:       true,
	}, nil
}

/**
 * Handle any incoming requests to the unix socket and service them.
 *
 */
func (u *UserFaultHandler) Handle() error {
	for {
		con, err := u.listener.AcceptUnix()
		if err != nil {
			return err
		}

		// Now handle the individual connection we got...
		go func(c *net.UnixConn) {
			defer func() {
				c.Close()
			}()

			for {
				pid_data := make([]byte, 4)
				_, err := io.ReadFull(c, pid_data)
				if err != nil {
					// TODO: Log this somewhere...
					return
				}

				pid := binary.LittleEndian.Uint32(pid_data)
				if pid == LAZY_PAGES_RESTORE_FINISHED {
					return // All done on this connection
				}

				fds, err := receiveFds(c, 1)
				if err != nil {
					// TODO: Log this somewhere...
					return
				}

				// Store it here so we can use it later...
				u.uffds_lock.Lock()
				// TODO: What if there's an existing one here for the same PID?
				// Should we close the existing one, or reject the new one?
				u.uffds[uint64(pid)] = UFFD(fds[0])
				u.pending_faults[uint64(pid)] = make(map[uint64]bool)
				u.uffds_lock.Unlock()

				// Now handle the userfault stuff
				go u.HandleUFFD(pid)
			}
		}(con)
	}
}

func (u *UserFaultHandler) Close() {
	u.listener.Close()
}

/**
 * Handle faults as they come in to us
 *
 */
func (u *UserFaultHandler) HandleUFFD(pid uint32) error {
	u.uffds_lock.Lock()
	uffd, ok := u.uffds[uint64(pid)]
	u.uffds_lock.Unlock()

	if !ok {
		return errors.New("PID not known yet")
	}

	pagesize := os.Getpagesize()

	for {
		fds := []unix.PollFd{{Fd: int32(uffd), Events: unix.POLLIN}}

		for {
			n, err := unix.Poll(fds, 500)

			if err != nil {
				return err
			}
			if n != 0 {
				break
			}
		}

		// Read the event
		buf := make([]byte, unsafe.Sizeof(C.struct_uffd_msg{}))
		_, err := syscall.Read(int(uffd), buf)

		if err != nil {
			return err
		}

		bufptr := unsafe.Pointer(&buf[0])
		msg := (*(*C.struct_uffd_msg)(bufptr))
		if msg.event != C.UFFD_EVENT_PAGEFAULT {
			return ErrUnexpectedEventType
		}

		msgptr := unsafe.Pointer(&msg.arg[0])
		pagefault := (*(*C.struct_uffd_pagefault)(msgptr))

		addr := uint64(pagefault.address) & ^uint64(pagesize-1)

		// Add it onto any pending faults...
		u.uffds_lock.Lock()
		u.pending_faults[uint64(pid)][addr] = true
		faults := make([]uint64, 0)
		for f := range u.pending_faults[uint64(pid)] {
			faults = append(faults, f)
		}
		u.uffds_lock.Unlock()

		err = u.faultHandler(pid, addr, faults)
		if err != nil {
			return err
		}
	}
}

func (u *UserFaultHandler) ClosePID(pid uint64) error {
	u.uffds_lock.Lock()
	defer u.uffds_lock.Unlock()
	uffd, ok := u.uffds[pid]

	if !ok {
		return errors.New("PID not known yet")
	}
	delete(u.uffds, pid)
	delete(u.pending_faults, pid)
	return syscall.Close(int(uffd))
}

/**
 * WriteData
 *
 */
func (u *UserFaultHandler) WriteData(pid uint64, address uint64, data []byte) error {
	u.uffds_lock.Lock()
	uffd, ok := u.uffds[pid]
	new_pending_faults := make(map[uint64]bool)
	for a := range u.pending_faults[pid] {
		new_pending_faults[a] = true
	}
	// Clear the ones we're providing data for...
	for ptr := 0; ptr < len(data); ptr += os.Getpagesize() {
		a := address + uint64(ptr)
		// Clear the fault
		delete(new_pending_faults, a)
	}

	u.uffds_lock.Unlock()

	if !ok {
		return errors.New("PID not known yet")
	}

	cpy := C.struct_uffdio_copy{
		src:  C.ulonglong(uintptr(unsafe.Pointer(&data[0]))),
		dst:  C.ulonglong(address),
		len:  C.ulonglong(len(data)),
		mode: C.ulonglong(0),
		copy: C.longlong(0),
	}

	if u.DONTWAKE && len(new_pending_faults) > 0 {
		// Don't bother resuming the process yet, there's still outstanding faults to fix...
		cpy.mode = C.UFFDIO_COPY_MODE_DONTWAKE
	}

	_, _, errno := syscall.Syscall(syscall.SYS_IOCTL, uintptr(uffd), UFFDIO_COPY, uintptr(unsafe.Pointer(&cpy)))
	if errno != 0 {
		return fmt.Errorf("COPY errno: %d", errno)
	}

	// The write was successful, clear the associated faults.
	u.uffds_lock.Lock()
	u.pending_faults[pid] = new_pending_faults
	u.uffds_lock.Unlock()

	fmt.Printf("WriteData %d - %x\n", pid, address)

	return nil
}

/**
 * Receive FDs through unix socket
 *
 */
func receiveFds(conn *net.UnixConn, num int) ([]uintptr, error) {
	if num <= 0 {
		return nil, nil
	}

	f, err := conn.File()

	if err != nil {
		return nil, err
	}

	defer f.Close()

	buf := make([]byte, syscall.CmsgSpace(num*4))
	_, _, _, _, err = syscall.Recvmsg(int(f.Fd()), nil, buf, 0)

	if err != nil {
		return nil, err
	}

	msgs, err := syscall.ParseSocketControlMessage(buf)

	if err != nil {
		return nil, err
	}

	fds := []uintptr{}
	for _, msg := range msgs {
		newFds, err := syscall.ParseUnixRights(&msg)
		if err != nil {
			return nil, err
		}

		for _, newFd := range newFds {
			fds = append(fds, uintptr(newFd))
		}
	}

	return fds, nil
}
