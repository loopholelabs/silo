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
	"syscall"
	"unsafe"

	"golang.org/x/sys/unix"
)

var (
	ErrUnexpectedEventType = errors.New("unexpected event type")
)

const LAZY_PAGES_RESTORE_FINISHED = 0x52535446 /* ReSTore Finished */
const UFFDIO_COPY = 3223890435                 // From <linux/userfaultfd.h>
type UFFD uintptr

type ProvideData func(address uint64, data []byte) error

type UserFaultHandler struct {
	listener     *net.UnixListener
	initHandler  func(uint32, ProvideData) error
	faultHandler func(uint32, uint64, ProvideData) error
}

func NewUserFaultHandler(socket string, initHandler func(uint32, ProvideData) error, faultHandler func(uint32, uint64, ProvideData) error) (*UserFaultHandler, error) {
	err := os.Remove(socket)
	if err != nil {
		return nil, err
	}

	addr, err := net.ResolveUnixAddr("unix", socket)
	if err != nil {
		return nil, err
	}

	l, err := net.ListenUnix("unix", addr)
	if err != nil {
		return nil, err
	}

	return &UserFaultHandler{
		listener:     l,
		initHandler:  initHandler,
		faultHandler: faultHandler,
	}, nil
}

func (u *UserFaultHandler) Handle() error {
	for {
		con, err := u.listener.AcceptUnix()
		if err != nil {
			return err
		}

		fmt.Printf("Got connection...%s\n", con.RemoteAddr().String())
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
					break // All done on this connection
				}

				fds, err := ReceiveFds(c, 1)
				if err != nil {
					// TODO: Log this somewhere...
					return
				}

				go u.HandleUFFD(pid, UFFD(fds[0]))
			}

		}(con)

	}
}

func (u *UserFaultHandler) Close() {
	u.listener.Close()
}

/**
 * Handle faults as they come in
 *
 */
func (u *UserFaultHandler) HandleUFFD(pid uint32, uffd UFFD) error {
	pagesize := os.Getpagesize()
	provideData := func(addr uint64, data []byte) error {
		return u.pushData(uffd, addr, data)
	}

	err := u.initHandler(pid, provideData)
	if err != nil {
		return err
	}

	for {
		fds := []unix.PollFd{{Fd: int32(uffd), Events: unix.POLLIN}}
		_, err = unix.Poll(fds, -1)

		if err != nil {
			return err
		}

		buf := make([]byte, unsafe.Sizeof(C.struct_uffd_msg{}))
		_, err = syscall.Read(int(uffd), buf)

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

		err = u.faultHandler(pid, addr, provideData)
		if err != nil {
			return err
		}
	}
}

func (u *UserFaultHandler) pushData(uffd UFFD, address uint64, data []byte) error {
	cpy := C.struct_uffdio_copy{
		src:  C.ulonglong(uintptr(unsafe.Pointer(&data[0]))),
		dst:  C.ulonglong(address),
		len:  C.ulonglong(len(data)),
		mode: C.ulonglong(0),
		copy: C.longlong(0),
	}

	_, _, errno := syscall.Syscall(syscall.SYS_IOCTL, uintptr(uffd), UFFDIO_COPY, uintptr(unsafe.Pointer(&cpy)))
	if errno != 0 {
		return fmt.Errorf("COPY errno: %d", errno)
	}
	fmt.Printf("pushData %x %d\n", address, len(data))
	return nil
}

/**
 * Receive FDs through unix socket
 *
 */
func ReceiveFds(conn *net.UnixConn, num int) ([]uintptr, error) {
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
