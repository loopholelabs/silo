package criu

import (
	"fmt"
	"io"
	"net"
)

type PageServer struct {
	Close       func(iov *PageServerIOV)
	GetPageData func(iov *PageServerIOV, buffer []byte)
	AddPageData func(iov *PageServerIOV, buffer []byte)
	Open2       func(iov *PageServerIOV) bool
	Parent      func(iov *PageServerIOV) bool
}

func NewPageServer() *PageServer {
	return &PageServer{}
}

/**
 * Handle a page server connection
 *
 */
func (ps *PageServer) Handle(conn net.Conn) error {
	for {
		buffer := make([]byte, 24)
		_, err := io.ReadFull(conn, buffer)
		if err != nil {
			return err
		}

		i := Decode(buffer)
		fmt.Printf("-> %s\n", i.String())

		if i.Command() == PS_IOV_GET {
			databuffer := make([]byte, i.Page_count*PAGE_SIZE)
			ps.GetPageData(i, databuffer)

			i.SetCommand(PS_IOV_ADD_F)
			ret := i.Encode()
			_, err := conn.Write(ret)
			if err != nil {
				return err
			}

			_, err = conn.Write(databuffer)
			if err != nil {
				return err
			}

		} else if i.Command() == PS_IOV_ADD_F {
			databuffer := make([]byte, i.Page_count*PAGE_SIZE)
			_, err := io.ReadFull(conn, databuffer)
			if err != nil {
				return err
			}
			ps.AddPageData(i, databuffer)
		} else if i.Command() == PS_IOV_OPEN2 {
			// byte signifies if we have parent
			b := make([]byte, 1)
			if ps.Open2(i) {
				b[0] = 1
			}
			_, err = conn.Write(b)
			if err != nil {
				return err
			}
		} else if i.Command() == PS_IOV_PARENT {
			b := make([]byte, 4)

			if ps.Parent(i) {
				b[0] = 1
			}
			_, err = conn.Write(b)
			if err != nil {
				return err
			}
		} else if i.Command() == PS_IOV_CLOSE {
			// status 4 bytes
			b := make([]byte, 4)

			ps.Close(i)
			_, err = conn.Write(b)
			if err != nil {
				return err
			}
		}
	}
}
