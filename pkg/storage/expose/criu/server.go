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
	page_adds_parent := 0
	page_adds_present := 0
	page_adds_lazy := 0
	page_added_data := 0

	defer func() {
		//		fmt.Printf("Handle finished for %s\n", conn.RemoteAddr().String())
		fmt.Printf("AddPageData parent(%d) present(%d) lazy(%d) - New data = %d bytes\n", page_adds_parent, page_adds_present, page_adds_lazy, page_added_data)
		conn.Close()
	}()

	for {
		buffer := make([]byte, 24)
		_, err := io.ReadFull(conn, buffer)
		if err != nil {
			return err
		}

		i := Decode(buffer)

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
			var databuffer []byte

			if i.FlagsParent() {
				page_adds_parent++
			}
			if i.FlagsPresent() {
				page_adds_present++
			}
			if i.FlagsLazy() {
				page_adds_lazy++
			}

			if i.FlagsPresent() {
				databuffer = make([]byte, i.Page_count*PAGE_SIZE)
				page_added_data += len(databuffer)
				_, err := io.ReadFull(conn, databuffer)
				if err != nil {
					return err
				}
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
