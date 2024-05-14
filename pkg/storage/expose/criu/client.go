package criu

import (
	"errors"
	"io"
	"net"
)

type PageRequest struct {
	Pid   uint32
	Vaddr uint64
	Pages uint32
}

type PageClient struct {
}

func NewPageClient() (*PageClient, error) {

	return &PageClient{}, nil
}

// This can be used to connect to a LAZY PAGES server to get missing pages
func (pc *PageClient) Connect(addr string, requests []*PageRequest, cb func(uint32, uint64, []byte)) error {
	con, err := net.Dial("tcp", addr)
	if err != nil {
		return err
	}
	defer con.Close()

	// Now do some requests...

	for _, req := range requests {
		iov_get := PageServerIOV{
			Cmd:        PS_IOV_GET,
			Dst_id:     uint64(req.Pid), // NOTE: Different to page server
			Vaddr:      req.Vaddr,
			Page_count: req.Pages,
		}
		d := iov_get.Encode()

		_, err = con.Write(d)
		if err != nil {
			return err
		}

		// Wait for a response...
		buffer := make([]byte, 24)
		_, err := io.ReadFull(con, buffer)
		if err != nil {
			return err
		}

		i := Decode(buffer)
		if i.Command() != PS_IOV_ADD_F {
			return errors.New("Expecting ADD_F")
		}
		databuffer := make([]byte, PAGE_SIZE*i.Page_count)
		_, err = io.ReadFull(con, databuffer)
		if err != nil {
			return err
		}

		cb(req.Pid, req.Vaddr, databuffer)
	}

	iov_close := PageServerIOV{
		Cmd:        PS_IOV_CLOSE,
		Dst_id:     0,
		Vaddr:      0,
		Page_count: 0,
	}
	d := iov_close.Encode()

	_, err = con.Write(d)
	if err != nil {
		return err
	}

	status := make([]byte, 4)
	_, err = con.Read(status)
	if err != nil {
		return err
	}

	//	fmt.Printf("Close status %x\n", status)

	// TODO: Check status is 0

	return nil
}
