package expose

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"

	"github.com/loopholelabs/silo/pkg/storage"
)

type DispatchSimple struct{}

func NewDispatchSimple() *DispatchSimple {
	return &DispatchSimple{}
}

func (d *DispatchSimple) Name() string {
	return "SimpleDispatch"
}

func (d *DispatchSimple) Wait() {}

/**
 * This dispatches incoming NBD requests sequentially to the provider.
 *
 */
func (d *DispatchSimple) Handle(fd int, prov storage.StorageProvider) error {
	fp := os.NewFile(uintptr(fd), "unix")

	request := Request{}
	response := Response{}
	buf := make([]byte, 28)
	for {
		_, err := fp.Read(buf)
		if err != nil {
			return err
		}

		request.Magic = binary.BigEndian.Uint32(buf)
		request.Type = binary.BigEndian.Uint32(buf[4:8])
		request.Handle = binary.BigEndian.Uint64(buf[8:16])
		request.From = binary.BigEndian.Uint64(buf[16:24])
		request.Length = binary.BigEndian.Uint32(buf[24:28])

		chunk := make([]byte, 0)
		response.Handle = request.Handle
		response.Error = 0

		if request.Magic != NBD_REQUEST_MAGIC {
			return fmt.Errorf("Received invalid MAGIC")
		}
		if request.Type == NBD_CMD_DISCONNECT {
			//			fmt.Printf("CMD_DISCONNECT")
			return nil // All done
		} else if request.Type == NBD_CMD_FLUSH {
			return fmt.Errorf("Not supported: Flush")
		} else if request.Type == NBD_CMD_READ {
			chunk = make([]byte, request.Length)

			// Ask the provider for some data
			_, e := prov.ReadAt(chunk, int64(request.From))
			if e != nil {
				response.Error = 1 // FIXME
			}
		} else if request.Type == NBD_CMD_TRIM {
			// Ask the provider
			//			e := prov.Trim(request.From, request.Length)
			//			if e != storage.StorageError_SUCCESS {
			//				response.Error = 1 // FIXME
			//			}
		} else if request.Type == NBD_CMD_WRITE {
			data := make([]byte, request.Length)

			_, err = io.ReadFull(fp, data)
			if err != nil {
				return err
			}

			// Ask the provider
			_, e := prov.WriteAt(data, int64(request.From))
			if e != nil {
				response.Error = 1 // FIXME
			}
		} else {
			return fmt.Errorf("NBD Not implemented %d\n", request.Type)
		}

		// Write a response...
		buff := make([]byte, 16)
		binary.BigEndian.PutUint32(buff, NBD_RESPONSE_MAGIC)
		binary.BigEndian.PutUint32(buff[4:], response.Error)
		binary.BigEndian.PutUint64(buff[8:], response.Handle)

		_, err = fp.Write(buff)
		if err != nil {
			return err
		}
		if len(chunk) > 0 {
			_, err = fp.Write(chunk)
			if err != nil {
				return err
			}
		}
	}
}
