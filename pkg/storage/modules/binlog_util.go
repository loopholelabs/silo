package modules

import "github.com/loopholelabs/silo/pkg/storage"

func CreateBinlogFromDevice(source storage.Provider, dest storage.Provider, blockSize int) error {
	buffer := make([]byte, blockSize)

	for offset := int64(0); offset < int64(source.Size()); offset += int64(blockSize) {
		n, err := source.ReadAt(buffer, offset)
		if err != nil {
			return err
		}

		// Check if it's all zeros
		empty := true
		for f := 0; f < n; f++ {
			if buffer[f] != 0 {
				empty = false
				break
			}
		}

		if !empty {
			_, err = dest.WriteAt(buffer[:n], offset)
			if err != nil {
				return err
			}
		}
	}
	return nil
}
