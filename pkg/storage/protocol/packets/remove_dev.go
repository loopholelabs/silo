package packets

import (
	"errors"
)

func EncodeRemoveDev() []byte {
	buff := make([]byte, 1)
	buff[0] = COMMAND_REMOVE_DEV
	return buff
}

func DecodeRemoveDev(buff []byte) error {
	if buff == nil || len(buff) < 1 || buff[0] != COMMAND_REMOVE_DEV {
		return errors.New("Invalid packet")
	}
	return nil
}
