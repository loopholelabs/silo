package packets

func EncodeRemoveDev() []byte {
	buff := make([]byte, 1)
	buff[0] = CommandRemoveDev
	return buff
}

func DecodeRemoveDev(buff []byte) error {
	if len(buff) < 1 || buff[0] != CommandRemoveDev {
		return ErrInvalidPacket
	}
	return nil
}
