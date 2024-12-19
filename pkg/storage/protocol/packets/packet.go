package packets

import "errors"

var ErrInvalidPacket = errors.New("invalid packet")
var ErrReadError = errors.New("remote read error")
var ErrWriteError = errors.New("remote write error")

const CommandRequest = byte(0)
const CommandResponse = byte(0x80)

const (
	CommandReadAt           = CommandRequest | byte(1)
	CommandWriteAt          = CommandRequest | byte(2)
	CommandNeedAt           = CommandRequest | byte(3)
	CommandDontNeedAt       = CommandRequest | byte(4)
	CommandDirtyList        = CommandRequest | byte(5)
	CommandDevInfo          = CommandRequest | byte(6)
	CommandEvent            = CommandRequest | byte(7)
	CommandHashes           = CommandRequest | byte(8)
	CommandWriteAtWithMap   = CommandRequest | byte(9)
	CommandRemoveDev        = CommandRequest | byte(10)
	CommandRemoveFromMap    = CommandRequest | byte(11)
	CommandAlternateSources = CommandRequest | byte(12)
	CommandDeviceGroupInfo  = CommandRequest | byte(13)
)

const (
	CommandReadAtResponse     = CommandResponse | byte(1)
	CommandReadAtResponseErr  = CommandResponse | byte(2)
	CommandWriteAtResponse    = CommandResponse | byte(3)
	CommandWriteAtResponseErr = CommandResponse | byte(4)
	CommandEventResponse      = CommandResponse | byte(5)
	CommandHashesResponse     = CommandResponse | byte(6)
	CommandDirtyListResponse  = CommandResponse | byte(7)
)

func IsResponse(cmd byte) bool {
	return (cmd & CommandResponse) == CommandResponse
}
