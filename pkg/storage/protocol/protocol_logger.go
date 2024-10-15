package protocol

import (
	"fmt"
	"io"
	"sync"
	"sync/atomic"

	"github.com/rs/zerolog/log"
)

type Protocol_logger struct {
	name                                  string
	proto                                 Protocol
	metric_total_send_packets             int64
	metric_total_send_packet_writers      int64
	metric_total_wait_for_packets         int64
	metric_total_wait_for_commands_lock   sync.Mutex
	metric_total_wait_for_commands        map[byte]int64
	metric_pending_send_packets           int64
	metric_pending_send_packet_writers    int64
	metric_pending_wait_for_packets       int64
	metric_pending_wait_for_commands_lock sync.Mutex
	metric_pending_wait_for_commands      map[byte]int64
}

func NewProtocolLogger(name string, proto Protocol) *Protocol_logger {
	return &Protocol_logger{
		proto:                            proto,
		name:                             name,
		metric_total_wait_for_commands:   make(map[byte]int64),
		metric_pending_wait_for_commands: make(map[byte]int64),
	}
}

type Metrics struct {
	Total_send_packets        int64
	Total_send_packet_writers int64
	Total_wait_for_packets    int64
	Total_wait_for_commands   map[byte]int64

	Pending_send_packets        int64
	Pending_send_packet_writers int64
	Pending_wait_for_packets    int64
	Pending_wait_for_commands   map[byte]int64
}

func (m *Metrics) String() string {
	totals := fmt.Sprintf("total send_packets %d send_packet_writers %d wait_for_packets %d wait_for_commands %v",
		m.Total_send_packets, m.Total_send_packet_writers, m.Total_wait_for_packets, m.Total_wait_for_commands)

	pendings := fmt.Sprintf("pending send_packets %d send_packet_writers %d wait_for_packets %d wait_for_commands %v",
		m.Pending_send_packets, m.Pending_send_packet_writers, m.Pending_wait_for_packets, m.Pending_wait_for_commands)

	return fmt.Sprintf("Metrics(%s)(%s)\n", totals, pendings)
}

func (pl *Protocol_logger) Metrics() *Metrics {
	total_cmds := make(map[byte]int64)
	pl.metric_total_wait_for_commands_lock.Lock()
	for c, v := range pl.metric_total_wait_for_commands {
		total_cmds[c] = v
	}
	pl.metric_total_wait_for_commands_lock.Unlock()

	pending_cmds := make(map[byte]int64)
	pl.metric_pending_wait_for_commands_lock.Lock()
	for c, v := range pl.metric_pending_wait_for_commands {
		pending_cmds[c] = v
	}
	pl.metric_pending_wait_for_commands_lock.Unlock()

	return &Metrics{
		Total_send_packets:        atomic.LoadInt64(&pl.metric_total_send_packets),
		Total_send_packet_writers: atomic.LoadInt64(&pl.metric_total_send_packet_writers),
		Total_wait_for_packets:    atomic.LoadInt64(&pl.metric_total_wait_for_packets),
		Total_wait_for_commands:   total_cmds,

		Pending_send_packets:        atomic.LoadInt64(&pl.metric_pending_send_packets),
		Pending_send_packet_writers: atomic.LoadInt64(&pl.metric_pending_send_packet_writers),
		Pending_wait_for_packets:    atomic.LoadInt64(&pl.metric_pending_wait_for_packets),
		Pending_wait_for_commands:   pending_cmds,
	}
}

func (pl *Protocol_logger) SendPacket(dev uint32, id uint32, data []byte) (uint32, error) {
	log.Trace().Str("name", pl.name).Uint32("dev", dev).Uint32("id", id).Int("data length", len(data)).Msg("protocol.SendPacket")
	atomic.AddInt64(&pl.metric_pending_send_packets, 1)
	n, err := pl.proto.SendPacket(dev, id, data)
	atomic.AddInt64(&pl.metric_pending_send_packets, -1)
	atomic.AddInt64(&pl.metric_total_send_packets, 1)
	log.Trace().Str("name", pl.name).Uint32("dev", dev).Uint32("id", id).Int("data length", len(data)).Uint32("n", n).Err(err).Msg("protocol.SendPacket.return")
	return n, err
}

func (pl *Protocol_logger) SendPacketWriter(dev uint32, id uint32, length uint32, data func(w io.Writer) error) (uint32, error) {
	log.Trace().Str("name", pl.name).Uint32("dev", dev).Uint32("id", id).Uint32("data length", length).Msg("protocol.SendPacketWriter")
	atomic.AddInt64(&pl.metric_pending_send_packet_writers, 1)
	n, err := pl.proto.SendPacketWriter(dev, id, length, data)
	atomic.AddInt64(&pl.metric_pending_send_packet_writers, -1)
	atomic.AddInt64(&pl.metric_total_send_packet_writers, 1)
	log.Trace().Str("name", pl.name).Uint32("dev", dev).Uint32("id", id).Uint32("data length", length).Uint32("n", n).Err(err).Msg("protocol.SendPacketWriter.return")
	return n, err
}

func (pl *Protocol_logger) WaitForPacket(dev uint32, id uint32) ([]byte, error) {
	log.Trace().Str("name", pl.name).Uint32("dev", dev).Uint32("id", id).Msg("protocol.WaitForPacket")
	atomic.AddInt64(&pl.metric_pending_wait_for_packets, 1)
	data, err := pl.proto.WaitForPacket(dev, id)
	atomic.AddInt64(&pl.metric_pending_wait_for_packets, -1)
	atomic.AddInt64(&pl.metric_total_wait_for_packets, 1)
	log.Trace().Str("name", pl.name).Uint32("dev", dev).Uint32("id", id).Int("data length", len(data)).Err(err).Msg("protocol.WaitForPacket.return")
	return data, err
}

func (pl *Protocol_logger) WaitForCommand(dev uint32, cmd byte) (uint32, []byte, error) {
	log.Trace().Str("name", pl.name).Uint32("dev", dev).Uint8("cmd", cmd).Msg("protocol.WaitForCommand")
	add_map(pl.metric_pending_wait_for_commands, cmd, &pl.metric_pending_wait_for_commands_lock, 1)
	id, data, err := pl.proto.WaitForCommand(dev, cmd)
	add_map(pl.metric_pending_wait_for_commands, cmd, &pl.metric_pending_wait_for_commands_lock, -1)
	add_map(pl.metric_total_wait_for_commands, cmd, &pl.metric_total_wait_for_commands_lock, 1)

	log.Trace().Str("name", pl.name).Uint32("dev", dev).Uint8("cmd", cmd).Uint32("id", id).Int("data length", len(data)).Err(err).Msg("protocol.WaitForCommand.return")
	return id, data, err
}

func add_map(m map[byte]int64, index byte, locker *sync.Mutex, delta int64) {
	locker.Lock()
	defer locker.Unlock()
	_, ok := m[index]
	if ok {
		m[index] += delta
	} else {
		m[index] = delta
	}
}
