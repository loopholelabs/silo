# Silo protocol

## Usage

To use Silo connections, you must first use `ToProtocol` and `FromProtocol` to serialize and deserialize storage requests to the protocol.
Both of these take a `Protocol` argument. One such implementation of `Protocol` is `ProtocolRW` which can use any `Reader` and `Writer` to communicate.

So you can easily use a TCP connection, pipe, QUIC connection etc etc.

The protocol itself is a bidirectional packet based protocol.

There is a source (src), and a destination (dst) in a Silo connection for migration.

All packets contain a DeviceID, and a transactionID.
Several Devices can be multiplexed down a single connection, and several operations for example WriteAt can operate concurrently.

## Simple example

    r1, w1 := io.Pipe()
  	r2, w2 := io.Pipe()

    destDev := make(chan uint32, 8)

    prSource := NewProtocolRW(context.TODO(), r1, w2, nil)
    prDest := NewProtocolRW(context.TODO(), r2, w1, func(p Protocol, dev uint32) {
      destDev <- dev
    })

    sourceToProtocol := NewToProtocol(uint64(size), 1, prSource)

    storeFactory := func(di *DevInfo) storage.StorageProvider {
      store = sources.NewMemoryStorage(int(di.Size))
      return store
    }

    destFromProtocol := NewFromProtocol(1, storeFactory, prDest)

There's a lot going on here, so lets go through what is happening.
First we setup a couple of `io.Pipe()` to simulate some transport.
We then wrap these pipes in `NewProtocolRW()`. The last argument here is a callback when a new device is detected (Any packet is received on a device not seen before). This callback can be used to start processing requests for that device for example. (In the example we just write to a channel).

Next up, we create a `NewToProtocol()`. This implements `storage.StorageProvider` and can be used as storage within silo. Any storage requests are then serialized and sent through the protocol.

Finally, we have a `NewFromProtocol()`, which deserializes storage requests.
When a `DevInfo` is received, the storeFactory callback is called to initialize some `storage.StorageProvider` to handle subsequent storage requests.



## Low level protocol

The first packet for a device should always be a DevInfo packet.

| Packet                | Direction | Description |
| --------------------- | --------- | ----------- |
| DevInfo               | src->dst  | Provides device information. eg size. |
| DirtyList             | src->dst  | List of blocks that are dirty and should be cache invalidated. |
| DontNeedAt            | dst->src  | A range of data that we do not need. eg send if we write. |
| Event                 | src->dst  | Event notifications for lock/unlock/complete. Things wait for an EventResponse. |
| EventResponse         | dst->src  | Confirmation that the event has been acknowledged, and it's safe to continue. |
| NeedAt                | dst->src  | A range of data that we need ASAP |
| ReadAt                | src->dst  | Remote read. Unused for migration. |
| ReadAtResponse        | dst->src  | Result of the ReadAt |
| WriteAt               | src->dst  | Remote write. Migration data is sent here. |
| WriteAtResponse       | dst->src  | Result of the WriteAt / confirmation. |

## Example migration conversation

The following would be a very minimal migration conversation

| Direction | Data |
| --------- | ---- |
| src->dst  | DevInfo size=1024 |
| src->dst  | WriteAt offset=0 length=1024 |
| dst->src  | WriteAtResponse ok |
| src->dst  | Event Complete |

## More complex conversation

| Direction | Data | Comments |
| --------- | ---- | -------- |
| src->dst  | DevInfo size=4096 | Device information |
| src->dst  | WriteAt offset=0 length=1024 | First block of data |
| dst->src  | NeedAt offset=2048 length=1024 | Prioritize this data |
| dst->src  | WriteAtResponse ok | Write ack |
| src->dst  | WriteAt offset=2048 length=1024 | Priority block of data |
| dst->src  | WriteAtResponse ok | Write ack |
| src->dst  | WriteAt offset=1024 length=1024 | More data |
| src->dst  | WriteAt offset=3072 length=1024 | More data |
| dst->src  | WriteAtResponse ok | Write ack |
| dst->src  | WriteAtResponse ok | Write ack |
| src->dst  | Event Complete |
