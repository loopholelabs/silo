# Silo protocol

There is a source (src), and a destination (dst) in a Silo connection for migration.

All packets contain a DeviceID, and a transactionID.
Several Devices can be multiplexed down a single connection, and several operations for example WriteAt can operate concurrently.

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
