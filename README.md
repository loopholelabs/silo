# silo
Silo is a general purpose storage engine. One of the core functionalities within Silo is the ability to migrate/sync storage while it is still in use.

## Sources

All storage sources within Silo implement `storage.StorageProvider`. You can find some example sources at [pkg/storage/sources](pkg/storage/sources/README.md).

## Expose

If you wish to expose a Silo storage device to an external consumer, one way would be to use the NBD kernel driver. See [pkg/expose/sources](pkg/storage/expose/README.md).

## Block orders

When you wish to move storage from one place to another, you'll need to specify an order. This can be dynamically changing. For example, there is a volatility monitor which can be used to migrate storage from least volatile to most volatile. Also you may wish to prioritize certain blocks for example if the destination is trying to read them. [pkg/storage/blocks](pkg/storage/blocks/README.md).

## Migration

Migration of storage is handled by a `Migrator`. For more information on this see [pkg/storage/migrator](pkg/storage/migrator/README.md).

Example of a basic migration. Here we have block number on the Y axis, and time on the X axis.
We start out by performing random writes to regions of the storage. We start migration at 500ms and you can see that less volatile blocks are moved first (in blue). Once that is completed, dirty blocks are synced up (in red).

![alt text](./graph.png?raw=true)

This example adds a device reading from the destination. The block order is by least volatile, but with a priority for the blocks needed for reading. You can also see on the graph that the average read latency drops as more of the storage is locally available at dest.

![alt text](./images/example1.png?raw=true)

Same as above, but with concurrency set to 32. As long as the destination can do concurrent writes, everything will flow.

![alt text](./images/example2.png?raw=true)


