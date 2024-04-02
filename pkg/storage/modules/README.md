## Modules

Modules can be chained together to create various storage workflows / setups.

| Module                | Description |
| --------------------- | ----------- |
| ArtificialLatency     | Adds a fixed latency to reads / writes for testing/benchmarking.            |
| Cache                 | This contains a source, and a store of what data is contained in the cache. If the data is not in the cache for a read, a cache miss error is returned. |
| FilterRedundantWrites | This can filter out any redundant writes (Write data is same as source data), and can also cut large writes into smaller writes which only contains changed data. |
| Lockable              | This can lock a source for some period. |
| Metrics               | General metrics module to track read/write ops,bytes and latency |
| Nothing               | Empty module - /dev/null |
| ReadDirtyTracker      | Similar to a DirtyTracker, but only starts tracking writes to a block after it's been read ONCE. |
| ShardedStorage        | This shards storage into multiple storage, which for example allows concurrent writes across shards. |
| Splitter              | This allows the insertion of cache type sources. Any cache hit reads return immediately, whilst misses result in a read from the source, which then also gets written to any of the cache type sources. |
| CopyOnWrite           | This allows a read-only source, and a cache. The cache is lazily filled as blocks are written. |