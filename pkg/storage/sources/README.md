# Sources

## Memory

MemoryStorage can be setup using `NewMemoryStorage(size)`. A RWMutex is used to ensure safety. If you wish to support concurrent writes, one way would be to use a `ShardedStorage` module to split the memory into several blocks which can then be written to concurrently.

## File

FileStorage can be setup using `NewFileStorage(f, size)` for an existing file, and `NewFileStorageCreate(f, size)` if you wish to create a new file.

## FileSparse

FileStorageSparse can be setup using `NewFileStorageSparseCreate(f, size, blockSize)` for creating a new file. Only blocks which have been written to are stored in the file. Only supports reads for blocks that have already been written. Partial block reads are supported. Partial block writes are discarded.
