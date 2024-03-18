# Waiting cache

A waiting cache is used at the destination of a migration. When you create `NewWaitingCache` it returns a local and a remote `storage.StorageProvider`.

* Any reads from the local result in a `NeedAt` being sent. It will then WAIT until all the data is available.
* Any writes to the remote result in the data being marked as locally available, and will unblock any reads waiting.
* Any writes to the local result in a `DontNeedAt` being sent, and the block is marked as locally available.
* Reads from remote are unsupported.

It should be noted that only COMPLETE blocks are tracked. For this reason remote `WriteAt`s should be aligned and complete blocks - as from a migrator.