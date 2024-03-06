# Dirty tracker

In order to do a migration, we need to keep track of writes, and which areas are 'dirty'. We can do this with a `DirtyTracker`.

## Usage

When you create a `NewDirtyTracker` you will get back a local and a remote.
The remote will be used in migration, and the local will be used for local consumption.

* When a remote block is read, tracking is enabled for that block.
* When a local write is written to a block with tracking enabled, that block is set to dirty.
* When `Sync()` is called on the remote, a snapshot is taken on the dirty blocks. All tracking is disabled, and all blocks set to clean.
