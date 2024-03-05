# Migrator

A migrator manages migrating storage from a source to a destination.
It does not know anything about networks or protocols. If you need a network and protocol you should also look at `toProtocol` `fromProtocol` and `protocolRW`.

## Functionality


    NewMigrator(source storage.TrackingStorageProvider,
	  dest storage.StorageProvider,
	  block_order storage.BlockOrder,
	  config *MigratorConfig) (*Migrator, error)


First we have a `storage.TrackingStorageProvider`. This serves as a source for data. It also tracks dirty blocks with a single `sync` function which returns a bitfield of dirty blocks.

Next we have a destination `storage.StorageProvider` which is where the data will be written to. Typically you may want this to be a `ToProtocol` which will serialize calls through a `Protocol`.

Next we have a `storage.BlockOrder` which tells the migrator which order it should send blocks. A typical block orderer would be a `PriorityOrderer` chained to a `VolatilityMonitor`. In that way, we can send any priority blocks (Blocks the destination needs ASAP), and then we can send blocks from least volatile to most.

Let's look at the `MigratorConfig`.

    type MigratorConfig struct {
	  BlockSize       int
	  LockerHandler   func()
	  UnlockerHandler func()
	  ErrorHandler    func(b *storage.BlockInfo, err error)
	  ProgressHandler func(p *MigrationProgress)
	  Concurrency     map[int]int
    }

`BlockSize` This is the size of a block in migration. It can be anything you wish.

`LockerHandler` and `UnlockerHandler` These functions are called to lock and unlock the source. That is, to pause whatever is writing to the source. You can use a `Lockable` here to just stop the writes, or ideally you can pause/resume whatever is doing the writes. You should also flush writes.

`ErrorHandler` This function is called if there was an error migrating a block. That is, there was an error reading or writing the block. In this instance, you might want to check things over, and re-schedule the block by adding it back into the `blockOrderer`.

`ProgressHandler` This callback allows you to monitor progress of the migration. It's called each time a block is migrated.

`Concurrency` This map defines how many blocks of each type can be migrated concurrently.

## Usage

Once you have a `Migrator` you can call `.Migrate(numBlocks)` to migrate some blocks. Note that this call may block depending on concurrency settings.
Also it will return before migration is completed.
To ensure migration is completed call `.WaitForCompletion()`.

If you'd like to re-migrate some dirty blocks, you can call `.GetLatestDirty()` and then `.MigrateDirty()`. Once again, this call may block depending on concurrency, and you should again call `.WaitForCompletion()` to ensure the migration has completed.

## Simple example

    blockSize := 65536
    numBlocks := 1024
    source := sources.NewMemoryStorage(blockSize * numBlocks)
    sourceDirty := modules.NewFilterReadDirtyTracker(source, blockSize)

    orderer := blocks.NewAnyBlockOrder(numBlocks, nil)
    orderer.AddAll()

    dest := sources.NewMemoryStorage(blockSize * numBlocks)

    conf := NewMigratorConfig().WithBlockSize(blockSize)

    mig, err := NewMigrator(sourceDirty, dest, orderer, conf)

    mig.Migrate(numBlocks)

    err = mig.WaitForCompletion()

This simple example migrates storage from source to dest.