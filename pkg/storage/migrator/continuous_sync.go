package migrator

import (
	"context"
	"time"

	"github.com/loopholelabs/silo/pkg/storage"
	"github.com/loopholelabs/silo/pkg/storage/blocks"
	"github.com/loopholelabs/silo/pkg/storage/dirtytracker"

	"github.com/rs/zerolog/log"
)

type Sync_config struct {
	Name     string
	Tracker  *dirtytracker.DirtyTrackerRemote // A dirty block tracker
	Lockable storage.LockableStorageProvider  // Lockable
	Orderer  *blocks.PriorityBlockOrder       // Block orderer

	Block_size int

	Destination storage.StorageProvider

	Progress_handler func(p *MigrationProgress)
	Error_handler    func(b *storage.BlockInfo, err error)

	// Dirty block params
	Dirty_check_period  time.Duration
	Dirty_block_max_age time.Duration
	Dirty_limit         int
	Dirty_block_shift   int
	Dirty_min_changed   int
}

/*
	// periodically show status?
	ood := sinfo.Tracker.MeasureDirty()
	ood_age := sinfo.Tracker.MeasureDirtyAge()
	fmt.Printf("DIRTY STATUS %dms old, with %d blocks\n", time.Since(ood_age).Milliseconds(), ood)
*/

/**
 *
 *
 */
func Continuous_sync(ctx context.Context, sinfo *Sync_config, sync_all_first bool) error {
	conf := NewMigratorConfig().WithBlockSize(sinfo.Block_size)
	conf.Locker_handler = func() {
		sinfo.Lockable.Lock()
	}
	conf.Unlocker_handler = func() {
		sinfo.Lockable.Unlock()
	}
	conf.Concurrency = map[int]int{
		storage.BlockTypeAny: 16,
	}
	conf.Integrity = false
	conf.Cancel_writes = true
	conf.Dedupe_writes = true

	conf.Progress_handler = func(p *MigrationProgress) {
		log.Info().
			Str("name", sinfo.Name).
			Float64("migrated_blocks_perc", p.Migrated_blocks_perc).
			Int("ready_blocks", p.Ready_blocks).
			Int("total_blocks", p.Total_blocks).
			Float64("ready_blocks_perc", p.Ready_blocks_perc).
			Int("active_blocks", p.Active_blocks).
			Int("total_migrated_blocks", p.Total_Migrated_blocks).
			Int("total_canceled_blocks", p.Total_Canceled_blocks).
			Int("total_duplicated_blocks", p.Total_Duplicated_blocks).
			Msg("Continuous sync progress")
		if sinfo.Progress_handler != nil {
			sinfo.Progress_handler(p)
		}
	}
	conf.Error_handler = func(b *storage.BlockInfo, err error) {
		log.Error().
			Str("name", sinfo.Name).
			Err(err).
			Int("block", b.Block).
			Int("type", b.Type).
			Msg("Continuous sync error")
		if sinfo.Error_handler != nil {
			sinfo.Error_handler(b, err)
		}
	}

	mig, err := NewMigrator(sinfo.Tracker, sinfo.Destination, sinfo.Orderer, conf)
	if err != nil {
		return err
	}

	num_blocks := (sinfo.Tracker.Size() + uint64(sinfo.Block_size) - 1) / uint64(sinfo.Block_size)

	if sync_all_first {
		// Now do the initial migration...
		err = mig.Migrate(int(num_blocks))
		if err != nil {
			return err
		}

		// Wait for completion.
		err = mig.WaitForCompletion()
		if err != nil {
			return err
		}
	} else {
		// We don't need to do an initial migration.
		for b := 0; b < int(num_blocks); b++ {
			mig.SetMigratedBlock(b)
		}
		// Track changes for everything.
		sinfo.Tracker.TrackAt(0, int64(sinfo.Tracker.Size()))
	}

	// Now enter a loop looking for more dirty blocks to migrate...

	// Get Dirty Blocks
	getter := func() []uint {
		return sinfo.Tracker.GetDirtyBlocks(sinfo.Dirty_block_max_age, sinfo.Dirty_limit, sinfo.Dirty_block_shift, sinfo.Dirty_min_changed)
	}

	for {
		select {
		case <-ctx.Done():
			// Context has been cancelled. We should wait for any pending migrations to complete
			err = mig.WaitForCompletion()
			if err != nil {
				return err
			}
			return ctx.Err()
		default:
		}
		blocks := mig.GetLatestDirtyFunc(getter)

		if blocks != nil {
			err = mig.MigrateDirty(blocks)
			if err != nil {
				return err
			}
		} else {
			mig.Unlock()
		}
		time.Sleep(sinfo.Dirty_check_period)
	}
}
