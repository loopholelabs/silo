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
	Name               string
	Tracker            *dirtytracker.DirtyTrackerRemote // A dirty block tracker
	Lockable           storage.LockableStorageProvider  // Lockable
	Locker_handler     func()
	Unlocker_handler   func()
	Destination        storage.StorageProvider
	Orderer            *blocks.PriorityBlockOrder // Block orderer
	Dirty_check_period time.Duration
	Dirty_block_getter func() []uint

	//	getter := func() []uint {
	//		return Tracker.GetDirtyBlocks(Dirty_block_max_age, Dirty_limit, Dirty_block_shift, Dirty_min_changed)
	//	}
	// Dirty_block_max_age time.Duration
	// Dirty_limit         int
	// Dirty_block_shift   int
	// Dirty_min_changed   int

	Block_size int

	Hashes_handler   func(map[uint][32]byte)
	Progress_handler func(p *MigrationProgress)
	Error_handler    func(b *storage.BlockInfo, err error)

	Concurrency   map[int]int
	Integrity     bool
	Cancel_writes bool
	Dedupe_writes bool
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
func Sync(ctx context.Context, sinfo *Sync_config, sync_all_first bool, continuous bool) error {
	conf := NewMigratorConfig().WithBlockSize(sinfo.Block_size)
	conf.Locker_handler = func() {
		if sinfo.Locker_handler != nil {
			sinfo.Locker_handler()
		} else {
			sinfo.Lockable.Lock()
		}
	}
	conf.Unlocker_handler = func() {
		if sinfo.Unlocker_handler != nil {
			sinfo.Unlocker_handler()
		} else {
			sinfo.Lockable.Unlock()
		}
	}
	conf.Concurrency = map[int]int{
		storage.BlockTypeAny: 16,
	}
	if sinfo.Concurrency != nil {
		conf.Concurrency = sinfo.Concurrency
	}

	conf.Integrity = sinfo.Integrity
	conf.Cancel_writes = sinfo.Cancel_writes
	conf.Dedupe_writes = sinfo.Dedupe_writes

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

		if sinfo.Hashes_handler != nil {
			hashes := mig.GetHashes()
			sinfo.Hashes_handler(hashes)
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
		blocks := mig.GetLatestDirtyFunc(sinfo.Dirty_block_getter)

		if blocks != nil {
			err = mig.MigrateDirty(blocks)
			if err != nil {
				return err
			}
		} else {
			if !continuous {
				// We are done! Everything is synced, and the source is locked.
				return nil
			}
			mig.Unlock()
		}
		time.Sleep(sinfo.Dirty_check_period)
	}
}
