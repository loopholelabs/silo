package migrator

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/loopholelabs/silo/pkg/storage"
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
	Orderer            storage.BlockOrder
	Dirty_check_period time.Duration
	Dirty_block_getter func() []uint

	Destination_content_check func(offset int, data []byte) bool

	// NB If you use dirty_block_shift here, you'll need to also shift the block size in DirtyTracker constructor
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

type Syncer struct {
	ctx               context.Context
	config            *Sync_config
	block_status_lock sync.Mutex
	block_status      []uint64
	block_updates     []uint64
	current_dirty_id  uint64
}

func NewSyncer(ctx context.Context, sinfo *Sync_config) *Syncer {
	num_blocks := (sinfo.Tracker.Size() + uint64(sinfo.Block_size) - 1) / uint64(sinfo.Block_size)

	return &Syncer{
		ctx:              ctx,
		config:           sinfo,
		block_status:     make([]uint64, num_blocks),
		block_updates:    make([]uint64, num_blocks),
		current_dirty_id: 0,
	}
}

/**
 * Get a list of blocks that are safe (On the destination)
 * NB This does not include the very latest dirty list, but it's a good starting point.
 */
func (s *Syncer) GetSafeBlockMap() []uint {
	blocks := make([]uint, 0)

	for b, id := range s.block_status {
		if id == s.block_updates[b] {
			blocks = append(blocks, uint(b))
		}
	}
	return blocks
}

/**
 *
 *
 */
func (s *Syncer) Sync(sync_all_first bool, continuous bool) (*MigrationProgress, error) {
	conf := NewMigratorConfig().WithBlockSize(s.config.Block_size)
	conf.Locker_handler = func() {
		if s.config.Locker_handler != nil {
			s.config.Locker_handler()
		} else {
			s.config.Lockable.Lock()
		}
	}
	conf.Unlocker_handler = func() {
		if s.config.Unlocker_handler != nil {
			s.config.Unlocker_handler()
		} else {
			s.config.Lockable.Unlock()
		}
	}
	conf.Concurrency = map[int]int{
		storage.BlockTypeAny: 16,
	}
	if s.config.Concurrency != nil {
		conf.Concurrency = s.config.Concurrency
	}

	conf.Integrity = s.config.Integrity
	conf.Cancel_writes = s.config.Cancel_writes
	conf.Dedupe_writes = s.config.Dedupe_writes

	conf.Dest_content_check = s.config.Destination_content_check

	conf.Progress_handler = func(p *MigrationProgress) {
		log.Info().
			Str("name", s.config.Name).
			Float64("migrated_blocks_perc", p.Migrated_blocks_perc).
			Int("ready_blocks", p.Ready_blocks).
			Int("total_blocks", p.Total_blocks).
			Float64("ready_blocks_perc", p.Ready_blocks_perc).
			Int("active_blocks", p.Active_blocks).
			Int("total_migrated_blocks", p.Total_Migrated_blocks).
			Int("total_canceled_blocks", p.Total_Canceled_blocks).
			Int("total_duplicated_blocks", p.Total_Duplicated_blocks).
			Int("total_unrequired_blocks", p.Total_Unrequired_blocks).
			Msg("Continuous sync progress")
		if s.config.Progress_handler != nil {
			s.config.Progress_handler(p)
		}
	}
	conf.Error_handler = func(b *storage.BlockInfo, err error) {
		log.Error().
			Str("name", s.config.Name).
			Err(err).
			Int("block", b.Block).
			Int("type", b.Type).
			Msg("Continuous sync error")
		if s.config.Error_handler != nil {
			s.config.Error_handler(b, err)
		}
	}

	// When a block is written, update block_status with the largest ID for that block
	conf.Block_handler = func(b *storage.BlockInfo, id uint64, data []byte) {
		// TODO: Hash the data here and store...
		s.block_status_lock.Lock()
		defer s.block_status_lock.Unlock()
		if id > s.block_status[b.Block] {
			s.block_status[b.Block] = id
		}
	}

	mig, err := NewMigrator(s.config.Tracker, s.config.Destination, s.config.Orderer, conf)
	if err != nil {
		return nil, err
	}

	num_blocks := (s.config.Tracker.Size() + uint64(s.config.Block_size) - 1) / uint64(s.config.Block_size)

	if sync_all_first {
		// Now do the initial migration...
		err = mig.Migrate(int(num_blocks))
		if err != nil {
			return nil, err
		}

		// Wait for completion.
		err = mig.WaitForCompletion()
		if err != nil {
			return nil, err
		}

		if s.config.Hashes_handler != nil {
			hashes := mig.GetHashes()
			s.config.Hashes_handler(hashes)
		}
	} else {
		// We don't need to do an initial migration.
		for b := 0; b < int(num_blocks); b++ {
			mig.SetMigratedBlock(b)
		}
		// Track changes for everything.
		s.config.Tracker.TrackAt(0, int64(s.config.Tracker.Size()))
	}

	// Now enter a loop looking for more dirty blocks to migrate...

	for {
		select {
		case <-s.ctx.Done():
			// Context has been cancelled. We should wait for any pending migrations to complete
			err = mig.WaitForCompletion()
			if err != nil {
				return mig.Status(), err
			}
			return mig.Status(), s.ctx.Err()
		default:
		}
		blocks := mig.GetLatestDirtyFunc(s.config.Dirty_block_getter)

		if blocks != nil {
			id := atomic.AddUint64(&s.current_dirty_id, 1)
			// Update block_updates with the new ID for these blocks
			s.block_status_lock.Lock()
			for _, b := range blocks {
				s.block_updates[b] = id
			}
			s.block_status_lock.Unlock()
			err = mig.MigrateDirtyWithId(blocks, id)
			if err != nil {
				return mig.Status(), err
			}
		} else {
			if !continuous {
				// We are done! Everything is synced, and the source is locked.
				err = mig.WaitForCompletion()
				return mig.Status(), err
			}
			mig.Unlock()
		}
		time.Sleep(s.config.Dirty_check_period)
	}
}
