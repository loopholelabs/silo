package migrator

import (
	"context"
	"crypto/sha256"
	"sync"
	"sync/atomic"
	"time"

	"github.com/loopholelabs/silo/pkg/storage"
	"github.com/loopholelabs/silo/pkg/storage/dirtytracker"

	"github.com/loopholelabs/logging/types"
)

type SyncConfig struct {
	Logger           types.Logger
	Name             string
	Tracker          *dirtytracker.Remote     // A dirty block tracker
	Lockable         storage.LockableProvider // Lockable
	LockerHandler    func()
	UnlockerHandler  func()
	Destination      storage.Provider
	Orderer          storage.BlockOrder
	DirtyCheckPeriod time.Duration
	DirtyBlockGetter func() []uint

	BlockSize int

	HashesHandler     func(map[uint][32]byte)
	ProgressHandler   func(p *MigrationProgress)
	ProgressRateLimit time.Duration
	ErrorHandler      func(b *storage.BlockInfo, err error)

	Concurrency  map[int]int
	Integrity    bool
	CancelWrites bool
	DedupeWrites bool
}

type Syncer struct {
	ctx             context.Context
	config          *SyncConfig
	blockStatusLock sync.Mutex
	blockStatus     []BlockStatus
	currentDirtyID  uint64
	migrator        *Migrator
}

type BlockStatus struct {
	Set         bool
	UpdatingID  uint64
	CurrentID   uint64
	CurrentHash [sha256.Size]byte
}

func NewSyncer(ctx context.Context, sinfo *SyncConfig) *Syncer {
	numBlocks := (sinfo.Tracker.Size() + uint64(sinfo.BlockSize) - 1) / uint64(sinfo.BlockSize)

	status := make([]BlockStatus, numBlocks)
	for b := 0; b < int(numBlocks); b++ {
		status[b] = BlockStatus{
			Set:         false,
			UpdatingID:  0,
			CurrentID:   0,
			CurrentHash: [sha256.Size]byte{},
		}
	}

	return &Syncer{
		ctx:            ctx,
		config:         sinfo,
		blockStatus:    status,
		currentDirtyID: 0,
	}
}

/**
 * Get a list of blocks that are safe (On the destination)
 * NB This does not include the very latest dirty list, but it's a good starting point.
 */
func (s *Syncer) GetSafeBlockMap() map[uint][sha256.Size]byte {
	blocks := make(map[uint][sha256.Size]byte, 0)

	for b, status := range s.blockStatus {
		if status.Set && status.CurrentID == status.UpdatingID {
			blocks[uint(b)] = status.CurrentHash
		}
	}
	return blocks
}

func (s *Syncer) GetMetrics() *MigrationProgress {
	if s.migrator == nil {
		return nil
	}
	return s.migrator.GetMetrics()
}

/**
 *
 *
 */
func (s *Syncer) Sync(syncAllFirst bool, continuous bool) (*MigrationProgress, error) {
	conf := NewConfig().WithBlockSize(s.config.BlockSize)
	conf.Logger = s.config.Logger
	conf.LockerHandler = func() {
		if s.config.LockerHandler != nil {
			s.config.LockerHandler()
		} else {
			s.config.Lockable.Lock()
		}
	}
	conf.UnlockerHandler = func() {
		if s.config.UnlockerHandler != nil {
			s.config.UnlockerHandler()
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
	conf.CancelWrites = s.config.CancelWrites
	conf.DedupeWrites = s.config.DedupeWrites

	conf.ProgressRateLimit = s.config.ProgressRateLimit
	conf.ProgressHandler = func(p *MigrationProgress) {
		if s.config.Logger != nil {
			s.config.Logger.Debug().
				Str("name", s.config.Name).
				Float64("migrated_blocks_perc", p.MigratedBlocksPerc).
				Int("ready_blocks", p.ReadyBlocks).
				Int("total_blocks", p.TotalBlocks).
				Float64("ready_blocks_perc", p.ReadyBlocksPerc).
				Int("active_blocks", p.ActiveBlocks).
				Int("total_migrated_blocks", p.TotalMigratedBlocks).
				Int("total_canceled_blocks", p.TotalCanceledBlocks).
				Int("total_duplicated_blocks", p.TotalDuplicatedBlocks).
				Msg("Continuous sync progress")
		}
		if s.config.ProgressHandler != nil {
			s.config.ProgressHandler(p)
		}
	}
	conf.ErrorHandler = func(b *storage.BlockInfo, err error) {
		if s.config.Logger != nil {
			s.config.Logger.Error().
				Str("name", s.config.Name).
				Err(err).
				Int("block", b.Block).
				Int("type", b.Type).
				Msg("Continuous sync error")
		}
		if s.config.ErrorHandler != nil {
			s.config.ErrorHandler(b, err)
		}
	}

	// When a block is written, update block_status with the largest ID for that block
	conf.BlockHandler = func(b *storage.BlockInfo, id uint64, data []byte) {
		hash := sha256.Sum256(data)

		s.blockStatusLock.Lock()
		if !s.blockStatus[b.Block].Set {
			s.blockStatus[b.Block].Set = true
			s.blockStatus[b.Block].CurrentID = id
			s.blockStatus[b.Block].CurrentHash = hash
		} else if id > s.blockStatus[b.Block].CurrentID {
			s.blockStatus[b.Block].CurrentID = id
			s.blockStatus[b.Block].CurrentHash = hash
		}
		s.blockStatusLock.Unlock()
	}

	mig, err := NewMigrator(s.config.Tracker, s.config.Destination, s.config.Orderer, conf)
	if err != nil {
		return nil, err
	}

	s.migrator = mig

	numBlocks := (s.config.Tracker.Size() + uint64(s.config.BlockSize) - 1) / uint64(s.config.BlockSize)

	if syncAllFirst {
		// Do initial migration
		for b := 0; b < int(numBlocks); b++ {
			select {
			case <-s.ctx.Done():
				// Context has been cancelled. We should wait for any pending migrations to complete
				err = mig.WaitForCompletion()
				if err != nil {
					return mig.GetMetrics(), err
				}
				return mig.GetMetrics(), s.ctx.Err()
			default:
			}

			err = mig.Migrate(1)
			if err != nil {
				return nil, err
			}
		}

		// Wait for completion.
		err = mig.WaitForCompletion()
		if err != nil {
			return nil, err
		}

		if s.config.HashesHandler != nil {
			hashes := mig.GetHashes()
			s.config.HashesHandler(hashes)
		}
	} else {
		// We don't need to do an initial migration.
		for b := 0; b < int(numBlocks); b++ {
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
				return mig.GetMetrics(), err
			}
			return mig.GetMetrics(), s.ctx.Err()
		default:
		}
		blocks := mig.GetLatestDirtyFunc(s.config.DirtyBlockGetter)

		if blocks != nil {
			id := atomic.AddUint64(&s.currentDirtyID, 1)
			// Update block_updates with the new ID for these blocks
			s.blockStatusLock.Lock()
			for _, b := range blocks {
				s.blockStatus[b].UpdatingID = id
			}
			s.blockStatusLock.Unlock()
			err = mig.MigrateDirtyWithID(blocks, id)
			if err != nil {
				return mig.GetMetrics(), err
			}
		} else {
			if !continuous {
				// We are done! Everything is synced, and the source is locked.
				err = mig.WaitForCompletion()
				return mig.GetMetrics(), err
			}
			mig.Unlock()
		}
		time.Sleep(s.config.DirtyCheckPeriod)
	}
}
