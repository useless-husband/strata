package store

import (
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
)

// A durable commit point needs two things: the data must leave the page
// cache (fsync), and the drive must move it from its volatile write cache
// to stable media. On Linux fsync does both. On macOS fsync(2) does only
// the first; F_FULLFSYNC does both, and its second half is a cache flush of
// the whole drive, not of one file.
//
// Issuing F_FULLFSYNC for every file of every disk at every commit point
// costs a full drive-cache flush each time (a few milliseconds on an Apple
// SSD): about twelve per PUT with six disks, and they serialise. Instead,
// strata calls fsync(2) on each file and then waits on a barrier shared by
// every disk on the same device (same st_dev): one F_FULLFSYNC that starts
// after the caller's fsyncs completed makes all of them durable. Callers
// that arrive while a flush is running wait for the next one, which they
// share (group commit), so concurrent PUTs on all disks of a device cost
// about one flush per commit phase between them.
//
// This relies on F_FULLFSYNC flushing the drive's entire write cache,
// which is what Apple documents ("asks the drive to flush all buffered
// data to the permanent storage device"). It is not tested by cutting
// power; see the README's limitations.
type barrier struct {
	f *os.File // any open file on the device

	mu       sync.Mutex
	cond     *sync.Cond
	started  uint64 // flushes started
	finished uint64 // flushes completed
	flushing bool
	err      error // result of the last completed flush

	requests atomic.Int64
	flushes  atomic.Int64
}

func newBarrier(f *os.File) *barrier {
	b := &barrier{f: f}
	b.cond = sync.NewCond(&b.mu)
	return b
}

// wait returns once a full flush that started after the call completed.
func (b *barrier) wait() error {
	b.requests.Add(1)
	b.mu.Lock()
	defer b.mu.Unlock()
	need := b.started + 1
	for b.finished < need {
		if b.flushing {
			b.cond.Wait()
			continue
		}
		b.flushing = true
		b.started++
		gen := b.started
		b.mu.Unlock()
		err := fullSync(b.f)
		b.flushes.Add(1)
		b.mu.Lock()
		b.flushing = false
		b.finished = gen
		b.err = err
		b.cond.Broadcast()
	}
	return b.err
}

// useBarriers reports whether commit points use shared device barriers:
// only with SyncFull on macOS, where fsync and the cache flush are
// separable.
func useBarriers(mode SyncMode) bool { return mode == SyncFull && runtime.GOOS == "darwin" }

// deviceOf returns the device a path is on.
func deviceOf(path string) (uint64, error) {
	var st syscall.Stat_t
	if err := syscall.Stat(path, &st); err != nil {
		return 0, err
	}
	return uint64(st.Dev), nil
}

// setupBarriers gives every disk the barrier of its device.
func (s *Store) setupBarriers() error {
	if !useBarriers(s.cfg.Sync) {
		return nil
	}
	byDev := map[uint64]*barrier{}
	for _, d := range s.disks {
		dev, err := deviceOf(d.root)
		if err != nil {
			return err
		}
		b := byDev[dev]
		if b == nil {
			f, err := os.Open(d.sysPath(lockFile))
			if err != nil {
				return err
			}
			b = newBarrier(f)
			byDev[dev] = b
			s.barriers = append(s.barriers, b)
		}
		d.barrier = b
	}
	return nil
}
