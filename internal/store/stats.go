package store

import (
	"sync/atomic"
	"syscall"
	"time"
)

// Stats are the store's counters, exported as Prometheus metrics.
type Stats struct {
	objectsWritten  atomic.Int64
	objectsDeleted  atomic.Int64
	corruptBlocks   atomic.Int64
	missingShards   atomic.Int64
	degradedStripes atomic.Int64
	readFailures    atomic.Int64
	healedObjects   atomic.Int64
	healedShards    atomic.Int64
	lostObjects     atomic.Int64
	diskReplaced    atomic.Int64
	scrubs          atomic.Int64
	scrubbedObjects atomic.Int64
	scrubbedBytes   atomic.Int64
}

func newStats(int) *Stats { return &Stats{} }

func bucketTime(ns int64) time.Time { return time.Unix(0, ns).UTC() }

// DiskStatus describes one disk.
type DiskStatus struct {
	Index      int
	Path       string
	Online     bool
	Errors     int64
	TotalBytes uint64
	FreeBytes  uint64
}

// StatsSnapshot is a point-in-time copy of the counters and gauges.
type StatsSnapshot struct {
	Buckets, Objects int
	ObjectsWritten   int64
	ObjectsDeleted   int64
	CorruptBlocks    int64 // shard blocks that failed their checksum
	MissingShards    int64 // shard files found missing
	DegradedStripes  int64 // stripes served by reconstruction
	ReadFailures     int64 // reads that found fewer than k intact shards
	HealedObjects    int64
	HealedShards     int64
	LostObjects      int64
	DisksReplaced    int64
	Scrubs           int64
	ScrubbedObjects  int64
	ScrubbedBytes    int64
	HealQueue        int
	// SyncRequests counts waits for a drive-cache flush, SyncFlushes the
	// flushes (F_FULLFSYNC) actually issued for them; see barrier.go.
	SyncRequests     int64
	SyncFlushes      int64
	MultipartUploads int
	Disks            []DiskStatus
	DataShards       int
	ParityShards     int
	WriteQuorum      int
	ReadQuorum       int
}

// Stats returns a snapshot of the store's counters and disk states.
func (s *Store) Stats() StatsSnapshot {
	st := s.stats
	snap := StatsSnapshot{
		ObjectsWritten: st.objectsWritten.Load(), ObjectsDeleted: st.objectsDeleted.Load(),
		CorruptBlocks: st.corruptBlocks.Load(), MissingShards: st.missingShards.Load(),
		DegradedStripes: st.degradedStripes.Load(), ReadFailures: st.readFailures.Load(),
		HealedObjects: st.healedObjects.Load(), HealedShards: st.healedShards.Load(),
		LostObjects: st.lostObjects.Load(), DisksReplaced: st.diskReplaced.Load(),
		Scrubs: st.scrubs.Load(), ScrubbedObjects: st.scrubbedObjects.Load(), ScrubbedBytes: st.scrubbedBytes.Load(),
		DataShards: s.k, ParityShards: s.m, WriteQuorum: s.writeQuorum, ReadQuorum: s.k,
	}
	s.mu.RLock()
	snap.Buckets = len(s.buckets)
	for _, b := range s.buckets {
		snap.Objects += b.index.len()
	}
	s.mu.RUnlock()
	if s.heal != nil {
		s.heal.mu.Lock()
		snap.HealQueue = len(s.heal.queue)
		s.heal.mu.Unlock()
	}
	for _, b := range s.barriers {
		snap.SyncRequests += b.requests.Load()
		snap.SyncFlushes += b.flushes.Load()
	}
	s.uploads.mu.Lock()
	snap.MultipartUploads = len(s.uploads.m)
	s.uploads.mu.Unlock()
	for _, d := range s.disks {
		ds := DiskStatus{Index: d.idx, Path: d.root, Online: d.online.Load(), Errors: d.errors.Load()}
		var fs syscall.Statfs_t
		if syscall.Statfs(d.root, &fs) == nil {
			ds.TotalBytes = fs.Blocks * uint64(fs.Bsize)
			ds.FreeBytes = fs.Bavail * uint64(fs.Bsize)
		}
		snap.Disks = append(snap.Disks, ds)
	}
	return snap
}
