// Package store is strata's storage engine: objects are cut into stripes,
// each stripe is Reed–Solomon coded into k data and m parity shards, and
// shard i of every stripe goes to one file on one of k+m disks (directories),
// with a CRC32C per block. Any k disks are enough to serve every object;
// missing or corrupt shards are found on read, by a background scrubber or
// by "strata heal", and rewritten.
//
// New object versions are committed atomically: data and metadata are
// written to a staging directory on each disk, synced, and renamed into
// place, metadata last. A version is visible only if at least k disks hold
// its metadata, so a crash at any point leaves either the previous version
// or the new one, never a mixture.
package store

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"regexp"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/useless-husband/strata/internal/rs"
	"github.com/useless-husband/strata/internal/s3err"
)

// Config configures a Store.
type Config struct {
	Disks        []string
	DataShards   int
	ParityShards int
	BlockSize    int // data bytes per stripe; 1 MiB if zero
	Sync         SyncMode
	Logger       *slog.Logger
	// HealWorkers is the number of objects healed concurrently (default 2).
	HealWorkers int
	// ScrubInterval, if positive, verifies every shard of every object at
	// this interval. A pass reads all data, so keep it long.
	ScrubInterval time.Duration
	// DiskCheckInterval is how often disks are checked for having been
	// wiped or replaced (default 5s; negative disables).
	DiskCheckInterval time.Duration
	// ReadOnly opens the disks for inspection only: no formatting, no
	// background work. Used by "strata info" and "strata scrub".
	ReadOnly bool
}

// Limits from the S3 API.
const (
	MaxObjectSize    = 5 << 40 // 5 TiB
	MaxPutSize       = 5 << 30 // 5 GiB in a single PUT or part
	MinPartSize      = 5 << 20 // every part but the last
	MaxPartNumber    = 10000
	MaxKeyLength     = 1024
	DefaultBlockSize = 1 << 20
)

// Store is an erasure-coded object store over a set of disks.
type Store struct {
	cfg         Config
	log         *slog.Logger
	disks       []*disk
	format      *format
	codec       *rs.Codec
	geo         geometry
	n, k, m     int
	writeQuorum int

	mu      sync.RWMutex
	buckets map[string]*bucket

	locks   *keyLocks
	leases  *leases
	uploads *uploads
	heal    *healer
	stats   *Stats

	lastDiskCheck atomic.Int64
	diskCheckMu   sync.Mutex

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

type bucket struct {
	name    string
	created time.Time
	index   *index
	// mu is held shared by commits into the bucket and exclusively by
	// DeleteBucket, so a bucket cannot vanish under a commit.
	mu      sync.RWMutex
	deleted bool
}

// Open opens (formatting if needed) the disks and loads the index.
func Open(cfg Config) (*Store, error) {
	if cfg.BlockSize == 0 {
		cfg.BlockSize = DefaultBlockSize
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.DiscardHandler)
	}
	if cfg.HealWorkers <= 0 {
		cfg.HealWorkers = 2
	}
	if cfg.DiskCheckInterval == 0 {
		cfg.DiskCheckInterval = 5 * time.Second
	}
	n := len(cfg.Disks)
	if cfg.DataShards < 1 || cfg.ParityShards < 0 || cfg.DataShards+cfg.ParityShards != n {
		return nil, fmt.Errorf("%d disks cannot hold %d data + %d parity shards; the counts must add up to the number of disks",
			n, cfg.DataShards, cfg.ParityShards)
	}
	if cfg.BlockSize < 4096 || cfg.BlockSize > 64<<20 {
		return nil, fmt.Errorf("block size %d out of range (4 KiB to 64 MiB)", cfg.BlockSize)
	}
	seen := map[string]bool{}
	for _, p := range cfg.Disks {
		if seen[p] {
			return nil, fmt.Errorf("disk %s is listed twice", p)
		}
		seen[p] = true
	}
	codec, err := rs.New(cfg.DataShards, cfg.ParityShards)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	s := &Store{
		cfg: cfg, log: cfg.Logger, codec: codec,
		n: n, k: cfg.DataShards, m: cfg.ParityShards,
		geo:     geometry{k: cfg.DataShards, blockSize: int64(cfg.BlockSize)},
		buckets: map[string]*bucket{},
		locks:   newKeyLocks(), leases: newLeases(), uploads: newUploads(),
		stats: newStats(n),
		ctx:   ctx, cancel: cancel,
	}
	// A write must reach enough disks that (a) any k disks a later read
	// uses overlap it, and (b) a version deleted from a write quorum can
	// never again be seen on k disks. Both hold with max(k, m+1).
	s.writeQuorum = max(s.k, s.m+1)

	for i, p := range cfg.Disks {
		d := &disk{idx: i, root: p, sync: cfg.Sync}
		if !cfg.ReadOnly {
			if err := d.acquireLock(); err != nil {
				s.closeDisks()
				return nil, err
			}
		}
		s.disks = append(s.disks, d)
	}
	var fresh []int
	if cfg.ReadOnly {
		for _, d := range s.disks {
			if f, err := d.readFormat(); err == nil {
				s.format = f
				break
			}
		}
		if s.format == nil {
			return nil, fmt.Errorf("no formatted strata disk among %v", cfg.Disks)
		}
	} else {
		s.format, fresh, err = loadFormats(s.disks, &cfg)
		if err != nil {
			s.closeDisks()
			return nil, err
		}
		for _, d := range s.disks {
			if err := d.ensureLayout(); err != nil {
				s.closeDisks()
				return nil, fmt.Errorf("%s: %w", d, err)
			}
			if err := d.cleanTmp(); err != nil {
				s.closeDisks()
				return nil, fmt.Errorf("%s: %w", d, err)
			}
		}
	}
	for _, d := range s.disks {
		d.online.Store(true)
	}

	start := time.Now()
	toHeal, err := s.scan()
	if err != nil {
		s.closeDisks()
		return nil, err
	}
	s.log.Info("store opened", "disks", n, "data", s.k, "parity", s.m, "writeQuorum", s.writeQuorum,
		"sync", cfg.Sync.String(), "kernel", rs.Kernel(), "buckets", len(s.buckets),
		"objects", s.objectCount(), "scan", time.Since(start).Round(time.Millisecond), "needHeal", len(toHeal))

	if cfg.ReadOnly {
		return s, nil
	}
	s.heal = newHealer(s)
	s.heal.start(cfg.HealWorkers)
	for _, idx := range fresh {
		s.log.Warn("disk was empty and has been formatted into the deployment; healing everything onto it", "disk", s.disks[idx].root)
	}
	if len(fresh) > 0 {
		s.heal.requestSweep(false)
	}
	for _, o := range toHeal {
		s.heal.enqueue(o.bucket, o.key)
	}
	if cfg.DiskCheckInterval > 0 {
		s.wg.Add(1)
		go s.watchDisks(cfg.DiskCheckInterval)
	}
	if cfg.ScrubInterval > 0 {
		s.wg.Add(1)
		go s.scrubLoop(cfg.ScrubInterval)
	}
	return s, nil
}

// Close stops background work and releases the disks.
func (s *Store) Close() error {
	s.cancel()
	if s.heal != nil {
		s.heal.stop()
	}
	s.wg.Wait()
	s.closeDisks()
	return nil
}

func (s *Store) closeDisks() {
	for _, d := range s.disks {
		d.releaseLock()
	}
}

// DataShards, ParityShards and the other accessors describe the layout.
func (s *Store) DataShards() int   { return s.k }
func (s *Store) ParityShards() int { return s.m }
func (s *Store) WriteQuorum() int  { return s.writeQuorum }
func (s *Store) Deployment() string {
	return s.format.Deployment
}

func (s *Store) objectCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	n := 0
	for _, b := range s.buckets {
		n += b.index.len()
	}
	return n
}

// watchDisks notices disks whose directory was wiped or replaced while
// running, reformats them and heals everything onto them.
func (s *Store) watchDisks(every time.Duration) {
	defer s.wg.Done()
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-t.C:
		}
		s.checkDisks()
	}
}

// checkDisks runs one round of disk checks; exported to tests through
// CheckDisks.
func (s *Store) checkDisks() {
	s.diskCheckMu.Lock()
	defer s.diskCheckMu.Unlock()
	healed := false
	for _, d := range s.disks {
		re, err := d.reformatIfMissing(s.format)
		if err != nil {
			if d.online.Swap(false) {
				s.log.Error("disk offline", "disk", d.root, "err", err)
			}
			continue
		}
		if !d.online.Swap(true) {
			s.log.Info("disk back online", "disk", d.root)
			healed = true
		}
		if re {
			s.log.Warn("disk was wiped or replaced; reformatted, healing everything onto it", "disk", d.root)
			s.stats.diskReplaced.Add(1)
			healed = true
		}
	}
	if healed && s.heal != nil {
		s.heal.requestSweep(false)
	}
}

// checkDisksSoon runs checkDisks unless it ran within the last second.
func (s *Store) checkDisksSoon() {
	now := time.Now().UnixNano()
	last := s.lastDiskCheck.Load()
	if now-last < int64(time.Second) || !s.lastDiskCheck.CompareAndSwap(last, now) {
		return
	}
	s.checkDisks()
}

// CheckDisks checks every disk now instead of waiting for the next tick.
func (s *Store) CheckDisks() { s.checkDisks() }

// --- Buckets ---

var bucketNameRE = regexp.MustCompile(`^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$`)
var ipLikeRE = regexp.MustCompile(`^\d+\.\d+\.\d+\.\d+$`)

// ValidBucketName applies the S3 naming rules for new buckets.
func ValidBucketName(name string) bool {
	return bucketNameRE.MatchString(name) && !ipLikeRE.MatchString(name) &&
		!strings.Contains(name, "..") && !strings.Contains(name, ".-") && !strings.Contains(name, "-.") &&
		!strings.HasPrefix(name, "xn--") && !strings.HasSuffix(name, "-s3alias")
}

// validKey checks the S3 object key rules.
func validKey(key string) error {
	if key == "" {
		return s3err.InvalidArgument.With("Object key must not be empty")
	}
	if len(key) > MaxKeyLength {
		return s3err.KeyTooLong
	}
	if !utf8.ValidString(key) {
		return s3err.InvalidArgument.With("Object key must be valid UTF-8")
	}
	return nil
}

func (s *Store) getBucket(name string) (*bucket, error) {
	s.mu.RLock()
	b := s.buckets[name]
	s.mu.RUnlock()
	if b == nil {
		return nil, s3err.NoSuchBucket
	}
	return b, nil
}

// BucketInfo describes a bucket.
type BucketInfo struct {
	Name    string
	Created time.Time
}

// MakeBucket creates a bucket on every disk; it succeeds if a write quorum
// of disks did.
func (s *Store) MakeBucket(name string) error {
	if !ValidBucketName(name) {
		return s3err.InvalidBucketName
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.buckets[name]; ok {
		return s3err.BucketAlreadyOwnedByYou
	}
	info := bucketInfo{Name: name, Created: time.Now().UnixNano()}
	data, err := encodeFramed(&info)
	if err != nil {
		return err
	}
	ok := s.onDisks(func(d *disk) error {
		if err := d.mkdirAll(d.path(bucketsDir, name, objectsDir)); err != nil {
			return err
		}
		return d.writeFileAtomic(d.path(bucketsDir, name, bucketMeta), data)
	})
	if ok < s.writeQuorum {
		s.onDisks(func(d *disk) error { return d.removeAll(d.bucketPath(name)) })
		return errWriteQuorum(ok, s.writeQuorum)
	}
	s.buckets[name] = &bucket{name: name, created: time.Unix(0, info.Created).UTC(), index: newIndex()}
	return nil
}

// HeadBucket reports whether a bucket exists.
func (s *Store) HeadBucket(name string) (BucketInfo, error) {
	b, err := s.getBucket(name)
	if err != nil {
		return BucketInfo{}, err
	}
	return BucketInfo{Name: b.name, Created: b.created}, nil
}

// ListBuckets returns all buckets sorted by name.
func (s *Store) ListBuckets() []BucketInfo {
	s.mu.RLock()
	out := make([]BucketInfo, 0, len(s.buckets))
	for _, b := range s.buckets {
		out = append(out, BucketInfo{Name: b.name, Created: b.created})
	}
	s.mu.RUnlock()
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// DeleteBucket deletes an empty bucket, aborting its multipart uploads.
func (s *Store) DeleteBucket(name string) error {
	b, err := s.getBucket(name)
	if err != nil {
		return err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.index.len() > 0 {
		return s3err.BucketNotEmpty
	}
	for _, u := range s.uploads.forBucket(name) {
		s.abortUpload(u)
	}
	ok := s.onDisks(func(d *disk) error { return d.removeAll(d.bucketPath(name)) })
	if ok < s.writeQuorum {
		return errWriteQuorum(ok, s.writeQuorum)
	}
	b.deleted = true
	s.mu.Lock()
	delete(s.buckets, name)
	s.mu.Unlock()
	return nil
}

// onDisks runs fn on every online disk in parallel and returns how many
// succeeded.
func (s *Store) onDisks(fn func(d *disk) error) int {
	errs := make([]error, len(s.disks))
	var wg sync.WaitGroup
	for i, d := range s.disks {
		if !d.online.Load() {
			errs[i] = errDiskOffline
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = d.noteErr(fn(d))
		}()
	}
	wg.Wait()
	ok := 0
	for i, err := range errs {
		if err == nil {
			ok++
		} else if !errors.Is(err, errDiskOffline) {
			s.log.Warn("disk operation failed", "disk", s.disks[i].root, "err", err)
		}
	}
	return ok
}

var errDiskOffline = errors.New("disk offline")

func errWriteQuorum(got, need int) error {
	return s3err.SlowDown.With("Not enough disks are available: the write reached %d disks and needs %d", got, need)
}

func errReadQuorum(got, need int) error {
	return s3err.InternalError.With("Not enough intact shards to read the object: %d available, %d needed", got, need)
}

func isNotExist(err error) bool { return errors.Is(err, fs.ErrNotExist) }

func removeIfExists(path string) error {
	if err := os.Remove(path); err != nil && !isNotExist(err) {
		return err
	}
	return nil
}

// codecFor returns the codec for an object's layout, which is the store's
// unless the object was written with different parameters.
func (s *Store) codecFor(m *ObjectMeta) *rs.Codec {
	if m.Erasure.Data == s.k && m.Erasure.Parity == s.m {
		return s.codec
	}
	c, err := rs.New(m.Erasure.Data, m.Erasure.Parity)
	if err != nil {
		return s.codec // unreachable for metadata this store wrote
	}
	return c
}
