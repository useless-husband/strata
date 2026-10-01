package store

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

// --- What the disks hold for one key ---

type diskView struct {
	readable bool
	metas    map[string]*ObjectMeta // valid metadata by version
	dataDirs map[string]bool        // version data directories present
	corrupt  int                    // metadata files that failed verification
}

type dirView []diskView

// readObjectDir reads the object directory of a key on every online disk.
func (s *Store) readObjectDir(bucket, key string) dirView {
	v := make(dirView, s.n)
	var wg sync.WaitGroup
	for i, d := range s.disks {
		if !d.online.Load() {
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			v[i] = readDiskView(d, d.objectDir(bucket, key), func(m *ObjectMeta) bool { return m.Key == key && m.Bucket == bucket })
		}()
	}
	wg.Wait()
	return v
}

func readDiskView(d *disk, dir string, accept func(*ObjectMeta) bool) diskView {
	dv := diskView{metas: map[string]*ObjectMeta{}, dataDirs: map[string]bool{}}
	entries, err := os.ReadDir(dir)
	if err != nil && !isNotExist(err) {
		d.noteErr(err)
		return dv
	}
	dv.readable = true
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() {
			dv.dataDirs[name] = true
			continue
		}
		version, ok := strings.CutSuffix(name, metaSuffix)
		if !ok {
			continue
		}
		var m ObjectMeta
		if err := readFramedHeader(filepath.Join(dir, name), &m); err != nil || m.VersionID != version || !accept(&m) {
			dv.corrupt++
			continue
		}
		dv.metas[version] = &m
	}
	return dv
}

func (v dirView) count(version string) int {
	n := 0
	for _, dv := range v {
		if dv.metas[version] != nil {
			n++
		}
	}
	return n
}

func (v dirView) anyMeta(version string) *ObjectMeta {
	for _, dv := range v {
		if m := dv.metas[version]; m != nil {
			return m
		}
	}
	return nil
}

// newer orders versions by modification time, then ID.
func newer(a, b *ObjectMeta) bool {
	if a.ModTime != b.ModTime {
		return a.ModTime > b.ModTime
	}
	return a.VersionID > b.VersionID
}

// newestWithQuorum returns the newest version whose metadata is on at
// least q disks: the version a reader of the disks must consider current.
func (v dirView) newestWithQuorum(q int) *ObjectMeta {
	var best *ObjectMeta
	seen := map[string]bool{}
	for _, dv := range v {
		for ver, m := range dv.metas {
			if seen[ver] {
				continue
			}
			seen[ver] = true
			if v.count(ver) >= q && (best == nil || newer(m, best)) {
				best = m
			}
		}
	}
	return best
}

// --- Healing one object ---

// HealOptions controls HealObject.
type HealOptions struct {
	// Deep reads and verifies every block; otherwise only metadata and
	// shard file sizes are checked.
	Deep bool
	// DryRun reports problems without repairing them.
	DryRun bool
}

// HealResult reports what HealObject found and did.
type HealResult struct {
	Bucket, Key, VersionID string
	BadDisks               []int // disks whose copy was missing or damaged
	Healed                 bool  // the bad copies were rewritten
	Promoted               bool  // a newer committed version was found and made current
	StaleRemoved           int   // superseded versions or orphan data removed
	Lost                   bool  // too few intact shards: data loss
	Skipped                bool  // the key changed while healing; nothing to do
	BytesRead              int64
}

// HealObject verifies a key's current version on every disk and rewrites
// missing or damaged copies from the intact ones. It also removes
// superseded versions left behind by crashes or deferred deletion.
func (s *Store) HealObject(ctx context.Context, bucketName, key string, opt HealOptions) (HealResult, error) {
	res := HealResult{Bucket: bucketName, Key: key}
	b, err := s.getBucket(bucketName)
	if err != nil {
		return res, err
	}
	// One heal per key at a time (a read and a sweep may both ask). This
	// lock is separate from the object lock, so reads and writes of the
	// key are not held up while shards are rebuilt.
	hl := s.healLocks.get(bucketName, key)
	hl.Lock()
	defer hl.Unlock()
	lk := s.locks.get(bucketName, key)

	// Phase A: snapshot the index and the disks.
	lk.RLock()
	cur, exists := b.index.get(key)
	view := s.readObjectDir(bucketName, key)
	lk.RUnlock()

	var meta *ObjectMeta
	if cand := view.newestWithQuorum(s.k); cand != nil && (!exists || cand.ModTime > cur.modTime) {
		// Only possible after a disk that was offline at start-up comes
		// back with a version committed while it was the deciding vote.
		meta, res.Promoted = cand, true
	} else if exists {
		meta = view.anyMeta(cur.versionID)
		if meta == nil {
			res.VersionID, res.Lost = cur.versionID, true
			s.stats.lostObjects.Add(1)
			return res, nil
		}
	} else {
		return res, nil
	}
	res.VersionID = meta.VersionID
	release := s.leases.acquire(meta.VersionID)
	defer release()

	// Phase B: find damaged copies and rebuild them, without the lock.
	bad := make([]bool, s.n)
	anyBad := false
	for d, dk := range s.disks {
		if !dk.online.Load() {
			continue
		}
		if !s.copyIntact(ctx, d, meta, view[d].metas[meta.VersionID] != nil, opt.Deep, &res) {
			bad[d] = true
			anyBad = true
			res.BadDisks = append(res.BadDisks, d)
		}
	}
	if err := ctx.Err(); err != nil {
		return res, err
	}
	stale := s.staleVersions(view, meta)
	if (!anyBad && len(stale) == 0 && !res.Promoted) || opt.DryRun {
		return res, nil
	}
	var st *stage
	if anyBad {
		st = &stage{s: s, id: newID(), alive: make([]bool, s.n), inline: meta.Inline}
		if meta.Inline {
			st.payload = make([][]byte, s.n)
		}
		defer st.discard()
		for d := range bad {
			if bad[d] && (meta.Inline || os.MkdirAll(st.dir(d, "data"), 0o755) == nil) {
				st.alive[d] = true
			}
		}
		lost, err := s.rebuild(ctx, meta, st, &res)
		if err != nil {
			return res, err
		}
		if lost {
			res.Lost = true
			s.stats.lostObjects.Add(1)
			s.log.Error("object has too few intact shards to heal", "bucket", bucketName, "key", key, "version", meta.VersionID)
			return res, nil
		}
	}

	// Phase C: install the rebuilt copies if the version is still current.
	b.mu.RLock()
	defer b.mu.RUnlock()
	lk.Lock()
	defer lk.Unlock()
	now, ok := b.index.get(key)
	switch {
	case b.deleted:
		res.Skipped = true
		return res, nil
	case res.Promoted && (!ok || now.modTime < meta.ModTime):
	case !res.Promoted && ok && now.versionID == meta.VersionID:
	default:
		res.Skipped = true
		return res, nil
	}
	if st != nil {
		for d := range s.disks {
			if !st.alive[d] {
				continue
			}
			dk := s.disks[d]
			objDir := dk.objectDir(bucketName, key)
			removeIfExists(filepath.Join(objDir, meta.VersionID+metaSuffix))
			dk.removeAll(filepath.Join(objDir, meta.VersionID))
		}
		s.publish(st, meta)
		if st.count() > 0 {
			res.Healed = true
			s.stats.healedObjects.Add(1)
			s.stats.healedShards.Add(int64(st.count()))
		}
	}
	if res.Promoted {
		b.index.put(entryOf(meta))
	}
	for _, sv := range stale {
		if s.leases.busy(sv.version) {
			continue
		}
		dk := s.disks[sv.disk]
		objDir := dk.objectDir(bucketName, key)
		removeIfExists(filepath.Join(objDir, sv.version+metaSuffix))
		dk.removeAll(filepath.Join(objDir, sv.version))
		res.StaleRemoved++
	}
	return res, nil
}

type staleVersion struct {
	disk    int
	version string
}

// staleVersions lists what can be deleted around the current version: older
// versions, and data directories whose version has no metadata anywhere.
// Newer versions without a quorum are left alone: they are either the
// remains of a failed write (removed at start-up, when nothing is in
// flight) or a version whose other copies are on a disk that is offline.
func (s *Store) staleVersions(view dirView, cur *ObjectMeta) []staleVersion {
	var out []staleVersion
	for d, dv := range view {
		for ver, m := range dv.metas {
			if ver != cur.VersionID && newer(cur, m) {
				out = append(out, staleVersion{d, ver})
			}
		}
		for ver := range dv.dataDirs {
			if ver == cur.VersionID || dv.metas[ver] != nil {
				continue
			}
			if m := view.anyMeta(ver); m == nil || newer(cur, m) {
				out = append(out, staleVersion{d, ver})
			}
		}
	}
	return out
}

// openShard opens disk d's shard file of one part of a version: a file in
// the version's data directory, or the payload of the metadata file of an
// inline object. It returns the file and its size.
func (s *Store) openShard(d int, meta *ObjectMeta, part int) (shardSource, int64, error) {
	dk := s.disks[d]
	objDir := dk.objectDir(meta.Bucket, meta.Key)
	if meta.Inline {
		var m ObjectMeta
		p, err := readFramedPayload(filepath.Join(objDir, meta.VersionID+metaSuffix), &m)
		if err != nil {
			return nil, 0, err
		}
		if m.VersionID != meta.VersionID {
			return nil, 0, errCorruptMeta
		}
		return inlineShard{bytes.NewReader(p)}, int64(len(p)), nil
	}
	f, err := os.Open(filepath.Join(objDir, meta.VersionID, "part."+strconv.Itoa(part)))
	if err != nil {
		return nil, 0, err
	}
	fi, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, 0, err
	}
	return f, fi.Size(), nil
}

// copyIntact checks disk d's copy of a version: metadata present, shard
// files of the right size, and with deep set, every block's checksum.
func (s *Store) copyIntact(ctx context.Context, d int, meta *ObjectMeta, hasMeta, deep bool, res *HealResult) bool {
	if !hasMeta {
		return false
	}
	geo := geometry{k: meta.Erasure.Data, blockSize: int64(meta.Erasure.BlockSize)}
	shard := meta.Erasure.Distribution[d]
	var buf []byte
	for _, p := range meta.Parts {
		f, size, err := s.openShard(d, meta, p.Number)
		if err != nil {
			s.stats.missingShards.Add(1)
			return false
		}
		if size != geo.shardFileSize(p.Size) {
			f.Close()
			s.stats.corruptBlocks.Add(1)
			return false
		}
		if !deep {
			f.Close()
			continue
		}
		if buf == nil {
			buf = make([]byte, crcSize+geo.shardLen(geo.blockSize))
		}
		for st := int64(0); st < geo.stripes(p.Size); st++ {
			if ctx.Err() != nil {
				f.Close()
				return true // interrupted: not evidence of damage
			}
			n := geo.shardLen(geo.stripeLen(p.Size, st))
			if _, err := readBlock(f, geo, st, shard, n, buf); err != nil {
				f.Close()
				s.stats.corruptBlocks.Add(1)
				return false
			}
			res.BytesRead += crcSize + n
		}
		f.Close()
	}
	return true
}

// rebuild writes complete shard files for the stage's disks (into its
// data directories, or its payloads for an inline object), reconstructing
// each stripe from any k intact blocks found on any disk. It reports lost
// if some stripe has fewer than k.
func (s *Store) rebuild(ctx context.Context, meta *ObjectMeta, st *stage, res *HealResult) (lost bool, err error) {
	n := meta.Erasure.Data + meta.Erasure.Parity
	k := meta.Erasure.Data
	geo := geometry{k: k, blockSize: int64(meta.Erasure.BlockSize)}
	codec := s.codecFor(meta)
	bufs := make([][]byte, n)  // read buffers: crc + block
	recon := make([][]byte, n) // reconstruction outputs
	shards := make([][]byte, n)
	for i := range bufs {
		bufs[i] = make([]byte, crcSize+geo.shardLen(geo.blockSize))
		recon[i] = make([]byte, 0, geo.shardLen(geo.blockSize))
	}
	var out []byte
	for _, p := range meta.Parts {
		name := "part." + strconv.Itoa(p.Number)
		in := make([]shardSource, n)
		outs := make([]*os.File, n)
		closeAll := func() {
			for i := range in {
				if in[i] != nil {
					in[i].Close()
				}
				if outs[i] != nil {
					outs[i].Close()
				}
			}
		}
		for d := 0; d < n; d++ {
			if s.disks[d].online.Load() {
				in[d], _, _ = s.openShard(d, meta, p.Number) // may be missing
			}
			if st.alive[d] && !st.inline {
				f, err := os.OpenFile(st.dir(d, "data", name), os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
				if err != nil {
					st.fail(d, err)
					continue
				}
				outs[d] = f
			}
		}
		for stripe := int64(0); stripe < geo.stripes(p.Size); stripe++ {
			if err := ctx.Err(); err != nil {
				closeAll()
				return false, err
			}
			sl := geo.shardLen(geo.stripeLen(p.Size, stripe))
			have := 0
			for i := range shards {
				shards[i] = nil
			}
			// Prefer the copies that are not being replaced; fall back to
			// blocks of the replaced ones that still verify.
			for pass := 0; pass < 2 && have < k; pass++ {
				for d := 0; d < n && (pass == 0 || have < k); d++ {
					idx := meta.Erasure.Distribution[d]
					if in[d] == nil || shards[idx] != nil || (pass == 0) == st.alive[d] {
						continue
					}
					if data, err := readBlock(in[d], geo, stripe, idx, sl, bufs[idx]); err == nil {
						shards[idx] = data
						have++
						res.BytesRead += crcSize + sl
					}
				}
			}
			if have < k {
				closeAll()
				return true, nil
			}
			for i := range shards {
				if shards[i] == nil {
					shards[i] = recon[i][:0]
				}
			}
			if err := codec.Reconstruct(shards); err != nil {
				closeAll()
				return false, err
			}
			for d := 0; d < n; d++ {
				if !st.alive[d] {
					continue
				}
				idx := meta.Erasure.Distribution[d]
				if st.inline {
					st.payload[d] = appendBlock(st.payload[d], stripe, idx, shards[idx])
					continue
				}
				if outs[d] == nil {
					continue
				}
				out = appendBlock(out[:0], stripe, idx, shards[idx])
				if _, err := outs[d].Write(out); err != nil {
					st.fail(d, err)
				}
			}
		}
		for d := 0; d < n; d++ {
			if outs[d] != nil && st.alive[d] {
				if err := s.disks[d].push(outs[d]); err != nil {
					st.fail(d, err)
				}
			}
		}
		closeAll()
	}
	return false, nil
}

// --- Background healing ---

type objRef struct{ bucket, key string }

// healer heals objects queued by reads, commits that missed disks and
// start-up checks, and runs whole-store sweeps on request.
type healer struct {
	s      *Store
	mu     sync.Mutex
	queue  []objRef
	queued map[objRef]bool
	wake   chan struct{}
	sweep  chan struct{}
	// sweepPending and sweepDeep record a requested sweep until it starts.
	sweepPending, sweepDeep bool
	ctx                     context.Context
	cancel                  context.CancelFunc
	wg                      sync.WaitGroup
	busy                    int        // heals in progress
	idle                    *sync.Cond // signalled when the queue drains, for WaitHealIdle
}

const maxHealQueue = 100000

func newHealer(s *Store) *healer {
	ctx, cancel := context.WithCancel(s.ctx)
	h := &healer{s: s, queued: map[objRef]bool{}, wake: make(chan struct{}, 1), sweep: make(chan struct{}, 1), ctx: ctx, cancel: cancel}
	h.idle = sync.NewCond(&h.mu)
	return h
}

func (h *healer) start(workers int) {
	for i := 0; i < workers; i++ {
		h.wg.Add(1)
		go h.work()
	}
	h.wg.Add(1)
	go h.sweeper()
}

func (h *healer) stop() {
	h.cancel()
	h.wg.Wait()
}

func (h *healer) enqueue(bucket, key string) {
	ref := objRef{bucket, key}
	h.mu.Lock()
	if h.queued[ref] {
		h.mu.Unlock()
		return
	}
	if len(h.queue) >= maxHealQueue {
		h.mu.Unlock()
		h.requestSweep(false) // too much to track one by one
		return
	}
	h.queued[ref] = true
	h.queue = append(h.queue, ref)
	h.mu.Unlock()
	select {
	case h.wake <- struct{}{}:
	default:
	}
}

func (h *healer) requestSweep(deep bool) {
	h.mu.Lock()
	h.sweepDeep = h.sweepDeep || deep
	pending := h.sweepPending
	h.sweepPending = true
	h.mu.Unlock()
	if !pending {
		select {
		case h.sweep <- struct{}{}:
		default:
		}
	}
}

func (h *healer) work() {
	defer h.wg.Done()
	for {
		h.mu.Lock()
		if len(h.queue) == 0 {
			h.idle.Broadcast()
			h.mu.Unlock()
			select {
			case <-h.ctx.Done():
				return
			case <-h.wake:
			}
			continue
		}
		ref := h.queue[0]
		h.queue = h.queue[1:]
		delete(h.queued, ref)
		h.busy++
		h.mu.Unlock()
		// Missing shards often mean a wiped or replaced disk: notice it
		// now rather than at the next periodic check.
		h.s.checkDisksSoon()
		res, err := h.s.HealObject(h.ctx, ref.bucket, ref.key, HealOptions{Deep: true})
		h.s.logHeal(res, err)
		h.mu.Lock()
		h.busy--
		h.idle.Broadcast()
		h.mu.Unlock()
		if len(h.queue) > 0 {
			select {
			case h.wake <- struct{}{}:
			default:
			}
		}
	}
}

func (h *healer) sweeper() {
	defer h.wg.Done()
	for {
		select {
		case <-h.ctx.Done():
			return
		case <-h.sweep:
			h.mu.Lock()
			deep := h.sweepDeep
			h.sweepPending, h.sweepDeep = false, false
			h.busy++
			h.mu.Unlock()
			r, err := h.s.Sweep(h.ctx, HealOptions{Deep: deep})
			h.mu.Lock()
			h.busy--
			h.idle.Broadcast()
			h.mu.Unlock()
			if err != nil && !errors.Is(err, context.Canceled) {
				h.s.log.Error("heal sweep failed", "err", err)
			} else if err == nil {
				h.s.log.Info("heal sweep finished", "deep", deep, "objects", r.Objects, "healed", r.Healed,
					"lost", r.Lost, "staleRemoved", r.StaleRemoved, "bytesRead", r.BytesRead, "took", r.Duration.Round(time.Millisecond))
			}
		}
	}
}

// WaitHealIdle blocks until the heal queue is empty and no heal or sweep
// is running or requested.
func (s *Store) WaitHealIdle() {
	if s.heal == nil {
		return
	}
	h := s.heal
	h.mu.Lock()
	for len(h.queue) > 0 || h.busy > 0 || h.sweepPending {
		h.idle.Wait()
	}
	h.mu.Unlock()
}

func (s *Store) logHeal(res HealResult, err error) {
	switch {
	case err != nil && !errors.Is(err, context.Canceled):
		s.log.Error("heal failed", "bucket", res.Bucket, "key", res.Key, "err", err)
	case res.Healed || res.Promoted:
		s.log.Info("healed object", "bucket", res.Bucket, "key", res.Key, "version", res.VersionID,
			"disks", res.BadDisks, "promoted", res.Promoted)
	}
}

// SweepResult summarises a sweep over the whole store.
type SweepResult struct {
	Buckets      int
	Objects      int
	Healed       int
	Damaged      int // objects with at least one bad copy
	Lost         int
	StaleRemoved int
	BytesRead    int64
	Duration     time.Duration
	LostObjects  []string // bucket/key, at most 100
}

// Sweep heals every object (and re-creates bucket directories on disks
// that lost them). With Deep set it is a scrub: every block is read and
// verified.
func (s *Store) Sweep(ctx context.Context, opt HealOptions) (SweepResult, error) {
	start := time.Now()
	var r SweepResult
	if opt.Deep {
		s.stats.scrubs.Add(1)
	}
	for _, bi := range s.ListBuckets() {
		r.Buckets++
		if !opt.DryRun {
			s.healBucket(bi.Name)
		}
		b, err := s.getBucket(bi.Name)
		if err != nil {
			continue
		}
		after := ""
		for {
			var keys []string
			b.index.ascend(after, func(e *entry) bool {
				if e.key == after {
					return true
				}
				keys = append(keys, e.key)
				return len(keys) < 1000
			})
			if len(keys) == 0 {
				break
			}
			for _, key := range keys {
				if err := ctx.Err(); err != nil {
					return r, err
				}
				res, err := s.HealObject(ctx, bi.Name, key, opt)
				if err != nil {
					if errors.Is(err, context.Canceled) {
						return r, err
					}
					s.logHeal(res, err)
					continue
				}
				r.Objects++
				r.BytesRead += res.BytesRead
				r.StaleRemoved += res.StaleRemoved
				if len(res.BadDisks) > 0 {
					r.Damaged++
				}
				if res.Healed {
					r.Healed++
				}
				if res.Lost {
					r.Lost++
					if len(r.LostObjects) < 100 {
						r.LostObjects = append(r.LostObjects, bi.Name+"/"+key)
					}
				}
				s.logHeal(res, nil)
			}
			after = keys[len(keys)-1]
		}
	}
	r.Duration = time.Since(start)
	if opt.Deep {
		s.stats.scrubbedObjects.Add(int64(r.Objects))
		s.stats.scrubbedBytes.Add(r.BytesRead)
	}
	return r, nil
}

// healBucket re-creates a bucket's directory and metadata on disks that
// lack them.
func (s *Store) healBucket(name string) {
	b, err := s.getBucket(name)
	if err != nil {
		return
	}
	data, err := encodeFramed(&bucketInfo{Name: name, Created: b.created.UnixNano()})
	if err != nil {
		return
	}
	s.onDisks(func(d *disk) error {
		var bi bucketInfo
		if readFramed(d.path(bucketsDir, name, bucketMeta), &bi) == nil {
			return nil
		}
		if err := d.mkdirAll(d.path(bucketsDir, name, objectsDir)); err != nil {
			return err
		}
		return d.writeFileAtomic(d.path(bucketsDir, name, bucketMeta), data)
	})
}

// scrubLoop runs a deep sweep at the configured interval.
func (s *Store) scrubLoop(every time.Duration) {
	defer s.wg.Done()
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-t.C:
			if s.heal != nil {
				s.heal.requestSweep(true)
			}
		}
	}
}
