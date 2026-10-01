package store

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"

	"github.com/useless-husband/strata/internal/s3err"
)

// Object is an open object version. Its files stay readable until Close,
// even if the key is overwritten or deleted meanwhile.
type Object struct {
	Info    ObjectInfo
	s       *Store
	meta    *ObjectMeta
	release func()
}

// Close releases the version.
func (o *Object) Close() { o.release() }

// metaRotor spreads metadata reads over the disks.
var metaRotor atomic.Uint32

// OpenObject opens the current version of a key.
func (s *Store) OpenObject(bucketName, key string) (*Object, error) {
	b, err := s.getBucket(bucketName)
	if err != nil {
		return nil, err
	}
	if err := validKey(key); err != nil {
		return nil, err
	}
	lk := s.locks.get(bucketName, key)
	lk.RLock()
	e, ok := b.index.get(key)
	if !ok {
		lk.RUnlock()
		return nil, s3err.NoSuchKey
	}
	release := s.leases.acquire(e.versionID)
	// Read the metadata under the lock: a DELETE removes it under the
	// write lock, and our lease only protects the data.
	meta, err := s.readMetaAny(bucketName, key, e.versionID)
	lk.RUnlock()
	if err != nil {
		release()
		return nil, err
	}
	return &Object{Info: meta.info(), s: s, meta: meta, release: release}, nil
}

// readMetaAny returns a valid copy of a version's metadata from any disk.
// Disks with a missing or corrupt copy get the object queued for healing.
func (s *Store) readMetaAny(bucket, key, version string) (*ObjectMeta, error) {
	start := int(metaRotor.Add(1))
	bad := 0
	for i := 0; i < s.n; i++ {
		d := s.disks[(start+i)%s.n]
		if !d.online.Load() {
			continue
		}
		var m ObjectMeta
		err := readFramed(filepath.Join(d.objectDir(bucket, key), version+metaSuffix), &m)
		if err == nil && (m.Key != key || m.VersionID != version) {
			err = errCorruptMeta
		}
		if err != nil {
			d.noteErr(err)
			bad++
			continue
		}
		if bad > 0 {
			s.queueHeal(bucket, key)
		}
		return &m, nil
	}
	return nil, errReadQuorum(0, s.k)
}

func (s *Store) queueHeal(bucket, key string) {
	if s.heal != nil {
		s.heal.enqueue(bucket, key)
	}
}

// WriteRange writes length bytes starting at off to w. If the response has
// already started when the read fails (fewer than k intact shards), the
// error is returned and the caller must abort the response.
func (o *Object) WriteRange(ctx context.Context, w io.Writer, off, length int64) error {
	if off < 0 || length < 0 || off+length > o.meta.Size {
		return s3err.InvalidRange
	}
	r := o.s.newObjectReader(o.meta)
	defer r.close()
	var partStart int64
	for _, p := range o.meta.Parts {
		partEnd := partStart + p.Size
		if partEnd > off && partStart < off+length {
			lo := max(off, partStart) - partStart
			hi := min(off+length, partEnd) - partStart
			if err := r.writePartRange(ctx, w, p, lo, hi); err != nil {
				return err
			}
		}
		partStart = partEnd
		if partStart >= off+length {
			break
		}
	}
	return nil
}

// objectReader reads stripes of one object version, falling back to parity
// and reconstruction when a shard is missing or fails its checksum.
type objectReader struct {
	s      *Store
	meta   *ObjectMeta
	geo    geometry
	part   int        // part whose files are open
	files  []*os.File // by disk
	bad    []bool     // by disk: unusable for the rest of this read
	shards [][]byte   // by shard index
	bufs   [][]byte   // by shard index: crc+block read buffers
	recon  [][]byte   // by shard index: reconstruction outputs
	heal   bool
}

func (s *Store) newObjectReader(meta *ObjectMeta) *objectReader {
	n := meta.Erasure.Data + meta.Erasure.Parity
	return &objectReader{
		s: s, meta: meta,
		geo:    geometry{k: meta.Erasure.Data, blockSize: int64(meta.Erasure.BlockSize)},
		files:  make([]*os.File, n),
		bad:    make([]bool, n),
		shards: make([][]byte, n),
		bufs:   make([][]byte, n),
		recon:  make([][]byte, n),
	}
}

func (r *objectReader) close() {
	r.closeFiles()
	if r.heal {
		r.s.queueHeal(r.meta.Bucket, r.meta.Key)
	}
}

func (r *objectReader) closeFiles() {
	for i, f := range r.files {
		if f != nil {
			f.Close()
			r.files[i] = nil
		}
	}
}

// file returns the open shard file of the current part on disk d.
func (r *objectReader) file(d int, part int) (*os.File, error) {
	if r.part != part {
		r.closeFiles()
		r.part = part
	}
	if r.files[d] != nil {
		return r.files[d], nil
	}
	dk := r.s.disks[d]
	if !dk.online.Load() {
		return nil, errDiskOffline
	}
	f, err := os.Open(filepath.Join(dk.objectDir(r.meta.Bucket, r.meta.Key), r.meta.VersionID, "part."+strconv.Itoa(part)))
	if err != nil {
		return nil, err
	}
	r.files[d] = f
	return f, nil
}

// readShard reads shard i of stripe st of a part into r.shards[i]. It
// reports false, and marks the disk bad for the rest of the read, if the
// block is missing or corrupt.
func (r *objectReader) readShard(p PartInfo, st int64, i int, n int64) bool {
	d := r.meta.Erasure.holder(i)
	if d < 0 || r.bad[d] {
		return false
	}
	f, err := r.file(d, p.Number)
	if err == nil {
		if cap(r.bufs[i]) < int(crcSize+n) {
			r.bufs[i] = make([]byte, crcSize+r.geo.shardLen(r.geo.blockSize))
		}
		var data []byte
		data, err = readBlock(f, r.geo, st, i, n, r.bufs[i])
		if err == nil {
			r.shards[i] = data
			return true
		}
	}
	r.bad[d] = true
	r.heal = true
	switch {
	case errors.Is(err, errCorruptBlock) || errors.Is(err, errShortShard):
		r.s.stats.corruptBlocks.Add(1)
	case errors.Is(err, os.ErrNotExist):
		r.s.stats.missingShards.Add(1)
	}
	r.s.disks[d].noteErr(err)
	r.s.log.Warn("shard unreadable; reconstructing from parity", "bucket", r.meta.Bucket, "key", r.meta.Key,
		"disk", r.s.disks[d].root, "part", p.Number, "stripe", st, "shard", i, "err", err)
	return false
}

// readStripe makes data shards first..last of stripe st available in
// r.shards, reading the minimum when all is well and reconstructing from
// any k shards otherwise.
func (r *objectReader) readStripe(p PartInfo, st int64, first, last int) error {
	k := r.meta.Erasure.Data
	n := len(r.shards)
	sl := r.geo.shardLen(r.geo.stripeLen(p.Size, st))
	for i := range r.shards {
		r.shards[i] = nil
	}
	ok := true
	for i := first; i <= last; i++ {
		if !r.readShard(p, st, i, sl) {
			ok = false
		}
	}
	if ok {
		return nil
	}
	// Degraded: gather any k intact shards, data shards first.
	have := 0
	for i := 0; i < n; i++ {
		if r.shards[i] != nil {
			have++
		}
	}
	for i := 0; i < n && have < k; i++ {
		if r.shards[i] == nil && r.readShard(p, st, i, sl) {
			have++
		}
	}
	if have < k {
		r.s.stats.readFailures.Add(1)
		return errReadQuorum(have, k)
	}
	var missing []int
	for i := 0; i < n; i++ {
		if r.shards[i] == nil {
			r.shards[i] = r.recon[i][:0]
			missing = append(missing, i)
		}
	}
	if err := r.s.codecFor(r.meta).ReconstructData(r.shards); err != nil {
		return err
	}
	for _, i := range missing {
		if i < k {
			r.recon[i] = r.shards[i][:0] // keep the buffer for the next stripe
		}
	}
	r.s.stats.degradedStripes.Add(1)
	return nil
}

// writePartRange writes bytes [lo, hi) of a part to w.
func (r *objectReader) writePartRange(ctx context.Context, w io.Writer, p PartInfo, lo, hi int64) error {
	for pos := lo; pos < hi; {
		if err := ctx.Err(); err != nil {
			return err
		}
		st := pos / r.geo.blockSize
		stripeStart := st * r.geo.blockSize
		stripeEnd := stripeStart + r.geo.stripeLen(p.Size, st)
		a, b := pos-stripeStart, min(hi, stripeEnd)-stripeStart
		sl := r.geo.shardLen(stripeEnd - stripeStart)
		first, last := int(a/sl), int((b-1)/sl)
		if err := r.readStripe(p, st, first, last); err != nil {
			return err
		}
		for i := first; i <= last; i++ {
			s0 := max(a-int64(i)*sl, 0)
			s1 := min(b-int64(i)*sl, sl)
			if _, err := w.Write(r.shards[i][s0:s1]); err != nil {
				return err
			}
		}
		pos = stripeStart + b
	}
	return nil
}
