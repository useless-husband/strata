package store

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"

	"github.com/useless-husband/strata/internal/s3err"
)

// Object is an open object version. Its data stays readable until Close,
// even if the key is overwritten or deleted meanwhile.
type Object struct {
	Info    ObjectInfo
	s       *Store
	meta    *ObjectMeta
	release func()
	inline  [][]byte // shard file of each disk, for inline objects
}

// Close releases the version.
func (o *Object) Close() { o.release() }

// metaRotor spreads metadata reads over the disks.
var metaRotor atomic.Uint32

// OpenObject opens the current version of a key for reading.
func (s *Store) OpenObject(bucketName, key string) (*Object, error) {
	return s.open(bucketName, key, true)
}

// StatObject returns the attributes of the current version of a key.
func (s *Store) StatObject(bucketName, key string) (ObjectInfo, error) {
	o, err := s.open(bucketName, key, false)
	if err != nil {
		return ObjectInfo{}, err
	}
	o.Close()
	return o.Info, nil
}

func (s *Store) open(bucketName, key string, data bool) (*Object, error) {
	b, err := s.getBucket(bucketName)
	if err != nil {
		return nil, err
	}
	if err := validKey(key); err != nil {
		return nil, err
	}
	lk := s.locks.get(bucketName, key)
	lk.RLock()
	defer lk.RUnlock()
	e, ok := b.index.get(key)
	if !ok {
		return nil, s3err.NoSuchKey
	}
	// The metadata (and the shard files of an inline object, which live in
	// it) is read under the lock: overwrites and deletes remove it under
	// the write lock. The lease protects data directories, which are
	// removed later.
	release := s.leases.acquire(e.versionID)
	meta, err := s.readMetaAny(bucketName, key, e.versionID)
	if err != nil {
		release()
		return nil, err
	}
	o := &Object{Info: meta.info(), s: s, meta: meta, release: release}
	if data && meta.Inline {
		o.inline = s.readInline(meta)
	}
	return o, nil
}

// readInline loads the inline shard files of an object: those of the data
// shards, which is all a healthy read needs, and those of every other disk
// if one of them is missing or damaged. Copies that cannot be read are
// left nil for the reader to work around.
func (s *Store) readInline(meta *ObjectMeta) [][]byte {
	out := make([][]byte, s.n)
	load := func(d int) bool {
		dk := s.disks[d]
		if !dk.online.Load() {
			return false
		}
		var m ObjectMeta
		p, err := readFramedPayload(filepath.Join(dk.objectDir(meta.Bucket, meta.Key), meta.VersionID+metaSuffix), &m)
		if err != nil || m.VersionID != meta.VersionID {
			return false
		}
		out[d] = p
		return true
	}
	// Inline shard files are small, so their block checksums are checked
	// here, to know whether the parity copies are needed too; the reader
	// checks them again.
	geo := geometry{k: meta.Erasure.Data, blockSize: int64(meta.Erasure.BlockSize)}
	intact := func(d int) bool {
		p := out[d]
		if int64(len(p)) != geo.shardFileSize(meta.Size) {
			return false
		}
		buf := make([]byte, crcSize+geo.shardLen(geo.blockSize))
		r := bytes.NewReader(p)
		for st := int64(0); st < geo.stripes(meta.Size); st++ {
			if _, err := readBlock(r, geo, st, meta.Erasure.Distribution[d], geo.shardLen(geo.stripeLen(meta.Size, st)), buf); err != nil {
				return false
			}
		}
		return true
	}
	ok := true
	for i := 0; i < meta.Erasure.Data; i++ {
		d := meta.Erasure.holder(i)
		if !load(d) || !intact(d) {
			ok = false
		}
	}
	if !ok {
		for d := range s.disks {
			if out[d] == nil {
				load(d)
			}
		}
	}
	return out
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
		err := readFramedHeader(filepath.Join(d.objectDir(bucket, key), version+metaSuffix), &m)
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
	r.inline = o.inline
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
	part   int           // part whose files are open
	files  []shardSource // by disk
	inline [][]byte      // by disk, for inline objects
	bad    []bool        // by disk: unusable for the rest of this read
	shards [][]byte      // by shard index
	bufs   [][]byte      // by shard index: crc+block read buffers
	recon  [][]byte      // by shard index: reconstruction outputs
	heal   bool
}

func (s *Store) newObjectReader(meta *ObjectMeta) *objectReader {
	n := meta.Erasure.Data + meta.Erasure.Parity
	return &objectReader{
		s: s, meta: meta,
		geo:    geometry{k: meta.Erasure.Data, blockSize: int64(meta.Erasure.BlockSize)},
		files:  make([]shardSource, n),
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

// shardSource is a shard file: an open file or an inline payload.
type shardSource interface {
	io.ReaderAt
	Close() error
}

type inlineShard struct{ *bytes.Reader }

func (inlineShard) Close() error { return nil }

// file returns the shard file of the given part on disk d.
func (r *objectReader) file(d int, part int) (shardSource, error) {
	if r.meta.Inline {
		if r.inline == nil || r.inline[d] == nil {
			return nil, os.ErrNotExist
		}
		return inlineShard{bytes.NewReader(r.inline[d])}, nil
	}
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
