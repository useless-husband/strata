package store

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/useless-husband/strata/internal/s3err"
)

// ObjectInfo describes an object version.
type ObjectInfo struct {
	Bucket, Key       string
	VersionID         string
	Size              int64
	ETag              string // hex MD5, or MD5-of-MD5s "-N" for multipart; no quotes
	ModTime           time.Time
	ContentType       string
	UserMeta          map[string]string
	Headers           map[string]string
	ChecksumAlgorithm string
	Checksum          string
	ChecksumType      string
	Parts             []PartInfo
}

func (m *ObjectMeta) info() ObjectInfo {
	return ObjectInfo{
		Bucket: m.Bucket, Key: m.Key, VersionID: m.VersionID, Size: m.Size, ETag: m.ETag,
		ModTime: m.modTime(), ContentType: m.ContentType, UserMeta: m.UserMeta, Headers: m.Headers,
		ChecksumAlgorithm: m.ChecksumAlgorithm, Checksum: m.Checksum, ChecksumType: m.ChecksumType,
		Parts: m.Parts,
	}
}

// Checksum is an x-amz-checksum-* value the API layer verified.
type Checksum struct {
	Algorithm string // CRC32, CRC32C, CRC64NVME, SHA1, SHA256
	Value     string // base64
	Type      string // FULL_OBJECT or COMPOSITE
}

// Conditions are the conditional-write headers of a PUT or
// CompleteMultipartUpload, checked atomically at commit time.
type Conditions struct {
	IfNoneMatch bool   // If-None-Match: * — fail if the key exists
	IfMatch     string // If-Match: <etag> — fail unless the current ETag matches
}

func (c Conditions) check(cur entry, exists bool) error {
	if c.IfNoneMatch && exists {
		return s3err.PreconditionFailed
	}
	if c.IfMatch != "" {
		if !exists {
			return s3err.NoSuchKey
		}
		if c.IfMatch != "*" && c.IfMatch != cur.etag {
			return s3err.PreconditionFailed
		}
	}
	return nil
}

// PutOptions are the attributes of a new object.
type PutOptions struct {
	ContentType string
	UserMeta    map[string]string
	Headers     map[string]string
	Conditions  Conditions
	// Precommit is called once the whole body has been read and stored but
	// before the object becomes visible, with the MD5 the store computed.
	// It lets the API layer verify Content-MD5, signatures and checksums
	// that are only known at the end of the stream; an error aborts the
	// upload and leaves no trace.
	Precommit func(md5sum []byte) (Checksum, error)
}

// stage is an object version or part being written to every disk. A
// regular stage writes shard files under <disk>/.strata/tmp/<id>/data/; an
// inline stage (small objects) keeps each disk's shard file in memory and
// writes it as the payload of that disk's metadata file.
type stage struct {
	s       *Store
	id      string
	alive   []bool
	inline  bool
	payload [][]byte // inline shard file of each disk
}

func (s *Store) newStage() *stage {
	st := &stage{s: s, id: newID(), alive: make([]bool, s.n)}
	for i, d := range s.disks {
		if !d.online.Load() {
			continue
		}
		if err := os.MkdirAll(st.dir(i, "data"), 0o755); err != nil {
			d.noteErr(err)
			continue
		}
		st.alive[i] = true
	}
	return st
}

func (s *Store) newInlineStage() *stage {
	st := &stage{s: s, id: newID(), alive: make([]bool, s.n), inline: true, payload: make([][]byte, s.n)}
	for i, d := range s.disks {
		st.alive[i] = d.online.Load()
	}
	return st
}

func (st *stage) dir(d int, elem ...string) string {
	return st.s.disks[d].sysPath(append([]string{tmpDir, st.id}, elem...)...)
}

// metaTmp is where disk d's metadata is written before the commit rename.
func (st *stage) metaTmp(d int) string {
	if st.inline {
		return st.s.disks[d].sysPath(tmpDir, st.id+metaSuffix)
	}
	return st.dir(d, "meta")
}

func (st *stage) count() int {
	n := 0
	for _, a := range st.alive {
		if a {
			n++
		}
	}
	return n
}

// fail drops a disk from the stage.
func (st *stage) fail(d int, err error) {
	if st.alive[d] {
		st.alive[d] = false
		st.s.disks[d].noteErr(err)
		st.s.log.Warn("disk dropped from write", "disk", st.s.disks[d].root, "err", err)
	}
}

// discard removes what is left of the stage.
func (st *stage) discard() {
	for i, d := range st.s.disks {
		if !d.online.Load() {
			continue
		}
		if st.inline {
			os.Remove(st.metaTmp(i))
		} else {
			os.RemoveAll(st.dir(i))
		}
	}
}

// defaultInlineLimit is the largest object stored inline by default
// (Config.InlineLimit). Each disk then holds
// one file per object instead of a metadata file, a directory and a shard
// file, which matters because creating, renaming and deleting files is
// what small-object writes spend their time on (on APFS every such
// operation also waits for any F_FULLFSYNC in flight; see BENCHMARKS).
const (
	defaultInlineLimit = 128 << 10
	maxInlineLimit     = 1 << 20
)

// encodeInline erasure-codes a small object into the stage's in-memory
// shard files and returns its MD5.
func (s *Store) encodeInline(st *stage, data []byte, dist []int) []byte {
	sum := md5.Sum(data)
	shards := make([][]byte, s.n)
	for stripe, off := int64(0), 0; off < len(data); stripe, off = stripe+1, off+s.cfg.BlockSize {
		shards = s.codec.Split(data[off:min(off+s.cfg.BlockSize, len(data))], shards)
		s.codec.Encode(shards)
		for d := range s.disks {
			idx := dist[d]
			st.payload[d] = appendBlock(st.payload[d], stripe, idx, shards[idx])
		}
	}
	return sum[:]
}

var bufPool sync.Pool // *[]byte of blockSize

func (s *Store) getBlockBuf() *[]byte {
	if b, ok := bufPool.Get().(*[]byte); ok && cap(*b) >= s.cfg.BlockSize {
		return b
	}
	b := make([]byte, s.cfg.BlockSize)
	return &b
}

// writePart reads body to EOF, erasure-codes it stripe by stripe and writes
// shard file "data/part.<number>" on every live disk of the stage. It
// returns the size and MD5 of what it read. limit caps the size.
func (s *Store) writePart(ctx context.Context, st *stage, number int, body io.Reader, dist []int, limit int64) (int64, []byte, error) {
	name := "part." + strconv.Itoa(number)
	files := make([]*os.File, s.n)
	defer func() {
		for _, f := range files {
			if f != nil {
				f.Close()
			}
		}
	}()
	for d := range s.disks {
		if !st.alive[d] {
			continue
		}
		f, err := os.OpenFile(st.dir(d, "data", name), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
		if err != nil {
			st.fail(d, err)
			continue
		}
		files[d] = f
	}
	if st.count() < s.writeQuorum {
		return 0, nil, errWriteQuorum(st.count(), s.writeQuorum)
	}

	bufp := s.getBlockBuf()
	defer bufPool.Put(bufp)
	data := (*bufp)[:s.cfg.BlockSize]
	shards := make([][]byte, s.n)
	out := make([][]byte, s.n)
	h := md5.New()
	var size, stripe int64
	for {
		if err := ctx.Err(); err != nil {
			return 0, nil, err
		}
		nr, rerr := io.ReadFull(body, data)
		if nr > 0 {
			size += int64(nr)
			if size > limit {
				return 0, nil, s3err.EntityTooLarge
			}
			h.Write(data[:nr])
			shards = s.codec.Split(data[:nr], shards)
			if err := s.codec.Encode(shards); err != nil {
				return 0, nil, err
			}
			for d, f := range files {
				if f == nil || !st.alive[d] {
					continue
				}
				idx := dist[d]
				out[d] = appendBlock(out[d][:0], stripe, idx, shards[idx])
				if _, err := f.Write(out[d]); err != nil {
					st.fail(d, err)
				}
			}
			if st.count() < s.writeQuorum {
				return 0, nil, errWriteQuorum(st.count(), s.writeQuorum)
			}
			stripe++
		}
		if rerr == io.EOF || rerr == io.ErrUnexpectedEOF {
			break
		}
		if rerr != nil {
			return 0, nil, rerr
		}
	}
	// Push the shard data to the drives; the flush before the commit
	// renames makes it durable.
	s.forStage(st, func(d int) error {
		if files[d] == nil {
			return nil
		}
		return s.disks[d].push(files[d])
	})
	if st.count() < s.writeQuorum {
		return 0, nil, errWriteQuorum(st.count(), s.writeQuorum)
	}
	return size, h.Sum(nil), nil
}

// PutObject stores a new object version read from body.
func (s *Store) PutObject(ctx context.Context, bucketName, key string, body io.Reader, opts PutOptions) (ObjectInfo, error) {
	b, err := s.getBucket(bucketName)
	if err != nil {
		return ObjectInfo{}, err
	}
	if err := validKey(key); err != nil {
		return ObjectInfo{}, err
	}
	// Fail fast on a conditional write that cannot succeed; the condition
	// is checked again, atomically, at commit.
	if cur, ok := b.index.get(key); opts.Conditions.check(cur, ok) != nil {
		return ObjectInfo{}, opts.Conditions.check(cur, ok)
	}
	dist := distribution(key, s.n)
	// Read up to the inline limit: a body that ends there is stored inline,
	// a longer one is streamed.
	limit := max(s.cfg.InlineLimit, -1)
	head := make([]byte, limit+1)
	n, err := io.ReadFull(body, head)
	if err != nil && err != io.ErrUnexpectedEOF && err != io.EOF {
		return ObjectInfo{}, err
	}
	var st *stage
	var size int64
	var sum []byte
	if n <= limit {
		st = s.newInlineStage()
		if st.count() < s.writeQuorum {
			return ObjectInfo{}, errWriteQuorum(st.count(), s.writeQuorum)
		}
		size, sum = int64(n), s.encodeInline(st, head[:n], dist)
	} else {
		st = s.newStage()
		size, sum, err = s.writePart(ctx, st, 1, io.MultiReader(bytes.NewReader(head[:n]), body), dist, MaxPutSize)
		if err != nil {
			st.discard()
			return ObjectInfo{}, err
		}
	}
	defer st.discard()
	meta := &ObjectMeta{
		Bucket: bucketName, Key: key, VersionID: newID(), Size: size, ETag: hex.EncodeToString(sum),
		ContentType: opts.ContentType, UserMeta: opts.UserMeta, Headers: opts.Headers,
		Erasure: ErasureInfo{Data: s.k, Parity: s.m, BlockSize: s.cfg.BlockSize, Distribution: dist},
		Parts:   []PartInfo{{Number: 1, Size: size, ETag: hex.EncodeToString(sum)}},
		Inline:  st.inline,
	}
	if opts.Precommit != nil {
		ck, err := opts.Precommit(sum)
		if err != nil {
			return ObjectInfo{}, err
		}
		meta.ChecksumAlgorithm, meta.Checksum, meta.ChecksumType = ck.Algorithm, ck.Value, ck.Type
		meta.Parts[0].Checksum = ck.Value
	}
	return s.commit(b, meta, st, opts.Conditions)
}

// commit makes a staged version the current version of its key.
func (s *Store) commit(b *bucket, meta *ObjectMeta, st *stage, cond Conditions) (ObjectInfo, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.deleted {
		return ObjectInfo{}, s3err.NoSuchBucket
	}
	lk := s.locks.get(b.name, meta.Key)
	lk.Lock()
	defer lk.Unlock()
	cur, exists := b.index.get(meta.Key)
	if err := cond.check(cur, exists); err != nil {
		return ObjectInfo{}, err
	}
	// Modification times of successive versions strictly increase, even if
	// the clock steps back, because "newest" decides which version wins
	// when the disks are read after a crash.
	meta.ModTime = time.Now().UnixNano()
	if exists && meta.ModTime <= cur.modTime {
		meta.ModTime = cur.modTime + 1
	}
	ok := s.publish(st, meta)
	if ok < s.writeQuorum {
		s.unpublish(meta)
		return ObjectInfo{}, errWriteQuorum(ok, s.writeQuorum)
	}
	b.index.put(entryOf(meta))
	if exists {
		s.removeVersion(b.name, meta.Key, cur)
	}
	if ok < s.n && s.heal != nil {
		s.heal.enqueue(b.name, meta.Key)
	}
	s.stats.objectsWritten.Add(1)
	return meta.info(), nil
}

// forStage runs fn for every live disk of the stage in parallel and drops
// the disks where it fails.
func (s *Store) forStage(st *stage, fn func(d int) error) {
	errs := make([]error, s.n)
	var wg sync.WaitGroup
	for d := range s.disks {
		if !st.alive[d] {
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[d] = fn(d)
		}()
	}
	wg.Wait()
	for d, err := range errs {
		if err != nil {
			st.fail(d, err)
		}
	}
}

// flushStage makes everything the stage's disks pushed durable: one flush
// per device, shared with concurrent commits.
func (s *Store) flushStage(st *stage) {
	done := map[*barrier]error{}
	for d, dk := range s.disks {
		if !st.alive[d] || dk.barrier == nil {
			continue
		}
		err, ok := done[dk.barrier]
		if !ok {
			err = dk.barrier.wait()
			done[dk.barrier] = err
		}
		if err != nil {
			st.fail(d, err)
		}
	}
}

// publish moves a staged version into place on each live disk of the
// stage, in two durable steps. First the metadata is written into the
// staging area and pushed, and each device is flushed: data and metadata
// are now on stable storage under temporary names. Then the data directory
// and the metadata are renamed into the object directory, metadata last,
// the directory is pushed, and each device is flushed again. The metadata
// rename is the commit point on that disk. It returns the number of disks
// that committed.
func (s *Store) publish(st *stage, meta *ObjectMeta) int {
	s.forStage(st, func(d int) error {
		var payload []byte
		if st.inline {
			payload = st.payload[d]
		}
		data, err := encodeFramedPayload(meta.forDisk(d), payload)
		if err != nil {
			return err
		}
		return s.disks[d].writeFilePushed(st.metaTmp(d), data)
	})
	s.flushStage(st)
	s.forStage(st, func(d int) error {
		dk := s.disks[d]
		objDir := dk.objectDir(meta.Bucket, meta.Key)
		if _, err := dk.mkdirAllPushed(objDir); err != nil {
			return err
		}
		if !st.inline {
			if err := os.Rename(st.dir(d, "data"), filepath.Join(objDir, meta.VersionID)); err != nil {
				return err
			}
		}
		if err := os.Rename(st.metaTmp(d), filepath.Join(objDir, meta.VersionID+metaSuffix)); err != nil {
			if !st.inline {
				dk.removeAll(filepath.Join(objDir, meta.VersionID))
			}
			return err
		}
		return dk.pushDir(objDir)
	})
	s.flushStage(st)
	return st.count()
}

// unpublish rolls back a version that did not reach a write quorum, on
// every disk it may have reached.
func (s *Store) unpublish(meta *ObjectMeta) {
	s.onDisks(func(d *disk) error {
		objDir := d.objectDir(meta.Bucket, meta.Key)
		if err := removeIfExists(filepath.Join(objDir, meta.VersionID+metaSuffix)); err != nil {
			return err
		}
		return d.removeAll(filepath.Join(objDir, meta.VersionID))
	})
}

// removeVersion deletes a superseded version: its metadata at once, its
// data now or, if a reader holds a lease on it, when the last reader is
// done. The caller holds the key lock.
func (s *Store) removeVersion(bucket, key string, old entry) {
	s.onDisks(func(d *disk) error {
		return removeIfExists(filepath.Join(d.objectDir(bucket, key), old.versionID+metaSuffix))
	})
	if !old.inline {
		s.dropData(bucket, key, old.versionID, false)
	}
}

// dropData deletes a version's data directories. If no reader holds the
// version (no new reader can lease it while the caller holds the key lock),
// it happens now, and the object directory is removed too if it became
// empty; otherwise it is deferred to the last reader's release, which runs
// without the key lock and so leaves the directory alone (a concurrent PUT
// may be about to rename into it).
func (s *Store) dropData(bucket, key, version string, removeDir bool) {
	del := func(locked bool) {
		s.onDisks(func(d *disk) error {
			objDir := d.objectDir(bucket, key)
			err := d.removeAll(filepath.Join(objDir, version))
			if locked && removeDir {
				os.Remove(objDir) // fails, harmlessly, unless empty
			}
			return err
		})
	}
	if !s.leases.busy(version) {
		del(true)
		return
	}
	s.leases.retire(version, func() { del(false) })
}

// DeleteObject removes a key. Deleting a key that does not exist succeeds,
// as in S3.
func (s *Store) DeleteObject(bucketName, key string) error {
	b, err := s.getBucket(bucketName)
	if err != nil {
		return err
	}
	if err := validKey(key); err != nil {
		return err
	}
	b.mu.RLock()
	defer b.mu.RUnlock()
	lk := s.locks.get(bucketName, key)
	lk.Lock()
	defer lk.Unlock()
	cur, ok := b.index.get(key)
	if !ok {
		return nil
	}
	// Remove the metadata from a write quorum of disks first, so the
	// version can never again be found on k disks.
	removed := s.onDisks(func(d *disk) error {
		return removeIfExists(filepath.Join(d.objectDir(bucketName, key), cur.versionID+metaSuffix))
	})
	if removed < s.writeQuorum {
		if s.heal != nil {
			s.heal.enqueue(bucketName, key) // restores the metadata copies
		}
		return errWriteQuorum(removed, s.writeQuorum)
	}
	b.index.delete(key)
	if cur.inline {
		s.onDisks(func(d *disk) error {
			os.Remove(d.objectDir(bucketName, key)) // only if empty
			return nil
		})
	} else {
		s.dropData(bucketName, key, cur.versionID, true)
	}
	s.stats.objectsDeleted.Add(1)
	return nil
}
