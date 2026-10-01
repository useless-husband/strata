package store

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/useless-husband/strata/internal/checksum"
	"github.com/useless-husband/strata/internal/s3err"
)

// A multipart upload lives under <disk>/.strata/multipart/<upload id>/:
//
//	upload.json        the upload (bucket, key, attributes of the object)
//	part.<n>.meta      the current version of part n
//	p<n>.<id>          a shard file of part n
//
// Each part is erasure-coded on its own, with the same shard distribution
// as the final object. Replacing a part writes a new shard file under a new
// name and then atomically replaces part.<n>.meta, so a crash never pairs
// metadata with the wrong data. Completing an upload hard-links the chosen
// shard files into a staged object version and commits it like a PUT; the
// upload is only removed once the object is committed, so a failed or
// interrupted completion can be retried.

type uploadMeta struct {
	ID           string            `json:"id"`
	Bucket       string            `json:"bucket"`
	Key          string            `json:"key"`
	Initiated    int64             `json:"initiated"`
	ContentType  string            `json:"contentType,omitempty"`
	UserMeta     map[string]string `json:"userMeta,omitempty"`
	Headers      map[string]string `json:"headers,omitempty"`
	Checksum     string            `json:"checksumAlgorithm,omitempty"`
	ChecksumType string            `json:"checksumType,omitempty"`
	Dist         []int             `json:"distribution"`
}

type partMeta struct {
	PartInfo
	File    string `json:"file"`
	ModTime int64  `json:"mtime"`
}

type upload struct {
	meta  uploadMeta
	mu    sync.Mutex // serialises part commits, completion and abort
	parts map[int]*partMeta
	done  bool
}

type uploads struct {
	mu sync.Mutex
	m  map[string]*upload
	// done remembers recently completed uploads, so a client that retries
	// CompleteMultipartUpload (because the first response was lost) gets
	// the same answer, as from S3. Kept in memory only.
	done  map[string]*completed
	order []string
}

type completed struct {
	bucket, key string
	parts       string // the completed part list, for matching retries
	info        ObjectInfo
}

const maxCompleted = 10000

func newUploads() *uploads { return &uploads{m: map[string]*upload{}, done: map[string]*completed{}} }

func (u *uploads) remember(id string, c *completed) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.done[id] = c
	u.order = append(u.order, id)
	if len(u.order) > maxCompleted {
		delete(u.done, u.order[0])
		u.order = u.order[1:]
	}
}

func (u *uploads) completedUpload(id string) *completed {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.done[id]
}

func (u *uploads) get(id string) *upload {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.m[id]
}

func (u *uploads) put(up *upload) {
	u.mu.Lock()
	u.m[up.meta.ID] = up
	u.mu.Unlock()
}

func (u *uploads) remove(id string) {
	u.mu.Lock()
	delete(u.m, id)
	u.mu.Unlock()
}

func (u *uploads) forBucket(bucket string) []*upload {
	u.mu.Lock()
	defer u.mu.Unlock()
	var out []*upload
	for _, up := range u.m {
		if up.meta.Bucket == bucket {
			out = append(out, up)
		}
	}
	return out
}

func (d *disk) uploadDir(id string) string { return d.sysPath(multipartDir, id) }

// UploadInfo describes an in-progress multipart upload.
type UploadInfo struct {
	UploadID  string
	Bucket    string
	Key       string
	Initiated time.Time
	// ChecksumAlgorithm is the algorithm requested at creation, if any.
	ChecksumAlgorithm string
	ChecksumType      string
}

func (up *upload) info() UploadInfo {
	return UploadInfo{UploadID: up.meta.ID, Bucket: up.meta.Bucket, Key: up.meta.Key,
		Initiated: time.Unix(0, up.meta.Initiated).UTC(), ChecksumAlgorithm: up.meta.Checksum, ChecksumType: up.meta.ChecksumType}
}

// NewMultipartUpload starts an upload.
// checksumAlgorithm and checksumType (COMPOSITE or FULL_OBJECT) select how
// the object's checksum is derived from its parts; empty for none.
func (s *Store) NewMultipartUpload(bucketName, key string, opts PutOptions, checksumAlgorithm, checksumType string) (UploadInfo, error) {
	if _, err := s.getBucket(bucketName); err != nil {
		return UploadInfo{}, err
	}
	if err := validKey(key); err != nil {
		return UploadInfo{}, err
	}
	m := uploadMeta{ID: newID(), Bucket: bucketName, Key: key, Initiated: time.Now().UnixNano(),
		ContentType: opts.ContentType, UserMeta: opts.UserMeta, Headers: opts.Headers,
		Checksum: checksumAlgorithm, ChecksumType: checksumType, Dist: distribution(key, s.n)}
	data, err := encodeFramed(&m)
	if err != nil {
		return UploadInfo{}, err
	}
	ok := s.onDisks(func(d *disk) error {
		if err := os.Mkdir(d.uploadDir(m.ID), 0o755); err != nil {
			return err
		}
		return d.writeFileAtomic(filepath.Join(d.uploadDir(m.ID), "upload.json"), data)
	})
	if ok < s.writeQuorum {
		s.onDisks(func(d *disk) error { return d.removeAll(d.uploadDir(m.ID)) })
		return UploadInfo{}, errWriteQuorum(ok, s.writeQuorum)
	}
	up := &upload{meta: m, parts: map[int]*partMeta{}}
	s.uploads.put(up)
	return up.info(), nil
}

func (s *Store) getUpload(bucket, key, id string) (*upload, error) {
	if _, err := s.getBucket(bucket); err != nil {
		return nil, err
	}
	up := s.uploads.get(id)
	if up == nil || up.meta.Bucket != bucket || up.meta.Key != key {
		return nil, s3err.NoSuchUpload
	}
	return up, nil
}

// GetUpload returns an in-progress upload.
func (s *Store) GetUpload(bucket, key, id string) (UploadInfo, error) {
	up, err := s.getUpload(bucket, key, id)
	if err != nil {
		return UploadInfo{}, err
	}
	return up.info(), nil
}

// PartOptions are the options of UploadPart.
type PartOptions struct {
	// Precommit, as in PutOptions.
	Precommit func(md5sum []byte) (Checksum, error)
}

// PutObjectPart stores part number n of an upload, replacing any earlier
// upload of the same part number.
func (s *Store) PutObjectPart(ctx context.Context, bucket, key, uploadID string, n int, body io.Reader, opts PartOptions) (PartInfo, error) {
	up, err := s.getUpload(bucket, key, uploadID)
	if err != nil {
		return PartInfo{}, err
	}
	if n < 1 || n > MaxPartNumber {
		return PartInfo{}, s3err.TooManyParts
	}
	st := s.newStage()
	defer st.discard()
	size, sum, err := s.writePart(ctx, st, n, body, up.meta.Dist, MaxPutSize)
	if err != nil {
		return PartInfo{}, err
	}
	pm := &partMeta{PartInfo: PartInfo{Number: n, Size: size, ETag: hex.EncodeToString(sum)},
		File: fmt.Sprintf("p%d.%s", n, newID()), ModTime: time.Now().UnixNano()}
	if opts.Precommit != nil {
		ck, err := opts.Precommit(sum)
		if err != nil {
			return PartInfo{}, err
		}
		pm.Checksum = ck.Value
	}
	data, err := encodeFramed(pm)
	if err != nil {
		return PartInfo{}, err
	}

	up.mu.Lock()
	defer up.mu.Unlock()
	if up.done {
		return PartInfo{}, s3err.NoSuchUpload
	}
	// Two durable steps, as in publish: the shard file under its final
	// name and the new part metadata under a temporary name, then the
	// rename of the metadata over the previous one.
	name := "part." + strconv.Itoa(n)
	s.forStage(st, func(d int) error {
		dk := s.disks[d]
		dir := dk.uploadDir(uploadID)
		if err := os.Rename(st.dir(d, "data", name), filepath.Join(dir, pm.File)); err != nil {
			return err
		}
		if err := dk.writeFilePushed(st.dir(d, "partmeta"), data); err != nil {
			return err
		}
		return dk.pushDir(dir)
	})
	s.flushStage(st)
	s.forStage(st, func(d int) error {
		dk := s.disks[d]
		dir := dk.uploadDir(uploadID)
		if err := os.Rename(st.dir(d, "partmeta"), filepath.Join(dir, name+metaSuffix)); err != nil {
			os.Remove(filepath.Join(dir, pm.File))
			return err
		}
		return dk.pushDir(dir)
	})
	s.flushStage(st)
	if ok := st.count(); ok < s.writeQuorum {
		// Disks that took the new part keep it; on the others the old part
		// (if any) is still current. Completion will only use disks that
		// agree, so drop the new file where it landed.
		s.onDisks(func(d *disk) error {
			return removeIfExists(filepath.Join(d.uploadDir(uploadID), pm.File))
		})
		return PartInfo{}, errWriteQuorum(ok, s.writeQuorum)
	}
	if old := up.parts[n]; old != nil {
		s.onDisks(func(d *disk) error {
			return removeIfExists(filepath.Join(d.uploadDir(uploadID), old.File))
		})
	}
	up.parts[n] = pm
	return pm.PartInfo, nil
}

// ListPartsResult is a page of ListParts.
type ListPartsResult struct {
	Parts                []PartInfo
	ModTimes             []time.Time
	NextPartNumberMarker int
	IsTruncated          bool
}

// ListParts lists the parts of an upload with numbers above marker.
func (s *Store) ListParts(bucket, key, uploadID string, marker, max int) (ListPartsResult, error) {
	up, err := s.getUpload(bucket, key, uploadID)
	if err != nil {
		return ListPartsResult{}, err
	}
	up.mu.Lock()
	var nums []int
	for n := range up.parts {
		if n > marker {
			nums = append(nums, n)
		}
	}
	sort.Ints(nums)
	var res ListPartsResult
	for _, n := range nums {
		if len(res.Parts) == max {
			res.IsTruncated = true
			break
		}
		p := up.parts[n]
		res.Parts = append(res.Parts, p.PartInfo)
		res.ModTimes = append(res.ModTimes, time.Unix(0, p.ModTime).UTC())
		res.NextPartNumberMarker = n
	}
	up.mu.Unlock()
	return res, nil
}

// CompletePart is a part named in CompleteMultipartUpload.
type CompletePart struct {
	Number   int
	ETag     string // without quotes
	Checksum string // the part's checksum, if the client listed it
}

// CompleteOptions are the options of CompleteMultipartUpload.
type CompleteOptions struct {
	Conditions Conditions
	// Checksum is the object checksum the client expects (the
	// x-amz-checksum-* header of the request), if any.
	Checksum string
}

func partListKey(parts []CompletePart) string {
	var b strings.Builder
	for _, p := range parts {
		fmt.Fprintf(&b, "%d:%s;", p.Number, strings.Trim(p.ETag, `"`))
	}
	return b.String()
}

// multipartETag is the ETag S3 gives multipart objects: the MD5 of the
// concatenated binary MD5s of the parts, then "-" and the part count.
func multipartETag(parts []PartInfo) string {
	h := md5.New()
	for _, p := range parts {
		b, _ := hex.DecodeString(p.ETag)
		h.Write(b)
	}
	return hex.EncodeToString(h.Sum(nil)) + "-" + strconv.Itoa(len(parts))
}

// CompleteMultipartUpload assembles the named parts into an object.
func (s *Store) CompleteMultipartUpload(ctx context.Context, bucketName, key, uploadID string, parts []CompletePart, opt CompleteOptions) (ObjectInfo, error) {
	b, err := s.getBucket(bucketName)
	if err != nil {
		return ObjectInfo{}, err
	}
	up, err := s.getUpload(bucketName, key, uploadID)
	if err != nil {
		// A retry of a completion that succeeded gets the same answer.
		if c := s.uploads.completedUpload(uploadID); c != nil && c.bucket == bucketName && c.key == key && c.parts == partListKey(parts) {
			return c.info, nil
		}
		return ObjectInfo{}, err
	}
	up.mu.Lock()
	defer up.mu.Unlock()
	if up.done {
		return ObjectInfo{}, s3err.NoSuchUpload
	}
	if len(parts) == 0 {
		return ObjectInfo{}, s3err.MalformedXML.With("You must specify at least one part")
	}
	for i := 1; i < len(parts); i++ {
		if parts[i].Number <= parts[i-1].Number {
			return ObjectInfo{}, s3err.InvalidPartOrder
		}
	}
	var chosen []*partMeta
	var size int64
	for i, cp := range parts {
		pm := up.parts[cp.Number]
		if pm == nil || strings.Trim(cp.ETag, `"`) != pm.ETag || (cp.Checksum != "" && cp.Checksum != pm.Checksum) {
			return ObjectInfo{}, s3err.InvalidPart
		}
		if i < len(parts)-1 && pm.Size < MinPartSize {
			return ObjectInfo{}, s3err.EntityTooSmall.With("Part %d is %d bytes; every part but the last must be at least 5 MiB", cp.Number, pm.Size)
		}
		chosen = append(chosen, pm)
		size += pm.Size
	}
	if size > MaxObjectSize {
		return ObjectInfo{}, s3err.EntityTooLarge
	}
	meta := &ObjectMeta{
		Bucket: bucketName, Key: key, VersionID: newID(), Size: size,
		ContentType: up.meta.ContentType, UserMeta: up.meta.UserMeta, Headers: up.meta.Headers,
		Erasure: ErasureInfo{Data: s.k, Parity: s.m, BlockSize: s.cfg.BlockSize, Distribution: up.meta.Dist},
	}
	for _, pm := range chosen {
		meta.Parts = append(meta.Parts, pm.PartInfo)
	}
	meta.ETag = multipartETag(meta.Parts)
	if algo := up.meta.Checksum; algo != "" {
		var cps []checksum.MultipartPart
		for _, pm := range chosen {
			cps = append(cps, checksum.MultipartPart{Checksum: pm.Checksum, Size: pm.Size})
		}
		sum, err := checksum.Multipart(algo, up.meta.ChecksumType, cps)
		if err != nil {
			return ObjectInfo{}, s3err.InvalidRequest.With("The upload was created with checksum %s; %v", algo, err)
		}
		if opt.Checksum != "" && opt.Checksum != sum {
			return ObjectInfo{}, s3err.BadDigest.With("The %s you specified did not match the calculated checksum.", algo)
		}
		meta.ChecksumAlgorithm, meta.Checksum, meta.ChecksumType = algo, sum, up.meta.ChecksumType
	}

	// Stage the version by hard-linking the shard files; the upload itself
	// stays intact until the object is committed.
	st := s.newStage()
	defer st.discard()
	for d, dk := range s.disks {
		if !st.alive[d] {
			continue
		}
		for _, pm := range chosen {
			src := filepath.Join(dk.uploadDir(uploadID), pm.File)
			if err := os.Link(src, st.dir(d, "data", "part."+strconv.Itoa(pm.Number))); err != nil {
				st.fail(d, err)
				break
			}
		}
	}
	if st.count() < s.writeQuorum {
		return ObjectInfo{}, errWriteQuorum(st.count(), s.writeQuorum)
	}
	info, err := s.commit(b, meta, st, opt.Conditions)
	if err != nil {
		return ObjectInfo{}, err
	}
	up.done = true
	s.uploads.remove(uploadID)
	s.uploads.remember(uploadID, &completed{bucket: bucketName, key: key, parts: partListKey(parts), info: info})
	s.onDisks(func(d *disk) error { return d.removeAll(d.uploadDir(uploadID)) })
	return info, nil
}

// AbortMultipartUpload discards an upload and its parts.
func (s *Store) AbortMultipartUpload(bucket, key, uploadID string) error {
	up, err := s.getUpload(bucket, key, uploadID)
	if err != nil {
		return err
	}
	s.abortUpload(up)
	return nil
}

func (s *Store) abortUpload(up *upload) {
	up.mu.Lock()
	defer up.mu.Unlock()
	if up.done {
		return
	}
	up.done = true
	s.uploads.remove(up.meta.ID)
	s.onDisks(func(d *disk) error { return d.removeAll(d.uploadDir(up.meta.ID)) })
}

// ListUploadsOptions selects a page of ListMultipartUploads.
type ListUploadsOptions struct {
	Prefix, Delimiter         string
	KeyMarker, UploadIDMarker string
	MaxUploads                int
}

// ListUploadsResult is a page of ListMultipartUploads.
type ListUploadsResult struct {
	Uploads            []UploadInfo
	CommonPrefixes     []string
	IsTruncated        bool
	NextKeyMarker      string
	NextUploadIDMarker string
}

// ListMultipartUploads lists in-progress uploads ordered by key, then by
// initiation time.
func (s *Store) ListMultipartUploads(bucket string, opt ListUploadsOptions) (ListUploadsResult, error) {
	if _, err := s.getBucket(bucket); err != nil {
		return ListUploadsResult{}, err
	}
	var all []UploadInfo
	for _, up := range s.uploads.forBucket(bucket) {
		if strings.HasPrefix(up.meta.Key, opt.Prefix) {
			all = append(all, up.info())
		}
	}
	sort.Slice(all, func(i, j int) bool {
		if all[i].Key != all[j].Key {
			return all[i].Key < all[j].Key
		}
		if !all[i].Initiated.Equal(all[j].Initiated) {
			return all[i].Initiated.Before(all[j].Initiated)
		}
		return all[i].UploadID < all[j].UploadID
	})
	var res ListUploadsResult
	pastMarker := opt.KeyMarker == ""
	seenPrefix := map[string]bool{}
	count := 0
	for _, u := range all {
		if !pastMarker {
			if u.Key < opt.KeyMarker {
				continue
			}
			if u.Key == opt.KeyMarker {
				if opt.UploadIDMarker == "" {
					continue
				}
				if u.UploadID == opt.UploadIDMarker {
					pastMarker = true
				}
				continue
			}
			pastMarker = true
		}
		if cp, ok := commonPrefix(u.Key, opt.Prefix, opt.Delimiter); ok {
			if seenPrefix[cp] || (opt.KeyMarker != "" && strings.HasPrefix(opt.KeyMarker, cp)) {
				continue
			}
			if count == opt.MaxUploads {
				res.IsTruncated = true
				break
			}
			seenPrefix[cp] = true
			res.CommonPrefixes = append(res.CommonPrefixes, cp)
			res.NextKeyMarker, res.NextUploadIDMarker = cp, ""
			count++
			continue
		}
		if count == opt.MaxUploads {
			res.IsTruncated = true
			break
		}
		res.Uploads = append(res.Uploads, u)
		res.NextKeyMarker, res.NextUploadIDMarker = u.Key, u.UploadID
		count++
	}
	if !res.IsTruncated {
		res.NextKeyMarker, res.NextUploadIDMarker = "", ""
	}
	return res, nil
}

// loadUploads rebuilds the in-memory state of multipart uploads from the
// disks at start-up. A part counts if k disks agree on its current file.
func (s *Store) loadUploads(allOnline bool) error {
	type seen struct {
		meta  *uploadMeta
		disks int
		parts map[int]map[string][]*partMeta // number -> file -> copies
	}
	found := map[string]*seen{}
	for _, d := range s.disks {
		entries, err := os.ReadDir(d.sysPath(multipartDir))
		if err != nil {
			if isNotExist(err) {
				continue
			}
			return err
		}
		for _, e := range entries {
			id := e.Name()
			var um uploadMeta
			if err := readFramed(filepath.Join(d.uploadDir(id), "upload.json"), &um); err != nil {
				continue
			}
			sn := found[id]
			if sn == nil {
				sn = &seen{meta: &um, parts: map[int]map[string][]*partMeta{}}
				found[id] = sn
			}
			sn.disks++
			files, _ := os.ReadDir(d.uploadDir(id))
			for _, f := range files {
				name := f.Name()
				if !strings.HasPrefix(name, "part.") || !strings.HasSuffix(name, metaSuffix) {
					continue
				}
				var pm partMeta
				if readFramed(filepath.Join(d.uploadDir(id), name), &pm) != nil {
					continue
				}
				if _, err := os.Stat(filepath.Join(d.uploadDir(id), pm.File)); err != nil {
					continue
				}
				if sn.parts[pm.Number] == nil {
					sn.parts[pm.Number] = map[string][]*partMeta{}
				}
				sn.parts[pm.Number][pm.File] = append(sn.parts[pm.Number][pm.File], &pm)
			}
		}
	}
	for id, sn := range found {
		if sn.disks < s.k {
			if allOnline {
				s.onDisks(func(d *disk) error { return d.removeAll(d.uploadDir(id)) })
			}
			continue
		}
		if _, err := s.getBucket(sn.meta.Bucket); err != nil {
			s.onDisks(func(d *disk) error { return d.removeAll(d.uploadDir(id)) })
			continue
		}
		up := &upload{meta: *sn.meta, parts: map[int]*partMeta{}}
		for n, byFile := range sn.parts {
			var best *partMeta
			for _, copies := range byFile {
				if len(copies) >= s.k && (best == nil || copies[0].ModTime > best.ModTime) {
					best = copies[0]
				}
			}
			if best != nil {
				up.parts[n] = best
			}
		}
		s.uploads.put(up)
	}
	return nil
}
