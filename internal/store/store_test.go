package store

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/useless-husband/strata/internal/s3err"
)

func testSeed(t testing.TB) uint64 {
	if s := os.Getenv("STRATA_SEED"); s != "" {
		v, err := strconv.ParseUint(s, 10, 64)
		if err != nil {
			t.Fatalf("bad STRATA_SEED: %v", err)
		}
		return v
	}
	return 20261001
}

type testStore struct {
	*Store
	t     testing.TB
	disks []string
	cfg   Config
}

// newTestStore opens a store over k+m fresh directories. Syncing is off:
// these tests are about logic, and a process crash (which the kill test
// covers) does not need it.
func newTestStore(t testing.TB, k, m int, mod ...func(*Config)) *testStore {
	t.Helper()
	root := t.TempDir()
	var disks []string
	for i := 0; i < k+m; i++ {
		disks = append(disks, filepath.Join(root, fmt.Sprintf("disk%d", i)))
	}
	cfg := Config{Disks: disks, DataShards: k, ParityShards: m, BlockSize: 64 << 10, Sync: SyncNone, DiskCheckInterval: -1}
	for _, f := range mod {
		f(&cfg)
	}
	s, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := &testStore{Store: s, t: t, disks: disks, cfg: cfg}
	t.Cleanup(func() { ts.Close() })
	return ts
}

// reopen closes and reopens the store over the same disks.
func (ts *testStore) reopen() {
	ts.t.Helper()
	ts.Close()
	s, err := Open(ts.cfg)
	if err != nil {
		ts.t.Fatal(err)
	}
	ts.Store = s
}

func (ts *testStore) put(bucket, key string, data []byte) ObjectInfo {
	ts.t.Helper()
	info, err := ts.PutObject(context.Background(), bucket, key, bytes.NewReader(data), PutOptions{})
	if err != nil {
		ts.t.Fatalf("put %s/%s: %v", bucket, key, err)
	}
	return info
}

func (ts *testStore) get(bucket, key string) ([]byte, error) {
	return ts.getRange(bucket, key, 0, -1)
}

func (ts *testStore) getRange(bucket, key string, off, n int64) ([]byte, error) {
	o, err := ts.OpenObject(bucket, key)
	if err != nil {
		return nil, err
	}
	defer o.Close()
	if n < 0 {
		n = o.Info.Size - off
	}
	var buf bytes.Buffer
	err = o.WriteRange(context.Background(), &buf, off, n)
	return buf.Bytes(), err
}

func (ts *testStore) mustGet(bucket, key string, want []byte) {
	ts.t.Helper()
	got, err := ts.get(bucket, key)
	if err != nil {
		ts.t.Fatalf("get %s/%s: %v", bucket, key, err)
	}
	if !bytes.Equal(got, want) {
		ts.t.Fatalf("get %s/%s: %d bytes differ from the %d written", bucket, key, len(got), len(want))
	}
}

func randData(r *rand.Rand, n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(r.Uint32())
	}
	return b
}

func md5hex(b []byte) string {
	h := md5.Sum(b)
	return hex.EncodeToString(h[:])
}

func TestOpenValidatesConfig(t *testing.T) {
	root := t.TempDir()
	d := func(i int) string { return filepath.Join(root, strconv.Itoa(i)) }
	for _, cfg := range []Config{
		{Disks: []string{d(0), d(1)}, DataShards: 2, ParityShards: 1},
		{Disks: []string{d(0), d(1)}, DataShards: 0, ParityShards: 2},
		{Disks: []string{d(0), d(0)}, DataShards: 1, ParityShards: 1},
		{Disks: []string{d(0), d(1)}, DataShards: 1, ParityShards: 1, BlockSize: 100},
	} {
		if s, err := Open(cfg); err == nil {
			s.Close()
			t.Errorf("Open(%+v) succeeded", cfg)
		}
	}
}

func TestDiskLockAndFormatChecks(t *testing.T) {
	ts := newTestStore(t, 2, 1)
	if _, err := Open(ts.cfg); err == nil || !strings.Contains(err.Error(), "in use") {
		t.Fatalf("second Open of the same disks: %v", err)
	}
	ts.Close()
	// Reordering the disks is refused.
	cfg := ts.cfg
	cfg.Disks = []string{ts.disks[1], ts.disks[0], ts.disks[2]}
	if _, err := Open(cfg); err == nil || !strings.Contains(err.Error(), "original order") {
		t.Fatalf("reordered disks: %v", err)
	}
	// Changing the erasure layout is refused.
	cfg = ts.cfg
	cfg.DataShards, cfg.ParityShards = 1, 2
	if _, err := Open(cfg); err == nil || !strings.Contains(err.Error(), "formatted") {
		t.Fatalf("changed layout: %v", err)
	}
	ts.reopen()
}

func TestBuckets(t *testing.T) {
	ts := newTestStore(t, 2, 1)
	for _, bad := range []string{"ab", "Upper", "-start", "end-", "a..b", "192.168.1.1", strings.Repeat("a", 64), "xn--abc", "has_underscore"} {
		if err := ts.MakeBucket(bad); !errors.Is(err, s3err.InvalidBucketName) {
			t.Errorf("MakeBucket(%q) = %v", bad, err)
		}
	}
	for _, name := range []string{"b2", "a-1", "c.d.e"} {
		if err := ts.MakeBucket(name); err != nil {
			if name == "b2" {
				continue // too short: 3 characters minimum
			}
			t.Fatal(err)
		}
	}
	if err := ts.MakeBucket("a-1"); !errors.Is(err, s3err.BucketAlreadyOwnedByYou) {
		t.Errorf("duplicate bucket: %v", err)
	}
	got := ts.ListBuckets()
	if len(got) != 2 || got[0].Name != "a-1" || got[1].Name != "c.d.e" {
		t.Fatalf("ListBuckets = %+v", got)
	}
	ts.put("a-1", "k", []byte("x"))
	if err := ts.DeleteBucket("a-1"); !errors.Is(err, s3err.BucketNotEmpty) {
		t.Fatalf("delete non-empty bucket: %v", err)
	}
	if err := ts.DeleteObject("a-1", "k"); err != nil {
		t.Fatal(err)
	}
	if err := ts.DeleteBucket("a-1"); err != nil {
		t.Fatal(err)
	}
	if _, err := ts.HeadBucket("a-1"); !errors.Is(err, s3err.NoSuchBucket) {
		t.Fatalf("deleted bucket: %v", err)
	}
	ts.reopen()
	if got := ts.ListBuckets(); len(got) != 1 || got[0].Name != "c.d.e" {
		t.Fatalf("after reopen ListBuckets = %+v", got)
	}
}

func TestPutGetRoundTrip(t *testing.T) {
	seed := testSeed(t)
	r := rand.New(rand.NewPCG(seed, 1))
	for _, cfg := range [][2]int{{1, 0}, {1, 1}, {2, 1}, {4, 2}, {5, 3}} {
		ts := newTestStore(t, cfg[0], cfg[1])
		ts.MakeBucket("bkt")
		bs := int(ts.geo.blockSize)
		for _, size := range []int{0, 1, 2, 3, 4, 5, 100, bs - 1, bs, bs + 1, 3*bs + 17} {
			data := randData(r, size)
			key := fmt.Sprintf("obj-%d", size)
			info := ts.put("bkt", key, data)
			if info.Size != int64(size) || info.ETag != md5hex(data) {
				t.Fatalf("%v size %d: info %+v", cfg, size, info)
			}
			ts.mustGet("bkt", key, data)
		}
	}
}

func TestRangedReads(t *testing.T) {
	seed := testSeed(t)
	r := rand.New(rand.NewPCG(seed, 2))
	ts := newTestStore(t, 3, 2, func(c *Config) { c.BlockSize = 4096 })
	ts.MakeBucket("bkt")
	data := randData(r, 3*4096+1000)
	ts.put("bkt", "k", data)
	check := func(off, n int64) {
		got, err := ts.getRange("bkt", "k", off, n)
		if err != nil {
			t.Fatalf("seed %d range %d+%d: %v", seed, off, n, err)
		}
		if !bytes.Equal(got, data[off:off+n]) {
			t.Fatalf("seed %d range %d+%d: wrong bytes", seed, off, n)
		}
	}
	// Every boundary of interest, then random ranges.
	for _, off := range []int64{0, 1, 1364, 1365, 1366, 4095, 4096, 4097, 12287, 12288, int64(len(data)) - 1} {
		for _, n := range []int64{0, 1, 2, 1365, 4096, 5000} {
			if off+n <= int64(len(data)) {
				check(off, n)
			}
		}
	}
	for i := 0; i < 300; i++ {
		off := r.Int64N(int64(len(data)))
		check(off, r.Int64N(int64(len(data))-off+1))
	}
	o, _ := ts.OpenObject("bkt", "k")
	defer o.Close()
	if err := o.WriteRange(context.Background(), io.Discard, 10, int64(len(data))); !errors.Is(err, s3err.InvalidRange) {
		t.Fatalf("range past the end: %v", err)
	}
}

func TestOverwriteAndDelete(t *testing.T) {
	ts := newTestStore(t, 2, 1)
	ts.MakeBucket("bkt")
	big := bytes.Repeat([]byte("big"), 100_000)
	ts.put("bkt", "k", []byte("one"))
	ts.put("bkt", "k", big)
	ts.mustGet("bkt", "k", big)
	// Exactly one version is left on each disk: <version>.meta and the
	// <version>/ data directory.
	for _, d := range ts.Store.disks {
		entries, _ := os.ReadDir(d.objectDir("bkt", "k"))
		if len(entries) != 2 {
			t.Fatalf("%s holds %d entries for the key, want 2", d, len(entries))
		}
	}
	// A small object is inline: only <version>.meta.
	ts.put("bkt", "k", []byte("second version"))
	ts.mustGet("bkt", "k", []byte("second version"))
	for _, d := range ts.Store.disks {
		entries, _ := os.ReadDir(d.objectDir("bkt", "k"))
		if len(entries) != 1 {
			t.Fatalf("%s holds %d entries for the inline key, want 1", d, len(entries))
		}
	}
	if err := ts.DeleteObject("bkt", "k"); err != nil {
		t.Fatal(err)
	}
	if _, err := ts.get("bkt", "k"); !errors.Is(err, s3err.NoSuchKey) {
		t.Fatalf("get after delete: %v", err)
	}
	if err := ts.DeleteObject("bkt", "k"); err != nil {
		t.Fatalf("deleting a missing key: %v", err)
	}
	for _, d := range ts.Store.disks {
		if _, err := os.Stat(d.objectDir("bkt", "k")); !os.IsNotExist(err) {
			t.Fatalf("%s still has the object directory", d)
		}
	}
}

func TestConditionalWrites(t *testing.T) {
	ts := newTestStore(t, 2, 1)
	ts.MakeBucket("bkt")
	ctx := context.Background()
	put := func(c Conditions) error {
		_, err := ts.PutObject(ctx, "bkt", "k", strings.NewReader("data"), PutOptions{Conditions: c})
		return err
	}
	if err := put(Conditions{IfMatch: "abc"}); !errors.Is(err, s3err.NoSuchKey) {
		t.Fatalf("If-Match on a missing key: %v", err)
	}
	if err := put(Conditions{IfNoneMatch: true}); err != nil {
		t.Fatal(err)
	}
	if err := put(Conditions{IfNoneMatch: true}); !errors.Is(err, s3err.PreconditionFailed) {
		t.Fatalf("If-None-Match: * on an existing key: %v", err)
	}
	if err := put(Conditions{IfMatch: "wrong"}); !errors.Is(err, s3err.PreconditionFailed) {
		t.Fatalf("If-Match with the wrong ETag: %v", err)
	}
	if err := put(Conditions{IfMatch: md5hex([]byte("data"))}); err != nil {
		t.Fatalf("If-Match with the right ETag: %v", err)
	}
}

func TestPrecommitAbortLeavesNoTrace(t *testing.T) {
	ts := newTestStore(t, 2, 1)
	ts.MakeBucket("bkt")
	ts.put("bkt", "k", []byte("original"))
	boom := errors.New("bad digest")
	_, err := ts.PutObject(context.Background(), "bkt", "k", strings.NewReader("replacement"), PutOptions{
		Precommit: func([]byte) (Checksum, error) { return Checksum{}, boom },
	})
	if !errors.Is(err, boom) {
		t.Fatalf("got %v", err)
	}
	// A body that fails mid-stream (as a bad chunk signature does).
	_, err = ts.PutObject(context.Background(), "bkt", "k", io.MultiReader(strings.NewReader("partial"), errReader{boom}), PutOptions{})
	if !errors.Is(err, boom) {
		t.Fatalf("got %v", err)
	}
	ts.mustGet("bkt", "k", []byte("original"))
	for _, d := range ts.Store.disks {
		entries, _ := os.ReadDir(d.sysPath(tmpDir))
		if len(entries) != 0 {
			t.Fatalf("%s: staging area not cleaned: %d entries", d, len(entries))
		}
	}
}

type errReader struct{ err error }

func (e errReader) Read([]byte) (int, error) { return 0, e.err }

// TestListMatchesModel compares listings with prefixes, delimiters and
// pagination against a brute-force implementation over random keys.
func TestListMatchesModel(t *testing.T) {
	seed := testSeed(t)
	r := rand.New(rand.NewPCG(seed, 3))
	ts := newTestStore(t, 1, 0)
	ts.MakeBucket("bkt")
	alphabet := []string{"a", "b", "/", "-", "é", "\x00", "z", "//"}
	keys := map[string]bool{}
	for len(keys) < 400 {
		var sb strings.Builder
		for n := 1 + r.IntN(6); n > 0; n-- {
			sb.WriteString(alphabet[r.IntN(len(alphabet))])
		}
		keys[sb.String()] = true
	}
	var sorted []string
	for k := range keys {
		ts.put("bkt", k, nil)
		sorted = append(sorted, k)
	}
	sort.Strings(sorted)

	model := func(prefix, delim string) []string {
		var out []string
		seen := map[string]bool{}
		for _, k := range sorted {
			if !strings.HasPrefix(k, prefix) {
				continue
			}
			if cp, ok := commonPrefix(k, prefix, delim); ok {
				if !seen[cp] {
					seen[cp] = true
					out = append(out, "P:"+cp)
				}
				continue
			}
			out = append(out, "K:"+k)
		}
		return out
	}
	for trial := 0; trial < 300; trial++ {
		prefix := ""
		if r.IntN(3) > 0 {
			prefix = sorted[r.IntN(len(sorted))]
			prefix = prefix[:r.IntN(len(prefix)+1)]
		}
		delim := []string{"", "/", "-", "//", "é"}[r.IntN(5)]
		page := 1 + r.IntN(30)
		want := model(prefix, delim)
		var got []string
		after := ""
		for pages := 0; ; pages++ {
			if pages > len(sorted)+2 {
				t.Fatalf("seed %d: listing does not terminate", seed)
			}
			res, err := ts.ListObjects("bkt", ListOptions{Prefix: prefix, Delimiter: delim, After: after, MaxKeys: page})
			if err != nil {
				t.Fatal(err)
			}
			if len(res.Objects)+len(res.CommonPrefixes) > page {
				t.Fatalf("seed %d: page of %d has %d entries", seed, page, len(res.Objects)+len(res.CommonPrefixes))
			}
			// Merge the page in order.
			var items []string
			for _, o := range res.Objects {
				items = append(items, "K:"+o.Key)
			}
			for _, p := range res.CommonPrefixes {
				items = append(items, "P:"+p)
			}
			sort.Slice(items, func(i, j int) bool { return items[i][2:] < items[j][2:] })
			got = append(got, items...)
			if !res.IsTruncated {
				break
			}
			if res.NextMarker == "" {
				t.Fatalf("seed %d: truncated page without a marker", seed)
			}
			after = res.NextMarker
		}
		if strings.Join(got, "|") != strings.Join(want, "|") {
			t.Fatalf("seed %d trial %d prefix %q delim %q page %d:\n got  %q\n want %q", seed, trial, prefix, delim, page, got, want)
		}
	}
}

func TestListSkipsCommonPrefixesQuickly(t *testing.T) {
	ts := newTestStore(t, 1, 0)
	ts.MakeBucket("bkt")
	for i := 0; i < 30000; i++ {
		ts.Store.buckets["bkt"].index.put(entry{key: fmt.Sprintf("dir%d/file%05d", i%3, i)})
	}
	res, _ := ts.ListObjects("bkt", ListOptions{Delimiter: "/", MaxKeys: 1000})
	if len(res.CommonPrefixes) != 3 || len(res.Objects) != 0 || res.IsTruncated {
		t.Fatalf("got %+v", res)
	}
	if res.visited > 10 {
		t.Fatalf("listing 3 common prefixes over 30000 keys examined %d entries", res.visited)
	}
}

func TestMultipart(t *testing.T) {
	seed := testSeed(t)
	r := rand.New(rand.NewPCG(seed, 4))
	ts := newTestStore(t, 2, 1)
	ts.MakeBucket("bkt")
	ctx := context.Background()
	up, err := ts.NewMultipartUpload("bkt", "big", PutOptions{ContentType: "application/x-test", UserMeta: map[string]string{"a": "b"}}, "", "")
	if err != nil {
		t.Fatal(err)
	}
	parts := [][]byte{randData(r, MinPartSize), randData(r, MinPartSize+123), randData(r, 777)}
	var infos []PartInfo
	for i, p := range parts {
		info, err := ts.PutObjectPart(ctx, "bkt", "big", up.UploadID, i+1, bytes.NewReader(p), PartOptions{})
		if err != nil {
			t.Fatal(err)
		}
		infos = append(infos, info)
	}
	// Replace part 2.
	parts[1] = randData(r, MinPartSize+5)
	info, err := ts.PutObjectPart(ctx, "bkt", "big", up.UploadID, 2, bytes.NewReader(parts[1]), PartOptions{})
	if err != nil {
		t.Fatal(err)
	}
	infos[1] = info
	lp, _ := ts.ListParts("bkt", "big", up.UploadID, 0, 2)
	if len(lp.Parts) != 2 || !lp.IsTruncated || lp.NextPartNumberMarker != 2 || lp.Parts[1].ETag != md5hex(parts[1]) {
		t.Fatalf("ListParts = %+v", lp)
	}
	complete := func(cps ...CompletePart) error {
		_, err := ts.CompleteMultipartUpload(ctx, "bkt", "big", up.UploadID, cps, CompleteOptions{})
		return err
	}
	cp := func(i int) CompletePart { return CompletePart{Number: i + 1, ETag: `"` + infos[i].ETag + `"`} }
	if err := complete(cp(1), cp(0), cp(2)); !errors.Is(err, s3err.InvalidPartOrder) {
		t.Fatalf("out of order: %v", err)
	}
	if err := complete(cp(0), CompletePart{Number: 2, ETag: "bogus"}); !errors.Is(err, s3err.InvalidPart) {
		t.Fatalf("wrong etag: %v", err)
	}
	if err := complete(cp(0), CompletePart{Number: 4, ETag: infos[2].ETag}); !errors.Is(err, s3err.InvalidPart) {
		t.Fatalf("missing part: %v", err)
	}
	if err := complete(cp(0), cp(1), cp(2)); err != nil {
		t.Fatal(err)
	}
	want := bytes.Join(parts, nil)
	ts.mustGet("bkt", "big", want)
	o, _ := ts.OpenObject("bkt", "big")
	o.Close()
	if o.Info.ETag != multipartETag(o.Info.Parts) || !strings.HasSuffix(o.Info.ETag, "-3") ||
		o.Info.ContentType != "application/x-test" || o.Info.UserMeta["a"] != "b" {
		t.Fatalf("info %+v", o.Info)
	}
	// Retrying the same completion gets the same answer, as from S3; a
	// different part list finds no upload. The parts were moved, not copied.
	again, err := ts.CompleteMultipartUpload(ctx, "bkt", "big", up.UploadID, []CompletePart{cp(0), cp(1), cp(2)}, CompleteOptions{})
	if err != nil || again.ETag != o.Info.ETag {
		t.Fatalf("retried completion: %+v %v", again, err)
	}
	if err := complete(cp(0), cp(1)); !errors.Is(err, s3err.NoSuchUpload) {
		t.Fatalf("completing again with other parts: %v", err)
	}
	for _, d := range ts.Store.disks {
		if entries, _ := os.ReadDir(d.sysPath(multipartDir)); len(entries) != 0 {
			t.Fatalf("%s still has %d uploads", d, len(entries))
		}
	}
	// Ranged reads across part boundaries.
	for i := 0; i < 50; i++ {
		off := r.Int64N(int64(len(want)))
		n := r.Int64N(min(int64(len(want))-off, 3<<20) + 1)
		got, err := ts.getRange("bkt", "big", off, n)
		if err != nil || !bytes.Equal(got, want[off:off+n]) {
			t.Fatalf("seed %d: range %d+%d wrong (%v)", seed, off, n, err)
		}
	}
	ts.reopen()
	ts.mustGet("bkt", "big", want)
}

func TestMultipartSmallPartsAndAbort(t *testing.T) {
	ts := newTestStore(t, 2, 1)
	ts.MakeBucket("bkt")
	ctx := context.Background()
	up, _ := ts.NewMultipartUpload("bkt", "k", PutOptions{}, "", "")
	p1, _ := ts.PutObjectPart(ctx, "bkt", "k", up.UploadID, 1, strings.NewReader("small"), PartOptions{})
	p2, _ := ts.PutObjectPart(ctx, "bkt", "k", up.UploadID, 2, strings.NewReader("small"), PartOptions{})
	_, err := ts.CompleteMultipartUpload(ctx, "bkt", "k", up.UploadID, []CompletePart{{Number: 1, ETag: p1.ETag}, {Number: 2, ETag: p2.ETag}}, CompleteOptions{})
	if !errors.Is(err, s3err.EntityTooSmall) {
		t.Fatalf("small first part: %v", err)
	}
	// Uploads survive a restart.
	ts.reopen()
	lp, err := ts.ListParts("bkt", "k", up.UploadID, 0, 100)
	if err != nil || len(lp.Parts) != 2 {
		t.Fatalf("after reopen: %+v %v", lp, err)
	}
	lu, _ := ts.ListMultipartUploads("bkt", ListUploadsOptions{MaxUploads: 10})
	if len(lu.Uploads) != 1 || lu.Uploads[0].UploadID != up.UploadID {
		t.Fatalf("ListMultipartUploads = %+v", lu)
	}
	if err := ts.AbortMultipartUpload("bkt", "k", up.UploadID); err != nil {
		t.Fatal(err)
	}
	if _, err := ts.ListParts("bkt", "k", up.UploadID, 0, 100); !errors.Is(err, s3err.NoSuchUpload) {
		t.Fatalf("after abort: %v", err)
	}
	// A single small part is fine.
	up, _ = ts.NewMultipartUpload("bkt", "k", PutOptions{}, "", "")
	p1, _ = ts.PutObjectPart(ctx, "bkt", "k", up.UploadID, 1, strings.NewReader("small"), PartOptions{})
	if _, err := ts.CompleteMultipartUpload(ctx, "bkt", "k", up.UploadID, []CompletePart{{Number: 1, ETag: p1.ETag}}, CompleteOptions{}); err != nil {
		t.Fatal(err)
	}
	ts.mustGet("bkt", "k", []byte("small"))
}

func TestReadDuringOverwriteKeepsOldVersion(t *testing.T) {
	ts := newTestStore(t, 2, 1, func(c *Config) { c.BlockSize = 4096 })
	ts.MakeBucket("bkt")
	r := rand.New(rand.NewPCG(1, 1))
	old := randData(r, 50000)
	ts.put("bkt", "k", old)
	o, err := ts.OpenObject("bkt", "k")
	if err != nil {
		t.Fatal(err)
	}
	ts.put("bkt", "k", []byte("new"))
	if err := ts.DeleteObject("bkt", "k"); err != nil {
		t.Fatal(err)
	}
	// Remove a data shard so the old reader must open parity files too.
	d0 := ts.Store.disks[0]
	os.RemoveAll(filepath.Join(d0.objectDir("bkt", "k"), o.Info.VersionID))
	var buf bytes.Buffer
	if err := o.WriteRange(context.Background(), &buf, 0, o.Info.Size); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(buf.Bytes(), old) {
		t.Fatal("the open reader did not see the version it opened")
	}
	o.Close()
	// Once released, the old data is gone from every disk.
	for _, d := range ts.Store.disks {
		if _, err := os.Stat(filepath.Join(d.objectDir("bkt", "k"), o.Info.VersionID)); !os.IsNotExist(err) {
			t.Fatalf("%s still has the released version", d)
		}
	}
}

func TestConcurrentWritersAndReaders(t *testing.T) {
	ts := newTestStore(t, 3, 2, func(c *Config) { c.BlockSize = 4096 })
	ts.MakeBucket("bkt")
	var wg sync.WaitGroup
	bodies := func(w, i int) []byte {
		return bytes.Repeat([]byte(fmt.Sprintf("writer %d version %d;", w, i)), 1+i%300)
	}
	for w := 0; w < 6; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 40; i++ {
				key := fmt.Sprintf("k%d", i%5)
				if _, err := ts.PutObject(context.Background(), "bkt", key, bytes.NewReader(bodies(w, i)), PutOptions{}); err != nil {
					t.Error(err)
					return
				}
				if i%7 == 0 {
					ts.DeleteObject("bkt", key)
				}
			}
		}()
	}
	for g := 0; g < 6; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				data, err := ts.get("bkt", fmt.Sprintf("k%d", i%5))
				if errors.Is(err, s3err.NoSuchKey) {
					continue
				}
				if err != nil {
					t.Error(err)
					return
				}
				// Every read is one whole version: a repetition of one
				// writer's record.
				if len(data) == 0 {
					t.Error("empty read")
					return
				}
				rec := data[:bytes.IndexByte(data, ';')+1]
				if !bytes.Equal(data, bytes.Repeat(rec, len(data)/len(rec))) {
					t.Errorf("torn read: %.80q", data)
					return
				}
			}
		}()
	}
	wg.Wait()
}
