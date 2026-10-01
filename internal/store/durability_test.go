package store

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/useless-husband/strata/internal/s3err"
)

// corpus writes a set of objects of awkward sizes, including a multipart
// object, and returns their contents by key.
func corpus(t *testing.T, ts *testStore, r *rand.Rand) map[string][]byte {
	t.Helper()
	ts.MakeBucket("bkt")
	bs := int(ts.geo.blockSize)
	objs := map[string][]byte{}
	for i, size := range []int{0, 1, 7, 1000, bs - 1, bs, bs + 1, 5*bs + 3} {
		key := fmt.Sprintf("dir%d/obj-%d", i%3, size)
		objs[key] = randData(r, size)
		ts.put("bkt", key, objs[key])
	}
	ctx := context.Background()
	up, err := ts.NewMultipartUpload("bkt", "multi/part", PutOptions{}, "", "")
	if err != nil {
		t.Fatal(err)
	}
	var cps []CompletePart
	var whole []byte
	for i, size := range []int{MinPartSize, 3*bs + 5} {
		data := randData(r, size)
		p, err := ts.PutObjectPart(ctx, "bkt", "multi/part", up.UploadID, i+1, bytes.NewReader(data), PartOptions{})
		if err != nil {
			t.Fatal(err)
		}
		cps = append(cps, CompletePart{Number: i + 1, ETag: p.ETag})
		whole = append(whole, data...)
	}
	if _, err := ts.CompleteMultipartUpload(ctx, "bkt", "multi/part", up.UploadID, cps, CompleteOptions{}); err != nil {
		t.Fatal(err)
	}
	objs["multi/part"] = whole
	return objs
}

func (ts *testStore) checkAll(objs map[string][]byte) {
	ts.t.Helper()
	for key, want := range objs {
		ts.mustGet("bkt", key, want)
	}
}

// deepClean runs a deep sweep and fails if anything is damaged.
func (ts *testStore) deepClean(when string) {
	ts.t.Helper()
	r, err := ts.Sweep(context.Background(), HealOptions{Deep: true, DryRun: true})
	if err != nil {
		ts.t.Fatal(err)
	}
	if r.Damaged != 0 || r.Lost != 0 {
		ts.t.Fatalf("%s: deep check found %d damaged and %d lost objects", when, r.Damaged, r.Lost)
	}
}

func (ts *testStore) wipe(disks ...int) {
	for _, d := range disks {
		if err := os.RemoveAll(ts.disks[d]); err != nil {
			ts.t.Fatal(err)
		}
	}
}

// TestSurvivesLosingParityDisks wipes every combination of m disks (the
// whole directory, format file included) of several codes, checks that
// every object still reads back bit-exact, heals, and checks the healed
// copies with a deep scrub.
func TestSurvivesLosingParityDisks(t *testing.T) {
	seed := testSeed(t)
	for _, cfg := range [][3]int{{2, 1, 0}, {2, 2, 0}, {4, 2, 0}, {3, 3, 0}, {4, 2, -1}, {3, 3, -1}} {
		k, m, inline := cfg[0], cfg[1], cfg[2]
		t.Run(fmt.Sprintf("%d+%d/inline=%v", k, m, inline == 0), func(t *testing.T) {
			r := rand.New(rand.NewPCG(seed, uint64(k*10+m)))
			ts := newTestStore(t, k, m, func(c *Config) { c.BlockSize = 4096; c.InlineLimit = inline })
			objs := corpus(t, ts, r)
			forEachSubset(k+m, m, func(lost []int) {
				before := ts.Stats().HealedObjects
				ts.wipe(lost...)
				ts.checkAll(objs)
				// Reads queued the objects for healing; a sweep (as after a
				// disk replacement) catches the rest.
				ts.CheckDisks()
				res, err := ts.Sweep(context.Background(), HealOptions{})
				if err != nil {
					t.Fatal(err)
				}
				ts.WaitHealIdle()
				if healed := ts.Stats().HealedObjects - before; healed < int64(len(objs)) || res.Lost != 0 {
					t.Fatalf("seed %d: wiped %v: %d of %d objects healed, %d lost", seed, lost, healed, len(objs), res.Lost)
				}
				ts.deepClean(fmt.Sprintf("after healing disks %v", lost))
			})
			// Losing m disks again after healing, then restarting, still works:
			// the healed copies are real.
			ts.wipe(r.Perm(k + m)[:m]...)
			ts.reopen()
			ts.checkAll(objs)
		})
	}
}

// TestBitRot flips random bits in up to m shard files of every object,
// checks reads are still bit-exact, and that on-read healing repairs them.
func TestBitRot(t *testing.T) {
	for _, inline := range []int{0, -1} {
		t.Run(fmt.Sprintf("inline=%v", inline == 0), func(t *testing.T) { testBitRot(t, inline) })
	}
}

// shardRegions returns, for disk d's copy of an object, the files holding
// its shard data and the offset where the shard data starts in each: the
// part files, or the metadata file of an inline object (after its header).
func (ts *testStore) shardRegions(d int, info ObjectInfo, inline bool) (paths []string, offsets []int) {
	dir := ts.Store.disks[d].objectDir("bkt", info.Key)
	if inline {
		path := filepath.Join(dir, info.VersionID+metaSuffix)
		b, err := os.ReadFile(path)
		if err != nil {
			ts.t.Fatal(err)
		}
		return []string{path}, []int{frameHeader + int(binary.BigEndian.Uint32(b[8:]))}
	}
	for _, p := range info.Parts {
		paths = append(paths, filepath.Join(dir, info.VersionID, "part."+strconv.Itoa(p.Number)))
		offsets = append(offsets, 0)
	}
	return paths, offsets
}

func testBitRot(t *testing.T, inlineLimit int) {
	seed := testSeed(t)
	r := rand.New(rand.NewPCG(seed, 99))
	ts := newTestStore(t, 4, 2, func(c *Config) { c.BlockSize = 4096; c.InlineLimit = inlineLimit })
	objs := corpus(t, ts, r)
	flipped := 0
	for key := range objs {
		o, err := ts.OpenObject("bkt", key)
		if err != nil {
			t.Fatal(err)
		}
		o.Close()
		for _, d := range r.Perm(6)[:1+r.IntN(2)] {
			paths, offsets := ts.shardRegions(d, o.Info, o.meta.Inline)
			for i, path := range paths {
				data, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				if len(data) == offsets[i] {
					continue
				}
				for j := 0; j < 1+r.IntN(5); j++ {
					data[offsets[i]+r.IntN(len(data)-offsets[i])] ^= 1 << r.IntN(8)
				}
				os.WriteFile(path, data, 0o644)
				flipped++
			}
		}
	}
	before := ts.Stats()
	ts.checkAll(objs)
	after := ts.Stats()
	if after.CorruptBlocks == before.CorruptBlocks || after.DegradedStripes == before.DegradedStripes {
		t.Fatalf("seed %d: flipped bits in %d files but reads detected nothing", seed, flipped)
	}
	ts.WaitHealIdle()
	if ts.Stats().HealedObjects == before.HealedObjects {
		t.Fatalf("seed %d: no object was healed on read", seed)
	}
	// Reads only touch the shards they need, so damage confined to parity
	// (or to the padding of a short last stripe) is the scrubber's job.
	if _, err := ts.Sweep(context.Background(), HealOptions{Deep: true}); err != nil {
		t.Fatal(err)
	}
	ts.deepClean("after on-read healing and a scrub")
	ts.checkAll(objs)
}

func TestCorruptMetadataIsHealed(t *testing.T) {
	ts := newTestStore(t, 2, 1)
	ts.MakeBucket("bkt")
	info := ts.put("bkt", "k", []byte("payload"))
	d := ts.Store.disks[1]
	metaPath := filepath.Join(d.objectDir("bkt", "k"), info.VersionID+metaSuffix)
	data, _ := os.ReadFile(metaPath)
	data[len(data)-3] ^= 0x20
	os.WriteFile(metaPath, data, 0o644)
	for i := 0; i < 5; i++ { // spread reads over all disks
		ts.mustGet("bkt", "k", []byte("payload"))
	}
	ts.WaitHealIdle()
	var m ObjectMeta
	if err := readFramed(metaPath, &m); err != nil {
		t.Fatalf("metadata not healed: %v", err)
	}
	// A restart with one bad metadata copy still finds the object.
	os.WriteFile(metaPath, []byte("garbage"), 0o644)
	ts.reopen()
	// Start-up counts copies by name and does not read them all; reads
	// that meet the bad copy (the rotor spreads them over every disk)
	// queue the heal.
	for i := 0; i < 5; i++ {
		ts.mustGet("bkt", "k", []byte("payload"))
	}
	ts.WaitHealIdle()
	ts.deepClean("after restart heal")
}

// TestLossBeyondParityIsReported wipes m+1 disks: reads must fail cleanly,
// never return wrong data, and healing must report the loss.
func TestLossBeyondParityIsReported(t *testing.T) {
	for _, size := range []int{20_000, 300_000} { // inline and in files
		r := rand.New(rand.NewPCG(5, 5))
		ts := newTestStore(t, 2, 1, func(c *Config) { c.BlockSize = 4096 })
		ts.MakeBucket("bkt")
		data := randData(r, size)
		ts.put("bkt", "k", data)
		o, _ := ts.OpenObject("bkt", "k")
		o.Close()
		for _, d := range ts.Store.disks[:2] {
			if o.meta.Inline {
				os.Remove(filepath.Join(d.objectDir("bkt", "k"), o.Info.VersionID+metaSuffix))
			} else {
				os.RemoveAll(filepath.Join(d.objectDir("bkt", "k"), o.Info.VersionID))
			}
		}
		got, err := ts.get("bkt", "k")
		if err == nil || bytes.Equal(got, data) {
			t.Fatalf("size %d: read with two of three shards gone: err=%v", size, err)
		}
		res, err := ts.HealObject(context.Background(), "bkt", "k", HealOptions{Deep: true})
		if err != nil || !res.Lost {
			t.Fatalf("size %d: heal: %+v %v", size, res, err)
		}
	}
}

// writeWithCrash stages a new version of key and publishes it only on the
// given disks, as if the process died part way through committing.
func (ts *testStore) writeWithCrash(key string, data []byte, onDisks []int) *ObjectMeta {
	ts.t.Helper()
	st := ts.newStage()
	dist := distribution(key, ts.n)
	size, sum, err := ts.writePart(context.Background(), st, 1, bytes.NewReader(data), dist, MaxPutSize)
	if err != nil {
		ts.t.Fatal(err)
	}
	cur, _ := ts.Store.buckets["bkt"].index.get(key)
	meta := &ObjectMeta{Bucket: "bkt", Key: key, VersionID: newID(), Size: size, ETag: fmt.Sprintf("%x", sum),
		ModTime: cur.modTime + 1000,
		Erasure: ErasureInfo{Data: ts.k, Parity: ts.m, BlockSize: ts.cfg.BlockSize, Distribution: dist},
		Parts:   []PartInfo{{Number: 1, Size: size, ETag: fmt.Sprintf("%x", sum)}},
	}
	keep := make([]bool, ts.n)
	for _, d := range onDisks {
		keep[d] = true
	}
	for d := range st.alive {
		st.alive[d] = st.alive[d] && keep[d]
	}
	if got := ts.publish(st, meta); got != len(onDisks) {
		ts.t.Fatalf("published on %d disks, want %d", got, len(onDisks))
	}
	st.discard()
	return meta
}

func TestCrashDuringCommit(t *testing.T) {
	ts := newTestStore(t, 4, 2)
	ts.MakeBucket("bkt")
	ts.put("bkt", "k", []byte("old"))
	// The new version reached 3 disks, fewer than k = 4: after restart
	// the old version is current and the remains are removed.
	partial := ts.writeWithCrash("k", []byte("new, never acknowledged"), []int{0, 2, 5})
	// A brand-new key that reached a single disk is not visible at all.
	ts.writeWithCrash("ghost", []byte("ghost"), []int{3})
	ts.reopen()
	ts.mustGet("bkt", "k", []byte("old"))
	if _, err := ts.get("bkt", "ghost"); !errors.Is(err, s3err.NoSuchKey) {
		t.Fatalf("ghost: %v", err)
	}
	for _, d := range ts.Store.disks {
		if _, err := os.Stat(filepath.Join(d.objectDir("bkt", "k"), partial.VersionID+metaSuffix)); !os.IsNotExist(err) {
			t.Fatalf("%s: the uncommitted version was not removed", d)
		}
		if _, err := os.Stat(d.objectDir("bkt", "ghost")); !os.IsNotExist(err) {
			t.Fatalf("%s: the uncommitted key was not removed", d)
		}
	}
	// A version that reached k disks before the crash is committed: it is
	// current after restart and healed onto the rest.
	done := ts.writeWithCrash("k", []byte("new, on four disks"), []int{0, 1, 2, 3})
	ts.reopen()
	ts.mustGet("bkt", "k", []byte("new, on four disks"))
	ts.WaitHealIdle()
	ts.deepClean("after healing the committed version")
	for _, d := range ts.Store.disks {
		entries, _ := os.ReadDir(d.objectDir("bkt", "k"))
		if len(entries) != 2 || !strings.HasPrefix(entries[0].Name(), done.VersionID) {
			t.Fatalf("%s: expected only the new version, found %d entries", d, len(entries))
		}
	}
}

func forEachSubset(n, size int, f func([]int)) {
	idx := make([]int, size)
	for i := range idx {
		idx[i] = i
	}
	for {
		f(append([]int(nil), idx...))
		i := size - 1
		for i >= 0 && idx[i] == n-size+i {
			i--
		}
		if i < 0 {
			return
		}
		idx[i]++
		for j := i + 1; j < size; j++ {
			idx[j] = idx[j-1] + 1
		}
	}
}

// TestInlineCorruptDataShardUsesParity is the regression test for inline
// reads: when the copy of a data shard has the right size but rotted
// content, the parity copies must be loaded too, or the read fails with
// fewer than k shards although k+m-1 are intact.
func TestInlineCorruptDataShardUsesParity(t *testing.T) {
	r := rand.New(rand.NewPCG(7, 7))
	for _, k := range []int{2, 4} {
		ts := newTestStore(t, k, 2, func(c *Config) { c.BlockSize = 4096 })
		ts.MakeBucket("bkt")
		data := randData(r, 10_000)
		ts.put("bkt", "small", data)
		o, err := ts.OpenObject("bkt", "small")
		if err != nil {
			t.Fatal(err)
		}
		o.Close()
		if !o.meta.Inline {
			t.Fatal("a 10 KB object was not stored inline")
		}
		for shard := 0; shard < 2; shard++ { // up to m data shards rot
			d := o.meta.Erasure.holder(shard)
			paths, offsets := ts.shardRegions(d, o.Info, true)
			b, _ := os.ReadFile(paths[0])
			b[offsets[0]+len(b[offsets[0]:])/2] ^= 0x10 // size unchanged
			os.WriteFile(paths[0], b, 0o644)
			ts.mustGet("bkt", "small", data)
		}
		ts.WaitHealIdle()
		ts.deepClean(fmt.Sprintf("%d+2 after healing rotted inline data shards", k))
	}
}

// TestHealRacingDeleteIsNotLoss deletes (or overwrites) a damaged object
// while it is being healed: the heal must step aside, not report the
// object lost.
func TestHealRacingDeleteIsNotLoss(t *testing.T) {
	for _, size := range []int{1000, 300_000} { // inline and in files
		for _, overwrite := range []bool{false, true} {
			ts := newTestStore(t, 2, 1, func(c *Config) { c.BlockSize = 4096 })
			ts.heal.stop() // no background heals in this test
			ts.Store.heal = nil
			ts.MakeBucket("bkt")
			data := randData(rand.New(rand.NewPCG(3, 3)), size)
			info := ts.put("bkt", "k", data)
			// Damage disk 0's copy so the heal has work to do.
			d0 := ts.Store.disks[0].objectDir("bkt", "k")
			os.Remove(filepath.Join(d0, info.VersionID+metaSuffix))
			os.RemoveAll(filepath.Join(d0, info.VersionID))
			healHookAfterSnapshot = func() {
				if overwrite {
					ts.put("bkt", "k", []byte("new"))
				} else if err := ts.DeleteObject("bkt", "k"); err != nil {
					t.Error(err)
				}
			}
			res, err := ts.HealObject(context.Background(), "bkt", "k", HealOptions{Deep: true})
			healHookAfterSnapshot = nil
			if err != nil || res.Lost || ts.Stats().LostObjects != 0 {
				t.Fatalf("size %d overwrite %v: heal racing a change reported %+v, err %v, lost counter %d",
					size, overwrite, res, err, ts.Stats().LostObjects)
			}
			if overwrite {
				ts.mustGet("bkt", "k", []byte("new"))
			}
		}
	}
}
