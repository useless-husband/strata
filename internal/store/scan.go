package store

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// scan rebuilds the in-memory state from the disks at start-up: buckets
// that are on at least k disks, and for every key the newest version whose
// metadata is on at least k disks. Keys whose copies are incomplete are
// returned for healing. With every disk online it also removes the remains
// of writes that never reached a quorum; nothing can be in flight while
// opening, so anything without a quorum is garbage.
func (s *Store) scan() ([]objRef, error) {
	allOnline := true
	for _, d := range s.disks {
		if _, err := d.readFormat(); err != nil {
			allOnline = false
		}
	}
	cleanup := allOnline && !s.cfg.ReadOnly

	// Buckets.
	type bseen struct {
		info  bucketInfo
		disks int
	}
	buckets := map[string]*bseen{}
	for _, d := range s.disks {
		entries, err := os.ReadDir(d.path(bucketsDir))
		if err != nil {
			if isNotExist(err) {
				continue
			}
			return nil, fmt.Errorf("%s: %w", d, err)
		}
		for _, e := range entries {
			var bi bucketInfo
			if !e.IsDir() || readFramed(d.path(bucketsDir, e.Name(), bucketMeta), &bi) != nil || bi.Name != e.Name() {
				continue
			}
			bs := buckets[bi.Name]
			if bs == nil {
				bs = &bseen{info: bi}
				buckets[bi.Name] = bs
			}
			bs.disks++
		}
	}
	for name, bs := range buckets {
		if bs.disks < s.k {
			s.log.Warn("ignoring bucket found on too few disks", "bucket", name, "disks", bs.disks)
			continue
		}
		s.buckets[name] = &bucket{name: name, created: bucketTime(bs.info.Created), index: newIndex()}
		if bs.disks < s.n && !s.cfg.ReadOnly {
			s.healBucket(name)
		}
	}

	// Objects, 256 hash directories per bucket, in parallel.
	var (
		mu     sync.Mutex
		toHeal []objRef
		wg     sync.WaitGroup
		work   = make(chan [2]string)
	)
	for w := 0; w < scanWorkers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range work {
				refs := s.scanHashDir(job[0], job[1], cleanup)
				if len(refs) > 0 {
					mu.Lock()
					toHeal = append(toHeal, refs...)
					mu.Unlock()
				}
			}
		}()
	}
	for name := range s.buckets {
		for i := 0; i < 256; i++ {
			work <- [2]string{name, fmt.Sprintf("%02x", i)}
		}
	}
	close(work)
	wg.Wait()
	if err := s.loadUploads(cleanup); err != nil {
		return nil, err
	}
	return toHeal, nil
}

// scanHashDir loads the keys under objects/<hh> of a bucket.
//
// Version IDs are in the metadata file names, so the quorum count needs
// only the directory listings; the metadata itself is read from one disk
// per version (normally one version per key). Corrupt metadata copies
// therefore count towards the quorum at start-up; reads skip them and
// queue a heal, and a scrub finds them all. Reading every copy here would
// make start-up cost a file read per object per disk.
func (s *Store) scanHashDir(bucketName, hh string, cleanup bool) []objRef {
	b := s.buckets[bucketName]
	dirs := map[string]bool{}
	for _, d := range s.disks {
		entries, _ := os.ReadDir(d.path(bucketsDir, bucketName, objectsDir, hh))
		for _, e := range entries {
			if e.IsDir() {
				dirs[e.Name()] = true
			}
		}
	}
	type listing struct{ metas, dataDirs map[string]bool }
	var toHeal []objRef
	for h := range dirs {
		dir := func(i int) string { return s.disks[i].path(bucketsDir, bucketName, objectsDir, hh, h) }
		ls := make([]listing, s.n)
		count := map[string]int{}
		for i := range s.disks {
			ls[i] = listing{metas: map[string]bool{}, dataDirs: map[string]bool{}}
			entries, _ := os.ReadDir(dir(i))
			for _, e := range entries {
				if e.IsDir() {
					ls[i].dataDirs[e.Name()] = true
				} else if v, ok := strings.CutSuffix(e.Name(), metaSuffix); ok {
					ls[i].metas[v] = true
					count[v]++
				}
			}
		}
		// One readable copy of each version's metadata.
		metas := map[string]*ObjectMeta{}
		for v := range count {
			for i := range s.disks {
				if !ls[i].metas[v] {
					continue
				}
				var m ObjectMeta
				err := readFramedHeader(filepath.Join(dir(i), v+metaSuffix), &m)
				if err == nil && m.VersionID == v && m.Bucket == bucketName && keyHash(m.Key) == h {
					metas[v] = &m
					break
				}
			}
		}
		byKey := map[string][]*ObjectMeta{}
		for _, m := range metas {
			byKey[m.Key] = append(byKey[m.Key], m)
		}
		for key, versions := range byKey {
			var cur *ObjectMeta
			for _, m := range versions {
				if count[m.VersionID] >= s.k && (cur == nil || newer(m, cur)) {
					cur = m
				}
			}
			if cleanup {
				// Keep only cur: drop versions superseded before a crash and
				// newer ones that never reached k disks.
				for _, m := range versions {
					if cur != nil && m.VersionID == cur.VersionID {
						continue
					}
					for i := range s.disks {
						os.Remove(filepath.Join(dir(i), m.VersionID+metaSuffix))
						os.RemoveAll(filepath.Join(dir(i), m.VersionID))
						delete(ls[i].metas, m.VersionID)
						delete(ls[i].dataDirs, m.VersionID)
					}
					delete(count, m.VersionID)
				}
			}
			if cur == nil {
				continue
			}
			b.index.put(entryOf(cur))
			heal := false
			for i := range s.disks {
				if !ls[i].metas[cur.VersionID] || (!cur.Inline && !ls[i].dataDirs[cur.VersionID]) ||
					len(ls[i].metas) > 1 || len(ls[i].dataDirs) > 1 || (cur.Inline && len(ls[i].dataDirs) > 0) {
					heal = true
				}
			}
			if heal {
				toHeal = append(toHeal, objRef{bucketName, key})
			}
		}
		if cleanup {
			// Data directories and metadata files of versions whose
			// metadata could not be read anywhere, and empty directories.
			for i := range s.disks {
				for v := range ls[i].dataDirs {
					if metas[v] == nil {
						os.RemoveAll(filepath.Join(dir(i), v))
					}
				}
				for v := range ls[i].metas {
					if metas[v] == nil {
						os.Remove(filepath.Join(dir(i), v+metaSuffix))
					}
				}
				os.Remove(dir(i)) // only if empty
			}
		}
	}
	return toHeal
}

// scanWorkers is the number of hash directories read in parallel at
// start-up. More is slower on APFS, whose metadata operations contend.
var scanWorkers = 4
