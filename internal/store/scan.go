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
	for w := 0; w < 8; w++ {
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
	var toHeal []objRef
	for h := range dirs {
		view := make(dirView, s.n)
		keys := map[string]bool{}
		for i, d := range s.disks {
			dir := d.path(bucketsDir, bucketName, objectsDir, hh, h)
			view[i] = readDiskView(d, dir, func(m *ObjectMeta) bool {
				return m.Bucket == bucketName && keyHash(m.Key) == h
			})
			for _, m := range view[i].metas {
				keys[m.Key] = true
			}
		}
		if len(keys) == 0 {
			// Data without any metadata: the remains of a write that
			// crashed before its first commit.
			if cleanup {
				for _, d := range s.disks {
					os.RemoveAll(d.path(bucketsDir, bucketName, objectsDir, hh, h))
				}
			}
			continue
		}
		referenced := map[string]bool{}
		for key := range keys {
			kv := make(dirView, s.n)
			for i, dv := range view {
				kv[i] = diskView{readable: dv.readable, corrupt: dv.corrupt, dataDirs: dv.dataDirs, metas: map[string]*ObjectMeta{}}
				for ver, m := range dv.metas {
					if m.Key == key {
						kv[i].metas[ver] = m
						referenced[ver] = true
					}
				}
			}
			cur := kv.newestWithQuorum(s.k)
			if cleanup {
				s.keepOnly(kv, cur, bucketName, key)
			}
			if cur == nil {
				continue
			}
			b.index.put(entryOf(cur))
			if needsHeal(kv, cur) {
				toHeal = append(toHeal, objRef{bucketName, key})
			}
		}
		if cleanup {
			// Data directories no metadata refers to, corrupt metadata
			// files, and directories left empty.
			for i, d := range s.disks {
				dir := d.path(bucketsDir, bucketName, objectsDir, hh, h)
				for ver := range view[i].dataDirs {
					if !referenced[ver] {
						os.RemoveAll(filepath.Join(dir, ver))
					}
				}
				if view[i].corrupt > 0 {
					entries, _ := os.ReadDir(dir)
					for _, e := range entries {
						if v, ok := strings.CutSuffix(e.Name(), metaSuffix); ok && view[i].metas[v] == nil {
							os.Remove(filepath.Join(dir, e.Name()))
						}
					}
				}
				os.Remove(dir) // only if empty
			}
		}
	}
	return toHeal
}

// keepOnly removes every version of a key except cur (all of them if cur is
// nil): versions superseded before a crash, and newer versions that never
// reached k disks. It is only called at start-up with every disk present,
// when no write can be in flight and no reader holds a version.
func (s *Store) keepOnly(view dirView, cur *ObjectMeta, bucket, key string) {
	for i, dv := range view {
		objDir := s.disks[i].objectDir(bucket, key)
		for ver := range dv.metas {
			if cur == nil || ver != cur.VersionID {
				os.Remove(filepath.Join(objDir, ver+metaSuffix))
				os.RemoveAll(filepath.Join(objDir, ver))
			}
		}
	}
}

// needsHeal reports whether some disk lacks an intact-looking copy of cur.
func needsHeal(view dirView, cur *ObjectMeta) bool {
	for _, dv := range view {
		if dv.metas[cur.VersionID] == nil || (!cur.Inline && !dv.dataDirs[cur.VersionID]) || dv.corrupt > 0 {
			return true
		}
	}
	return false
}
