package store

import (
	"sort"
	"sync"
)

// entry is what listings need to know about the current version of a key.
// Everything else (user metadata, parts, erasure layout) is read from disk
// when the object is opened.
type entry struct {
	key       string
	versionID string
	size      int64
	etag      string
	modTime   int64
}

// index is an ordered map from key to entry for one bucket: a sorted list of
// sorted leaves of at most maxLeaf entries, a one-level B+ tree. Lookups
// and inserts cost a binary search over the leaves plus one within a leaf;
// iteration from any key is sequential. Keys compare as raw bytes, which is
// the UTF-8 binary order S3 lists in.
type index struct {
	mu     sync.RWMutex
	leaves []*leaf
	n      int
}

type leaf struct {
	entries []entry
}

const maxLeaf = 512

func newIndex() *index { return &index{} }

// findLeaf returns the leaf that would contain key: the last leaf whose
// first key is <= key (or the first leaf).
func (x *index) findLeaf(key string) int {
	i := sort.Search(len(x.leaves), func(i int) bool { return x.leaves[i].entries[0].key > key })
	if i > 0 {
		i--
	}
	return i
}

func (l *leaf) find(key string) (int, bool) {
	i := sort.Search(len(l.entries), func(i int) bool { return l.entries[i].key >= key })
	return i, i < len(l.entries) && l.entries[i].key == key
}

func (x *index) get(key string) (entry, bool) {
	x.mu.RLock()
	defer x.mu.RUnlock()
	if len(x.leaves) == 0 {
		return entry{}, false
	}
	l := x.leaves[x.findLeaf(key)]
	if i, ok := l.find(key); ok {
		return l.entries[i], true
	}
	return entry{}, false
}

func (x *index) put(e entry) {
	x.mu.Lock()
	defer x.mu.Unlock()
	if len(x.leaves) == 0 {
		x.leaves = []*leaf{{entries: []entry{e}}}
		x.n = 1
		return
	}
	li := x.findLeaf(e.key)
	l := x.leaves[li]
	i, ok := l.find(e.key)
	if ok {
		l.entries[i] = e
		return
	}
	l.entries = append(l.entries, entry{})
	copy(l.entries[i+1:], l.entries[i:])
	l.entries[i] = e
	x.n++
	if len(l.entries) > maxLeaf {
		half := len(l.entries) / 2
		right := &leaf{entries: append([]entry(nil), l.entries[half:]...)}
		l.entries = append([]entry(nil), l.entries[:half]...)
		x.leaves = append(x.leaves, nil)
		copy(x.leaves[li+2:], x.leaves[li+1:])
		x.leaves[li+1] = right
	}
}

func (x *index) delete(key string) bool {
	x.mu.Lock()
	defer x.mu.Unlock()
	if len(x.leaves) == 0 {
		return false
	}
	li := x.findLeaf(key)
	l := x.leaves[li]
	i, ok := l.find(key)
	if !ok {
		return false
	}
	l.entries = append(l.entries[:i], l.entries[i+1:]...)
	x.n--
	if len(l.entries) == 0 {
		x.leaves = append(x.leaves[:li], x.leaves[li+1:]...)
	}
	return true
}

func (x *index) len() int {
	x.mu.RLock()
	defer x.mu.RUnlock()
	return x.n
}

// ascend calls fn for each entry with key >= from, in order, until fn
// returns false. The index is read-locked during the walk, so fn must not
// modify it.
func (x *index) ascend(from string, fn func(e *entry) bool) {
	x.mu.RLock()
	defer x.mu.RUnlock()
	if len(x.leaves) == 0 {
		return
	}
	li := x.findLeaf(from)
	i, _ := x.leaves[li].find(from)
	for ; li < len(x.leaves); li, i = li+1, 0 {
		l := x.leaves[li]
		for ; i < len(l.entries); i++ {
			if !fn(&l.entries[i]) {
				return
			}
		}
	}
}
