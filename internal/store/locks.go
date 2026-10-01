package store

import (
	"hash/maphash"
	"sync"
)

// keyLocks serialises commits to the same object. Readers take the read
// lock only for the instant it takes to look the key up and lease its
// version; writers hold the write lock only while committing, never while
// receiving the body.
type keyLocks struct {
	seed  maphash.Seed
	locks [1024]sync.RWMutex
}

func newKeyLocks() *keyLocks { return &keyLocks{seed: maphash.MakeSeed()} }

func (l *keyLocks) get(bucket, key string) *sync.RWMutex {
	var h maphash.Hash
	h.SetSeed(l.seed)
	h.WriteString(bucket)
	h.WriteByte(0)
	h.WriteString(key)
	return &l.locks[h.Sum64()%uint64(len(l.locks))]
}

// leases keeps an object version's files alive while it is being read.
// Deleting a version (after an overwrite or a DELETE) is deferred until the
// last reader releases it, so a GET that has started never finds its shard
// files gone, even if it needs to open a parity shard halfway through.
type leases struct {
	mu      sync.Mutex
	readers map[string]int    // version ID -> active readers
	retired map[string]func() // version ID -> deferred deletion
}

func newLeases() *leases {
	return &leases{readers: map[string]int{}, retired: map[string]func(){}}
}

// acquire registers a reader of a version and returns its release function.
func (l *leases) acquire(version string) func() {
	l.mu.Lock()
	l.readers[version]++
	l.mu.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			l.mu.Lock()
			l.readers[version]--
			var del func()
			if l.readers[version] == 0 {
				delete(l.readers, version)
				del = l.retired[version]
				delete(l.retired, version)
			}
			l.mu.Unlock()
			if del != nil {
				del()
			}
		})
	}
}

// retire runs del now if nobody reads the version, or when the last reader
// releases it.
func (l *leases) retire(version string, del func()) {
	l.mu.Lock()
	if l.readers[version] > 0 {
		l.retired[version] = del
		l.mu.Unlock()
		return
	}
	l.mu.Unlock()
	del()
}

// busy reports whether a version has readers.
func (l *leases) busy(version string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.readers[version] > 0
}
