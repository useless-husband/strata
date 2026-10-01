package store

import (
	"os"
	"path/filepath"
	"sync"
	"testing"
)

// TestBarrierGroupCommit checks the group-commit rule: every waiter
// returns only after a flush that started after it arrived, and
// concurrent waiters share flushes.
func TestBarrierGroupCommit(t *testing.T) {
	f, err := os.Create(filepath.Join(t.TempDir(), "f"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	b := newBarrier(f)
	const n = 64
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			b.mu.Lock()
			arrived := b.started // flushes started before this waiter arrived
			b.mu.Unlock()
			if err := b.wait(); err != nil {
				t.Error(err)
			}
			b.mu.Lock()
			defer b.mu.Unlock()
			if b.finished <= arrived {
				t.Errorf("returned after flush %d, but flush %d had already started on arrival", b.finished, arrived)
			}
		}()
	}
	wg.Wait()
	if got := b.requests.Load(); got != n {
		t.Fatalf("%d requests, want %d", got, n)
	}
	t.Logf("%d waiters, %d flushes", n, b.flushes.Load())
	if b.flushes.Load() > n {
		t.Fatalf("%d flushes for %d waiters", b.flushes.Load(), n)
	}
}

func TestBarriersPerDevice(t *testing.T) {
	ts := newTestStore(t, 2, 1, func(c *Config) { c.Sync = SyncFull })
	if !useBarriers(SyncFull) {
		if len(ts.Store.barriers) != 0 {
			t.Fatal("barriers set up off macOS")
		}
		t.Skip("barriers are used on macOS only")
	}
	if len(ts.Store.barriers) != 1 {
		t.Fatalf("three disks on one device got %d barriers", len(ts.Store.barriers))
	}
	ts.MakeBucket("bkt")
	ts.put("bkt", "k", []byte("durable"))
	st := ts.Stats()
	if st.SyncFlushes == 0 || st.SyncFlushes > st.SyncRequests {
		t.Fatalf("flushes %d, requests %d", st.SyncFlushes, st.SyncRequests)
	}
	ts.mustGet("bkt", "k", []byte("durable"))
}
