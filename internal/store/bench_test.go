package store

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand/v2"
	"testing"
)

// Benchmarks of the storage engine alone (no HTTP, no SigV4): one stream
// writing and reading 64 MiB objects on 4+2 disks.

func benchStore(b *testing.B, sync SyncMode) *testStore {
	ts := newTestStore(b, 4, 2, func(c *Config) { c.BlockSize = DefaultBlockSize; c.Sync = sync })
	ts.MakeBucket("bkt")
	return ts
}

func BenchmarkPut64MiB(b *testing.B) {
	for _, sync := range []SyncMode{SyncNone, SyncFull} {
		b.Run("sync="+sync.String(), func(b *testing.B) {
			ts := benchStore(b, sync)
			data := randData(rand.New(rand.NewPCG(1, 1)), 64<<20)
			b.SetBytes(int64(len(data)))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := ts.PutObject(context.Background(), "bkt", fmt.Sprint("k", i%2), bytes.NewReader(data), PutOptions{}); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkGet64MiB(b *testing.B) {
	for _, lost := range []int{0, 2} {
		b.Run(fmt.Sprintf("lost=%d", lost), func(b *testing.B) {
			ts := benchStore(b, SyncNone)
			// No healing: lost shards must stay lost for the whole run.
			ts.heal.stop()
			ts.Store.heal = nil
			data := randData(rand.New(rand.NewPCG(1, 1)), 64<<20)
			ts.put("bkt", "k", data)
			if lost > 0 {
				ts.wipe(0, 1) // every read reconstructs
			}
			b.SetBytes(int64(len(data)))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				o, err := ts.OpenObject("bkt", "k")
				if err != nil {
					b.Fatal(err)
				}
				if err := o.WriteRange(context.Background(), io.Discard, 0, o.Info.Size); err != nil {
					b.Fatal(err)
				}
				o.Close()
			}
		})
	}
}
