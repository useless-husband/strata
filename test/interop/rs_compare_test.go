package interop

import (
	"crypto/rand"
	"fmt"
	"testing"

	"github.com/klauspost/reedsolomon"
)

// BenchmarkKlauspost measures github.com/klauspost/reedsolomon, the
// Reed–Solomon library MinIO uses, with the same shapes and accounting as
// strata's internal/rs benchmarks (MB/s of 1 MiB stripes, one goroutine),
// so the two can be compared on the same machine:
//
//	go test -run XXX -bench Klauspost -cpu 1 ./test/interop
//	go test -run XXX -bench . -cpu 1 ./internal/rs
func BenchmarkKlauspost(b *testing.B) {
	const stripe = 1 << 20
	for _, cfg := range [][2]int{{4, 2}, {8, 4}, {12, 4}} {
		k, m := cfg[0], cfg[1]
		enc, err := reedsolomon.New(k, m, reedsolomon.WithMaxGoroutines(1))
		if err != nil {
			b.Fatal(err)
		}
		data := make([]byte, stripe)
		rand.Read(data)
		shards, _ := enc.Split(data)
		b.Run(fmt.Sprintf("encode/%d+%d/1MiB", k, m), func(b *testing.B) {
			b.SetBytes(stripe)
			for i := 0; i < b.N; i++ {
				enc.Encode(shards)
			}
		})
	}
	for _, cfg := range [][3]int{{4, 2, 1}, {4, 2, 2}, {8, 4, 4}} {
		k, m, lost := cfg[0], cfg[1], cfg[2]
		enc, _ := reedsolomon.New(k, m, reedsolomon.WithMaxGoroutines(1))
		data := make([]byte, stripe)
		rand.Read(data)
		orig, _ := enc.Split(data)
		enc.Encode(orig)
		shards := make([][]byte, k+m)
		scratch := make([][]byte, lost)
		for j := range scratch {
			scratch[j] = make([]byte, 0, len(orig[0]))
		}
		b.Run(fmt.Sprintf("reconstruct-data/%d+%d/lost%d/1MiB", k, m, lost), func(b *testing.B) {
			b.SetBytes(stripe)
			for i := 0; i < b.N; i++ {
				copy(shards, orig)
				for j := 0; j < lost; j++ {
					shards[j] = scratch[j][:0]
				}
				if err := enc.ReconstructData(shards); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
