package rs

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"strconv"
	"sync"
	"testing"

	"github.com/useless-husband/strata/internal/gf"
)

// testSeed returns the seed for randomised tests: STRATA_SEED if set, else a
// fixed value. Tests print it on failure.
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

func randomBytes(r *rand.Rand, n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(r.Uint32())
	}
	return b
}

// referenceParity computes parity byte by byte straight from the encoding
// matrix, independent of the kernels and their chunking.
func referenceParity(c *Codec, data [][]byte) [][]byte {
	size := len(data[0])
	out := make([][]byte, c.m)
	for i := range out {
		out[i] = make([]byte, size)
		for b := 0; b < size; b++ {
			var s byte
			for j := 0; j < c.k; j++ {
				s ^= gf.Mul(c.matrix[c.k+i][j], data[j][b])
			}
			out[i][b] = s
		}
	}
	return out
}

func newShards(r *rand.Rand, c *Codec, size int) [][]byte {
	shards := make([][]byte, c.TotalShards())
	for i := range shards {
		if i < c.k {
			shards[i] = randomBytes(r, size)
		} else {
			shards[i] = make([]byte, size)
		}
	}
	return shards
}

func clone(shards [][]byte) [][]byte {
	c := make([][]byte, len(shards))
	for i, s := range shards {
		c[i] = bytes.Clone(s)
	}
	return c
}

func TestNewRejectsBadParams(t *testing.T) {
	for _, p := range [][2]int{{0, 1}, {-1, 2}, {1, -1}, {200, 57}, {256, 1}} {
		if _, err := New(p[0], p[1]); !errors.Is(err, ErrInvalidParams) {
			t.Errorf("New(%d,%d) = %v, want ErrInvalidParams", p[0], p[1], err)
		}
	}
	for _, p := range [][2]int{{1, 0}, {1, 1}, {4, 2}, {255, 1}, {128, 128}} {
		if _, err := New(p[0], p[1]); err != nil {
			t.Errorf("New(%d,%d): %v", p[0], p[1], err)
		}
	}
}

func TestMatrixIsSystematic(t *testing.T) {
	c, _ := New(5, 3)
	m := c.Matrix()
	if !gf.Matrix(m[:5]).Equal(gf.Identity(5)) {
		t.Fatalf("top of encoding matrix is not the identity:\n%v", m)
	}
}

// TestEveryKRowsInvertible checks the MDS property exhaustively for every
// k+m <= 12: every choice of k rows of the encoding matrix is invertible.
func TestEveryKRowsInvertible(t *testing.T) {
	checked := 0
	for n := 2; n <= 12; n++ {
		for k := 1; k < n; k++ {
			c, err := New(k, n-k)
			if err != nil {
				t.Fatal(err)
			}
			forEachSubset(n, k, func(rows []int) {
				if _, err := c.matrix.SelectRows(rows).Invert(); err != nil {
					t.Fatalf("%d+%d: rows %v singular", k, n-k, rows)
				}
				checked++
			})
		}
	}
	t.Logf("checked %d row subsets", checked)
}

func TestEncodeMatchesReference(t *testing.T) {
	seed := testSeed(t)
	r := rand.New(rand.NewPCG(seed, 1))
	configs := [][2]int{{1, 1}, {2, 1}, {3, 2}, {4, 2}, {6, 3}, {8, 4}, {10, 4}, {12, 4}, {16, 16}, {17, 3}}
	sizes := []int{1, 7, 63, 64, 65, 127, 128, 1000, chunk - 1, chunk, chunk + 65, 3*chunk + 13}
	for _, cfg := range configs {
		c, _ := New(cfg[0], cfg[1])
		for _, size := range sizes {
			shards := newShards(r, c, size)
			if err := c.Encode(shards); err != nil {
				t.Fatal(err)
			}
			want := referenceParity(c, shards[:c.k])
			for i := range want {
				if !bytes.Equal(shards[c.k+i], want[i]) {
					t.Fatalf("seed %d: %d+%d size %d: parity %d differs from reference", seed, cfg[0], cfg[1], size, i)
				}
			}
		}
	}
}

// TestKernelsAgree compares the assembly kernel (if any) with the portable
// one for every constant and many lengths and alignments.
func TestKernelsAgree(t *testing.T) {
	if !useAsm {
		t.Skip("no assembly kernel on this platform; the portable kernel is tested against the reference")
	}
	seed := testSeed(t)
	r := rand.New(rand.NewPCG(seed, 2))
	in := randomBytes(r, 4096+64)
	base := randomBytes(r, 4096+64)
	for cv := 0; cv < 256; cv++ {
		co := newCoef(byte(cv))
		for _, n := range []int{0, 1, 63, 64, 65, 128, 129, 1000, 4096} {
			for _, off := range []int{0, 1, 3, 8} {
				src := in[off : off+n]
				var a, b [2][]byte
				for i := range a {
					a[i] = bytes.Clone(base[:n])
					b[i] = bytes.Clone(base[:n])
				}
				useAsm = true
				mulSlice(&co, src, a[0])
				mulAddSlice(&co, src, a[1])
				useAsm = false
				mulSlice(&co, src, b[0])
				mulAddSlice(&co, src, b[1])
				useAsm = true
				if !bytes.Equal(a[0], b[0]) || !bytes.Equal(a[1], b[1]) {
					t.Fatalf("seed %d: kernels differ for c=%d n=%d offset=%d", seed, cv, n, off)
				}
				for i := 0; i < n; i++ {
					if a[0][i] != gf.Mul(byte(cv), src[i]) {
						t.Fatalf("c=%d n=%d: byte %d wrong", cv, n, i)
					}
				}
			}
		}
	}
}

// TestReconstructAllErasurePatterns erases every subset of up to m shards
// for small codes and checks that every shard comes back bit-exact.
func TestReconstructAllErasurePatterns(t *testing.T) {
	seed := testSeed(t)
	r := rand.New(rand.NewPCG(seed, 3))
	for _, cfg := range [][2]int{{1, 1}, {2, 2}, {3, 2}, {4, 2}, {4, 4}, {5, 3}, {6, 3}, {8, 4}} {
		k, m := cfg[0], cfg[1]
		c, _ := New(k, m)
		orig := newShards(r, c, 333)
		if err := c.Encode(orig); err != nil {
			t.Fatal(err)
		}
		for lost := 1; lost <= m; lost++ {
			forEachSubset(k+m, lost, func(gone []int) {
				for _, dataOnly := range []bool{false, true} {
					shards := clone(orig)
					for _, g := range gone {
						shards[g] = shards[g][:0]
					}
					var err error
					if dataOnly {
						err = c.ReconstructData(shards)
					} else {
						err = c.Reconstruct(shards)
					}
					if err != nil {
						t.Fatalf("%d+%d lost %v: %v", k, m, gone, err)
					}
					for i := range shards {
						if dataOnly && i >= k {
							continue
						}
						if !bytes.Equal(shards[i], orig[i]) {
							t.Fatalf("seed %d: %d+%d lost %v dataOnly=%v: shard %d wrong", seed, k, m, gone, dataOnly, i)
						}
					}
				}
			})
		}
	}
}

func TestReconstructRandomLargeCodes(t *testing.T) {
	seed := testSeed(t)
	r := rand.New(rand.NewPCG(seed, 4))
	for trial := 0; trial < 60; trial++ {
		k := 1 + r.IntN(40)
		m := 1 + r.IntN(20)
		c, _ := New(k, m)
		size := 1 + r.IntN(5000)
		orig := newShards(r, c, size)
		c.Encode(orig)
		shards := clone(orig)
		lost := 1 + r.IntN(m)
		for _, g := range r.Perm(k + m)[:lost] {
			shards[g] = nil
		}
		if err := c.Reconstruct(shards); err != nil {
			t.Fatalf("seed %d trial %d: %v", seed, trial, err)
		}
		for i := range shards {
			if !bytes.Equal(shards[i], orig[i]) {
				t.Fatalf("seed %d trial %d: %d+%d size %d: shard %d wrong", seed, trial, k, m, size, i)
			}
		}
	}
}

func TestReconstructErrors(t *testing.T) {
	c, _ := New(4, 2)
	r := rand.New(rand.NewPCG(1, 1))
	shards := newShards(r, c, 100)
	c.Encode(shards)

	tooFew := clone(shards)
	tooFew[0], tooFew[1], tooFew[5] = nil, nil, nil
	if err := c.Reconstruct(tooFew); !errors.Is(err, ErrTooFewShards) {
		t.Errorf("3 of 6 shards: got %v, want ErrTooFewShards", err)
	}
	uneven := clone(shards)
	uneven[0] = nil
	uneven[2] = uneven[2][:99]
	if err := c.Reconstruct(uneven); !errors.Is(err, ErrShardSize) {
		t.Errorf("uneven shards: got %v, want ErrShardSize", err)
	}
	if err := c.Reconstruct(shards[:5]); !errors.Is(err, ErrShardCount) {
		t.Errorf("5 shards: got %v, want ErrShardCount", err)
	}
	if err := c.Encode(uneven); !errors.Is(err, ErrShardSize) {
		t.Errorf("Encode uneven: got %v", err)
	}
	if err := c.Reconstruct(clone(shards)); err != nil {
		t.Errorf("nothing missing: %v", err)
	}
}

func TestReconstructReusesCapacity(t *testing.T) {
	c, _ := New(4, 2)
	r := rand.New(rand.NewPCG(1, 2))
	orig := newShards(r, c, 256)
	c.Encode(orig)
	shards := clone(orig)
	buf := make([]byte, 0, 1024)
	shards[1] = buf
	if err := c.Reconstruct(shards); err != nil {
		t.Fatal(err)
	}
	if &shards[1][0] != &buf[:1][0] {
		t.Error("Reconstruct allocated although the slice had enough capacity")
	}
	if !bytes.Equal(shards[1], orig[1]) {
		t.Error("reconstructed shard wrong")
	}
}

func TestVerify(t *testing.T) {
	c, _ := New(6, 3)
	r := rand.New(rand.NewPCG(1, 3))
	shards := newShards(r, c, 4000)
	c.Encode(shards)
	if ok, err := c.Verify(shards); !ok || err != nil {
		t.Fatalf("Verify of fresh shards = %v, %v", ok, err)
	}
	for i := range shards {
		bad := clone(shards)
		bad[i][r.IntN(4000)] ^= 1 << r.IntN(8)
		if ok, _ := c.Verify(bad); ok {
			t.Fatalf("Verify missed a flipped bit in shard %d", i)
		}
	}
}

// Encoding is linear: parity(a ^ b) = parity(a) ^ parity(b).
func TestLinearity(t *testing.T) {
	c, _ := New(5, 3)
	r := rand.New(rand.NewPCG(1, 4))
	a, b := newShards(r, c, 777), newShards(r, c, 777)
	sum := make([][]byte, 8)
	for i := range sum {
		sum[i] = make([]byte, 777)
		if i < 5 {
			for j := range sum[i] {
				sum[i][j] = a[i][j] ^ b[i][j]
			}
		}
	}
	c.Encode(a)
	c.Encode(b)
	c.Encode(sum)
	for i := 5; i < 8; i++ {
		for j := 0; j < 777; j++ {
			if sum[i][j] != a[i][j]^b[i][j] {
				t.Fatalf("parity %d byte %d not linear", i, j)
			}
		}
	}
}

func TestSplit(t *testing.T) {
	c, _ := New(4, 2)
	for _, n := range []int{1, 3, 4, 5, 100, 1001} {
		data := make([]byte, n)
		for i := range data {
			data[i] = byte(i + 1)
		}
		shards := c.Split(data, nil)
		size := ShardSize(n, 4)
		var joined []byte
		for i, s := range shards {
			if len(s) != size {
				t.Fatalf("n=%d: shard %d has %d bytes, want %d", n, i, len(s), size)
			}
			if i < 4 {
				joined = append(joined, s...)
			}
		}
		if !bytes.Equal(joined[:n], data) || bytes.Count(joined[n:], []byte{0}) != len(joined)-n {
			t.Fatalf("n=%d: split does not concatenate back to the data plus zero padding", n)
		}
		// Reusing dirty buffers must still zero the padding.
		for _, s := range shards {
			for i := range s {
				s[i] = 0xff
			}
		}
		shards = c.Split(data, shards)
		joined = joined[:0]
		for _, s := range shards[:4] {
			joined = append(joined, s...)
		}
		if !bytes.Equal(joined[:n], data) || bytes.Count(joined[n:], []byte{0}) != len(joined)-n {
			t.Fatalf("n=%d: reused split buffers keep stale bytes", n)
		}
	}
}

func TestConcurrentUse(t *testing.T) {
	c, _ := New(8, 4)
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			r := rand.New(rand.NewPCG(uint64(g), 5))
			for i := 0; i < 50; i++ {
				orig := newShards(r, c, 512)
				c.Encode(orig)
				shards := clone(orig)
				for _, x := range r.Perm(12)[:4] {
					shards[x] = nil
				}
				if err := c.Reconstruct(shards); err != nil {
					t.Error(err)
					return
				}
				for j := range shards {
					if !bytes.Equal(shards[j], orig[j]) {
						t.Errorf("goroutine %d: shard %d wrong", g, j)
						return
					}
				}
			}
		}(g)
	}
	wg.Wait()
}

// FuzzReconstruct encodes fuzzer-chosen data, erases the shards the fuzzer
// picks (as long as at least k remain) and checks they come back exactly.
func FuzzReconstruct(f *testing.F) {
	f.Add(uint8(4), uint8(2), uint16(0b000011), []byte("hello, erasure coding"))
	f.Add(uint8(1), uint8(3), uint16(0b0111), []byte{0})
	f.Add(uint8(10), uint8(4), uint16(0b11000000000011), bytes.Repeat([]byte{0xA5}, 300))
	f.Fuzz(func(t *testing.T, kb, mb uint8, mask uint16, data []byte) {
		k, m := int(kb%12)+1, int(mb%5)
		if len(data) == 0 {
			return
		}
		c, err := New(k, m)
		if err != nil {
			t.Fatal(err)
		}
		shards := c.Split(data, nil)
		if err := c.Encode(shards); err != nil {
			t.Fatal(err)
		}
		orig := clone(shards)
		lost := 0
		for i := 0; i < k+m && lost < m; i++ {
			if mask&(1<<i) != 0 {
				shards[i] = nil
				lost++
			}
		}
		if err := c.Reconstruct(shards); err != nil {
			t.Fatalf("%d+%d mask %b: %v", k, m, mask, err)
		}
		for i := range shards {
			if !bytes.Equal(shards[i], orig[i]) {
				t.Fatalf("%d+%d mask %b: shard %d wrong", k, m, mask, i)
			}
		}
	})
}

func forEachSubset(n, size int, f func([]int)) {
	idx := make([]int, size)
	for i := range idx {
		idx[i] = i
	}
	for {
		f(idx)
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

// Benchmarks report throughput as data bytes processed per second on one
// goroutine. Run with -cpu=1 to measure per-core rates.

func benchEncode(b *testing.B, k, m, stripe int) {
	c, _ := New(k, m)
	r := rand.New(rand.NewPCG(1, 1))
	shards := c.Split(randomBytes(r, stripe), nil)
	b.SetBytes(int64(stripe))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Encode(shards)
	}
}

func benchReconstruct(b *testing.B, k, m, stripe, lost int) {
	c, _ := New(k, m)
	r := rand.New(rand.NewPCG(1, 1))
	orig := c.Split(randomBytes(r, stripe), nil)
	c.Encode(orig)
	shards := make([][]byte, k+m)
	scratch := make([][]byte, lost)
	for j := range scratch {
		scratch[j] = make([]byte, 0, len(orig[0]))
	}
	b.SetBytes(int64(stripe))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		copy(shards, orig)
		for j := 0; j < lost; j++ {
			shards[j] = scratch[j][:0] // lose data shards: the expensive case
		}
		if err := c.ReconstructData(shards); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkEncode(b *testing.B) {
	for _, asm := range kernels() {
		for _, cfg := range [][2]int{{4, 2}, {8, 4}, {12, 4}} {
			b.Run(fmt.Sprintf("%s/%d+%d/1MiB", asm.name, cfg[0], cfg[1]), func(b *testing.B) {
				defer setAsm(asm.on)()
				benchEncode(b, cfg[0], cfg[1], 1<<20)
			})
		}
	}
}

func BenchmarkReconstructData(b *testing.B) {
	for _, asm := range kernels() {
		for _, cfg := range [][3]int{{4, 2, 1}, {4, 2, 2}, {8, 4, 4}} {
			b.Run(fmt.Sprintf("%s/%d+%d/lost%d/1MiB", asm.name, cfg[0], cfg[1], cfg[2]), func(b *testing.B) {
				defer setAsm(asm.on)()
				benchReconstruct(b, cfg[0], cfg[1], 1<<20, cfg[2])
			})
		}
	}
}

type kernel struct {
	name string
	on   bool
}

func kernels() []kernel {
	if useAsm {
		return []kernel{{"neon", true}, {"table", false}}
	}
	return []kernel{{"table", false}}
}

func setAsm(on bool) func() {
	old := useAsm
	useAsm = on
	return func() { useAsm = old }
}
