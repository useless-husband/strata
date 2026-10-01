// Package rs implements systematic Reed–Solomon erasure coding over GF(2^8).
//
// A Codec with k data shards and m parity shards turns k equal-length data
// shards into m parity shards such that any k of the k+m shards are enough to
// recover all of them. The code is systematic (the data shards are stored
// unchanged) and uses a Cauchy matrix for the parity rows, which makes every
// choice of k rows of the encoding matrix invertible without the search or
// row reduction a Vandermonde construction needs.
//
// The inner loop multiplies a buffer by a constant and adds it to another
// buffer. On arm64 this runs on NEON using two 16-entry shuffle tables per
// constant (TBL); elsewhere it uses a 256-entry product table per constant.
package rs

import (
	"errors"
	"fmt"
	"sync"

	"github.com/useless-husband/strata/internal/gf"
)

// Errors returned by the codec.
var (
	ErrTooFewShards  = errors.New("rs: too few shards to reconstruct")
	ErrShardSize     = errors.New("rs: shards have different sizes")
	ErrShardCount    = errors.New("rs: wrong number of shards")
	ErrInvalidParams = errors.New("rs: invalid number of data or parity shards")
)

// MaxShards is the largest total number of shards the field allows.
const MaxShards = 256

// chunk is the number of bytes processed per pass over the inputs, chosen
// so that one output chunk stays in L1 while all inputs are added into it.
const chunk = 16 << 10

// coef is a multiplication-by-constant in the two forms the kernels use.
type coef struct {
	low, high [16]byte   // NEON: c*x = low[x&15] ^ high[x>>4]
	full      *[256]byte // portable: c*x = full[x]
}

func newCoef(c byte) coef {
	low, high := gf.NibbleTables(c)
	return coef{low: low, high: high, full: gf.MulTable(c)}
}

// Codec encodes and reconstructs shards. It is safe for concurrent use.
type Codec struct {
	k, m   int
	matrix gf.Matrix // (k+m)×k encoding matrix; the top k rows are the identity
	parity [][]coef  // m×k coefficients of the parity rows

	mu       sync.Mutex
	invCache map[[4]uint64]gf.Matrix // decode matrices by set of rows used
}

// New returns a codec for k data and m parity shards. k must be at least 1,
// m at least 0, and k+m at most 256.
func New(k, m int) (*Codec, error) {
	if k < 1 || m < 0 || k+m > MaxShards {
		return nil, fmt.Errorf("%w: %d+%d", ErrInvalidParams, k, m)
	}
	c := &Codec{k: k, m: m, invCache: map[[4]uint64]gf.Matrix{}}
	c.matrix = gf.NewMatrix(k+m, k)
	for i := 0; i < k; i++ {
		c.matrix[i][i] = 1
	}
	cauchy := gf.Cauchy(m, k)
	c.parity = make([][]coef, m)
	for i := 0; i < m; i++ {
		copy(c.matrix[k+i], cauchy[i])
		c.parity[i] = make([]coef, k)
		for j := 0; j < k; j++ {
			c.parity[i][j] = newCoef(cauchy[i][j])
		}
	}
	return c, nil
}

// DataShards returns k.
func (c *Codec) DataShards() int { return c.k }

// ParityShards returns m.
func (c *Codec) ParityShards() int { return c.m }

// TotalShards returns k+m.
func (c *Codec) TotalShards() int { return c.k + c.m }

// Matrix returns a copy of the (k+m)×k encoding matrix.
func (c *Codec) Matrix() gf.Matrix { return c.matrix.Clone() }

// Encode computes the parity shards from the data shards. shards must hold
// k+m slices of equal length; the first k are read and the last m are
// overwritten.
func (c *Codec) Encode(shards [][]byte) error {
	if len(shards) != c.k+c.m {
		return ErrShardCount
	}
	size := len(shards[0])
	for _, s := range shards {
		if len(s) != size {
			return ErrShardSize
		}
	}
	c.encodeRows(c.parity, shards[:c.k], shards[c.k:], size)
	return nil
}

// encodeRows computes outputs[i] = sum_j rows[i][j] * inputs[j].
func (c *Codec) encodeRows(rows [][]coef, inputs, outputs [][]byte, size int) {
	for start := 0; start < size; start += chunk {
		end := min(start+chunk, size)
		for i, out := range outputs {
			o := out[start:end]
			row := rows[i]
			mulSlice(&row[0], inputs[0][start:end], o)
			for j := 1; j < len(inputs); j++ {
				mulAddSlice(&row[j], inputs[j][start:end], o)
			}
		}
	}
}

// Verify reports whether the parity shards match the data shards.
func (c *Codec) Verify(shards [][]byte) (bool, error) {
	if len(shards) != c.k+c.m {
		return false, ErrShardCount
	}
	size := len(shards[0])
	for _, s := range shards {
		if len(s) != size {
			return false, ErrShardSize
		}
	}
	tmp := make([][]byte, c.m)
	for i := range tmp {
		tmp[i] = make([]byte, size)
	}
	c.encodeRows(c.parity, shards[:c.k], tmp, size)
	for i := range tmp {
		if string(tmp[i]) != string(shards[c.k+i]) {
			return false, nil
		}
	}
	return true, nil
}

// Reconstruct rebuilds every missing shard. A shard is missing when its
// slice has length zero; its slice is replaced (reusing its capacity when
// large enough). At least k shards must be present.
func (c *Codec) Reconstruct(shards [][]byte) error {
	return c.reconstruct(shards, false)
}

// ReconstructData rebuilds only the missing data shards, which is all a
// degraded read needs.
func (c *Codec) ReconstructData(shards [][]byte) error {
	return c.reconstruct(shards, true)
}

func (c *Codec) reconstruct(shards [][]byte, dataOnly bool) error {
	n := c.k + c.m
	if len(shards) != n {
		return ErrShardCount
	}
	size, present := 0, 0
	for _, s := range shards {
		if len(s) == 0 {
			continue
		}
		if present == 0 {
			size = len(s)
		} else if len(s) != size {
			return ErrShardSize
		}
		present++
	}
	if present == n {
		return nil
	}
	if present < c.k {
		return ErrTooFewShards
	}

	dataMissing := false
	for i := 0; i < c.k; i++ {
		if len(shards[i]) == 0 {
			dataMissing = true
			break
		}
	}
	if dataMissing {
		// Use the first k present shards. Scanning in order prefers data
		// shards, which keeps the decode matrix as close to the identity as
		// possible.
		rows := make([]int, 0, c.k)
		var key [4]uint64
		for i := 0; i < n && len(rows) < c.k; i++ {
			if len(shards[i]) != 0 {
				rows = append(rows, i)
				key[i/64] |= 1 << (i % 64)
			}
		}
		dec, err := c.decodeMatrix(rows, key)
		if err != nil {
			return err
		}
		inputs := make([][]byte, c.k)
		for i, r := range rows {
			inputs[i] = shards[r]
		}
		var outRows [][]coef
		var outputs [][]byte
		for i := 0; i < c.k; i++ {
			if len(shards[i]) != 0 {
				continue
			}
			shards[i] = resize(shards[i], size)
			row := make([]coef, c.k)
			for j := range row {
				row[j] = newCoef(dec[i][j])
			}
			outRows = append(outRows, row)
			outputs = append(outputs, shards[i])
		}
		c.encodeRows(outRows, inputs, outputs, size)
	}
	if dataOnly {
		return nil
	}

	var outRows [][]coef
	var outputs [][]byte
	for i := c.k; i < n; i++ {
		if len(shards[i]) != 0 {
			continue
		}
		shards[i] = resize(shards[i], size)
		outRows = append(outRows, c.parity[i-c.k])
		outputs = append(outputs, shards[i])
	}
	if len(outputs) > 0 {
		c.encodeRows(outRows, shards[:c.k], outputs, size)
	}
	return nil
}

// decodeMatrix returns the inverse of the encoding matrix restricted to the
// given rows; multiplying it by those shards yields the data shards.
func (c *Codec) decodeMatrix(rows []int, key [4]uint64) (gf.Matrix, error) {
	c.mu.Lock()
	dec, ok := c.invCache[key]
	c.mu.Unlock()
	if ok {
		return dec, nil
	}
	dec, err := c.matrix.SelectRows(rows).Invert()
	if err != nil {
		// Cannot happen for a Cauchy-based systematic matrix; reported
		// rather than hidden in case the construction is ever changed.
		return nil, fmt.Errorf("rs: decode matrix for rows %v: %w", rows, err)
	}
	c.mu.Lock()
	if len(c.invCache) >= 1024 {
		clear(c.invCache)
	}
	c.invCache[key] = dec
	c.mu.Unlock()
	return dec, nil
}

func resize(b []byte, size int) []byte {
	if cap(b) >= size {
		return b[:size]
	}
	return make([]byte, size)
}

// ShardSize returns the size of each shard when a block of n bytes is split
// over k data shards: n/k rounded up.
func ShardSize(n, k int) int { return (n + k - 1) / k }

// Split lays data out over the k data shards of dst (which must have k+m
// entries), zero-padding the last one, and sizes the parity shards. It
// reuses the capacity of the slices in dst when it can.
func (c *Codec) Split(data []byte, dst [][]byte) [][]byte {
	if len(dst) != c.k+c.m {
		dst = make([][]byte, c.k+c.m)
	}
	size := ShardSize(len(data), c.k)
	for i := range dst {
		dst[i] = resize(dst[i], size)
		if i >= c.k {
			continue
		}
		n := copy(dst[i], data[min(i*size, len(data)):])
		clear(dst[i][n:])
	}
	return dst
}
