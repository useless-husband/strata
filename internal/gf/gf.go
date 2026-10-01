// Package gf implements arithmetic in the finite field GF(2^8) and the small
// amount of linear algebra over it that Reed–Solomon coding needs.
//
// Elements are bytes. Addition is XOR. Multiplication is polynomial
// multiplication modulo the primitive polynomial x^8 + x^4 + x^3 + x^2 + 1
// (0x11D), the same field used by most storage erasure codes; 2 (the
// polynomial x) generates the multiplicative group.
package gf

import "fmt"

// Poly is the primitive polynomial that defines the field, including the x^8 term.
const Poly = 0x11D

var (
	// expTable[i] = 2^i. It is doubled in length so that Exp(Log(a)+Log(b))
	// never needs a reduction modulo 255.
	expTable [510]byte
	// logTable[a] = log_2(a) for a != 0. logTable[0] is unused.
	logTable [256]byte
	// mulTable[a][b] = a*b. 64 KiB, used by the portable fast path.
	mulTable [256][256]byte
	// invTable[a] = 1/a for a != 0.
	invTable [256]byte
)

func init() {
	x := 1
	for i := 0; i < 255; i++ {
		expTable[i] = byte(x)
		expTable[i+255] = byte(x)
		logTable[x] = byte(i)
		x <<= 1
		if x&0x100 != 0 {
			x ^= Poly
		}
	}
	for a := 0; a < 256; a++ {
		for b := 0; b < 256; b++ {
			mulTable[a][b] = mulSlow(byte(a), byte(b))
		}
	}
	for a := 1; a < 256; a++ {
		invTable[a] = expTable[255-int(logTable[a])]
	}
}

// mulSlow multiplies by shift-and-add ("Russian peasant"), independent of
// the log tables. It is the reference the tables are built from and tested
// against.
func mulSlow(a, b byte) byte {
	var p byte
	x, y := uint16(a), b
	for y != 0 {
		if y&1 != 0 {
			p ^= byte(x)
		}
		x <<= 1
		if x&0x100 != 0 {
			x ^= Poly
		}
		y >>= 1
	}
	return p
}

// Add returns a+b (and a-b: the field has characteristic 2).
func Add(a, b byte) byte { return a ^ b }

// Mul returns a*b.
func Mul(a, b byte) byte { return mulTable[a][b] }

// Inv returns the multiplicative inverse of a. It panics if a is 0.
func Inv(a byte) byte {
	if a == 0 {
		panic("gf: inverse of zero")
	}
	return invTable[a]
}

// Div returns a/b. It panics if b is 0.
func Div(a, b byte) byte {
	if b == 0 {
		panic("gf: division by zero")
	}
	if a == 0 {
		return 0
	}
	return expTable[int(logTable[a])+255-int(logTable[b])]
}

// Exp returns 2^n for n >= 0.
func Exp(n int) byte { return expTable[n%255] }

// Log returns the discrete logarithm of a to base 2. It panics if a is 0.
func Log(a byte) int {
	if a == 0 {
		panic("gf: log of zero")
	}
	return int(logTable[a])
}

// MulTable returns the 256-entry row of products c*x, for table-driven
// multiplication of a whole buffer by the constant c.
func MulTable(c byte) *[256]byte { return &mulTable[c] }

// NibbleTables returns the two 16-entry tables used by SIMD shuffle-based
// multiplication: c*x = low[x&15] ^ high[x>>4].
func NibbleTables(c byte) (low, high [16]byte) {
	for i := 0; i < 16; i++ {
		low[i] = mulTable[c][i]
		high[i] = mulTable[c][i<<4]
	}
	return low, high
}

// Matrix is a dense row-major matrix over GF(2^8).
type Matrix [][]byte

// NewMatrix returns a zero matrix with the given shape.
func NewMatrix(rows, cols int) Matrix {
	buf := make([]byte, rows*cols)
	m := make(Matrix, rows)
	for i := range m {
		m[i] = buf[i*cols : (i+1)*cols : (i+1)*cols]
	}
	return m
}

// Identity returns the n×n identity matrix.
func Identity(n int) Matrix {
	m := NewMatrix(n, n)
	for i := 0; i < n; i++ {
		m[i][i] = 1
	}
	return m
}

// Cauchy returns the rows×cols Cauchy matrix with entries 1/(x_i + y_j),
// where y_j = j and x_i = cols + i. All x_i and y_j are distinct, so every
// square submatrix is invertible. rows+cols must not exceed 256.
func Cauchy(rows, cols int) Matrix {
	if rows+cols > 256 {
		panic(fmt.Sprintf("gf: Cauchy matrix %dx%d needs more than 256 distinct elements", rows, cols))
	}
	m := NewMatrix(rows, cols)
	for i := 0; i < rows; i++ {
		for j := 0; j < cols; j++ {
			m[i][j] = Inv(byte(cols+i) ^ byte(j))
		}
	}
	return m
}

// Rows returns the number of rows.
func (m Matrix) Rows() int { return len(m) }

// Cols returns the number of columns.
func (m Matrix) Cols() int {
	if len(m) == 0 {
		return 0
	}
	return len(m[0])
}

// Clone returns a deep copy.
func (m Matrix) Clone() Matrix {
	c := NewMatrix(m.Rows(), m.Cols())
	for i := range m {
		copy(c[i], m[i])
	}
	return c
}

// Equal reports whether two matrices have the same shape and entries.
func (m Matrix) Equal(o Matrix) bool {
	if m.Rows() != o.Rows() || m.Cols() != o.Cols() {
		return false
	}
	for i := range m {
		for j := range m[i] {
			if m[i][j] != o[i][j] {
				return false
			}
		}
	}
	return true
}

// Mul returns the product m×o.
func (m Matrix) Mul(o Matrix) Matrix {
	if m.Cols() != o.Rows() {
		panic(fmt.Sprintf("gf: cannot multiply %dx%d by %dx%d", m.Rows(), m.Cols(), o.Rows(), o.Cols()))
	}
	r := NewMatrix(m.Rows(), o.Cols())
	for i := range m {
		for j := 0; j < o.Cols(); j++ {
			var s byte
			for k := 0; k < m.Cols(); k++ {
				s ^= mulTable[m[i][k]][o[k][j]]
			}
			r[i][j] = s
		}
	}
	return r
}

// SelectRows returns a new matrix made of the given rows of m, in order.
func (m Matrix) SelectRows(rows []int) Matrix {
	r := NewMatrix(len(rows), m.Cols())
	for i, row := range rows {
		copy(r[i], m[row])
	}
	return r
}

// ErrSingular is returned when inverting a matrix that has no inverse.
var ErrSingular = fmt.Errorf("gf: matrix is singular")

// Invert returns the inverse of a square matrix by Gauss–Jordan elimination.
// m is not modified.
func (m Matrix) Invert() (Matrix, error) {
	n := m.Rows()
	if n != m.Cols() {
		return nil, fmt.Errorf("gf: cannot invert a %dx%d matrix", n, m.Cols())
	}
	a := m.Clone()
	inv := Identity(n)
	for col := 0; col < n; col++ {
		// Find a pivot at or below the diagonal.
		p := -1
		for r := col; r < n; r++ {
			if a[r][col] != 0 {
				p = r
				break
			}
		}
		if p < 0 {
			return nil, ErrSingular
		}
		a[col], a[p] = a[p], a[col]
		inv[col], inv[p] = inv[p], inv[col]
		// Scale the pivot row so the pivot is 1.
		if s := a[col][col]; s != 1 {
			t := &mulTable[Inv(s)]
			for j := 0; j < n; j++ {
				a[col][j] = t[a[col][j]]
				inv[col][j] = t[inv[col][j]]
			}
		}
		// Eliminate the column from every other row.
		for r := 0; r < n; r++ {
			if r == col || a[r][col] == 0 {
				continue
			}
			t := &mulTable[a[r][col]]
			for j := 0; j < n; j++ {
				a[r][j] ^= t[a[col][j]]
				inv[r][j] ^= t[inv[col][j]]
			}
		}
	}
	return inv, nil
}
