package gf

import (
	"math/rand/v2"
	"testing"
)

func TestTablesMatchShiftAndAdd(t *testing.T) {
	for a := 0; a < 256; a++ {
		for b := 0; b < 256; b++ {
			if got, want := Mul(byte(a), byte(b)), mulSlow(byte(a), byte(b)); got != want {
				t.Fatalf("Mul(%d,%d) = %d, want %d", a, b, got, want)
			}
		}
	}
}

func TestGeneratorHasFullOrder(t *testing.T) {
	seen := map[byte]bool{}
	x := byte(1)
	for i := 0; i < 255; i++ {
		if seen[x] {
			t.Fatalf("2^%d = %d repeats: 2 is not a generator", i, x)
		}
		seen[x] = true
		x = mulSlow(x, 2)
	}
	if x != 1 {
		t.Fatalf("2^255 = %d, want 1", x)
	}
	if len(seen) != 255 || seen[0] {
		t.Fatalf("powers of 2 cover %d elements", len(seen))
	}
}

// TestFieldAxioms checks the field laws exhaustively: 2^24 triples.
func TestFieldAxioms(t *testing.T) {
	for a := 0; a < 256; a++ {
		A := byte(a)
		if Mul(A, 1) != A || Mul(A, 0) != 0 {
			t.Fatalf("identity/zero law fails for %d", a)
		}
		if a != 0 {
			if Mul(A, Inv(A)) != 1 {
				t.Fatalf("%d * Inv(%d) != 1", a, a)
			}
			if Exp(Log(A)) != A {
				t.Fatalf("Exp(Log(%d)) != %d", a, a)
			}
		}
		for b := 0; b < 256; b++ {
			B := byte(b)
			if Mul(A, B) != Mul(B, A) {
				t.Fatalf("multiplication not commutative at %d,%d", a, b)
			}
			if b != 0 && Mul(Div(A, B), B) != A {
				t.Fatalf("Div(%d,%d)*%d != %d", a, b, b, a)
			}
			for c := 0; c < 256; c++ {
				C := byte(c)
				if Mul(Mul(A, B), C) != Mul(A, Mul(B, C)) {
					t.Fatalf("multiplication not associative at %d,%d,%d", a, b, c)
				}
				if Mul(A, B^C) != Mul(A, B)^Mul(A, C) {
					t.Fatalf("not distributive at %d,%d,%d", a, b, c)
				}
			}
		}
	}
}

func TestNibbleTables(t *testing.T) {
	for c := 0; c < 256; c++ {
		low, high := NibbleTables(byte(c))
		for x := 0; x < 256; x++ {
			if got := low[x&15] ^ high[x>>4]; got != Mul(byte(c), byte(x)) {
				t.Fatalf("nibble tables of %d wrong at %d", c, x)
			}
		}
	}
}

func TestPanicsOnZero(t *testing.T) {
	for name, f := range map[string]func(){
		"Inv": func() { Inv(0) },
		"Div": func() { Div(1, 0) },
		"Log": func() { Log(0) },
	} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("%s(0) did not panic", name)
				}
			}()
			f()
		}()
	}
}

func randomMatrix(r *rand.Rand, n int) Matrix {
	m := NewMatrix(n, n)
	for i := range m {
		for j := range m[i] {
			m[i][j] = byte(r.IntN(256))
		}
	}
	return m
}

func TestInvertRandom(t *testing.T) {
	const seed = 1
	r := rand.New(rand.NewPCG(seed, 0))
	invertible := 0
	for trial := 0; trial < 2000; trial++ {
		n := 1 + r.IntN(16)
		m := randomMatrix(r, n)
		inv, err := m.Invert()
		if err == ErrSingular {
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		invertible++
		if !m.Mul(inv).Equal(Identity(n)) || !inv.Mul(m).Equal(Identity(n)) {
			t.Fatalf("seed %d trial %d: M*M^-1 != I for\n%v", seed, trial, m)
		}
	}
	if invertible < 1500 {
		t.Fatalf("only %d of 2000 random matrices were invertible; expected about 99.6%%", invertible)
	}
}

func TestInvertSingular(t *testing.T) {
	m := Matrix{{1, 2, 3}, {4, 5, 6}, {1 ^ 4, 2 ^ 5, 3 ^ 6}} // row 3 = row 1 + row 2
	if _, err := m.Invert(); err != ErrSingular {
		t.Fatalf("got %v, want ErrSingular", err)
	}
	if _, err := NewMatrix(2, 3).Invert(); err == nil {
		t.Fatal("inverting a non-square matrix succeeded")
	}
	// The input must not be modified.
	before := m.Clone()
	m.Invert()
	if !m.Equal(before) {
		t.Fatal("Invert modified its input")
	}
}

// Every square submatrix of a Cauchy matrix is invertible. Check every
// square submatrix of an 8×8 Cauchy matrix (12,869 of them).
func TestCauchySubmatricesInvertible(t *testing.T) {
	const n = 8
	c := Cauchy(n, n)
	count := 0
	for size := 1; size <= n; size++ {
		forEachSubset(n, size, func(rows []int) {
			forEachSubset(n, size, func(cols []int) {
				sub := NewMatrix(size, size)
				for i, r := range rows {
					for j, col := range cols {
						sub[i][j] = c[r][col]
					}
				}
				if _, err := sub.Invert(); err != nil {
					t.Fatalf("Cauchy submatrix rows %v cols %v is singular", rows, cols)
				}
				count++
			})
		})
	}
	if count != 12869 {
		t.Fatalf("checked %d submatrices, want 12869", count)
	}
}

func TestCauchyTooLarge(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("Cauchy(200, 100) did not panic")
		}
	}()
	Cauchy(200, 100)
}

// forEachSubset calls f with every size-element subset of {0..n-1} in
// lexicographic order. The slice is reused between calls.
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
