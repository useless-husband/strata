//go:build arm64 && !purego

package rs

// useAsm selects the NEON kernel. Every arm64 CPU Go supports has Advanced
// SIMD, so it is on by default; tests turn it off to compare the two paths.
var useAsm = true

// Kernel names the multiplication kernel in use, for benchmarks and logs.
func Kernel() string {
	if useAsm {
		return "NEON TBL (arm64)"
	}
	return "table (portable Go)"
}

//go:noescape
func mulNEON(low, high *[16]byte, in, out []byte)

//go:noescape
func mulAddNEON(low, high *[16]byte, in, out []byte)

// mulSlice sets out = c*in.
func mulSlice(c *coef, in, out []byte) {
	n := 0
	if useAsm {
		n = len(in) &^ 63
		if n > 0 {
			mulNEON(&c.low, &c.high, in[:n], out[:n])
		}
	}
	if n < len(in) {
		mulSliceGeneric(c.full, in[n:], out[n:])
	}
}

// mulAddSlice sets out ^= c*in.
func mulAddSlice(c *coef, in, out []byte) {
	n := 0
	if useAsm {
		n = len(in) &^ 63
		if n > 0 {
			mulAddNEON(&c.low, &c.high, in[:n], out[:n])
		}
	}
	if n < len(in) {
		mulAddSliceGeneric(c.full, in[n:], out[n:])
	}
}
