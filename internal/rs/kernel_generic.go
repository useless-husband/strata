package rs

// mulSliceGeneric sets out[i] = t[in[i]], where t is the product table of a
// constant. out must be at least as long as in.
func mulSliceGeneric(t *[256]byte, in, out []byte) {
	out = out[:len(in)]
	for len(in) >= 8 {
		out[0] = t[in[0]]
		out[1] = t[in[1]]
		out[2] = t[in[2]]
		out[3] = t[in[3]]
		out[4] = t[in[4]]
		out[5] = t[in[5]]
		out[6] = t[in[6]]
		out[7] = t[in[7]]
		in, out = in[8:], out[8:]
	}
	for i, x := range in {
		out[i] = t[x]
	}
}

// mulAddSliceGeneric sets out[i] ^= t[in[i]].
func mulAddSliceGeneric(t *[256]byte, in, out []byte) {
	out = out[:len(in)]
	for len(in) >= 8 {
		out[0] ^= t[in[0]]
		out[1] ^= t[in[1]]
		out[2] ^= t[in[2]]
		out[3] ^= t[in[3]]
		out[4] ^= t[in[4]]
		out[5] ^= t[in[5]]
		out[6] ^= t[in[6]]
		out[7] ^= t[in[7]]
		in, out = in[8:], out[8:]
	}
	for i, x := range in {
		out[i] ^= t[x]
	}
}
