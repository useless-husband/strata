//go:build arm64 && !purego

#include "textflag.h"

// Multiplication of a buffer by a constant c in GF(2^8) with NEON.
//
// c*x is linear over GF(2), so c*x = c*(x & 0x0f) ^ c*(x & 0xf0). Each half
// takes 16 values, so two 16-byte tables (low, high) and the TBL instruction
// look up 16 products at once. Each iteration handles 64 bytes. The lengths
// passed in are multiples of 64; the Go wrapper handles the tail.

// func mulNEON(low, high *[16]byte, in, out []byte)
TEXT ·mulNEON(SB), NOSPLIT, $0-64
	MOVD low+0(FP), R0
	MOVD high+8(FP), R1
	MOVD in_base+16(FP), R2
	MOVD in_len+24(FP), R3
	MOVD out_base+40(FP), R4
	VLD1 (R0), [V0.B16]
	VLD1 (R1), [V1.B16]
	VMOVI $15, V2.B16
	LSR $6, R3, R3
	CBZ R3, mulDone

mulLoop:
	VLD1.P 64(R2), [V4.B16, V5.B16, V6.B16, V7.B16]
	VUSHR $4, V4.B16, V8.B16
	VUSHR $4, V5.B16, V9.B16
	VUSHR $4, V6.B16, V10.B16
	VUSHR $4, V7.B16, V11.B16
	VAND V2.B16, V4.B16, V4.B16
	VAND V2.B16, V5.B16, V5.B16
	VAND V2.B16, V6.B16, V6.B16
	VAND V2.B16, V7.B16, V7.B16
	VTBL V4.B16, [V0.B16], V12.B16
	VTBL V5.B16, [V0.B16], V13.B16
	VTBL V6.B16, [V0.B16], V14.B16
	VTBL V7.B16, [V0.B16], V15.B16
	VTBL V8.B16, [V1.B16], V16.B16
	VTBL V9.B16, [V1.B16], V17.B16
	VTBL V10.B16, [V1.B16], V18.B16
	VTBL V11.B16, [V1.B16], V19.B16
	VEOR V16.B16, V12.B16, V12.B16
	VEOR V17.B16, V13.B16, V13.B16
	VEOR V18.B16, V14.B16, V14.B16
	VEOR V19.B16, V15.B16, V15.B16
	VST1.P [V12.B16, V13.B16, V14.B16, V15.B16], 64(R4)
	SUBS $1, R3, R3
	BNE mulLoop

mulDone:
	RET

// func mulAddNEON(low, high *[16]byte, in, out []byte)
TEXT ·mulAddNEON(SB), NOSPLIT, $0-64
	MOVD low+0(FP), R0
	MOVD high+8(FP), R1
	MOVD in_base+16(FP), R2
	MOVD in_len+24(FP), R3
	MOVD out_base+40(FP), R4
	VLD1 (R0), [V0.B16]
	VLD1 (R1), [V1.B16]
	VMOVI $15, V2.B16
	LSR $6, R3, R3
	CBZ R3, addDone

addLoop:
	VLD1.P 64(R2), [V4.B16, V5.B16, V6.B16, V7.B16]
	VLD1 (R4), [V20.B16, V21.B16, V22.B16, V23.B16]
	VUSHR $4, V4.B16, V8.B16
	VUSHR $4, V5.B16, V9.B16
	VUSHR $4, V6.B16, V10.B16
	VUSHR $4, V7.B16, V11.B16
	VAND V2.B16, V4.B16, V4.B16
	VAND V2.B16, V5.B16, V5.B16
	VAND V2.B16, V6.B16, V6.B16
	VAND V2.B16, V7.B16, V7.B16
	VTBL V4.B16, [V0.B16], V12.B16
	VTBL V5.B16, [V0.B16], V13.B16
	VTBL V6.B16, [V0.B16], V14.B16
	VTBL V7.B16, [V0.B16], V15.B16
	VTBL V8.B16, [V1.B16], V16.B16
	VTBL V9.B16, [V1.B16], V17.B16
	VTBL V10.B16, [V1.B16], V18.B16
	VTBL V11.B16, [V1.B16], V19.B16
	VEOR V16.B16, V12.B16, V12.B16
	VEOR V17.B16, V13.B16, V13.B16
	VEOR V18.B16, V14.B16, V14.B16
	VEOR V19.B16, V15.B16, V15.B16
	VEOR V12.B16, V20.B16, V20.B16
	VEOR V13.B16, V21.B16, V21.B16
	VEOR V14.B16, V22.B16, V22.B16
	VEOR V15.B16, V23.B16, V23.B16
	VST1.P [V20.B16, V21.B16, V22.B16, V23.B16], 64(R4)
	SUBS $1, R3, R3
	BNE addLoop

addDone:
	RET
