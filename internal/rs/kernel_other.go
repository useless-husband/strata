//go:build !arm64 || purego

package rs

// useAsm is always false on platforms without an assembly kernel.
var useAsm = false

// Kernel names the multiplication kernel in use, for benchmarks and logs.
func Kernel() string { return "table (portable Go)" }

func mulSlice(c *coef, in, out []byte)    { mulSliceGeneric(c.full, in, out) }
func mulAddSlice(c *coef, in, out []byte) { mulAddSliceGeneric(c.full, in, out) }
