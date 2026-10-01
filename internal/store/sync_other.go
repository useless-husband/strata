//go:build !darwin

package store

import "os"

// fullSync makes f's data and metadata durable with fsync(2), which on
// Linux also flushes the drive's volatile write cache.
func fullSync(f *os.File) error { return f.Sync() }

// plainSync is the same as fullSync off macOS.
func plainSync(f *os.File) error { return f.Sync() }
