//go:build darwin

package store

import (
	"os"
	"syscall"
)

// fullSync makes f's data and metadata durable. On macOS fsync(2) only
// moves data to the drive, whose volatile cache may still lose it on power
// failure; F_FULLFSYNC also asks the drive to flush that cache. Some file
// systems (network, FUSE) do not support it; fall back to fsync then.
func fullSync(f *os.File) error {
	_, _, errno := syscall.Syscall(syscall.SYS_FCNTL, f.Fd(), syscall.F_FULLFSYNC, 0)
	if errno == 0 {
		return nil
	}
	return syscall.Fsync(int(f.Fd()))
}

// plainSync is fsync(2): data reaches the drive but not necessarily stable
// storage.
func plainSync(f *os.File) error { return syscall.Fsync(int(f.Fd())) }
