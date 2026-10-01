package store

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync/atomic"
	"syscall"
)

// SyncMode selects how hard strata pushes writes to stable storage before
// acknowledging them.
type SyncMode int

const (
	// SyncFull flushes the drive's write cache at each commit point
	// (F_FULLFSYNC on macOS, fsync on Linux). This is the default.
	SyncFull SyncMode = iota
	// SyncFsync uses fsync(2) only. On macOS this survives a process or
	// kernel crash but not necessarily a power loss.
	SyncFsync
	// SyncNone never syncs. A process crash is still safe (the page cache
	// survives it); an OS crash or power loss may lose acknowledged writes.
	SyncNone
)

func (m SyncMode) String() string {
	switch m {
	case SyncFull:
		return "full"
	case SyncFsync:
		return "fsync"
	case SyncNone:
		return "none"
	}
	return fmt.Sprintf("SyncMode(%d)", int(m))
}

// ParseSyncMode parses "full", "fsync" or "none".
func ParseSyncMode(s string) (SyncMode, error) {
	switch s {
	case "full":
		return SyncFull, nil
	case "fsync":
		return SyncFsync, nil
	case "none":
		return SyncNone, nil
	}
	return 0, fmt.Errorf("unknown sync mode %q (want full, fsync or none)", s)
}

// On-disk layout of one disk:
//
//	<root>/.strata/format.json         identity of the disk and its deployment
//	<root>/.strata/lock                flock held by the process using the disk
//	<root>/.strata/tmp/                staging area; emptied at start-up
//	<root>/.strata/trash/              renamed-away data waiting for deletion
//	<root>/.strata/multipart/<id>/     in-progress multipart uploads
//	<root>/buckets/<bucket>/bucket.json
//	<root>/buckets/<bucket>/objects/<hh>/<hash>/<version>.meta
//	<root>/buckets/<bucket>/objects/<hh>/<hash>/<version>/part.<n>
//
// <hash> is the first 128 bits of SHA-256(key) in hex and <hh> its first
// byte, which bounds directory sizes whatever the key names look like and
// keeps arbitrary S3 keys (any UTF-8, "..", "//", 1024 bytes) away from the
// file system's path rules. The key itself is stored in the metadata.
const (
	sysDir       = ".strata"
	formatFile   = "format.json"
	lockFile     = "lock"
	tmpDir       = "tmp"
	trashDir     = "trash"
	multipartDir = "multipart"
	bucketsDir   = "buckets"
	bucketMeta   = "bucket.json"
	objectsDir   = "objects"
	metaSuffix   = ".meta"
)

// disk is one of the directories an erasure set is spread over.
type disk struct {
	idx    int
	root   string
	sync   SyncMode
	lock   *os.File
	online atomic.Bool
	// errors counts I/O errors other than "not found", for metrics.
	errors atomic.Int64
}

func (d *disk) String() string { return fmt.Sprintf("disk %d (%s)", d.idx, d.root) }

func (d *disk) path(elem ...string) string {
	return filepath.Join(append([]string{d.root}, elem...)...)
}

func (d *disk) sysPath(elem ...string) string {
	return d.path(append([]string{sysDir}, elem...)...)
}

func (d *disk) bucketPath(bucket string) string { return d.path(bucketsDir, bucket) }

// keyHash names an object's directory.
func keyHash(key string) string {
	h := sha256.Sum256([]byte(key))
	return hex.EncodeToString(h[:16])
}

func (d *disk) objectDir(bucket, key string) string {
	h := keyHash(key)
	return d.path(bucketsDir, bucket, objectsDir, h[:2], h)
}

// newID returns a random 128-bit identifier in hex.
func newID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b[:])
}

// noteErr records an I/O error for the disk's error counter and returns it.
func (d *disk) noteErr(err error) error {
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		d.errors.Add(1)
	}
	return err
}

// syncFile applies the disk's sync mode to an open file.
func (d *disk) syncFile(f *os.File) error {
	switch d.sync {
	case SyncFull:
		return fullSync(f)
	case SyncFsync:
		return plainSync(f)
	}
	return nil
}

// syncDir makes a directory's entries (creations, renames) durable.
func (d *disk) syncDir(dir string) error {
	if d.sync == SyncNone {
		return nil
	}
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer f.Close()
	err = d.syncFile(f)
	if errors.Is(err, syscall.EINVAL) || errors.Is(err, syscall.ENOTSUP) {
		return nil // some file systems cannot sync directories
	}
	return err
}

// mkdirAll creates dir and any missing parents, syncing each parent whose
// entries changed so the new directories survive a crash.
func (d *disk) mkdirAll(dir string) error {
	if _, err := os.Stat(dir); err == nil {
		return nil
	}
	parent := filepath.Dir(dir)
	if parent != dir {
		if err := d.mkdirAll(parent); err != nil {
			return err
		}
	}
	if err := os.Mkdir(dir, 0o755); err != nil {
		if errors.Is(err, fs.ErrExist) {
			return nil
		}
		return err
	}
	return d.syncDir(parent)
}

// writeFileAtomic writes data to path so that a crash leaves either the old
// file or the complete new one: write to a temporary file, sync it, rename
// it into place and sync the directory.
func (d *disk) writeFileAtomic(path string, data []byte) error {
	tmp := d.sysPath(tmpDir, newID())
	if err := d.writeFileSynced(tmp, data); err != nil {
		return err
	}
	if err := d.mkdirAll(filepath.Dir(path)); err != nil {
		os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		os.Remove(tmp)
		return err
	}
	return d.syncDir(filepath.Dir(path))
}

// writeFileSynced creates path with data and syncs it.
func (d *disk) writeFileSynced(path string, data []byte) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		os.Remove(path)
		return err
	}
	if err := d.syncFile(f); err != nil {
		f.Close()
		os.Remove(path)
		return err
	}
	return f.Close()
}

// removeAll deletes a tree by first renaming it into the trash (atomic) and
// then deleting it.
func (d *disk) removeAll(path string) error {
	trash := d.sysPath(trashDir, newID())
	if err := os.Rename(path, trash); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		return os.RemoveAll(path)
	}
	return os.RemoveAll(trash)
}
