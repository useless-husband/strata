package store

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"syscall"
)

// format identifies a disk as position Index of a deployment. Every disk of
// a deployment carries the same Deployment, Data, Parity and BlockSize.
type format struct {
	Format     string `json:"format"` // always "strata-v1"
	Deployment string `json:"deployment"`
	Disk       string `json:"disk"`
	Index      int    `json:"index"`
	Disks      int    `json:"disks"`
	Data       int    `json:"data"`
	Parity     int    `json:"parity"`
	BlockSize  int    `json:"blockSize"`
}

const formatName = "strata-v1"

func (d *disk) readFormat() (*format, error) {
	b, err := os.ReadFile(d.sysPath(formatFile))
	if err != nil {
		return nil, err
	}
	var f format
	if err := json.Unmarshal(b, &f); err != nil || f.Format != formatName {
		return nil, fmt.Errorf("%s: %s is not a strata format file", d, formatFile)
	}
	return &f, nil
}

func (d *disk) writeFormat(f *format) error {
	if err := d.mkdirAll(d.sysPath(tmpDir)); err != nil {
		return err
	}
	b, _ := json.MarshalIndent(f, "", "  ")
	return d.writeFileAtomic(d.sysPath(formatFile), b)
}

// ensureLayout creates the system directories.
func (d *disk) ensureLayout() error {
	for _, dir := range []string{tmpDir, trashDir, multipartDir} {
		if err := d.mkdirAll(d.sysPath(dir)); err != nil {
			return err
		}
	}
	return d.mkdirAll(d.path(bucketsDir))
}

// acquireLock takes an exclusive flock on the disk so two strata processes
// (say, a server and an offline "strata heal") never use it at once.
func (d *disk) acquireLock() error {
	if err := d.mkdirAll(d.sysPath()); err != nil {
		return err
	}
	f, err := os.OpenFile(d.sysPath(lockFile), os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return fmt.Errorf("%s is in use by another strata process", d.root)
		}
		return err
	}
	d.lock = f
	return nil
}

func (d *disk) releaseLock() {
	if d.lock != nil {
		syscall.Flock(int(d.lock.Fd()), syscall.LOCK_UN)
		d.lock.Close()
		d.lock = nil
	}
}

// loadFormats reads or creates the format of every disk. Fresh disks are
// formatted into the deployment of the others (a replaced disk) or, if all
// are fresh, into a new deployment. It returns the indices of disks that
// were (re)formatted, which need a full heal.
func loadFormats(disks []*disk, cfg *Config) (*format, []int, error) {
	formats := make([]*format, len(disks))
	var ref *format
	for i, d := range disks {
		f, err := d.readFormat()
		switch {
		case err == nil:
			formats[i] = f
			if ref == nil {
				ref = f
			}
		case errors.Is(err, fs.ErrNotExist):
		default:
			return nil, nil, err
		}
	}
	if ref == nil {
		ref = &format{Format: formatName, Deployment: newID(), Disks: len(disks),
			Data: cfg.DataShards, Parity: cfg.ParityShards, BlockSize: cfg.BlockSize}
	}
	formatted := 0
	for i, f := range formats {
		if f == nil {
			continue
		}
		formatted++
		if f.Deployment != ref.Deployment {
			return nil, nil, fmt.Errorf("%s belongs to deployment %s, but %s belongs to %s",
				disks[i].root, f.Deployment, disks[0].root, ref.Deployment)
		}
		if f.Index != i {
			return nil, nil, fmt.Errorf("%s was disk %d of the deployment but is given as disk %d; keep the disks in their original order", disks[i].root, f.Index, i)
		}
	}
	if ref.Disks != len(disks) || ref.Data != cfg.DataShards || ref.Parity != cfg.ParityShards {
		return nil, nil, fmt.Errorf("the disks were formatted for %d disks as %d data + %d parity; started with %d disks as %d + %d",
			ref.Disks, ref.Data, ref.Parity, len(disks), cfg.DataShards, cfg.ParityShards)
	}
	if ref.BlockSize != cfg.BlockSize {
		return nil, nil, fmt.Errorf("the disks were formatted with block size %d, not %d", ref.BlockSize, cfg.BlockSize)
	}
	if formatted > 0 && formatted < cfg.DataShards {
		return nil, nil, fmt.Errorf("only %d of %d disks are formatted, fewer than the %d needed to read anything; check the paths", formatted, len(disks), cfg.DataShards)
	}
	var fresh []int
	for i, f := range formats {
		if f != nil {
			continue
		}
		nf := *ref
		nf.Index = i
		nf.Disk = newID()
		if err := disks[i].writeFormat(&nf); err != nil {
			return nil, nil, fmt.Errorf("formatting %s: %w", disks[i].root, err)
		}
		if formatted > 0 {
			fresh = append(fresh, i)
		}
	}
	return ref, fresh, nil
}

// reformatIfMissing recreates the layout and format of a disk whose
// directory was wiped or replaced while running. It reports whether it did.
func (d *disk) reformatIfMissing(ref *format) (bool, error) {
	if _, err := os.Stat(d.sysPath(formatFile)); err == nil {
		return false, nil
	} else if !errors.Is(err, fs.ErrNotExist) {
		return false, err
	}
	if err := d.mkdirAll(d.root); err != nil {
		return false, err
	}
	// The lock file went with the directory; take a new lock.
	d.releaseLock()
	if err := d.acquireLock(); err != nil {
		return false, err
	}
	nf := *ref
	nf.Index = d.idx
	nf.Disk = newID()
	if err := d.writeFormat(&nf); err != nil {
		return false, err
	}
	return true, d.ensureLayout()
}

// cleanTmp empties the staging and trash areas. Called at start-up, when
// nothing can be in flight.
func (d *disk) cleanTmp() error {
	for _, dir := range []string{tmpDir, trashDir} {
		p := d.sysPath(dir)
		entries, err := os.ReadDir(p)
		if err != nil && !errors.Is(err, fs.ErrNotExist) {
			return err
		}
		for _, e := range entries {
			if err := os.RemoveAll(filepath.Join(p, e.Name())); err != nil {
				return err
			}
		}
	}
	return nil
}
