package store

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"time"
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// Metadata files are framed as magic, CRC32C of the body, body (JSON), so a
// torn or rotted metadata file is detected rather than misread.
var metaMagic = [4]byte{'S', 'T', 'M', '1'}

var errCorruptMeta = errors.New("corrupt metadata file")

func encodeFramed(v any) ([]byte, error) {
	body, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	out := make([]byte, 8+len(body))
	copy(out, metaMagic[:])
	binary.BigEndian.PutUint32(out[4:], crc32.Checksum(body, castagnoli))
	copy(out[8:], body)
	return out, nil
}

func decodeFramed(b []byte, v any) error {
	if len(b) < 8 || !bytes.Equal(b[:4], metaMagic[:]) {
		return errCorruptMeta
	}
	if crc32.Checksum(b[8:], castagnoli) != binary.BigEndian.Uint32(b[4:]) {
		return errCorruptMeta
	}
	if err := json.Unmarshal(b[8:], v); err != nil {
		return fmt.Errorf("%w: %v", errCorruptMeta, err)
	}
	return nil
}

func readFramed(path string, v any) error {
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return decodeFramed(b, v)
}

// ErasureInfo describes how an object version is spread over the disks.
type ErasureInfo struct {
	Data      int `json:"data"`
	Parity    int `json:"parity"`
	BlockSize int `json:"blockSize"`
	// Index is the shard held by the disk this copy of the metadata is on.
	Index int `json:"index"`
	// Distribution[d] is the shard held by disk d. It is a rotation chosen
	// from the key, so parity shards are spread over all disks.
	Distribution []int `json:"distribution"`
}

// holder returns the disk that holds shard i.
func (e *ErasureInfo) holder(i int) int {
	for d, s := range e.Distribution {
		if s == i {
			return d
		}
	}
	return -1
}

// PartInfo describes one part of an object. Objects written with a single
// PUT have one part, number 1.
type PartInfo struct {
	Number int    `json:"n"`
	Size   int64  `json:"size"`
	ETag   string `json:"etag"`
	// Checksum is the part's checksum in its algorithm, base64, if the
	// client supplied one.
	Checksum string `json:"checksum,omitempty"`
}

// ObjectMeta is the metadata of one object version, stored next to its
// shards on every disk.
type ObjectMeta struct {
	Bucket      string            `json:"bucket"`
	Key         string            `json:"key"`
	VersionID   string            `json:"version"`
	ModTime     int64             `json:"mtime"` // Unix nanoseconds
	Size        int64             `json:"size"`
	ETag        string            `json:"etag"`
	ContentType string            `json:"contentType,omitempty"`
	UserMeta    map[string]string `json:"userMeta,omitempty"`
	Headers     map[string]string `json:"headers,omitempty"`
	// ChecksumAlgorithm and Checksum hold a full-object checksum
	// (x-amz-checksum-*) if the client supplied one.
	ChecksumAlgorithm string      `json:"checksumAlgorithm,omitempty"`
	Checksum          string      `json:"checksum,omitempty"`
	ChecksumType      string      `json:"checksumType,omitempty"`
	Erasure           ErasureInfo `json:"erasure"`
	Parts             []PartInfo  `json:"parts"`
}

func (m *ObjectMeta) modTime() time.Time { return time.Unix(0, m.ModTime).UTC() }

// sameVersion reports whether two copies describe the same version with the
// same content (they differ only in Erasure.Index).
func (m *ObjectMeta) sameVersion(o *ObjectMeta) bool {
	return m.VersionID == o.VersionID && m.Size == o.Size && m.ETag == o.ETag &&
		m.Key == o.Key && len(m.Parts) == len(o.Parts)
}

// forDisk returns a copy of the metadata as stored on disk d.
func (m *ObjectMeta) forDisk(d int) *ObjectMeta {
	c := *m
	c.Erasure.Index = m.Erasure.Distribution[d]
	return &c
}

// distribution returns the rotation of shards over n disks for a key.
func distribution(key string, n int) []int {
	rot := int(crc32.Checksum([]byte(key), castagnoli) % uint32(n))
	d := make([]int, n)
	for i := range d {
		d[i] = (i + rot) % n
	}
	return d
}

// bucketInfo is stored as bucket.json in each bucket directory.
type bucketInfo struct {
	Name    string `json:"name"`
	Created int64  `json:"created"` // Unix nanoseconds
}
