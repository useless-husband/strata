package store

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"time"
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// Metadata files are framed as
//
//	magic "STM1" | CRC32C of the JSON | length of the JSON | JSON | payload
//
// so a torn or rotted header is detected rather than misread. The payload
// is the inline shard data of a small object (see inlineLimit); it is
// covered by its own per-block checksums, not by the header CRC, so that
// listings and HEADs can read the header without the data.
var metaMagic = [4]byte{'S', 'T', 'M', '1'}

const frameHeader = 12

var errCorruptMeta = errors.New("corrupt metadata file")

func encodeFramed(v any) ([]byte, error) { return encodeFramedPayload(v, nil) }

func encodeFramedPayload(v any, payload []byte) ([]byte, error) {
	body, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	out := make([]byte, frameHeader+len(body)+len(payload))
	copy(out, metaMagic[:])
	binary.BigEndian.PutUint32(out[4:], crc32.Checksum(body, castagnoli))
	binary.BigEndian.PutUint32(out[8:], uint32(len(body)))
	copy(out[frameHeader:], body)
	copy(out[frameHeader+len(body):], payload)
	return out, nil
}

// decodeFramedPayload decodes a frame and returns its payload (aliasing b).
func decodeFramedPayload(b []byte, v any) ([]byte, error) {
	if len(b) < frameHeader || !bytes.Equal(b[:4], metaMagic[:]) {
		return nil, errCorruptMeta
	}
	n := int(binary.BigEndian.Uint32(b[8:]))
	if n > len(b)-frameHeader {
		return nil, errCorruptMeta
	}
	body := b[frameHeader : frameHeader+n]
	if crc32.Checksum(body, castagnoli) != binary.BigEndian.Uint32(b[4:]) {
		return nil, errCorruptMeta
	}
	if err := json.Unmarshal(body, v); err != nil {
		return nil, fmt.Errorf("%w: %v", errCorruptMeta, err)
	}
	return b[frameHeader+n:], nil
}

func decodeFramed(b []byte, v any) error {
	_, err := decodeFramedPayload(b, v)
	return err
}

// readFramed reads and decodes a whole metadata file.
func readFramed(path string, v any) error {
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return decodeFramed(b, v)
}

// readFramedPayload reads a whole metadata file and returns its payload.
func readFramedPayload(path string, v any) ([]byte, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return decodeFramedPayload(b, v)
}

// readFramedHeader decodes a metadata file without reading its payload.
func readFramedHeader(path string, v any) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	buf := make([]byte, 4096)
	n, err := io.ReadFull(f, buf)
	if err != nil && err != io.ErrUnexpectedEOF {
		if err == io.EOF {
			return errCorruptMeta
		}
		return err
	}
	buf = buf[:n]
	if n < frameHeader {
		return errCorruptMeta
	}
	if want := frameHeader + int(binary.BigEndian.Uint32(buf[8:])); want > n {
		if want > 1<<20 {
			return errCorruptMeta
		}
		more := make([]byte, want-n)
		if _, err := io.ReadFull(f, more); err != nil {
			return errCorruptMeta
		}
		buf = append(buf, more...)
	}
	_, err = decodeFramedPayload(buf, v)
	return err
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
	// Inline objects keep each disk's shard file inside that disk's
	// metadata file, as its payload, instead of in a data directory.
	Inline bool `json:"inline,omitempty"`
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
