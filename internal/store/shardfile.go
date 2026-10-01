package store

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
)

// A shard file holds one shard of every stripe of a part:
//
//	[crc32c][block of stripe 0][crc32c][block of stripe 1]...
//
// Every block but the last has the same length, so block s starts at
// s*(4+blockLen) and a ranged read touches only the blocks it needs.
//
// The checksum is CRC32C (hardware-accelerated on arm64 and amd64, so it
// costs far less than the erasure coding) seeded with the stripe number and
// shard index. Seeding means a block written at the wrong offset, or a shard
// file that ends up under the wrong disk or part, fails verification even
// though its bytes are intact. CRC32C detects every error burst up to 32
// bits and any other random corruption with probability 1-2^-32 per block;
// it is not a defence against deliberate tampering, which needs a keyed MAC
// and is outside strata's threat model (whoever can write the disks can also
// rewrite the checksums).
const crcSize = 4

var (
	errCorruptBlock = errors.New("shard block checksum mismatch")
	errShortShard   = errors.New("shard file is truncated")
)

func blockCRC(stripe int64, shard int, data []byte) uint32 {
	var hdr [12]byte
	binary.BigEndian.PutUint64(hdr[:8], uint64(stripe))
	binary.BigEndian.PutUint32(hdr[8:], uint32(shard))
	return crc32.Update(crc32.Update(0, castagnoli, hdr[:]), castagnoli, data)
}

// geometry computes the layout of a part of a given size.
type geometry struct {
	k         int
	blockSize int64 // data bytes per stripe
}

func (g geometry) stripes(size int64) int64 {
	return (size + g.blockSize - 1) / g.blockSize
}

// stripeLen is the number of data bytes in stripe s.
func (g geometry) stripeLen(size, s int64) int64 {
	return min(g.blockSize, size-s*g.blockSize)
}

// shardLen is the length of each shard block of a stripe with n data bytes.
func (g geometry) shardLen(n int64) int64 { return (n + int64(g.k) - 1) / int64(g.k) }

// blockOffset is where stripe s starts in a shard file.
func (g geometry) blockOffset(s int64) int64 {
	return s * (crcSize + g.shardLen(g.blockSize))
}

// shardFileSize is the size of each shard file of a part of size bytes.
func (g geometry) shardFileSize(size int64) int64 {
	n := g.stripes(size)
	if n == 0 {
		return 0
	}
	last := n - 1
	return g.blockOffset(last) + crcSize + g.shardLen(g.stripeLen(size, last))
}

// readBlock reads and verifies the block of stripe s from a shard file. buf
// must have room for crcSize+n bytes; the returned slice aliases it.
func readBlock(f *os.File, g geometry, s int64, shard int, n int64, buf []byte) ([]byte, error) {
	buf = buf[:crcSize+n]
	if _, err := f.ReadAt(buf, g.blockOffset(s)); err != nil {
		if errors.Is(err, io.EOF) {
			return nil, errShortShard
		}
		return nil, err
	}
	data := buf[crcSize:]
	if binary.BigEndian.Uint32(buf) != blockCRC(s, shard, data) {
		return nil, fmt.Errorf("%w (stripe %d, shard %d)", errCorruptBlock, s, shard)
	}
	return data, nil
}

// appendBlock appends a checksummed block to dst.
func appendBlock(dst []byte, s int64, shard int, data []byte) []byte {
	dst = binary.BigEndian.AppendUint32(dst, blockCRC(s, shard, data))
	return append(dst, data...)
}
