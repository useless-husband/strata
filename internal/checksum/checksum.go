// Package checksum implements the S3 additional checksums
// (x-amz-checksum-*): CRC32, CRC32C, CRC64NVME, SHA1 and SHA256, and the
// two ways S3 derives a multipart object's checksum from its parts:
// COMPOSITE (a checksum of the parts' checksums, "-N" appended) and
// FULL_OBJECT (the CRC of the whole object, combined from the parts' CRCs
// without reading the data again).
package checksum

import (
	"crypto/sha1"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"hash"
	"hash/crc32"
	"hash/crc64"
	"strconv"
)

// Algorithms lists the supported algorithms in their S3 spelling.
var Algorithms = []string{"CRC32", "CRC32C", "CRC64NVME", "SHA1", "SHA256"}

// Checksum types.
const (
	Composite  = "COMPOSITE"
	FullObject = "FULL_OBJECT"
)

// Reflected polynomials of the CRCs. CRC-64/NVME is 0xad93d23594c93659;
// hash/crc64 wants it bit-reversed, like the others.
const (
	polyCRC32     = 0xedb88320
	polyCRC32C    = 0x82f63b78
	polyCRC64NVME = 0x9a6c9329ac4bc9b5
)

var (
	tableCRC32C    = crc32.MakeTable(crc32.Castagnoli)
	tableCRC64NVME = crc64.MakeTable(polyCRC64NVME)
)

// New returns a hash for an algorithm, or nil if it is unknown.
func New(algo string) hash.Hash {
	switch algo {
	case "CRC32":
		return crc32.NewIEEE()
	case "CRC32C":
		return crc32.New(tableCRC32C)
	case "CRC64NVME":
		return crc64.New(tableCRC64NVME)
	case "SHA1":
		return sha1.New()
	case "SHA256":
		return sha256.New()
	}
	return nil
}

// Valid reports whether algo is supported.
func Valid(algo string) bool { return New(algo) != nil }

// IsCRC reports whether algo is a CRC, which can be combined.
func IsCRC(algo string) bool { return algo == "CRC32" || algo == "CRC32C" || algo == "CRC64NVME" }

// Header returns the HTTP header that carries algo.
func Header(algo string) string {
	switch algo {
	case "CRC32":
		return "x-amz-checksum-crc32"
	case "CRC32C":
		return "x-amz-checksum-crc32c"
	case "CRC64NVME":
		return "x-amz-checksum-crc64nvme"
	case "SHA1":
		return "x-amz-checksum-sha1"
	}
	return "x-amz-checksum-sha256"
}

// DefaultType is the checksum type S3 gives a multipart upload created
// with algo when the client does not choose: FULL_OBJECT for CRC64NVME,
// COMPOSITE otherwise.
func DefaultType(algo string) string {
	if algo == "CRC64NVME" {
		return FullObject
	}
	return Composite
}

// TypeAllowed reports whether a multipart upload may use the type with
// algo: SHA checksums can only be COMPOSITE and CRC64NVME only FULL_OBJECT.
func TypeAllowed(algo, typ string) bool {
	switch typ {
	case Composite:
		return algo != "CRC64NVME"
	case FullObject:
		return IsCRC(algo)
	}
	return false
}

// Encode returns the base64 form S3 uses.
func Encode(sum []byte) string { return base64.StdEncoding.EncodeToString(sum) }

// MultipartPart is one part's checksum and size.
type MultipartPart struct {
	Checksum string // base64, as returned by UploadPart
	Size     int64
}

// Multipart computes a multipart object's checksum from its parts.
func Multipart(algo, typ string, parts []MultipartPart) (string, error) {
	raw := make([][]byte, len(parts))
	for i, p := range parts {
		b, err := base64.StdEncoding.DecodeString(p.Checksum)
		if err != nil || (IsCRC(algo) && len(b) != crcLen(algo)) {
			return "", fmt.Errorf("part %d has an invalid %s checksum", i+1, algo)
		}
		raw[i] = b
	}
	switch typ {
	case Composite:
		h := New(algo)
		for _, b := range raw {
			h.Write(b)
		}
		return Encode(h.Sum(nil)) + "-" + strconv.Itoa(len(parts)), nil
	case FullObject:
		if !IsCRC(algo) {
			return "", fmt.Errorf("%s cannot be a full-object checksum", algo)
		}
		var crc uint64
		for i, b := range raw {
			c := decodeCRC(b)
			if i == 0 {
				crc = c
				continue
			}
			crc = Combine(algo, crc, c, parts[i].Size)
		}
		return Encode(encodeCRC(algo, crc)), nil
	}
	return "", fmt.Errorf("unknown checksum type %q", typ)
}

func crcLen(algo string) int {
	if algo == "CRC64NVME" {
		return 8
	}
	return 4
}

func decodeCRC(b []byte) uint64 {
	if len(b) == 4 {
		return uint64(binary.BigEndian.Uint32(b))
	}
	return binary.BigEndian.Uint64(b)
}

func encodeCRC(algo string, crc uint64) []byte {
	if algo == "CRC64NVME" {
		return binary.BigEndian.AppendUint64(nil, crc)
	}
	return binary.BigEndian.AppendUint32(nil, uint32(crc))
}

// Combine returns the CRC of A‖B from crc1 = CRC(A), crc2 = CRC(B) and the
// length of B, for the reflected CRCs with all-ones initial value and final
// XOR used here. This is zlib's crc32_combine: appending len2 zero bytes to
// A is a linear map on the CRC register, applied by repeated squaring of
// the one-zero-bit operator as a matrix over GF(2).
func Combine(algo string, crc1, crc2 uint64, len2 int64) uint64 {
	if len2 <= 0 {
		return crc1
	}
	width, poly := 32, uint64(polyCRC32)
	switch algo {
	case "CRC32C":
		poly = polyCRC32C
	case "CRC64NVME":
		width, poly = 64, polyCRC64NVME
	}
	odd := make([]uint64, width)  // operator for one zero bit
	even := make([]uint64, width) // operator for two zero bits
	odd[0] = poly
	row := uint64(1)
	for n := 1; n < width; n++ {
		odd[n] = row
		row <<= 1
	}
	square(even, odd) // two zero bits
	square(odd, even) // four zero bits
	for {
		square(even, odd)
		if len2&1 != 0 {
			crc1 = times(even, crc1)
		}
		len2 >>= 1
		if len2 == 0 {
			break
		}
		square(odd, even)
		if len2&1 != 0 {
			crc1 = times(odd, crc1)
		}
		len2 >>= 1
		if len2 == 0 {
			break
		}
	}
	return crc1 ^ crc2
}

func times(mat []uint64, vec uint64) uint64 {
	var sum uint64
	for i := 0; vec != 0; i, vec = i+1, vec>>1 {
		if vec&1 != 0 {
			sum ^= mat[i]
		}
	}
	return sum
}

func square(dst, mat []uint64) {
	for n := range mat {
		dst[n] = times(mat, mat[n])
	}
}
