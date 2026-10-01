package checksum

import (
	"bytes"
	"crypto/sha256"
	"hash/crc64"
	"math/rand/v2"
	"testing"
)

// Check values from the CRC catalogue: the CRC of "123456789".
func TestCheckValues(t *testing.T) {
	for algo, want := range map[string]uint64{
		"CRC32":     0xcbf43926,
		"CRC32C":    0xe3069283,
		"CRC64NVME": 0xae8b14860a799888,
	} {
		h := New(algo)
		h.Write([]byte("123456789"))
		if got := decodeCRC(h.Sum(nil)); got != want {
			t.Errorf("%s(123456789) = %x, want %x", algo, got, want)
		}
	}
	if crc64.Checksum([]byte("123456789"), tableCRC64NVME) != 0xae8b14860a799888 {
		t.Error("table mismatch")
	}
}

// Combine must agree with computing the CRC of the concatenation, for
// random splits, including empty and odd-length pieces.
func TestCombineMatchesConcatenation(t *testing.T) {
	r := rand.New(rand.NewPCG(1, 2))
	for _, algo := range []string{"CRC32", "CRC32C", "CRC64NVME"} {
		for trial := 0; trial < 200; trial++ {
			a := make([]byte, r.IntN(3000))
			b := make([]byte, r.IntN(3000))
			for i := range a {
				a[i] = byte(r.Uint32())
			}
			for i := range b {
				b[i] = byte(r.Uint32())
			}
			sum := func(p []byte) uint64 { h := New(algo); h.Write(p); return decodeCRC(h.Sum(nil)) }
			if got, want := Combine(algo, sum(a), sum(b), int64(len(b))), sum(append(append([]byte(nil), a...), b...)); got != want {
				t.Fatalf("%s trial %d (%d+%d bytes): combined %x, want %x", algo, trial, len(a), len(b), got, want)
			}
		}
	}
}

func TestMultipart(t *testing.T) {
	parts := [][]byte{bytes.Repeat([]byte("A"), 5<<20), bytes.Repeat([]byte("B"), 5<<20), []byte("tail")}
	var mp []MultipartPart
	var whole []byte
	for _, p := range parts {
		h := New("CRC32")
		h.Write(p)
		mp = append(mp, MultipartPart{Checksum: Encode(h.Sum(nil)), Size: int64(len(p))})
		whole = append(whole, p...)
	}
	full, err := Multipart("CRC32", FullObject, mp)
	if err != nil {
		t.Fatal(err)
	}
	h := New("CRC32")
	h.Write(whole)
	if full != Encode(h.Sum(nil)) {
		t.Fatalf("full-object CRC32 %s is not the CRC32 of the object", full)
	}
	// COMPOSITE: the checksum of the concatenated binary part checksums.
	var sh []MultipartPart
	inner := sha256.New()
	for _, p := range parts {
		s := sha256.Sum256(p)
		sh = append(sh, MultipartPart{Checksum: Encode(s[:]), Size: int64(len(p))})
		inner.Write(s[:])
	}
	comp, err := Multipart("SHA256", Composite, sh)
	if err != nil || comp != Encode(inner.Sum(nil))+"-3" {
		t.Fatalf("composite = %s, %v", comp, err)
	}
	if _, err := Multipart("SHA256", FullObject, sh); err == nil {
		t.Fatal("a full-object SHA256 was accepted")
	}
	if _, err := Multipart("CRC32", Composite, []MultipartPart{{Checksum: "not base64!"}}); err == nil {
		t.Fatal("an invalid part checksum was accepted")
	}
}

// The values ceph/s3-tests expects for three parts of 5 MiB of 'A', 'B'
// and 'C' (test_multipart_use_cksum_helper_crc32).
func TestS3TestsVector(t *testing.T) {
	var mp []MultipartPart
	for _, c := range []byte("ABC") {
		h := New("CRC32")
		h.Write(bytes.Repeat([]byte{c}, 5<<20))
		mp = append(mp, MultipartPart{Checksum: Encode(h.Sum(nil)), Size: 5 << 20})
	}
	if mp[0].Checksum != "JRTCyQ==" || mp[1].Checksum != "QoZTGg==" || mp[2].Checksum != "YAgjqw==" {
		t.Fatalf("part checksums %v", mp)
	}
	if got, _ := Multipart("CRC32", FullObject, mp); got != "WgDhBQ==" {
		t.Fatalf("full-object CRC32 %s, want WgDhBQ==", got)
	}
}
