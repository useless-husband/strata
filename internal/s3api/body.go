package s3api

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"hash"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/useless-husband/strata/internal/checksum"
	"github.com/useless-husband/strata/internal/s3err"
	"github.com/useless-husband/strata/internal/sigv4"
	"github.com/useless-husband/strata/internal/store"
)

func newChecksum(algo string) hash.Hash { return checksum.New(algo) }

func checksumHeader(algo string) string { return checksum.Header(algo) }

// requestChecksum works out which checksum the client sent, if any: named
// in x-amz-trailer (value arrives after the body) or present as a header.
// A value that is not a checksum at all fails the comparison at the end
// of the body like a wrong one does (BadDigest, as S3 answers).
func requestChecksum(r *http.Request) (algo, value string, trailer bool, err error) {
	if t := r.Header.Get("X-Amz-Trailer"); t != "" {
		for _, name := range strings.Split(t, ",") {
			name = strings.ToLower(strings.TrimSpace(name))
			for _, a := range checksum.Algorithms {
				if name == checksumHeader(a) {
					return a, "", true, nil
				}
			}
		}
		return "", "", false, s3err.InvalidRequest.With("The value specified in the x-amz-trailer header is not supported")
	}
	for _, a := range checksum.Algorithms {
		if v := r.Header.Get(checksumHeader(a)); v != "" {
			if algo != "" {
				return "", "", false, s3err.InvalidRequest.With("Expecting a single x-amz-checksum- header. Multiple checksum Types are not allowed.")
			}
			algo, value = a, v
		}
	}
	return algo, value, false, nil
}

func sumBase64(h hash.Hash) string { return base64.StdEncoding.EncodeToString(h.Sum(nil)) }

// bodyReader delivers a request body to the store and verifies, at the end
// of the stream and before the store commits, everything the client
// promised about it: the decoded length, the signed SHA-256, and the
// x-amz-checksum value. A mismatch is returned instead of io.EOF, which
// makes the store discard the upload.
type bodyReader struct {
	r        io.Reader
	chunked  *sigv4.ChunkedReader
	size     int64 // expected decoded length; -1 if unknown
	n        int64
	sha      hash.Hash
	wantSHA  string
	ck       hash.Hash
	ckAlgo   string
	ckWant   string
	ckFromTr bool
	err      error
}

// newBodyReader wraps the body of an authenticated PUT or POST.
func newBodyReader(r *http.Request, auth *sigv4.Auth) (*bodyReader, error) {
	b := &bodyReader{r: r.Body, size: r.ContentLength}
	switch auth.Mode {
	case sigv4.PayloadStreaming, sigv4.PayloadStreamingTrailer, sigv4.PayloadStreamingUnsignedTrailer:
		dl := r.Header.Get("X-Amz-Decoded-Content-Length")
		if dl == "" {
			return nil, s3err.MissingContentLength.With("x-amz-decoded-content-length is required with aws-chunked encoding")
		}
		n, err := strconv.ParseInt(dl, 10, 64)
		if err != nil || n < 0 {
			return nil, s3err.InvalidArgument.With("x-amz-decoded-content-length is not a valid length")
		}
		b.size = n
		b.chunked = sigv4.NewChunkedReader(r.Body, auth)
		b.r = b.chunked
	case sigv4.PayloadSigned:
		if auth.PayloadHash != sigv4.EmptySHA256 || r.ContentLength != 0 {
			b.sha = sha256.New()
			b.wantSHA = auth.PayloadHash
		}
	}
	// HTTP chunked transfer encoding (not aws-chunked) has no length up
	// front; accept it and count, within the size limits of the store.
	if b.size < 0 && !(len(r.TransferEncoding) > 0 && r.TransferEncoding[0] == "chunked") {
		return nil, s3err.MissingContentLength
	}
	algo, value, fromTrailer, err := requestChecksum(r)
	if err != nil {
		return nil, err
	}
	if fromTrailer && b.chunked == nil {
		return nil, s3err.InvalidRequest.With("x-amz-trailer requires an aws-chunked body")
	}
	if algo != "" {
		b.ck, b.ckAlgo, b.ckWant, b.ckFromTr = newChecksum(algo), algo, value, fromTrailer
	}
	return b, nil
}

func (b *bodyReader) Read(p []byte) (int, error) {
	if b.err != nil {
		return 0, b.err
	}
	n, err := b.r.Read(p)
	if n > 0 {
		b.n += int64(n)
		if b.size >= 0 && b.n > b.size {
			b.err = s3err.IncompleteBody.With("The request body is longer than its declared length")
			return 0, b.err
		}
		if b.sha != nil {
			b.sha.Write(p[:n])
		}
		if b.ck != nil {
			b.ck.Write(p[:n])
		}
	}
	if err == io.EOF {
		b.err = b.finish()
		if b.err == nil {
			b.err = io.EOF
		}
		return n, b.err
	}
	if err != nil {
		b.err = err
	}
	return n, err
}

// finish runs the end-of-stream checks.
func (b *bodyReader) finish() error {
	if b.size >= 0 && b.n != b.size {
		return s3err.IncompleteBody
	}
	if b.sha != nil && hex.EncodeToString(b.sha.Sum(nil)) != b.wantSHA {
		return s3err.XAmzContentSHA256Mismatch
	}
	if b.ck != nil {
		want := b.ckWant
		if b.ckFromTr {
			want = b.chunked.Trailers().Get(checksumHeader(b.ckAlgo))
			if want == "" {
				return s3err.InvalidRequest.With("The trailer %s announced in x-amz-trailer is missing", checksumHeader(b.ckAlgo))
			}
		}
		if got := sumBase64(b.ck); got != want {
			return s3err.BadDigest.With("The %s you specified did not match the calculated checksum.", b.ckAlgo)
		}
	}
	return nil
}

// checksum returns the verified x-amz-checksum, for storing with the
// object. Only valid after the stream ended without error.
func (b *bodyReader) checksum() store.Checksum {
	if b.ck == nil {
		return store.Checksum{}
	}
	return store.Checksum{Algorithm: b.ckAlgo, Value: sumBase64(b.ck), Type: "FULL_OBJECT"}
}

// precommit verifies Content-MD5 against the MD5 the store computed and
// hands back the verified checksum.
func (b *bodyReader) precommit(r *http.Request) func([]byte) (store.Checksum, error) {
	want := r.Header.Get("Content-MD5")
	return func(md5sum []byte) (store.Checksum, error) {
		if want != "" && base64.StdEncoding.EncodeToString(md5sum) != want {
			return store.Checksum{}, s3err.BadDigest
		}
		return b.checksum(), nil
	}
}

// checkContentMD5 validates the syntax of Content-MD5 before reading.
func checkContentMD5(r *http.Request) error {
	v := r.Header.Get("Content-MD5")
	if v == "" {
		if _, ok := r.Header["Content-Md5"]; ok {
			return s3err.InvalidDigest
		}
		return nil
	}
	b, err := base64.StdEncoding.DecodeString(v)
	if err != nil || len(b) != 16 {
		return s3err.InvalidDigest
	}
	return nil
}
