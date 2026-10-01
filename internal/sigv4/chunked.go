package sigv4

import (
	"bufio"
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"

	"github.com/useless-husband/strata/internal/s3err"
)

// MaxChunkSize bounds the size of one aws-chunked chunk. A chunk is buffered
// in full so its signature can be checked before any of it is released.
const MaxChunkSize = 16 << 20

const (
	maxLineLength = 4096
	maxTrailers   = 16
)

var errMalformedChunk = s3err.IncompleteBody.With("The aws-chunked request body is malformed")

// ChunkedReader decodes an aws-chunked request body. With signed chunks it
// verifies each chunk's signature (chained from the request's seed
// signature) before returning any of its bytes; with trailers it parses
// them, verifies the trailer signature if there is one, and makes them
// available through Trailers once Read has returned io.EOF.
type ChunkedReader struct {
	r        *bufio.Reader
	auth     *Auth
	signed   bool
	trailer  bool
	prevSig  string
	buf      []byte
	off      int
	err      error
	trailers http.Header
}

// NewChunkedReader returns a decoder for a request authenticated as a. The
// payload mode must be one of the streaming modes.
func NewChunkedReader(body io.Reader, a *Auth) *ChunkedReader {
	return &ChunkedReader{
		r:       bufio.NewReaderSize(body, 64<<10),
		auth:    a,
		signed:  a.Mode == PayloadStreaming || a.Mode == PayloadStreamingTrailer,
		trailer: a.Mode == PayloadStreamingTrailer || a.Mode == PayloadStreamingUnsignedTrailer,
		prevSig: a.Signature,
	}
}

// Trailers returns the trailing headers. It is only complete after Read
// has returned io.EOF.
func (c *ChunkedReader) Trailers() http.Header { return c.trailers }

func (c *ChunkedReader) Read(p []byte) (int, error) {
	for c.off == len(c.buf) {
		if c.err != nil {
			return 0, c.err
		}
		c.err = c.next()
		if c.err != nil && c.err != io.EOF {
			// Never release bytes from a chunk that failed to decode.
			c.buf, c.off = c.buf[:0], 0
		}
	}
	n := copy(p, c.buf[c.off:])
	c.off += n
	return n, nil
}

// readLine reads one CRLF-terminated line, without the CRLF.
func (c *ChunkedReader) readLine() (string, error) {
	var line []byte
	for {
		frag, err := c.r.ReadSlice('\n')
		line = append(line, frag...)
		if len(line) > maxLineLength {
			return "", errMalformedChunk
		}
		if err == nil {
			break
		}
		if errors.Is(err, bufio.ErrBufferFull) {
			continue
		}
		if errors.Is(err, io.EOF) {
			return "", s3err.IncompleteBody
		}
		return "", err
	}
	if !bytes.HasSuffix(line, []byte("\r\n")) {
		return "", errMalformedChunk
	}
	return string(line[:len(line)-2]), nil
}

func (c *ChunkedReader) next() error {
	header, err := c.readLine()
	if err != nil {
		return err
	}
	sizeStr, ext, _ := strings.Cut(header, ";")
	size, err := strconv.ParseInt(strings.TrimSpace(sizeStr), 16, 64)
	if err != nil || size < 0 {
		return errMalformedChunk
	}
	if size > MaxChunkSize {
		return s3err.InvalidArgument.With("aws-chunked chunk of %d bytes exceeds the limit of %d", size, MaxChunkSize)
	}
	var sig string
	if c.signed {
		name, val, ok := strings.Cut(ext, "=")
		if !ok || name != "chunk-signature" || len(val) != 64 {
			return errMalformedChunk
		}
		sig = val
	}
	if cap(c.buf) < int(size) {
		c.buf = make([]byte, size)
	}
	c.buf, c.off = c.buf[:size], 0
	if _, err := io.ReadFull(c.r, c.buf); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return s3err.IncompleteBody
		}
		return err
	}
	if c.signed {
		h := sha256.Sum256(c.buf)
		sts := chunkStringToSign(c.auth.Time, c.auth.Scope, c.prevSig, hex.EncodeToString(h[:]))
		want := hex.EncodeToString(hmacSHA256(c.auth.SigningKey, sts))
		if !hmac.Equal([]byte(want), []byte(sig)) {
			return s3err.SignatureDoesNotMatch.With("The chunk signature does not match")
		}
		c.prevSig = sig
	}
	if size > 0 {
		if err := c.expectCRLF(); err != nil {
			return err
		}
		return nil
	}
	// The final, empty chunk.
	if c.trailer {
		if err := c.readTrailers(); err != nil {
			return err
		}
		return io.EOF
	}
	if err := c.expectCRLF(); err != nil && !errors.Is(err, s3err.IncompleteBody) {
		return err // a missing final CRLF at end of stream is tolerated
	}
	return io.EOF
}

func (c *ChunkedReader) expectCRLF() error {
	var b [2]byte
	if _, err := io.ReadFull(c.r, b[:]); err != nil {
		return s3err.IncompleteBody
	}
	if b != [2]byte{'\r', '\n'} {
		return errMalformedChunk
	}
	return nil
}

func (c *ChunkedReader) readTrailers() error {
	c.trailers = http.Header{}
	var canonical []string
	var trailerSig string
	for i := 0; ; i++ {
		if i > maxTrailers+1 {
			return errMalformedChunk
		}
		line, err := c.readLine()
		if err != nil {
			// Some encoders omit the final blank line.
			if errors.Is(err, s3err.IncompleteBody) && i > 0 {
				break
			}
			return err
		}
		if line == "" {
			break
		}
		name, val, ok := strings.Cut(line, ":")
		if !ok {
			return errMalformedChunk
		}
		name = strings.ToLower(strings.TrimSpace(name))
		val = strings.TrimSpace(val)
		if name == "x-amz-trailer-signature" {
			trailerSig = val
			continue
		}
		c.trailers.Add(name, val)
		canonical = append(canonical, name+":"+val+"\n")
	}
	if !c.signed {
		return nil
	}
	if trailerSig == "" {
		return s3err.SignatureDoesNotMatch.With("The trailer signature is missing")
	}
	sort.Strings(canonical)
	h := sha256.Sum256([]byte(strings.Join(canonical, "")))
	sts := trailerStringToSign(c.auth.Time, c.auth.Scope, c.prevSig, hex.EncodeToString(h[:]))
	want := hex.EncodeToString(hmacSHA256(c.auth.SigningKey, sts))
	if !hmac.Equal([]byte(want), []byte(trailerSig)) {
		return s3err.SignatureDoesNotMatch.With("The trailer signature does not match")
	}
	return nil
}
