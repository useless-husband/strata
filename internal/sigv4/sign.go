package sigv4

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Signer signs requests with SigV4. strata uses it for its admin commands,
// load generator and tests; the interoperability tests use the official
// AWS clients instead.
type Signer struct {
	AccessKey, SecretKey string
	Region               string
	Service              string           // "s3" if empty
	Now                  func() time.Time // time.Now if nil
}

func (s *Signer) now() time.Time {
	if s.Now != nil {
		return s.Now().UTC()
	}
	return time.Now().UTC()
}

func (s *Signer) service() string {
	if s.Service == "" {
		return "s3"
	}
	return s.Service
}

// headersToSign lists the request's headers in canonical (lowercase,
// sorted) form, always including host. Like the AWS SDKs it leaves out
// headers that proxies may change.
func headersToSign(r *http.Request) []string {
	set := map[string]bool{"host": true}
	for name := range r.Header {
		l := strings.ToLower(name)
		switch l {
		case "authorization", "user-agent", "expect", "x-amzn-trace-id", "connection":
			continue
		}
		set[l] = true
	}
	out := make([]string, 0, len(set))
	for h := range set {
		out = append(out, h)
	}
	sort.Strings(out)
	return out
}

// Sign signs r in the Authorization header. payloadHash is the hex SHA-256
// of the body, UnsignedPayload, or a streaming mode. r.Host must be set.
func (s *Signer) Sign(r *http.Request, payloadHash string) {
	t := s.now()
	date := t.Format(shortFormat)
	r.Header.Set("X-Amz-Date", t.Format(timeFormat))
	r.Header.Set("X-Amz-Content-Sha256", payloadHash)
	if r.Host == "" {
		r.Host = r.URL.Host
	}
	signed := headersToSign(r)
	canonical, err := canonicalRequest(r, signed, payloadHash)
	if err != nil {
		panic(err) // every header in signed comes from the request
	}
	sc := scope(date, s.Region, s.service())
	key := SigningKey(s.SecretKey, date, s.Region, s.service())
	sig := hex.EncodeToString(hmacSHA256(key, stringToSign(t, sc, canonical)))
	r.Header.Set("Authorization", fmt.Sprintf("%s Credential=%s/%s, SignedHeaders=%s, Signature=%s",
		Algorithm, s.AccessKey, sc, strings.Join(signed, ";"), sig))
}

// SignBytes signs a request whose whole body is body and attaches it.
func (s *Signer) SignBytes(r *http.Request, body []byte) {
	h := sha256.Sum256(body)
	r.ContentLength = int64(len(body))
	if len(body) == 0 {
		// A non-nil empty body would be sent with chunked transfer
		// encoding, which S3 rejects for lack of a Content-Length.
		r.Body, r.GetBody = http.NoBody, nil
	} else {
		r.Body = io.NopCloser(bytes.NewReader(body))
		r.GetBody = func() (io.ReadCloser, error) { return io.NopCloser(bytes.NewReader(body)), nil }
	}
	s.Sign(r, hex.EncodeToString(h[:]))
}

// Presign returns a URL for r that is valid for the given duration. Only
// the host header is signed, as the AWS SDKs do by default.
func (s *Signer) Presign(r *http.Request, expires time.Duration) *url.URL {
	t := s.now()
	date := t.Format(shortFormat)
	sc := scope(date, s.Region, s.service())
	u := *r.URL
	q := u.Query()
	q.Set("X-Amz-Algorithm", Algorithm)
	q.Set("X-Amz-Credential", s.AccessKey+"/"+sc)
	q.Set("X-Amz-Date", t.Format(timeFormat))
	q.Set("X-Amz-Expires", strconv.Itoa(int(expires/time.Second)))
	q.Set("X-Amz-SignedHeaders", "host")
	u.RawQuery = encodeQuery(q)
	pr := &http.Request{Method: r.Method, URL: &u, Host: r.Host, Header: http.Header{}}
	if pr.Host == "" {
		pr.Host = u.Host
	}
	canonical, _ := canonicalRequest(pr, []string{"host"}, UnsignedPayload)
	key := SigningKey(s.SecretKey, date, s.Region, s.service())
	q.Set("X-Amz-Signature", hex.EncodeToString(hmacSHA256(key, stringToSign(t, sc, canonical))))
	u.RawQuery = encodeQuery(q)
	return &u
}

// encodeQuery encodes like url.Values.Encode but with %20 for spaces, which
// is what SigV4 canonicalisation produces.
func encodeQuery(q url.Values) string {
	keys := make([]string, 0, len(q))
	for k := range q {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	for _, k := range keys {
		for _, v := range q[k] {
			if b.Len() > 0 {
				b.WriteByte('&')
			}
			b.WriteString(URIEncode(k, false))
			b.WriteByte('=')
			b.WriteString(URIEncode(v, false))
		}
	}
	return b.String()
}

// ChunkedOptions configures SignChunked.
type ChunkedOptions struct {
	ChunkSize int // bytes of payload per chunk; 64 KiB if zero
	// Unsigned selects STREAMING-UNSIGNED-PAYLOAD-TRAILER instead of
	// signed chunks.
	Unsigned bool
	// Trailer, if set, is sent as trailing headers (for example
	// x-amz-checksum-crc32) after the last chunk.
	Trailer http.Header
}

// SignChunked signs r for an aws-chunked upload of body (size bytes) and
// replaces r.Body with the encoded stream.
func (s *Signer) SignChunked(r *http.Request, body io.Reader, size int64, opt ChunkedOptions) {
	if opt.ChunkSize <= 0 {
		opt.ChunkSize = 64 << 10
	}
	mode := StreamingPayload
	switch {
	case opt.Unsigned:
		mode = StreamingUnsignedTrailer
	case opt.Trailer != nil:
		mode = StreamingPayloadTrailer
	}
	var trailerNames []string
	for name := range opt.Trailer {
		trailerNames = append(trailerNames, strings.ToLower(name))
	}
	sort.Strings(trailerNames)
	if len(trailerNames) > 0 {
		r.Header.Set("X-Amz-Trailer", strings.Join(trailerNames, ","))
	}
	r.Header.Set("Content-Encoding", "aws-chunked")
	r.Header.Set("X-Amz-Decoded-Content-Length", strconv.FormatInt(size, 10))
	signedChunks := !opt.Unsigned
	r.ContentLength = encodedLength(size, opt.ChunkSize, signedChunks, opt.Trailer)
	r.Header.Set("Content-Length", strconv.FormatInt(r.ContentLength, 10))
	s.Sign(r, mode)

	t, _ := time.Parse(timeFormat, r.Header.Get("X-Amz-Date"))
	date := t.Format(shortFormat)
	enc := &chunkEncoder{
		src: body, chunk: opt.ChunkSize, signed: signedChunks, trailer: opt.Trailer,
		t: t, scope: scope(date, s.Region, s.service()),
		key:     SigningKey(s.SecretKey, date, s.Region, s.service()),
		prevSig: authSignature(r.Header.Get("Authorization")),
	}
	r.Body = io.NopCloser(enc)
	r.Header.Del("Content-Length") // net/http sends r.ContentLength itself
}

func authSignature(authz string) string {
	i := strings.LastIndex(authz, "Signature=")
	return authz[i+len("Signature="):]
}

func encodedLength(size int64, chunk int, signed bool, trailer http.Header) int64 {
	chunkLen := func(n int64) int64 {
		l := int64(len(strconv.FormatInt(n, 16))) + 2 + n + 2
		if signed {
			l += int64(len(";chunk-signature=")) + 64
		}
		return l
	}
	full := size / int64(chunk)
	total := full * chunkLen(int64(chunk))
	if rest := size % int64(chunk); rest > 0 {
		total += chunkLen(rest)
	}
	if trailer == nil {
		return total + chunkLen(0)
	}
	total += chunkLen(0) - 2 // the final chunk has no data CRLF before trailers
	for name, vs := range trailer {
		total += int64(len(name) + 1 + len(vs[0]) + 2)
	}
	if signed {
		total += int64(len("x-amz-trailer-signature:") + 64 + 2)
	}
	return total + 2
}

type chunkEncoder struct {
	src     io.Reader
	chunk   int
	signed  bool
	trailer http.Header
	t       time.Time
	scope   string
	key     []byte
	prevSig string
	out     bytes.Buffer
	done    bool
}

func (e *chunkEncoder) Read(p []byte) (int, error) {
	for e.out.Len() == 0 {
		if e.done {
			return 0, io.EOF
		}
		buf := make([]byte, e.chunk)
		n, err := io.ReadFull(e.src, buf)
		if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
			return 0, err
		}
		e.writeChunk(buf[:n])
		if n == 0 {
			e.writeTrailer()
			e.done = true
		}
	}
	return e.out.Read(p)
}

func (e *chunkEncoder) writeChunk(data []byte) {
	fmt.Fprintf(&e.out, "%x", len(data))
	if e.signed {
		h := sha256.Sum256(data)
		sig := hex.EncodeToString(hmacSHA256(e.key, chunkStringToSign(e.t, e.scope, e.prevSig, hex.EncodeToString(h[:]))))
		e.prevSig = sig
		e.out.WriteString(";chunk-signature=" + sig)
	}
	e.out.WriteString("\r\n")
	e.out.Write(data)
	if len(data) > 0 || e.trailer == nil {
		e.out.WriteString("\r\n")
	}
}

func (e *chunkEncoder) writeTrailer() {
	if e.trailer == nil {
		return
	}
	var lines []string
	for name, vs := range e.trailer {
		lines = append(lines, strings.ToLower(name)+":"+vs[0]+"\n")
	}
	sort.Strings(lines)
	for _, l := range lines {
		e.out.WriteString(strings.TrimSuffix(l, "\n") + "\r\n")
	}
	if e.signed {
		h := sha256.Sum256([]byte(strings.Join(lines, "")))
		sig := hex.EncodeToString(hmacSHA256(e.key, trailerStringToSign(e.t, e.scope, e.prevSig, hex.EncodeToString(h[:]))))
		e.out.WriteString("x-amz-trailer-signature:" + sig + "\r\n")
	}
	e.out.WriteString("\r\n")
}
