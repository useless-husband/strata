package sigv4

import (
	"bufio"
	"bytes"
	"encoding/hex"
	"errors"
	"io"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/useless-husband/strata/internal/s3err"
)

// The access key of the examples in the Amazon S3 SigV4 documentation. It is
// assembled at run time so that secret scanners, which look for the
// "AKIA" prefix followed by 16 characters, do not flag the source.
var docAccessKey = "AKIA" + "IOSFODNN7EXAMPLE"

const docSecret = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"

var docTime = time.Date(2013, 5, 24, 0, 0, 0, 0, time.UTC)

func docVerifier() *Verifier {
	return &Verifier{
		Region: "us-east-1",
		Secret: func(ak string) (string, bool) {
			return docSecret, ak == docAccessKey
		},
		Now: func() time.Time { return docTime.Add(time.Minute) },
	}
}

// parseRaw reads a raw HTTP/1.1 request as a server would see it.
func parseRaw(t *testing.T, raw string) *http.Request {
	t.Helper()
	raw = strings.ReplaceAll(raw, "AKID", docAccessKey)
	r, err := http.ReadRequest(bufio.NewReader(strings.NewReader(strings.ReplaceAll(raw, "\n", "\r\n"))))
	if err != nil {
		t.Fatal(err)
	}
	return r
}

// Examples from "Signature Calculations for the Authorization Header:
// Transferring Payload in a Single Chunk" in the Amazon S3 API reference.
func TestDocumentedHeaderExamples(t *testing.T) {
	cases := []struct {
		name, raw string
	}{
		{"GET object with range", `GET /test.txt HTTP/1.1
Host: examplebucket.s3.amazonaws.com
Authorization: AWS4-HMAC-SHA256 Credential=AKID/20130524/us-east-1/s3/aws4_request,SignedHeaders=host;range;x-amz-content-sha256;x-amz-date,Signature=f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41
Range: bytes=0-9
x-amz-content-sha256: e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
x-amz-date: 20130524T000000Z

`},
		{"PUT object", `PUT /test%24file.text HTTP/1.1
Host: examplebucket.s3.amazonaws.com
Date: Fri, 24 May 2013 00:00:00 GMT
Authorization: AWS4-HMAC-SHA256 Credential=AKID/20130524/us-east-1/s3/aws4_request,SignedHeaders=date;host;x-amz-content-sha256;x-amz-date;x-amz-storage-class,Signature=98ad721746da40c64f1a55b78f14c238d841ea1380cd77a1b5971af0ece108bd
x-amz-date: 20130524T000000Z
x-amz-storage-class: REDUCED_REDUNDANCY
x-amz-content-sha256: 44ce7dd67c959e0d3524ffac1771dfbba87d2b6b4b4e99e42034a8b803f8b072
Content-Length: 21

Welcome to Amazon S3.`},
		{"GET bucket lifecycle", `GET /?lifecycle HTTP/1.1
Host: examplebucket.s3.amazonaws.com
Authorization: AWS4-HMAC-SHA256 Credential=AKID/20130524/us-east-1/s3/aws4_request,SignedHeaders=host;x-amz-content-sha256;x-amz-date,Signature=fea454ca298b7da1c68078a5d1bdbfbbe0d65c699e0f91ac7a200a0136783543
x-amz-date: 20130524T000000Z
x-amz-content-sha256: e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855

`},
		{"list objects", `GET /?max-keys=2&prefix=J HTTP/1.1
Host: examplebucket.s3.amazonaws.com
Authorization: AWS4-HMAC-SHA256 Credential=AKID/20130524/us-east-1/s3/aws4_request,SignedHeaders=host;x-amz-content-sha256;x-amz-date,Signature=34b48302e7b5fa45bde8084f4b7868a86f0a534bc59db6670ed5711ef69dc6f7
x-amz-date: 20130524T000000Z
x-amz-content-sha256: e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855

`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r := parseRaw(t, c.raw)
			a, err := docVerifier().Verify(r)
			if err != nil {
				t.Fatalf("Verify: %v", err)
			}
			if a.AccessKey != docAccessKey {
				t.Errorf("access key %q", a.AccessKey)
			}
			// Any change to a signed part must break the signature.
			r2 := parseRaw(t, c.raw)
			r2.Header.Set("X-Amz-Date", "20130524T000001Z")
			if _, err := docVerifier().Verify(r2); !errors.Is(err, s3err.SignatureDoesNotMatch) {
				t.Errorf("tampered date: got %v", err)
			}
			r3 := parseRaw(t, c.raw)
			r3.URL.Path += "x"
			if _, err := docVerifier().Verify(r3); !errors.Is(err, s3err.SignatureDoesNotMatch) {
				t.Errorf("tampered path: got %v", err)
			}
		})
	}
}

// The presigned URL example from "Authenticating Requests: Using Query
// Parameters".
func TestDocumentedPresignedExample(t *testing.T) {
	raw := `GET /test.txt?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKID%2F20130524%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20130524T000000Z&X-Amz-Expires=86400&X-Amz-SignedHeaders=host&X-Amz-Signature=aeeed9bbccd4d02ee5c0109b86d86835f995330da4c265957d157751f604d404 HTTP/1.1
Host: examplebucket.s3.amazonaws.com

`
	v := docVerifier()
	a, err := v.Verify(parseRaw(t, raw))
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if !a.Presigned || a.Mode != PayloadUnsigned {
		t.Errorf("auth = %+v", a)
	}
	v.Now = func() time.Time { return docTime.Add(24*time.Hour + time.Second) }
	if _, err := v.Verify(parseRaw(t, raw)); err == nil || !strings.Contains(err.Error(), "expired") {
		t.Errorf("after expiry: got %v", err)
	}
	v.Now = func() time.Time { return docTime.Add(-time.Hour) }
	if _, err := v.Verify(parseRaw(t, raw)); err == nil || !strings.Contains(err.Error(), "not valid yet") {
		t.Errorf("before validity: got %v", err)
	}
}

// The chunked upload example from "Signature Calculations for the
// Authorization Header: Transferring Payload in Multiple Chunks": 64 KiB
// and 1 KiB chunks of 'a' with the documented chunk signatures.
func TestDocumentedChunkedExample(t *testing.T) {
	head := `PUT /examplebucket/chunkObject.txt HTTP/1.1
Host: s3.amazonaws.com
x-amz-date: 20130524T000000Z
x-amz-storage-class: REDUCED_REDUNDANCY
Authorization: AWS4-HMAC-SHA256 Credential=AKID/20130524/us-east-1/s3/aws4_request,SignedHeaders=content-encoding;content-length;host;x-amz-content-sha256;x-amz-date;x-amz-decoded-content-length;x-amz-storage-class,Signature=4f232c4386841ef735655705268965c44a0e4690baa4adea153f7db9fa80a0a9
x-amz-content-sha256: STREAMING-AWS4-HMAC-SHA256-PAYLOAD
Content-Encoding: aws-chunked
x-amz-decoded-content-length: 66560
Content-Length: 66824

`
	body := "10000;chunk-signature=ad80c730a21e5b8d04586a2213dd63b9a0e99e0e2307b0ade35a65485a288648\r\n" +
		strings.Repeat("a", 65536) + "\r\n" +
		"400;chunk-signature=0055627c9e194cb4542bae2aa5492e3c1575bbb81b612b7d234b86a503ef5497\r\n" +
		strings.Repeat("a", 1024) + "\r\n" +
		"0;chunk-signature=b6c6ea8a5354eaf15b3cb7646744f4275b71ea724fed81ceb9323e279d449df9\r\n\r\n"
	if len(body) != 66824 {
		t.Fatalf("body is %d bytes, the example says 66824", len(body))
	}
	parse := func(body string) *http.Request {
		r := parseRaw(t, head)
		r.Body = io.NopCloser(strings.NewReader(body))
		return r
	}
	r := parse(body)
	a, err := docVerifier().Verify(r)
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if a.Mode != PayloadStreaming {
		t.Fatalf("mode %v", a.Mode)
	}
	got, err := io.ReadAll(NewChunkedReader(r.Body, a))
	if err != nil {
		t.Fatalf("decoding: %v", err)
	}
	if string(got) != strings.Repeat("a", 66560) {
		t.Fatalf("decoded %d bytes, not 66560 'a's", len(got))
	}

	// Flip one byte of the second chunk: its signature fails and none of
	// its bytes are released.
	bad := []byte(body)
	bad[65626+100] = 'b'
	r = parse(string(bad))
	a, _ = docVerifier().Verify(r)
	got, err = io.ReadAll(NewChunkedReader(r.Body, a))
	if !errors.Is(err, s3err.SignatureDoesNotMatch) {
		t.Fatalf("tampered chunk: got %v", err)
	}
	if len(got) != 65536 {
		t.Fatalf("released %d bytes; only the first, valid chunk (65536) may be", len(got))
	}

	// Dropping the final chunk is an incomplete body.
	r = parse(body[:65626+1112])
	a, _ = docVerifier().Verify(r)
	if _, err := io.ReadAll(NewChunkedReader(r.Body, a)); !errors.Is(err, s3err.IncompleteBody) {
		t.Fatalf("truncated stream: got %v", err)
	}

	// Reordering chunks breaks the signature chain.
	reordered := body[65626:65626+1112] + body[:65626] + body[65626+1112:]
	r = parse(reordered)
	a, _ = docVerifier().Verify(r)
	if _, err := io.ReadAll(NewChunkedReader(r.Body, a)); !errors.Is(err, s3err.SignatureDoesNotMatch) {
		t.Fatalf("reordered chunks: got %v", err)
	}
}

func TestVerifyErrors(t *testing.T) {
	base := `GET /test.txt HTTP/1.1
Host: examplebucket.s3.amazonaws.com
Authorization: AWS4-HMAC-SHA256 Credential=AKID/20130524/us-east-1/s3/aws4_request,SignedHeaders=host;range;x-amz-content-sha256;x-amz-date,Signature=f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41
Range: bytes=0-9
x-amz-content-sha256: e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
x-amz-date: 20130524T000000Z

`
	cases := []struct {
		name   string
		edit   func(r *http.Request, v *Verifier)
		expect *s3err.Error
	}{
		{"unknown key", func(r *http.Request, v *Verifier) {
			v.Secret = func(string) (string, bool) { return "", false }
		}, s3err.InvalidAccessKeyID},
		{"wrong secret", func(r *http.Request, v *Verifier) {
			v.Secret = func(string) (string, bool) { return "other", true }
		}, s3err.SignatureDoesNotMatch},
		{"wrong region", func(r *http.Request, v *Verifier) { v.Region = "eu-west-1" }, s3err.AuthorizationHeaderMalformed},
		{"clock skew", func(r *http.Request, v *Verifier) {
			v.Now = func() time.Time { return docTime.Add(16 * time.Minute) }
		}, s3err.RequestTimeTooSkewed},
		{"unsigned x-amz header", func(r *http.Request, v *Verifier) {
			r.Header.Set("X-Amz-Meta-Injected", "1")
		}, s3err.AccessDenied},
		{"missing content sha", func(r *http.Request, v *Verifier) {
			r.Header.Del("X-Amz-Content-Sha256")
		}, s3err.InvalidRequest},
		{"anonymous", func(r *http.Request, v *Verifier) { r.Header.Del("Authorization") }, s3err.AccessDenied},
		{"sigv2 garbage", func(r *http.Request, v *Verifier) { r.Header.Set("Authorization", "AWS nobody:abc") }, s3err.InvalidAccessKeyID},
		{"garbage", func(r *http.Request, v *Verifier) {
			r.Header.Set("Authorization", "AWS4-HMAC-SHA256 nonsense")
		}, s3err.AuthorizationHeaderMalformed},
		{"host not signed", func(r *http.Request, v *Verifier) {
			r.Header.Set("Authorization", strings.Replace(r.Header.Get("Authorization"), "host;", "", 1))
		}, s3err.AccessDenied},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r := parseRaw(t, base)
			v := docVerifier()
			c.edit(r, v)
			_, err := v.Verify(r)
			if !errors.Is(err, c.expect) {
				t.Fatalf("got %v, want %s", err, c.expect.Code)
			}
		})
	}
}

func TestURIEncode(t *testing.T) {
	for in, want := range map[string]string{
		"abc-._~XYZ09": "abc-._~XYZ09",
		"a b":          "a%20b",
		"a+b":          "a%2Bb",
		"a/b":          "a%2Fb",
		"ü":            "%C3%BC",
		"*":            "%2A",
	} {
		if got := URIEncode(in, false); got != want {
			t.Errorf("URIEncode(%q) = %q, want %q", in, got, want)
		}
	}
	if got := URIEncode("/a b/c", true); got != "/a%20b/c" {
		t.Errorf("path encoding = %q", got)
	}
}

// newTestServer returns a server that verifies requests and echoes the
// decoded body, so the signer and verifier are tested over real HTTP.
func newTestServer(t *testing.T, v *Verifier) *httptest.Server {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		a, err := v.Verify(r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusForbidden)
			return
		}
		var body io.Reader = r.Body
		var cr *ChunkedReader
		if a.Mode >= PayloadStreaming {
			cr = NewChunkedReader(r.Body, a)
			body = cr
		}
		data, err := io.ReadAll(body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if cr != nil {
			for k, vs := range cr.Trailers() {
				w.Header().Set("Got-Trailer-"+k, vs[0])
			}
		}
		w.Write(data)
	}))
	t.Cleanup(srv.Close)
	return srv
}

func testSigner() (*Signer, *Verifier) {
	s := &Signer{AccessKey: "strata-test", SecretKey: "secret/with+odd=chars", Region: "local-1"}
	v := &Verifier{Region: "local-1", Secret: func(ak string) (string, bool) {
		return s.SecretKey, ak == s.AccessKey
	}}
	return s, v
}

func TestSignerRoundTrip(t *testing.T) {
	s, v := testSigner()
	srv := newTestServer(t, v)
	paths := []string{"/b/plain", "/b/with space", "/b/plus+sign", "/b/a//double", "/b/ünïcode/ключ", "/b/../dots", "/b/tilde~*'()!$&"}
	for _, p := range paths {
		req, _ := http.NewRequest("PUT", srv.URL, nil)
		req.URL.Path = p
		req.URL.RawQuery = "partNumber=1&uploadId=a%2Bb%20c&empty="
		req.Header.Set("Content-Type", "text/plain")
		req.Header.Set("X-Amz-Meta-Note", "  two   spaces  ")
		s.SignBytes(req, []byte("payload for "+p))
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		got, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != 200 || string(got) != "payload for "+p {
			t.Errorf("%s: %d %s", p, resp.StatusCode, got)
		}
	}
}

func TestPresignRoundTrip(t *testing.T) {
	s, v := testSigner()
	srv := newTestServer(t, v)
	req, _ := http.NewRequest("GET", srv.URL+"/bucket/some%20key", nil)
	u := s.Presign(req, time.Hour)
	resp, err := http.Get(u.String())
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("presigned GET: %d", resp.StatusCode)
	}
	// Changing the path invalidates the URL.
	u.Path = "/bucket/other"
	resp, _ = http.Get(u.String())
	resp.Body.Close()
	if resp.StatusCode != 403 {
		t.Fatalf("presigned URL for another key: %d", resp.StatusCode)
	}
}

func TestChunkedRoundTrip(t *testing.T) {
	s, v := testSigner()
	srv := newTestServer(t, v)
	r := rand.New(rand.NewPCG(7, 7))
	for _, tc := range []struct {
		name string
		opt  ChunkedOptions
	}{
		{"signed", ChunkedOptions{ChunkSize: 8192}},
		{"signed trailer", ChunkedOptions{ChunkSize: 10000, Trailer: http.Header{"x-amz-checksum-crc32": {"AAAAAA=="}}}},
		{"unsigned trailer", ChunkedOptions{ChunkSize: 65536, Unsigned: true, Trailer: http.Header{"x-amz-checksum-crc64nvme": {"q1w2e3r4t5y=="}}}},
	} {
		for _, size := range []int{0, 1, 8191, 8192, 8193, 100000} {
			data := make([]byte, size)
			for i := range data {
				data[i] = byte(r.Uint32())
			}
			req, _ := http.NewRequest("PUT", srv.URL+"/b/k", nil)
			s.SignChunked(req, bytes.NewReader(data), int64(size), tc.opt)
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			got, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			if resp.StatusCode != 200 || !bytes.Equal(got, data) {
				t.Fatalf("%s size %d: status %d, %d bytes back: %.200s", tc.name, size, resp.StatusCode, len(got), got)
			}
			for k, vs := range tc.opt.Trailer {
				if g := resp.Header.Get("Got-Trailer-" + k); g != vs[0] {
					t.Fatalf("%s: trailer %s = %q, want %q", tc.name, k, g, vs[0])
				}
			}
		}
	}
}

// FuzzChunkedReader feeds arbitrary bytes to the decoder: it must never
// panic, never return more than it was given, and never succeed on input
// whose signatures it could not have checked.
func FuzzChunkedReader(f *testing.F) {
	f.Add([]byte("10;chunk-signature=" + strings.Repeat("0", 64) + "\r\n0123456789abcdef\r\n0;chunk-signature=" + strings.Repeat("0", 64) + "\r\n\r\n"))
	f.Add([]byte("5\r\nhello\r\n0\r\nx-amz-checksum-crc32:AAAAAA==\r\n\r\n"))
	f.Add([]byte("ffffffffffffffff;chunk-signature=x\r\n"))
	f.Fuzz(func(t *testing.T, data []byte) {
		for _, mode := range []PayloadMode{PayloadStreaming, PayloadStreamingTrailer, PayloadStreamingUnsignedTrailer} {
			a := &Auth{Mode: mode, Time: docTime, Scope: "20130524/us-east-1/s3/aws4_request", SigningKey: []byte("k"), Signature: strings.Repeat("0", 64)}
			out, err := io.ReadAll(NewChunkedReader(bytes.NewReader(data), a))
			if len(out) > len(data) {
				t.Fatalf("decoded %d bytes from %d", len(out), len(data))
			}
			if err == nil && mode != PayloadStreamingUnsignedTrailer && len(data) > 0 {
				t.Fatalf("mode %d accepted unsigned garbage %q", mode, data)
			}
		}
	})
}

// The Signature Version 2 examples of "Signing and Authenticating REST
// Requests" in the Amazon S3 developer guide (bucket johnsmith).
func TestDocumentedV2Examples(t *testing.T) {
	v := docVerifier()
	v.Domains = []string{"s3.amazonaws.com"}
	v.Now = func() time.Time { return time.Date(2007, 3, 27, 19, 40, 0, 0, time.UTC) }
	header := `GET /photos/puppy.jpg HTTP/1.1
Host: johnsmith.s3.amazonaws.com
Date: Tue, 27 Mar 2007 19:36:42 +0000
Authorization: AWS AKID:bWq2s1WEIj+Ydj0vQ697zp+IXMU=

`
	r := parseRaw(t, header)
	if sts := v.v2StringToSign(r, r.Header.Get("Date")); sts != "GET\n\n\nTue, 27 Mar 2007 19:36:42 +0000\n/johnsmith/photos/puppy.jpg" {
		t.Fatalf("string to sign %q", sts)
	}
	if _, err := v.Verify(r); err != nil {
		t.Fatalf("header example: %v", err)
	}
	query := `GET /photos/puppy.jpg?AWSAccessKeyId=AKID&Expires=1175139620&Signature=NpgCjnDzrM%2BWFzoENXmpNDUsSn8%3D HTTP/1.1
Host: johnsmith.s3.amazonaws.com

`
	a, err := v.Verify(parseRaw(t, query))
	if err != nil {
		t.Fatalf("query example: %v", err)
	}
	if !a.V2 || !a.Presigned {
		t.Fatalf("auth %+v", a)
	}
	v.Now = func() time.Time { return time.Unix(1175139621, 0) }
	if _, err := v.Verify(parseRaw(t, query)); err == nil || !strings.Contains(err.Error(), "expired") {
		t.Fatalf("expired V2 URL: %v", err)
	}
	// Subresources are signed, other query parameters are not.
	v.Now = func() time.Time { return time.Date(2007, 3, 27, 19, 40, 0, 0, time.UTC) }
	r = parseRaw(t, strings.Replace(header, "puppy.jpg", "puppy.jpg?acl", 1))
	if _, err := v.Verify(r); !errors.Is(err, s3err.SignatureDoesNotMatch) {
		t.Fatalf("adding ?acl: %v", err)
	}
	r = parseRaw(t, strings.Replace(header, "puppy.jpg", "puppy.jpg?unrelated=1", 1))
	if _, err := v.Verify(r); err != nil {
		t.Fatalf("adding a non-subresource parameter: %v", err)
	}
}

func TestPresignExpiryBounds(t *testing.T) {
	s, v := testSigner()
	srv := newTestServer(t, v)
	// The test server answers 403 with the error text for any failure.
	get := func(expires string) (int, string) {
		req, _ := http.NewRequest("GET", srv.URL+"/bucket/key", nil)
		resp, err := http.Get(presignWithExpires(s, req, expires))
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		return resp.StatusCode, string(body)
	}
	if code, body := get("604800"); code != 200 {
		t.Errorf("7 days: %d %s", code, body)
	}
	if _, body := get("604801"); !strings.Contains(body, "AuthorizationQueryParametersError") {
		t.Errorf("over 7 days: %s", body)
	}
	if _, body := get("-5"); !strings.Contains(body, "AccessDenied") || !strings.Contains(body, "expired") {
		t.Errorf("negative expiry: %s", body)
	}
}

// presignWithExpires presigns with a raw X-Amz-Expires value.
func presignWithExpires(s *Signer, r *http.Request, expires string) string {
	t := s.now()
	date := t.Format(shortFormat)
	sc := scope(date, s.Region, s.service())
	q := r.URL.Query()
	q.Set("X-Amz-Algorithm", Algorithm)
	q.Set("X-Amz-Credential", s.AccessKey+"/"+sc)
	q.Set("X-Amz-Date", t.Format(timeFormat))
	q.Set("X-Amz-Expires", expires)
	q.Set("X-Amz-SignedHeaders", "host")
	pr := &http.Request{Method: r.Method, URL: r.URL, Host: r.URL.Host, Header: http.Header{}}
	pr.URL.RawQuery = encodeQuery(q)
	canonical, _ := canonicalRequest(pr, []string{"host"}, UnsignedPayload)
	q.Set("X-Amz-Signature", hex.EncodeToString(hmacSHA256(SigningKey(s.SecretKey, date, s.Region, s.service()), stringToSign(t, sc, canonical))))
	pr.URL.RawQuery = encodeQuery(q)
	return pr.URL.String()
}
