package s3api

import (
	"bytes"
	"crypto/md5"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"hash/crc32"
	"io"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/useless-husband/strata/internal/checksum"
	"github.com/useless-husband/strata/internal/sigv4"
	"github.com/useless-husband/strata/internal/store"
)

const (
	testAccess = "test-access"
	testSecret = "test-secret-key"
	testRegion = "us-east-1"
)

type env struct {
	t      testing.TB
	srv    *httptest.Server
	st     *store.Store
	api    *Server
	signer *sigv4.Signer
	cfg    store.Config
}

func newEnv(t testing.TB, k, m int) *env {
	t.Helper()
	root := t.TempDir()
	var disks []string
	for i := 0; i < k+m; i++ {
		disks = append(disks, filepath.Join(root, fmt.Sprintf("d%d", i)))
	}
	cfg := store.Config{Disks: disks, DataShards: k, ParityShards: m, BlockSize: 16 << 10, Sync: store.SyncNone, DiskCheckInterval: -1}
	e := &env{t: t, cfg: cfg, signer: &sigv4.Signer{AccessKey: testAccess, SecretKey: testSecret, Region: testRegion}}
	e.start()
	t.Cleanup(e.stop)
	return e
}

func (e *env) start() {
	st, err := store.Open(e.cfg)
	if err != nil {
		e.t.Fatal(err)
	}
	e.st = st
	e.api = New(Config{Store: st, Region: testRegion, Credentials: map[string]string{testAccess: testSecret}, Version: "test"})
	e.srv = httptest.NewServer(e.api)
}

func (e *env) stop() {
	if e.srv != nil {
		e.srv.Close()
		e.st.Close()
		e.srv = nil
	}
}

// restart stops the server and reopens the store from disk.
func (e *env) restart() {
	e.stop()
	e.start()
}

type response struct {
	*http.Response
	body []byte
}

func (r response) code() string {
	var er errorResponse
	if xml.Unmarshal(r.body, &er) == nil {
		return er.Code
	}
	return ""
}

// do sends a signed request. path may include a query string.
func (e *env) do(method, path string, body []byte, headers ...string) response {
	e.t.Helper()
	req := e.newRequest(method, path, headers...)
	e.signer.SignBytes(req, body)
	return e.send(req)
}

func (e *env) newRequest(method, path string, headers ...string) *http.Request {
	u, err := url.Parse(e.srv.URL + path)
	if err != nil {
		e.t.Fatal(err)
	}
	req := &http.Request{Method: method, URL: u, Host: u.Host, Header: http.Header{}}
	for i := 0; i+1 < len(headers); i += 2 {
		req.Header.Set(headers[i], headers[i+1])
	}
	return req
}

func (e *env) send(req *http.Request) response {
	e.t.Helper()
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		e.t.Fatalf("%s %s: %v", req.Method, req.URL, err)
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	return response{resp, b}
}

func (e *env) expect(r response, status int, code string) {
	e.t.Helper()
	if r.StatusCode != status || (code != "" && r.code() != code) {
		e.t.Fatalf("%s %s: got %d %s, want %d %s\n%s", r.Request.Method, r.Request.URL.RequestURI(), r.StatusCode, r.code(), status, code, r.body)
	}
}

func b64md5(b []byte) string {
	h := md5.Sum(b)
	return base64.StdEncoding.EncodeToString(h[:])
}

func hexmd5(b []byte) string {
	h := md5.Sum(b)
	return hex.EncodeToString(h[:])
}

func TestBucketsAPI(t *testing.T) {
	e := newEnv(t, 2, 1)
	e.expect(e.do("GET", "/", nil), 200, "")
	e.expect(e.do("PUT", "/Bad_Name", nil), 400, "InvalidBucketName")
	e.expect(e.do("PUT", "/alpha", nil), 200, "")
	e.do("PUT", "/alpha/kept", []byte("k"))
	// In us-east-1 re-creating a bucket you own succeeds and changes nothing.
	e.expect(e.do("PUT", "/alpha", nil), 200, "")
	e.expect(e.do("GET", "/alpha/kept", nil), 200, "")
	e.expect(e.do("DELETE", "/alpha/kept", nil), 204, "")
	e.expect(e.do("HEAD", "/alpha", nil), 200, "")
	e.expect(e.do("HEAD", "/missing", nil), 404, "")
	e.expect(e.do("GET", "/missing?list-type=2", nil), 404, "NoSuchBucket")
	r := e.do("GET", "/alpha?location", nil)
	e.expect(r, 200, "")
	if !strings.Contains(string(r.body), "<LocationConstraint") {
		t.Fatalf("location: %s", r.body)
	}
	cfg := `<CreateBucketConfiguration><LocationConstraint>eu-west-1</LocationConstraint></CreateBucketConfiguration>`
	e.expect(e.do("PUT", "/beta", []byte(cfg)), 400, "InvalidArgument")
	r = e.do("GET", "/", nil)
	var lb listAllMyBucketsResult
	xml.Unmarshal(r.body, &lb)
	if len(lb.Buckets) != 1 || lb.Buckets[0].Name != "alpha" {
		t.Fatalf("ListBuckets: %s", r.body)
	}
	e.expect(e.do("PUT", "/alpha/obj", []byte("x")), 200, "")
	e.expect(e.do("DELETE", "/alpha", nil), 409, "BucketNotEmpty")
	e.expect(e.do("DELETE", "/alpha/obj", nil), 204, "")
	e.expect(e.do("DELETE", "/alpha", nil), 204, "")
	e.expect(e.do("DELETE", "/alpha", nil), 404, "NoSuchBucket")
	e.expect(e.do("GET", "/alpha?versioning", nil), 404, "NoSuchBucket")
	e.expect(e.do("PUT", "/gamma", nil), 200, "")
	e.expect(e.do("GET", "/gamma?lifecycle", nil), 501, "NotImplemented")
	e.expect(e.do("PUT", "/gamma?versioning", []byte("<VersioningConfiguration/>")), 501, "NotImplemented")
	e.expect(e.do("PUT", "/gamma", nil, "X-Amz-Bucket-Object-Lock-Enabled", "true"), 501, "NotImplemented")
}

func TestObjectAPI(t *testing.T) {
	e := newEnv(t, 2, 1)
	e.do("PUT", "/bkt", nil)
	data := bytes.Repeat([]byte("0123456789"), 5000)
	r := e.do("PUT", "/bkt/dir/file.txt", data,
		"Content-Type", "text/plain", "X-Amz-Meta-Color", "blue", "X-Amz-Meta-Empty", "",
		"Cache-Control", "max-age=60", "Content-Disposition", `attachment; filename="f.txt"`,
		"Content-MD5", b64md5(data))
	e.expect(r, 200, "")
	if r.Header.Get("ETag") != `"`+hexmd5(data)+`"` {
		t.Fatalf("ETag %s", r.Header.Get("ETag"))
	}
	r = e.do("GET", "/bkt/dir/file.txt", nil)
	e.expect(r, 200, "")
	if !bytes.Equal(r.body, data) || r.Header.Get("Content-Type") != "text/plain" || r.Header.Get("X-Amz-Meta-Color") != "blue" ||
		r.Header.Get("Cache-Control") != "max-age=60" || r.Header.Get("Content-Disposition") != `attachment; filename="f.txt"` ||
		r.Header.Get("Content-Length") != strconv.Itoa(len(data)) || r.Header.Get("Last-Modified") == "" {
		t.Fatalf("GET headers %v", r.Header)
	}
	r = e.do("HEAD", "/bkt/dir/file.txt", nil)
	e.expect(r, 200, "")
	if len(r.body) != 0 || r.Header.Get("Content-Length") != strconv.Itoa(len(data)) {
		t.Fatalf("HEAD: %v %d", r.Header, len(r.body))
	}
	// Default content type and the response-* overrides of a GET.
	e.do("PUT", "/bkt/noct", []byte("x"))
	r = e.do("GET", "/bkt/noct?response-content-type=image%2Fpng&response-cache-control=no-cache", nil)
	if r.Header.Get("Content-Type") != "image/png" || r.Header.Get("Cache-Control") != "no-cache" {
		t.Fatalf("overrides: %v", r.Header)
	}
	r = e.do("HEAD", "/bkt/noct", nil)
	if r.Header.Get("Content-Type") != "binary/octet-stream" {
		t.Fatalf("default content type %q", r.Header.Get("Content-Type"))
	}

	e.expect(e.do("GET", "/bkt/missing", nil), 404, "NoSuchKey")
	e.expect(e.do("HEAD", "/bkt/missing", nil), 404, "")
	e.expect(e.do("DELETE", "/bkt/missing", nil), 204, "")
	e.expect(e.do("PUT", "/bkt/k", data, "Content-MD5", b64md5([]byte("other"))), 400, "BadDigest")
	e.expect(e.do("PUT", "/bkt/k", data, "Content-MD5", "notbase64!"), 400, "InvalidDigest")
	e.expect(e.do("GET", "/bkt/k", nil), 404, "NoSuchKey")
	e.expect(e.do("PUT", "/bkt/"+strings.Repeat("k", 1025), nil), 400, "KeyTooLongError")
	e.expect(e.do("PUT", "/bkt/big-meta", nil, "X-Amz-Meta-A", strings.Repeat("v", 2100)), 400, "MetadataTooLarge")
	e.expect(e.do("PUT", "/bkt/sse", nil, "X-Amz-Server-Side-Encryption", "AES256"), 501, "NotImplemented")
	e.expect(e.do("PUT", "/bkt/class", nil, "X-Amz-Storage-Class", "NONSENSE"), 400, "InvalidStorageClass")
	e.expect(e.do("PUT", "/bkt/acl", nil, "X-Amz-Acl", "public-read"), 501, "NotImplemented")
	e.expect(e.do("GET", "/bkt/dir/file.txt?versionId=abc", nil), 400, "InvalidArgument")

	// Keys that stress path handling.
	for _, key := range []string{"a b", "a+b", "ü/ключ", "a//b", "../up", "trailing/", "q?x=1", "%41", "*"} {
		path := "/bkt/" + sigv4.URIEncode(key, true)
		e.expect(e.do("PUT", path, []byte(key)), 200, "")
		r := e.do("GET", path, nil)
		e.expect(r, 200, "")
		if string(r.body) != key {
			t.Fatalf("key %q came back as %q", key, r.body)
		}
	}
}

func TestPayloadVerification(t *testing.T) {
	e := newEnv(t, 2, 1)
	e.do("PUT", "/bkt", nil)
	data := []byte("the payload")
	// A signed payload hash that does not match the body.
	req := e.newRequest("PUT", "/bkt/k")
	req.Body = io.NopCloser(bytes.NewReader(data))
	req.ContentLength = int64(len(data))
	e.signer.Sign(req, sigv4.EmptySHA256)
	e.expect(e.send(req), 400, "XAmzContentSHA256Mismatch")
	e.expect(e.do("GET", "/bkt/k", nil), 404, "NoSuchKey")
	// UNSIGNED-PAYLOAD is accepted.
	req = e.newRequest("PUT", "/bkt/k")
	req.Body = io.NopCloser(bytes.NewReader(data))
	req.ContentLength = int64(len(data))
	e.signer.Sign(req, sigv4.UnsignedPayload)
	e.expect(e.send(req), 200, "")

	// x-amz-checksum-* headers are verified and returned.
	crc := crc32.ChecksumIEEE(data)
	good := base64.StdEncoding.EncodeToString([]byte{byte(crc >> 24), byte(crc >> 16), byte(crc >> 8), byte(crc)})
	r := e.do("PUT", "/bkt/ck", data, "X-Amz-Checksum-Crc32", good)
	e.expect(r, 200, "")
	if r.Header.Get("X-Amz-Checksum-Crc32") != good {
		t.Fatalf("checksum not echoed: %v", r.Header)
	}
	e.expect(e.do("PUT", "/bkt/ck2", data, "X-Amz-Checksum-Crc32", "AAAAAA=="), 400, "BadDigest")
	e.expect(e.do("GET", "/bkt/ck2", nil), 404, "NoSuchKey")
	r = e.do("HEAD", "/bkt/ck", nil, "X-Amz-Checksum-Mode", "ENABLED")
	if r.Header.Get("X-Amz-Checksum-Crc32") != good {
		t.Fatalf("checksum not returned with checksum mode: %v", r.Header)
	}
	if r = e.do("HEAD", "/bkt/ck", nil); r.Header.Get("X-Amz-Checksum-Crc32") != "" {
		t.Fatal("checksum returned without checksum mode")
	}
}

func TestCRC64NVMECheckValue(t *testing.T) {
	// The check value of CRC-64/NVME from the CRC catalogue.
	h := checksum.New("CRC64NVME")
	h.Write([]byte("123456789"))
	if got := binary.BigEndian.Uint64(h.Sum(nil)); got != 0xae8b14860a799888 {
		t.Fatalf("CRC-64/NVME(123456789) = %x", got)
	}
}

func TestChunkedUploads(t *testing.T) {
	e := newEnv(t, 2, 1)
	e.do("PUT", "/bkt", nil)
	r := rand.New(rand.NewPCG(1, 1))
	data := make([]byte, 200_000)
	for i := range data {
		data[i] = byte(r.Uint32())
	}
	h := checksum.New("CRC64NVME")
	h.Write(data)
	crc := base64.StdEncoding.EncodeToString(h.Sum(nil))
	for _, tc := range []struct {
		name    string
		opt     sigv4.ChunkedOptions
		status  int
		errCode string
	}{
		{"signed", sigv4.ChunkedOptions{ChunkSize: 65536}, 200, ""},
		{"signed trailer", sigv4.ChunkedOptions{ChunkSize: 8192, Trailer: http.Header{"x-amz-checksum-crc64nvme": {crc}}}, 200, ""},
		{"unsigned trailer", sigv4.ChunkedOptions{ChunkSize: 65536, Unsigned: true, Trailer: http.Header{"x-amz-checksum-crc64nvme": {crc}}}, 200, ""},
		{"wrong trailer checksum", sigv4.ChunkedOptions{ChunkSize: 65536, Unsigned: true, Trailer: http.Header{"x-amz-checksum-crc64nvme": {"AAAAAAAAAAA="}}}, 400, "BadDigest"},
	} {
		key := "/bkt/" + strings.ReplaceAll(tc.name, " ", "-")
		req := e.newRequest("PUT", key)
		e.signer.SignChunked(req, bytes.NewReader(data), int64(len(data)), tc.opt)
		e.expect(e.send(req), tc.status, tc.errCode)
		got := e.do("GET", key, nil)
		if tc.status == 200 && !bytes.Equal(got.body, data) {
			t.Fatalf("%s: stored body differs", tc.name)
		}
		if tc.status != 200 && got.StatusCode != 404 {
			t.Fatalf("%s: failed upload left an object behind", tc.name)
		}
	}
	// A chunk whose signature is wrong: the upload fails and nothing is
	// stored.
	req := e.newRequest("PUT", "/bkt/tampered")
	e.signer.SignChunked(req, bytes.NewReader(data), int64(len(data)), sigv4.ChunkedOptions{ChunkSize: 65536})
	enc, _ := io.ReadAll(req.Body)
	enc[70000] ^= 1
	req.Body = io.NopCloser(bytes.NewReader(enc))
	e.expect(e.send(req), 403, "SignatureDoesNotMatch")
	e.expect(e.do("GET", "/bkt/tampered", nil), 404, "NoSuchKey")
	// A declared length that does not match the data.
	req = e.newRequest("PUT", "/bkt/short")
	e.signer.SignChunked(req, bytes.NewReader(data), int64(len(data)), sigv4.ChunkedOptions{ChunkSize: 65536})
	req.Header.Set("X-Amz-Decoded-Content-Length", "10")
	e.signer.Sign(req, sigv4.StreamingPayload) // re-sign with the wrong length
	resp := e.send(req)
	if resp.StatusCode == 200 {
		t.Fatal("a body longer than x-amz-decoded-content-length was accepted")
	}
}

func TestRangeRequests(t *testing.T) {
	e := newEnv(t, 3, 2)
	e.do("PUT", "/bkt", nil)
	data := make([]byte, 100_000)
	for i := range data {
		data[i] = byte(i * 7)
	}
	e.do("PUT", "/bkt/k", data)
	e.do("PUT", "/bkt/empty", nil)
	cases := []struct {
		rng        string
		status     int
		start, end int // inclusive; -1 for the whole object
	}{
		{"bytes=0-0", 206, 0, 0},
		{"bytes=0-99", 206, 0, 99},
		{"bytes=99990-", 206, 99990, 99999},
		{"bytes=-10", 206, 99990, 99999},
		{"bytes=50000-200000", 206, 50000, 99999},
		{"bytes=-200000", 206, 0, 99999},
		{"bytes=16383-16385", 206, 16383, 16385},
		{"bytes=100000-", 416, 0, 0},
		{"bytes=-0", 416, 0, 0},
		{"bytes=5-1", 200, -1, -1},     // invalid: ignored
		{"bytes=0-1,5-6", 200, -1, -1}, // multiple ranges: ignored
		{"items=0-1", 200, -1, -1},
	}
	for _, c := range cases {
		r := e.do("GET", "/bkt/k", nil, "Range", c.rng)
		if r.StatusCode != c.status {
			t.Fatalf("%s: status %d, want %d", c.rng, r.StatusCode, c.status)
		}
		switch c.status {
		case 206:
			if !bytes.Equal(r.body, data[c.start:c.end+1]) {
				t.Fatalf("%s: wrong bytes", c.rng)
			}
			if want := fmt.Sprintf("bytes %d-%d/%d", c.start, c.end, len(data)); r.Header.Get("Content-Range") != want {
				t.Fatalf("%s: Content-Range %q, want %q", c.rng, r.Header.Get("Content-Range"), want)
			}
		case 200:
			if !bytes.Equal(r.body, data) {
				t.Fatalf("%s: expected the whole object", c.rng)
			}
		case 416:
			if r.code() != "InvalidRange" || r.Header.Get("Content-Range") != "bytes */100000" {
				t.Fatalf("%s: %s %v", c.rng, r.body, r.Header)
			}
		}
	}
	e.expect(e.do("GET", "/bkt/empty", nil, "Range", "bytes=0-0"), 416, "InvalidRange")
}

func TestConditionalRequests(t *testing.T) {
	e := newEnv(t, 2, 1)
	e.do("PUT", "/bkt", nil)
	r := e.do("PUT", "/bkt/k", []byte("content"))
	etag := r.Header.Get("ETag")
	head := e.do("HEAD", "/bkt/k", nil)
	lm, _ := http.ParseTime(head.Header.Get("Last-Modified"))
	before := lm.Add(-time.Hour).Format(http.TimeFormat)
	after := lm.Add(time.Hour).Format(http.TimeFormat)
	same := lm.Format(http.TimeFormat)
	cases := []struct {
		name    string
		headers []string
		status  int
	}{
		{"If-Match ok", []string{"If-Match", etag}, 200},
		{"If-Match unquoted", []string{"If-Match", strings.Trim(etag, `"`)}, 200},
		{"If-Match list", []string{"If-Match", `"x", ` + etag}, 200},
		{"If-Match star", []string{"If-Match", "*"}, 200},
		{"If-Match fails", []string{"If-Match", `"nope"`}, 412},
		{"If-None-Match matches", []string{"If-None-Match", etag}, 304},
		{"If-None-Match star", []string{"If-None-Match", "*"}, 304},
		{"If-None-Match differs", []string{"If-None-Match", `"nope"`}, 200},
		{"If-Modified-Since earlier", []string{"If-Modified-Since", before}, 200},
		{"If-Modified-Since same", []string{"If-Modified-Since", same}, 304},
		{"If-Modified-Since later", []string{"If-Modified-Since", after}, 304},
		{"If-Unmodified-Since later", []string{"If-Unmodified-Since", after}, 200},
		{"If-Unmodified-Since same", []string{"If-Unmodified-Since", same}, 200},
		{"If-Unmodified-Since earlier", []string{"If-Unmodified-Since", before}, 412},
		// S3: If-Match true and If-Unmodified-Since false returns 200.
		{"If-Match wins over If-Unmodified-Since", []string{"If-Match", etag, "If-Unmodified-Since", before}, 200},
		// S3: If-None-Match false and If-Modified-Since true returns 304.
		{"If-None-Match wins over If-Modified-Since", []string{"If-None-Match", etag, "If-Modified-Since", before}, 304},
		{"If-None-Match differs, not modified", []string{"If-None-Match", `"x"`, "If-Modified-Since", after}, 200},
		{"unparseable date ignored", []string{"If-Modified-Since", "yesterday"}, 200},
	}
	for _, c := range cases {
		for _, method := range []string{"GET", "HEAD"} {
			r := e.do(method, "/bkt/k", nil, c.headers...)
			if r.StatusCode != c.status {
				t.Errorf("%s %s: got %d, want %d", method, c.name, r.StatusCode, c.status)
			}
			if c.status == 304 && (r.Header.Get("ETag") != etag || len(r.body) != 0) {
				t.Errorf("%s %s: 304 without ETag or with a body", method, c.name)
			}
		}
	}
	// Conditional writes.
	e.expect(e.do("PUT", "/bkt/k", []byte("v2"), "If-None-Match", "*"), 412, "PreconditionFailed")
	e.expect(e.do("PUT", "/bkt/new", []byte("v1"), "If-None-Match", "*"), 200, "")
	e.expect(e.do("PUT", "/bkt/k", []byte("v2"), "If-Match", `"wrong"`), 412, "PreconditionFailed")
	e.expect(e.do("PUT", "/bkt/k", []byte("v2"), "If-Match", etag), 200, "")
	e.expect(e.do("PUT", "/bkt/absent", []byte("v2"), "If-Match", etag), 404, "NoSuchKey")
	// If-None-Match with an ETag (an RGW extension strata accepts).
	cur := e.do("HEAD", "/bkt/k", nil).Header.Get("ETag")
	e.expect(e.do("PUT", "/bkt/k", []byte("v3"), "If-None-Match", cur), 412, "PreconditionFailed")
	e.expect(e.do("PUT", "/bkt/k", []byte("v3"), "If-None-Match", `"other"`), 200, "")
}

func TestListingAPI(t *testing.T) {
	e := newEnv(t, 2, 1)
	e.do("PUT", "/bkt", nil)
	keys := []string{"a/1", "a/2", "a/b/3", "b", "c d/e+f", "c d/g", "z/", "ü"} // UTF-8 byte order
	for _, k := range keys {
		e.expect(e.do("PUT", "/bkt/"+sigv4.URIEncode(k, true), []byte(k)), 200, "")
	}
	listV2 := func(q string) listBucketResultV2 {
		r := e.do("GET", "/bkt?list-type=2&"+q, nil)
		e.expect(r, 200, "")
		var res listBucketResultV2
		if err := xml.Unmarshal(r.body, &res); err != nil {
			t.Fatal(err)
		}
		return res
	}
	res := listV2("delimiter=%2F")
	var got []string
	for _, c := range res.Contents {
		got = append(got, c.Key)
	}
	for _, p := range res.CommonPrefixes {
		got = append(got, "P:"+p.Prefix)
	}
	if strings.Join(got, "|") != "b|ü|P:a/|P:c d/|P:z/" || res.KeyCount != 5 {
		t.Fatalf("delimiter listing: %q (KeyCount %d)", got, res.KeyCount)
	}
	// Pagination with continuation tokens visits every key once, in order.
	var all []string
	token := ""
	for {
		q := "max-keys=3"
		if token != "" {
			q += "&continuation-token=" + url.QueryEscape(token)
		}
		res := listV2(q)
		for _, c := range res.Contents {
			all = append(all, c.Key)
		}
		if !res.IsTruncated {
			break
		}
		token = res.NextContinuationToken
	}
	if strings.Join(all, "|") != strings.Join(keys, "|") {
		t.Fatalf("paginated: %q", all)
	}
	// encoding-type=url encodes keys and prefixes and says so.
	res = listV2("prefix=c%20d%2F&encoding-type=url")
	if res.EncodingType != "url" || res.Prefix != "c%20d/" || len(res.Contents) != 2 || res.Contents[0].Key != "c%20d/e%2Bf" {
		t.Fatalf("encoding-type=url: %+v", res)
	}
	res = listV2("start-after=c%20d%2Fe%2Bf")
	if len(res.Contents) != 3 || res.Contents[0].Key != "c d/g" {
		t.Fatalf("start-after: %+v", res.Contents)
	}
	e.expect(e.do("GET", "/bkt?list-type=2&continuation-token=%21%21", nil), 400, "InvalidArgument")
	e.expect(e.do("GET", "/bkt?list-type=2&max-keys=-1", nil), 400, "InvalidArgument")
	e.expect(e.do("GET", "/bkt?list-type=2&encoding-type=base64", nil), 400, "InvalidArgument")

	// ListObjects (v1): NextMarker only with a delimiter.
	r := e.do("GET", "/bkt?max-keys=2", nil)
	var v1 listBucketResultV1
	xml.Unmarshal(r.body, &v1)
	if !v1.IsTruncated || v1.NextMarker != "" || len(v1.Contents) != 2 {
		t.Fatalf("v1: %s", r.body)
	}
	r = e.do("GET", "/bkt?max-keys=2&delimiter=%2F&marker=a%2F2", nil)
	v1 = listBucketResultV1{}
	xml.Unmarshal(r.body, &v1)
	if !v1.IsTruncated || v1.NextMarker != "c d/" || len(v1.Contents) != 1 || len(v1.CommonPrefixes) != 1 {
		t.Fatalf("v1 with delimiter: %s", r.body)
	}
	// ListObjectVersions reports the single "null" version of each key.
	r = e.do("GET", "/bkt?versions&prefix=a%2F", nil)
	var lv listVersionsResult
	xml.Unmarshal(r.body, &lv)
	if len(lv.Versions) != 3 || lv.Versions[0].VersionID != "null" || !lv.Versions[0].IsLatest {
		t.Fatalf("versions: %s", r.body)
	}
}

func TestMultipartAPI(t *testing.T) {
	e := newEnv(t, 2, 1)
	e.do("PUT", "/bkt", nil)
	r := e.do("POST", "/bkt/mp?uploads", nil, "Content-Type", "video/mp4", "X-Amz-Meta-Origin", "test")
	e.expect(r, 200, "")
	var init initiateMultipartUploadResult
	xml.Unmarshal(r.body, &init)
	id := init.UploadID
	part1 := bytes.Repeat([]byte("A"), store.MinPartSize)
	part2 := []byte("tail")
	e1 := e.do("PUT", "/bkt/mp?partNumber=1&uploadId="+id, part1).Header.Get("ETag")
	e2 := e.do("PUT", "/bkt/mp?partNumber=2&uploadId="+id, part2).Header.Get("ETag")
	e.expect(e.do("PUT", "/bkt/mp?partNumber=0&uploadId="+id, part2), 400, "InvalidArgument")
	e.expect(e.do("PUT", "/bkt/mp?partNumber=10001&uploadId="+id, part2), 400, "InvalidArgument")
	e.expect(e.do("PUT", "/bkt/mp?partNumber=1&uploadId=nope", part2), 404, "NoSuchUpload")

	r = e.do("GET", "/bkt/mp?uploadId="+id, nil)
	var lp listPartsResult
	xml.Unmarshal(r.body, &lp)
	if len(lp.Parts) != 2 || lp.Parts[1].ETag != e2 || lp.Parts[0].Size != int64(len(part1)) {
		t.Fatalf("ListParts: %s", r.body)
	}
	r = e.do("GET", "/bkt?uploads", nil)
	var lu listMultipartUploadsResult
	xml.Unmarshal(r.body, &lu)
	if len(lu.Uploads) != 1 || lu.Uploads[0].UploadID != id {
		t.Fatalf("ListMultipartUploads: %s", r.body)
	}
	complete := func(parts ...string) response {
		var b strings.Builder
		b.WriteString("<CompleteMultipartUpload>")
		for i := 0; i < len(parts); i += 2 {
			fmt.Fprintf(&b, "<Part><PartNumber>%s</PartNumber><ETag>%s</ETag></Part>", parts[i], parts[i+1])
		}
		b.WriteString("</CompleteMultipartUpload>")
		return e.do("POST", "/bkt/mp?uploadId="+id, []byte(b.String()))
	}
	e.expect(complete("2", e2, "1", e1), 400, "InvalidPartOrder")
	e.expect(complete("1", e1, "3", e2), 400, "InvalidPart")
	e.expect(e.do("POST", "/bkt/mp?uploadId="+id, []byte("<garbage")), 400, "MalformedXML")
	r = complete("1", e1, "2", e2)
	e.expect(r, 200, "")
	var res completeMultipartUploadResult
	xml.Unmarshal(r.body, &res)
	h := md5.New()
	for _, et := range []string{e1, e2} {
		b, _ := hex.DecodeString(strings.Trim(et, `"`))
		h.Write(b)
	}
	if want := `"` + hex.EncodeToString(h.Sum(nil)) + `-2"`; res.ETag != want {
		t.Fatalf("multipart ETag %s, want %s", res.ETag, want)
	}
	r = e.do("GET", "/bkt/mp", nil)
	if !bytes.Equal(r.body, append(append([]byte(nil), part1...), part2...)) || r.Header.Get("Content-Type") != "video/mp4" ||
		r.Header.Get("X-Amz-Meta-Origin") != "test" || r.Header.Get("ETag") != res.ETag {
		t.Fatalf("assembled object wrong: %v", r.Header)
	}
	r = e.do("GET", "/bkt/mp?partNumber=2", nil)
	if r.StatusCode != 206 || string(r.body) != "tail" || r.Header.Get("X-Amz-Mp-Parts-Count") != "2" {
		t.Fatalf("GET partNumber=2: %d %q %v", r.StatusCode, r.body, r.Header)
	}
	// A retried completion gets the same answer; another part list does not.
	again := complete("1", e1, "2", e2)
	e.expect(again, 200, "")
	if !bytes.Contains(again.body, []byte(res.ETag[1:len(res.ETag)-1])) {
		t.Fatalf("retried completion: %s", again.body)
	}
	e.expect(complete("1", e1), 404, "NoSuchUpload")

	// Abort.
	r = e.do("POST", "/bkt/mp2?uploads", nil)
	xml.Unmarshal(r.body, &init)
	e.do("PUT", "/bkt/mp2?partNumber=1&uploadId="+init.UploadID, part2)
	e.expect(e.do("DELETE", "/bkt/mp2?uploadId="+init.UploadID, nil), 204, "")
	e.expect(e.do("GET", "/bkt/mp2?uploadId="+init.UploadID, nil), 404, "NoSuchUpload")
}

func TestCopyAPI(t *testing.T) {
	e := newEnv(t, 2, 1)
	e.do("PUT", "/bkt", nil)
	e.do("PUT", "/cpy", nil)
	data := bytes.Repeat([]byte("copy me "), 10000)
	src := e.do("PUT", "/bkt/src key", data, "Content-Type", "text/x-src", "X-Amz-Meta-A", "1")
	etag := src.Header.Get("ETag")

	r := e.do("PUT", "/cpy/dst", nil, "X-Amz-Copy-Source", "/bkt/src%20key")
	e.expect(r, 200, "")
	var cr copyObjectResult
	xml.Unmarshal(r.body, &cr)
	if cr.ETag != etag {
		t.Fatalf("copy ETag %s, want %s", cr.ETag, etag)
	}
	r = e.do("GET", "/cpy/dst", nil)
	if !bytes.Equal(r.body, data) || r.Header.Get("Content-Type") != "text/x-src" || r.Header.Get("X-Amz-Meta-A") != "1" {
		t.Fatalf("copied object wrong: %v", r.Header)
	}
	r = e.do("PUT", "/cpy/dst2", nil, "X-Amz-Copy-Source", "bkt/src%20key", "X-Amz-Metadata-Directive", "REPLACE",
		"Content-Type", "text/new", "X-Amz-Meta-B", "2")
	e.expect(r, 200, "")
	r = e.do("HEAD", "/cpy/dst2", nil)
	if r.Header.Get("Content-Type") != "text/new" || r.Header.Get("X-Amz-Meta-B") != "2" || r.Header.Get("X-Amz-Meta-A") != "" {
		t.Fatalf("REPLACE directive: %v", r.Header)
	}
	e.expect(e.do("PUT", "/bkt/src%20key", nil, "X-Amz-Copy-Source", "/bkt/src%20key"), 400, "InvalidRequest")
	e.expect(e.do("PUT", "/bkt/src%20key", nil, "X-Amz-Copy-Source", "/bkt/src%20key", "X-Amz-Metadata-Directive", "REPLACE"), 200, "")
	e.expect(e.do("PUT", "/cpy/x", nil, "X-Amz-Copy-Source", "/bkt/missing"), 404, "NoSuchKey")
	e.expect(e.do("PUT", "/cpy/x", nil, "X-Amz-Copy-Source", "/nobucket/k"), 404, "NoSuchBucket")
	e.expect(e.do("PUT", "/cpy/x", nil, "X-Amz-Copy-Source", "nokey"), 400, "InvalidArgument")
	e.expect(e.do("PUT", "/cpy/x", nil, "X-Amz-Copy-Source", "/bkt/src%20key", "X-Amz-Copy-Source-If-Match", `"nope"`), 412, "PreconditionFailed")
	e.expect(e.do("PUT", "/cpy/x", nil, "X-Amz-Copy-Source", "/bkt/src%20key", "X-Amz-Copy-Source-If-None-Match", etag), 412, "PreconditionFailed")
	e.expect(e.do("PUT", "/cpy/x", nil, "X-Amz-Copy-Source", "/bkt/src%20key", "X-Amz-Copy-Source-If-Match", etag), 200, "")

	// UploadPartCopy with a range.
	r = e.do("POST", "/cpy/assembled?uploads", nil)
	var init initiateMultipartUploadResult
	xml.Unmarshal(r.body, &init)
	r = e.do("PUT", "/cpy/assembled?partNumber=1&uploadId="+init.UploadID, nil, "X-Amz-Copy-Source", "/bkt/src%20key", "X-Amz-Copy-Source-Range", "bytes=8-15")
	e.expect(r, 200, "")
	var cp copyPartResult
	xml.Unmarshal(r.body, &cp)
	body := fmt.Sprintf("<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>%s</ETag></Part></CompleteMultipartUpload>", cp.ETag)
	e.expect(e.do("POST", "/cpy/assembled?uploadId="+init.UploadID, []byte(body)), 200, "")
	if r := e.do("GET", "/cpy/assembled", nil); string(r.body) != "copy me " {
		t.Fatalf("UploadPartCopy range: %q", r.body)
	}
}

func TestDeleteObjectsAPI(t *testing.T) {
	e := newEnv(t, 2, 1)
	e.do("PUT", "/bkt", nil)
	for _, k := range []string{"x", "y", "z"} {
		e.do("PUT", "/bkt/"+k, []byte(k))
	}
	body := []byte(`<Delete><Object><Key>x</Key></Object><Object><Key>y</Key><VersionId>v9</VersionId></Object><Object><Key>never</Key></Object></Delete>`)
	r := e.do("POST", "/bkt?delete", body, "Content-MD5", b64md5(body))
	e.expect(r, 200, "")
	var dr deleteResult
	xml.Unmarshal(r.body, &dr)
	if len(dr.Deleted) != 2 || len(dr.Errors) != 1 || dr.Errors[0].Key != "y" {
		t.Fatalf("DeleteObjects: %s", r.body)
	}
	body = []byte(`<Delete><Quiet>true</Quiet><Object><Key>z</Key></Object></Delete>`)
	r = e.do("POST", "/bkt?delete", body, "Content-MD5", b64md5(body))
	dr = deleteResult{}
	xml.Unmarshal(r.body, &dr)
	if len(dr.Deleted) != 0 || len(dr.Errors) != 0 {
		t.Fatalf("quiet DeleteObjects: %s", r.body)
	}
	e.expect(e.do("GET", "/bkt/z", nil), 404, "NoSuchKey")
	e.expect(e.do("POST", "/bkt?delete", body, "Content-MD5", b64md5([]byte("x"))), 400, "BadDigest")
	e.expect(e.do("POST", "/bkt?delete", []byte("<Delete></Delete>")), 400, "MalformedXML")
}

func TestAuthAPI(t *testing.T) {
	e := newEnv(t, 2, 1)
	e.do("PUT", "/bkt", nil)
	e.do("PUT", "/bkt/k", []byte("secret data"))
	resp, err := http.Get(e.srv.URL + "/bkt/k")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 403 {
		t.Fatalf("anonymous GET: %d", resp.StatusCode)
	}
	bad := &sigv4.Signer{AccessKey: testAccess, SecretKey: "wrong", Region: testRegion}
	req := e.newRequest("GET", "/bkt/k")
	bad.SignBytes(req, nil)
	e.expect(e.send(req), 403, "SignatureDoesNotMatch")
	unknown := &sigv4.Signer{AccessKey: "nobody", SecretKey: "x", Region: testRegion}
	req = e.newRequest("GET", "/bkt/k")
	unknown.SignBytes(req, nil)
	e.expect(e.send(req), 403, "InvalidAccessKeyId")
	wrongRegion := &sigv4.Signer{AccessKey: testAccess, SecretKey: testSecret, Region: "eu-west-1"}
	req = e.newRequest("GET", "/bkt/k")
	wrongRegion.SignBytes(req, nil)
	e.expect(e.send(req), 400, "AuthorizationHeaderMalformed")
	skewed := &sigv4.Signer{AccessKey: testAccess, SecretKey: testSecret, Region: testRegion,
		Now: func() time.Time { return time.Now().Add(-20 * time.Minute) }}
	req = e.newRequest("GET", "/bkt/k")
	skewed.SignBytes(req, nil)
	e.expect(e.send(req), 403, "RequestTimeTooSkewed")

	// Presigned GET and PUT.
	u := e.signer.Presign(e.newRequest("GET", "/bkt/k"), time.Minute)
	resp, _ = http.Get(u.String())
	got, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 || string(got) != "secret data" {
		t.Fatalf("presigned GET: %d %s", resp.StatusCode, got)
	}
	u = e.signer.Presign(e.newRequest("PUT", "/bkt/uploaded"), time.Minute)
	preq, _ := http.NewRequest("PUT", u.String(), strings.NewReader("via presigned PUT"))
	resp, _ = http.DefaultClient.Do(preq)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("presigned PUT: %d", resp.StatusCode)
	}
	if r := e.do("GET", "/bkt/uploaded", nil); string(r.body) != "via presigned PUT" {
		t.Fatalf("presigned PUT stored %q", r.body)
	}
	old := &sigv4.Signer{AccessKey: testAccess, SecretKey: testSecret, Region: testRegion,
		Now: func() time.Time { return time.Now().Add(-2 * time.Hour) }}
	u = old.Presign(e.newRequest("GET", "/bkt/k"), time.Hour)
	resp, _ = http.Get(u.String())
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 403 || !strings.Contains(string(body), "expired") {
		t.Fatalf("expired presigned URL: %d %s", resp.StatusCode, body)
	}
}

func TestInternalEndpoints(t *testing.T) {
	e := newEnv(t, 2, 1)
	e.do("PUT", "/bkt", nil)
	e.do("PUT", "/bkt/k", []byte("x"))
	e.do("GET", "/bkt/k", nil)
	resp, _ := http.Get(e.srv.URL + "/-/health")
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("health: %d", resp.StatusCode)
	}
	resp, _ = http.Get(e.srv.URL + "/-/metrics")
	m, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	for _, want := range []string{
		`strata_requests_total{api="PutObject",code="200"} 1`,
		`strata_request_duration_seconds_bucket{api="GetObject",le="+Inf"} 1`,
		`strata_objects 1`,
		`strata_disk_online{disk="0",path=`,
		`# TYPE strata_corrupt_blocks_total counter`,
	} {
		if !strings.Contains(string(m), want) {
			t.Fatalf("metrics lack %q:\n%s", want, m)
		}
	}
	// Admin endpoints need a signature.
	resp, _ = http.Get(e.srv.URL + "/-/admin/info")
	resp.Body.Close()
	if resp.StatusCode != 403 {
		t.Fatalf("unsigned admin request: %d", resp.StatusCode)
	}
	r := e.do("POST", "/-/admin/heal?deep=true", nil)
	if r.StatusCode != 200 || !strings.Contains(string(r.body), `"Objects": 1`) {
		t.Fatalf("admin heal: %d %s", r.StatusCode, r.body)
	}
}

// TestReadFailureAbortsResponse removes more shards than parity covers
// after the headers of a large GET are sent: the client must see a broken
// transfer, never a complete-looking wrong body.
func TestReadFailureAbortsResponse(t *testing.T) {
	e := newEnv(t, 2, 1)
	e.do("PUT", "/bkt", nil)
	data := bytes.Repeat([]byte("z"), 1<<20)
	e.do("PUT", "/bkt/k", data)
	o, err := e.st.OpenObject("bkt", "k")
	if err != nil {
		t.Fatal(err)
	}
	version := o.Info.VersionID
	o.Close()
	// Truncate two of the three shard files (the readers find the first
	// stripes intact only if they were cached; truncation breaks them all).
	for _, d := range e.cfg.Disks[:2] {
		matches, _ := filepath.Glob(filepath.Join(d, "buckets", "bkt", "objects", "*", "*", version, "part.1"))
		for _, p := range matches {
			os.Truncate(p, 100)
		}
	}
	req := e.newRequest("GET", "/bkt/k")
	e.signer.SignBytes(req, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return // aborted before the headers: fine
	}
	got, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err == nil && bytes.Equal(got, data) {
		t.Fatal("a read with two of three shards destroyed returned the full object")
	}
	if err == nil && resp.StatusCode == 200 && len(got) == len(data) {
		t.Fatal("a failed read was delivered as a complete 200 response")
	}
}

// TestS3BehavioursFromConformanceSuite covers behaviours that running
// ceph/s3-tests showed strata had wrong (see docs/COMPATIBILITY.md).
func TestS3BehavioursFromConformanceSuite(t *testing.T) {
	e := newEnv(t, 2, 1)
	e.do("PUT", "/bkt", nil)

	// Paginated ListBuckets.
	e.do("PUT", "/bkt2", nil)
	r := e.do("GET", "/?max-buckets=1", nil)
	var lb listAllMyBucketsResult
	xml.Unmarshal(r.body, &lb)
	if len(lb.Buckets) != 1 || lb.Buckets[0].Name != "bkt" || lb.ContinuationToken == "" {
		t.Fatalf("first page: %s", r.body)
	}
	r = e.do("GET", "/?max-buckets=1&continuation-token="+lb.ContinuationToken, nil)
	lb = listAllMyBucketsResult{}
	xml.Unmarshal(r.body, &lb)
	if len(lb.Buckets) != 1 || lb.Buckets[0].Name != "bkt2" || lb.ContinuationToken != "" {
		t.Fatalf("second page: %s", r.body)
	}

	// Content-Encoding loses aws-chunked and nothing else.
	for in, want := range map[string]string{"deflate, gzip": "deflate, gzip", "gzip, aws-chunked": "gzip", "aws-chunked": ""} {
		e.do("PUT", "/bkt/enc", []byte("x"), "Content-Encoding", in)
		if got := e.do("HEAD", "/bkt/enc", nil).Header.Get("Content-Encoding"); got != want {
			t.Errorf("Content-Encoding %q stored as %q, want %q", in, got, want)
		}
	}

	// Conditional DELETE.
	etag := e.do("PUT", "/bkt/del", []byte("x")).Header.Get("ETag")
	e.expect(e.do("DELETE", "/bkt/del", nil, "If-Match", `"nope"`), 412, "PreconditionFailed")
	e.expect(e.do("DELETE", "/bkt/del", nil, "If-Match", etag), 204, "")
	e.expect(e.do("DELETE", "/bkt/del", nil, "If-Match", `"nope"`), 204, "") // absent: no error

	// partNumber on a single-PUT object: 1 is the object, 2 is InvalidPart.
	e.do("PUT", "/bkt/single", []byte("body"))
	r = e.do("GET", "/bkt/single?partNumber=1", nil)
	if r.StatusCode != 200 || string(r.body) != "body" {
		t.Fatalf("partNumber=1 of a single-part object: %d %q", r.StatusCode, r.body)
	}
	e.expect(e.do("GET", "/bkt/single?partNumber=2", nil), 400, "InvalidPart")

	// An empty continuation token is no token, and is echoed.
	r = e.do("GET", "/bkt?list-type=2&continuation-token=", nil)
	e.expect(r, 200, "")
	if !bytes.Contains(r.body, []byte("<ContinuationToken></ContinuationToken>")) {
		t.Fatalf("empty continuation token not echoed: %s", r.body)
	}

	// A body sent with HTTP chunked transfer encoding (no Content-Length).
	req := e.newRequest("PUT", "/bkt/te")
	req.Body = io.NopCloser(strings.NewReader("chunked body"))
	req.ContentLength = -1
	e.signer.Sign(req, sigv4.UnsignedPayload)
	e.expect(e.send(req), 200, "")
	if got := e.do("GET", "/bkt/te", nil); string(got.body) != "chunked body" {
		t.Fatalf("chunked upload stored %q", got.body)
	}

	// UploadPartCopy with a range past the source is InvalidRange.
	r = e.do("POST", "/bkt/mp?uploads", nil)
	var init initiateMultipartUploadResult
	xml.Unmarshal(r.body, &init)
	r = e.do("PUT", "/bkt/mp?partNumber=1&uploadId="+init.UploadID, nil,
		"X-Amz-Copy-Source", "/bkt/single", "X-Amz-Copy-Source-Range", "bytes=0-21")
	if r.code() != "InvalidRange" {
		t.Fatalf("copy range past the source: %d %s", r.StatusCode, r.body)
	}
}

// Multipart checksums: COMPOSITE for SHA256 and FULL_OBJECT for CRC32,
// verified against the request, returned on completion, on HEAD with
// checksum mode, per part, and by GetObjectAttributes.
func TestMultipartChecksums(t *testing.T) {
	e := newEnv(t, 2, 1)
	e.do("PUT", "/bkt", nil)
	parts := [][]byte{bytes.Repeat([]byte("A"), store.MinPartSize), []byte("tail")}
	for _, tc := range []struct{ algo, typ string }{{"SHA256", "COMPOSITE"}, {"CRC32", "FULL_OBJECT"}, {"CRC64NVME", ""}} {
		key := "/bkt/" + tc.algo
		hdr := []string{"X-Amz-Checksum-Algorithm", tc.algo}
		if tc.typ != "" {
			hdr = append(hdr, "X-Amz-Checksum-Type", tc.typ)
		}
		r := e.do("POST", key+"?uploads", nil, hdr...)
		e.expect(r, 200, "")
		wantType := tc.typ
		if wantType == "" {
			wantType = "FULL_OBJECT" // the default for CRC64NVME
		}
		if r.Header.Get("X-Amz-Checksum-Type") != wantType {
			t.Fatalf("%s: type %q", tc.algo, r.Header.Get("X-Amz-Checksum-Type"))
		}
		var init initiateMultipartUploadResult
		xml.Unmarshal(r.body, &init)
		var mps []checksum.MultipartPart
		var cmu strings.Builder
		cmu.WriteString("<CompleteMultipartUpload>")
		var whole []byte
		for i, p := range parts {
			h := checksum.New(tc.algo)
			h.Write(p)
			sum := checksum.Encode(h.Sum(nil))
			r := e.do("PUT", fmt.Sprintf("%s?partNumber=%d&uploadId=%s", key, i+1, init.UploadID), p, checksumHeader(tc.algo), sum)
			e.expect(r, 200, "")
			mps = append(mps, checksum.MultipartPart{Checksum: sum, Size: int64(len(p))})
			fmt.Fprintf(&cmu, "<Part><PartNumber>%d</PartNumber><ETag>%s</ETag><Checksum%s>%s</Checksum%s></Part>", i+1, r.Header.Get("ETag"), tc.algo, sum, tc.algo)
			whole = append(whole, p...)
		}
		cmu.WriteString("</CompleteMultipartUpload>")
		want, _ := checksum.Multipart(tc.algo, wantType, mps)
		if wantType == "FULL_OBJECT" {
			h := checksum.New(tc.algo)
			h.Write(whole)
			if want != checksum.Encode(h.Sum(nil)) {
				t.Fatalf("%s: combined CRC is not the CRC of the object", tc.algo)
			}
		}
		e.expect(e.do("POST", key+"?uploadId="+init.UploadID, []byte(cmu.String()), checksumHeader(tc.algo), "AAAA"), 400, "BadDigest")
		r = e.do("POST", key+"?uploadId="+init.UploadID, []byte(cmu.String()), checksumHeader(tc.algo), want)
		e.expect(r, 200, "")
		if !bytes.Contains(r.body, []byte(want)) || !bytes.Contains(r.body, []byte(wantType)) {
			t.Fatalf("%s: completion result %s", tc.algo, r.body)
		}
		if got := e.do("HEAD", key, nil, "X-Amz-Checksum-Mode", "ENABLED").Header.Get(checksumHeader(tc.algo)); got != want {
			t.Fatalf("%s: HEAD checksum %q, want %q", tc.algo, got, want)
		}
		if got := e.do("GET", key+"?partNumber=2", nil, "X-Amz-Checksum-Mode", "ENABLED").Header.Get(checksumHeader(tc.algo)); got != mps[1].Checksum {
			t.Fatalf("%s: part 2 checksum %q, want %q", tc.algo, got, mps[1].Checksum)
		}
		r = e.do("GET", key+"?attributes", nil, "X-Amz-Object-Attributes", "ETag,Checksum,ObjectParts,ObjectSize")
		var oa objectAttributesXML
		xml.Unmarshal(r.body, &oa)
		if oa.Checksum == nil || oa.Checksum.checksum(tc.algo) != want || oa.ObjectParts == nil || oa.ObjectParts.TotalPartsCount != 2 ||
			oa.ObjectSize == nil || *oa.ObjectSize != int64(len(whole)) {
			t.Fatalf("%s: GetObjectAttributes %s", tc.algo, r.body)
		}
	}
	e.expect(e.do("POST", "/bkt/x?uploads", nil, "X-Amz-Checksum-Algorithm", "SHA256", "X-Amz-Checksum-Type", "FULL_OBJECT"), 400, "InvalidRequest")
}
