package s3api

import (
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"math/rand/v2"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/useless-husband/strata/internal/sigv4"
	"github.com/useless-husband/strata/internal/store"
)

// The model-based test runs random S3 operations against strata and
// against a few dozen lines of in-memory S3 semantics, and requires the
// same answers: status codes, error codes, bodies, ETags, attributes and
// listings. The server is restarted from disk now and then, so the model
// also checks durability across restarts.

type mObject struct {
	data        []byte
	etag        string
	contentType string
	meta        map[string]string
}

type model struct {
	buckets map[string]map[string]*mObject
}

func (m *model) sortedKeys(bucket string) []string {
	var keys []string
	for k := range m.buckets[bucket] {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// listing is what ListObjectsV2 must return, flattened: "K:key:size:etag"
// for objects and "P:prefix" for common prefixes, in order.
func (m *model) listing(bucket, prefix, delim string) []string {
	var out []string
	seen := map[string]bool{}
	for _, k := range m.sortedKeys(bucket) {
		if !strings.HasPrefix(k, prefix) {
			continue
		}
		if delim != "" {
			if i := strings.Index(k[len(prefix):], delim); i >= 0 {
				cp := k[:len(prefix)+i+len(delim)]
				if !seen[cp] {
					seen[cp] = true
					out = append(out, "P:"+cp)
				}
				continue
			}
		}
		o := m.buckets[bucket][k]
		out = append(out, fmt.Sprintf("K:%s:%d:%s", k, len(o.data), o.etag))
	}
	return out
}

var (
	modelBuckets = []string{"alpha", "bravo", "charlie"}
	modelKeys    = []string{"a", "a/b", "a/c", "a/c/d", "b/x/y", "b/x/z", "c", "d e", "ü/1", "k+1", "z/"}
	modelPrefix  = []string{"", "a", "a/", "a/c", "b/", "b/x/", "ü", "z", "nothing"}
	modelDelims  = []string{"", "/", "x", "/x/"}
	modelTypes   = []string{"", "text/plain", "application/octet-stream", "image/png"}
)

func md5hex(b []byte) string {
	h := md5.Sum(b)
	return hex.EncodeToString(h[:])
}

func TestModelRandomOperations(t *testing.T) {
	seed := uint64(20261001)
	if s := os.Getenv("STRATA_SEED"); s != "" {
		seed, _ = strconv.ParseUint(s, 10, 64)
	}
	ops := 1500
	if testing.Short() {
		ops = 300
	}
	r := rand.New(rand.NewPCG(seed, 42))
	e := newEnv(t, 3, 2)
	m := &model{buckets: map[string]map[string]*mObject{}}
	pick := func(xs []string) string { return xs[r.IntN(len(xs))] }
	path := func(b, k string) string {
		if k == "" {
			return "/" + b
		}
		return "/" + b + "/" + sigv4.URIEncode(k, true)
	}
	randData := func() []byte {
		n := []int{0, 1, 17, 4096, 16 << 10, (16 << 10) + 1, 40_000, 100_000}[r.IntN(8)]
		b := make([]byte, n)
		for i := range b {
			b[i] = byte(r.Uint32())
		}
		return b
	}
	counts := map[string]int{}
	for i := 0; i < ops; i++ {
		b, k := pick(modelBuckets), pick(modelKeys)
		bm := m.buckets[b]
		fail := func(format string, args ...any) {
			t.Helper()
			t.Fatalf("seed %d op %d: "+format, append([]any{seed, i}, args...)...)
		}
		check := func(what string, r response, status int, code string) {
			t.Helper()
			if r.StatusCode != status || (code != "" && r.code() != code) {
				fail("%s: got %d %s, want %d %s: %s", what, r.StatusCode, r.code(), status, code, r.body)
			}
		}
		op := r.IntN(100)
		switch {
		case op < 6:
			counts["CreateBucket"]++
			resp := e.do("PUT", "/"+b, nil)
			if bm != nil {
				check("CreateBucket "+b, resp, 200, "") // us-east-1: no-op
			} else {
				check("CreateBucket "+b, resp, 200, "")
				m.buckets[b] = map[string]*mObject{}
			}
		case op < 9:
			counts["DeleteBucket"]++
			resp := e.do("DELETE", "/"+b, nil)
			switch {
			case bm == nil:
				check("DeleteBucket "+b, resp, 404, "NoSuchBucket")
			case len(bm) > 0:
				check("DeleteBucket "+b, resp, 409, "BucketNotEmpty")
			default:
				check("DeleteBucket "+b, resp, 204, "")
				delete(m.buckets, b)
			}
		case op < 30:
			counts["PutObject"]++
			data := randData()
			ct := pick(modelTypes)
			meta := map[string]string{}
			hdr := []string{}
			if ct != "" {
				hdr = append(hdr, "Content-Type", ct)
			}
			if r.IntN(2) == 0 {
				meta["n"] = strconv.Itoa(i)
				hdr = append(hdr, "X-Amz-Meta-N", meta["n"])
			}
			cond := r.IntN(6) == 0
			if cond {
				hdr = append(hdr, "If-None-Match", "*")
			}
			resp := e.do("PUT", path(b, k), data, hdr...)
			switch {
			case bm == nil:
				check("PutObject", resp, 404, "NoSuchBucket")
			case cond && bm[k] != nil:
				check("conditional PutObject", resp, 412, "PreconditionFailed")
			default:
				check("PutObject", resp, 200, "")
				if got := resp.Header.Get("ETag"); got != `"`+md5hex(data)+`"` {
					fail("PutObject ETag %s", got)
				}
				bm[k] = &mObject{data: data, etag: md5hex(data), contentType: ct, meta: meta}
			}
		case op < 45:
			counts["GetObject"]++
			resp := e.do("GET", path(b, k), nil)
			switch {
			case bm == nil:
				check("GetObject", resp, 404, "NoSuchBucket")
			case bm[k] == nil:
				check("GetObject", resp, 404, "NoSuchKey")
			default:
				o := bm[k]
				check("GetObject", resp, 200, "")
				wantCT := o.contentType
				if wantCT == "" {
					wantCT = "binary/octet-stream"
				}
				if !bytes.Equal(resp.body, o.data) || resp.Header.Get("ETag") != `"`+o.etag+`"` ||
					resp.Header.Get("Content-Type") != wantCT || resp.Header.Get("X-Amz-Meta-N") != o.meta["n"] {
					fail("GetObject %s/%s: %d bytes (want %d), headers %v", b, k, len(resp.body), len(o.data), resp.Header)
				}
			}
		case op < 52:
			counts["RangedGet"]++
			if bm == nil || bm[k] == nil {
				continue
			}
			o := bm[k]
			size := len(o.data)
			start := r.IntN(size + 2)
			end := start + r.IntN(50_000)
			resp := e.do("GET", path(b, k), nil, "Range", fmt.Sprintf("bytes=%d-%d", start, end))
			if start >= size {
				check("ranged GetObject", resp, 416, "InvalidRange")
				continue
			}
			check("ranged GetObject", resp, 206, "")
			if !bytes.Equal(resp.body, o.data[start:min(end+1, size)]) {
				fail("range %d-%d of %d bytes wrong", start, end, size)
			}
		case op < 57:
			counts["HeadObject"]++
			resp := e.do("HEAD", path(b, k), nil)
			switch {
			case bm == nil || bm[k] == nil:
				check("HeadObject", resp, 404, "")
			default:
				check("HeadObject", resp, 200, "")
				if resp.Header.Get("Content-Length") != strconv.Itoa(len(bm[k].data)) {
					fail("HeadObject length %s", resp.Header.Get("Content-Length"))
				}
			}
		case op < 62:
			counts["ConditionalGet"]++
			if bm == nil || bm[k] == nil {
				continue
			}
			resp := e.do("GET", path(b, k), nil, "If-None-Match", `"`+bm[k].etag+`"`)
			check("If-None-Match", resp, 304, "")
			resp = e.do("GET", path(b, k), nil, "If-Match", `"0123"`)
			check("If-Match", resp, 412, "PreconditionFailed")
		case op < 72:
			counts["DeleteObject"]++
			resp := e.do("DELETE", path(b, k), nil)
			if bm == nil {
				check("DeleteObject", resp, 404, "NoSuchBucket")
			} else {
				check("DeleteObject", resp, 204, "")
				delete(bm, k)
			}
		case op < 75:
			counts["DeleteObjects"]++
			if bm == nil {
				continue
			}
			var sb strings.Builder
			sb.WriteString("<Delete>")
			var keys []string
			for _, kk := range modelKeys {
				if r.IntN(3) == 0 {
					keys = append(keys, kk)
					fmt.Fprintf(&sb, "<Object><Key>%s</Key></Object>", xmlEscape(kk))
				}
			}
			sb.WriteString("</Delete>")
			if len(keys) == 0 {
				continue
			}
			resp := e.do("POST", "/"+b+"?delete", []byte(sb.String()))
			check("DeleteObjects", resp, 200, "")
			var dr deleteResult
			xml.Unmarshal(resp.body, &dr)
			if len(dr.Deleted) != len(keys) || len(dr.Errors) != 0 {
				fail("DeleteObjects: %s", resp.body)
			}
			for _, kk := range keys {
				delete(bm, kk)
			}
		case op < 80:
			counts["CopyObject"]++
			sb, sk := pick(modelBuckets), pick(modelKeys)
			resp := e.do("PUT", path(b, k), nil, "X-Amz-Copy-Source", "/"+sb+"/"+url.PathEscape(sk))
			src := m.buckets[sb]
			switch {
			case src == nil:
				check("CopyObject", resp, 404, "NoSuchBucket")
			case src[sk] == nil:
				check("CopyObject", resp, 404, "NoSuchKey")
			case sb == b && sk == k:
				check("CopyObject onto itself", resp, 400, "InvalidRequest")
			case bm == nil:
				check("CopyObject", resp, 404, "NoSuchBucket")
			default:
				check("CopyObject", resp, 200, "")
				// "Objects created by the PUT Object, POST Object, or Copy
				// operation [...] have ETags that are an MD5 digest of their
				// object data", even when the source was a multipart upload.
				o := *src[sk]
				o.etag = md5hex(o.data)
				bm[k] = &o
			}
		case op < 92:
			counts["ListObjectsV2"]++
			if bm == nil {
				check("ListObjectsV2", e.do("GET", "/"+b+"?list-type=2", nil), 404, "NoSuchBucket")
				continue
			}
			prefix, delim := pick(modelPrefix), pick(modelDelims)
			page := 1 + r.IntN(5)
			var got []string
			token := ""
			for pages := 0; ; pages++ {
				q := url.Values{"list-type": {"2"}, "prefix": {prefix}, "delimiter": {delim}, "max-keys": {strconv.Itoa(page)}}
				if token != "" {
					q.Set("continuation-token", token)
				}
				resp := e.do("GET", "/"+b+"?"+strings.ReplaceAll(q.Encode(), "+", "%20"), nil)
				check("ListObjectsV2", resp, 200, "")
				var res listBucketResultV2
				if err := xml.Unmarshal(resp.body, &res); err != nil {
					fail("ListObjectsV2 XML: %v", err)
				}
				var items []string
				for _, c := range res.Contents {
					items = append(items, fmt.Sprintf("K:%s:%d:%s", c.Key, c.Size, strings.Trim(c.ETag, `"`)))
				}
				for _, p := range res.CommonPrefixes {
					items = append(items, "P:"+p.Prefix)
				}
				sort.Slice(items, func(i, j int) bool { return listName(items[i]) < listName(items[j]) })
				if res.KeyCount != len(items) || len(items) > page {
					fail("ListObjectsV2 page: KeyCount %d, %d items, max %d", res.KeyCount, len(items), page)
				}
				got = append(got, items...)
				if !res.IsTruncated {
					break
				}
				if pages > 50 {
					fail("ListObjectsV2 does not terminate")
				}
				token = res.NextContinuationToken
			}
			if want := m.listing(b, prefix, delim); strings.Join(got, "|") != strings.Join(want, "|") {
				fail("ListObjectsV2 %s prefix %q delim %q page %d:\n got  %q\n want %q", b, prefix, delim, page, got, want)
			}
		case op < 95:
			counts["Multipart"]++
			resp := e.do("POST", path(b, k)+"?uploads", nil)
			if bm == nil {
				check("CreateMultipartUpload", resp, 404, "NoSuchBucket")
				continue
			}
			check("CreateMultipartUpload", resp, 200, "")
			var init initiateMultipartUploadResult
			xml.Unmarshal(resp.body, &init)
			var parts [][]byte
			if r.IntN(10) == 0 {
				parts = append(parts, bytes.Repeat([]byte{byte(i)}, store.MinPartSize))
			}
			parts = append(parts, randData())
			var cmu strings.Builder
			cmu.WriteString("<CompleteMultipartUpload>")
			for n, p := range parts {
				resp := e.do("PUT", fmt.Sprintf("%s?partNumber=%d&uploadId=%s", path(b, k), n+1, init.UploadID), p)
				check("UploadPart", resp, 200, "")
				fmt.Fprintf(&cmu, "<Part><PartNumber>%d</PartNumber><ETag>%s</ETag></Part>", n+1, resp.Header.Get("ETag"))
			}
			cmu.WriteString("</CompleteMultipartUpload>")
			if r.IntN(4) == 0 {
				check("AbortMultipartUpload", e.do("DELETE", path(b, k)+"?uploadId="+init.UploadID, nil), 204, "")
				continue
			}
			resp = e.do("POST", path(b, k)+"?uploadId="+init.UploadID, []byte(cmu.String()))
			check("CompleteMultipartUpload", resp, 200, "")
			h := md5.New()
			var all []byte
			for _, p := range parts {
				s := md5.Sum(p)
				h.Write(s[:])
				all = append(all, p...)
			}
			etag := hex.EncodeToString(h.Sum(nil)) + "-" + strconv.Itoa(len(parts))
			var res completeMultipartUploadResult
			xml.Unmarshal(resp.body, &res)
			if res.ETag != `"`+etag+`"` {
				fail("multipart ETag %s, want %s", res.ETag, etag)
			}
			bm[k] = &mObject{data: all, etag: etag, meta: map[string]string{}}
		case op < 97:
			counts["ListBuckets"]++
			resp := e.do("GET", "/", nil)
			var lb listAllMyBucketsResult
			xml.Unmarshal(resp.body, &lb)
			var got, want []string
			for _, bb := range lb.Buckets {
				got = append(got, bb.Name)
			}
			for name := range m.buckets {
				want = append(want, name)
			}
			sort.Strings(want)
			if strings.Join(got, ",") != strings.Join(want, ",") {
				fail("ListBuckets %v, want %v", got, want)
			}
		default:
			counts["Restart"]++
			e.restart()
		}
	}
	t.Logf("seed %d: %d operations: %v", seed, ops, counts)
}

func listName(item string) string {
	if strings.HasPrefix(item, "P:") {
		return item[2:]
	}
	s := item[2:]
	// K:key:size:etag — the key may itself contain ':'; strip the last two fields.
	for n := 0; n < 2; n++ {
		s = s[:strings.LastIndex(s, ":")]
	}
	return s
}

func xmlEscape(s string) string {
	var b strings.Builder
	xml.EscapeText(&b, []byte(s))
	return b.String()
}
