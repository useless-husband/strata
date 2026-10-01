package s3api

import (
	"context"
	"crypto/md5"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/useless-husband/strata/internal/s3err"
	"github.com/useless-husband/strata/internal/store"
)

// maxUserMetadata is S3's limit on the size of user metadata (names and
// values of the x-amz-meta-* headers).
const maxUserMetadata = 2 << 10

// storedHeaders are the standard headers kept with an object and returned
// on GET and HEAD.
var storedHeaders = []string{"Cache-Control", "Content-Disposition", "Content-Encoding", "Content-Language", "Expires"}

var storageClasses = map[string]bool{"": true, "STANDARD": true, "REDUCED_REDUNDANCY": true, "STANDARD_IA": true,
	"ONEZONE_IA": true, "INTELLIGENT_TIERING": true, "GLACIER": true, "DEEP_ARCHIVE": true, "GLACIER_IR": true, "OUTPOSTS": true, "EXPRESS_ONEZONE": true}

// objectAttributes extracts content type, user metadata and the stored
// standard headers of a PUT, copy or multipart creation.
func objectAttributes(r *http.Request) (store.PutOptions, error) {
	var opts store.PutOptions
	if !storageClasses[r.Header.Get("X-Amz-Storage-Class")] {
		return opts, s3err.InvalidStorageClass
	}
	for _, h := range []string{"X-Amz-Server-Side-Encryption", "X-Amz-Server-Side-Encryption-Customer-Algorithm",
		"X-Amz-Object-Lock-Mode", "X-Amz-Object-Lock-Retain-Until-Date", "X-Amz-Object-Lock-Legal-Hold",
		"X-Amz-Tagging", "X-Amz-Website-Redirect-Location"} {
		if r.Header.Get(h) != "" {
			return opts, s3err.NotImplemented.With("The %s header is not supported by strata", strings.ToLower(h))
		}
	}
	if err := checkCannedACL(r); err != nil {
		return opts, err
	}
	opts.ContentType = r.Header.Get("Content-Type")
	size := 0
	for name, vals := range r.Header {
		l := strings.ToLower(name)
		if m, ok := strings.CutPrefix(l, "x-amz-meta-"); ok {
			if opts.UserMeta == nil {
				opts.UserMeta = map[string]string{}
			}
			v := strings.Join(vals, ",")
			opts.UserMeta[m] = v
			size += len(m) + len(v)
		}
	}
	if size > maxUserMetadata {
		return opts, s3err.MetadataTooLarge
	}
	for _, h := range storedHeaders {
		v := r.Header.Get(h)
		if h == "Content-Encoding" {
			v = stripAWSChunked(v)
		}
		if v != "" {
			if opts.Headers == nil {
				opts.Headers = map[string]string{}
			}
			opts.Headers[h] = v
		}
	}
	return opts, nil
}

// stripAWSChunked removes the aws-chunked transfer coding from a
// Content-Encoding value; it describes the request, not the object.
func stripAWSChunked(v string) string {
	var keep []string
	for _, p := range strings.Split(v, ",") {
		if p = strings.TrimSpace(p); p != "" && p != "aws-chunked" {
			keep = append(keep, p)
		}
	}
	return strings.Join(keep, ",")
}

func trimETag(s string) string {
	s = strings.TrimSpace(s)
	s = strings.TrimPrefix(s, "W/")
	return strings.Trim(s, `"`)
}

// writeConditions reads If-Match / If-None-Match on PUT and
// CompleteMultipartUpload.
func writeConditions(r *http.Request) (store.Conditions, error) {
	var c store.Conditions
	if v := r.Header.Get("If-None-Match"); v != "" {
		if strings.TrimSpace(v) != "*" {
			return c, s3err.NotImplemented.With("If-None-Match on writes only supports *")
		}
		c.IfNoneMatch = true
	}
	if v := r.Header.Get("If-Match"); v != "" {
		c.IfMatch = trimETag(v)
		if strings.TrimSpace(v) == "*" {
			c.IfMatch = "*"
		}
	}
	return c, nil
}

func (s *Server) putObject(w http.ResponseWriter, r *http.Request, req *request) {
	opts, err := objectAttributes(r)
	if err == nil {
		err = checkContentMD5(r)
	}
	if err == nil {
		opts.Conditions, err = writeConditions(r)
	}
	if err != nil {
		s.writeError(w, r, req, err)
		return
	}
	body, err := newBodyReader(r, req.auth)
	if err != nil {
		s.writeError(w, r, req, err)
		return
	}
	if body.size > store.MaxPutSize {
		s.writeError(w, r, req, s3err.EntityTooLarge)
		return
	}
	opts.Precommit = body.precommit(r)
	info, err := s.store.PutObject(r.Context(), req.bucket, req.key, body, opts)
	if err != nil {
		s.writeError(w, r, req, err)
		return
	}
	w.Header().Set("ETag", `"`+info.ETag+`"`)
	if info.Checksum != "" {
		w.Header().Set(checksumHeader(info.ChecksumAlgorithm), info.Checksum)
		w.Header().Set("X-Amz-Checksum-Type", info.ChecksumType)
	}
	w.WriteHeader(http.StatusOK)
}

// readSmallBody reads and verifies a small request body (XML documents).
func (s *Server) readSmallBody(r *http.Request, req *request) ([]byte, error) {
	if err := checkContentMD5(r); err != nil {
		return nil, err
	}
	br, err := newBodyReader(r, req.auth)
	if err != nil {
		return nil, err
	}
	if br.size > maxXMLBody {
		return nil, s3err.MalformedXML.With("The XML you provided was larger than the maximum allowed")
	}
	data, err := io.ReadAll(br)
	if err != nil {
		return nil, err
	}
	if want := r.Header.Get("Content-MD5"); want != "" {
		sum := md5.Sum(data)
		if base64.StdEncoding.EncodeToString(sum[:]) != want {
			return nil, s3err.BadDigest
		}
	}
	return data, nil
}

// --- Conditional requests (RFC 9110 section 13.2.2, as S3 applies it) ---

func etagMatches(header, etag string) bool {
	for _, t := range strings.Split(header, ",") {
		t = strings.TrimSpace(t)
		if t == "*" || trimETag(t) == etag {
			return true
		}
	}
	return false
}

// checkPreconditions evaluates If-Match, If-Unmodified-Since,
// If-None-Match and If-Modified-Since in the RFC's order. Times compare at
// one-second resolution, the resolution of Last-Modified.
func checkPreconditions(h http.Header, info store.ObjectInfo, get bool) error {
	mod := info.ModTime.Truncate(time.Second)
	if v := h.Get("If-Match"); v != "" {
		if !etagMatches(v, info.ETag) {
			return s3err.PreconditionFailed
		}
	} else if v := h.Get("If-Unmodified-Since"); v != "" {
		if t, err := http.ParseTime(v); err == nil && mod.After(t) {
			return s3err.PreconditionFailed
		}
	}
	if v := h.Get("If-None-Match"); v != "" {
		if etagMatches(v, info.ETag) {
			if get {
				return s3err.NotModified
			}
			return s3err.PreconditionFailed
		}
	} else if v := h.Get("If-Modified-Since"); v != "" {
		if t, err := http.ParseTime(v); err == nil && !mod.After(t) {
			if get {
				return s3err.NotModified
			}
			return s3err.PreconditionFailed
		}
	}
	return nil
}

// copySourcePreconditions evaluates the x-amz-copy-source-if-* headers;
// any failure is 412.
func copySourcePreconditions(h http.Header, info store.ObjectInfo) error {
	cond := http.Header{}
	for _, n := range []string{"Match", "None-Match", "Modified-Since", "Unmodified-Since"} {
		if v := h.Get("X-Amz-Copy-Source-If-" + n); v != "" {
			cond.Set("If-"+n, v)
		}
	}
	if err := checkPreconditions(cond, info, true); err != nil {
		return s3err.PreconditionFailed
	}
	return nil
}

// parseRange parses a single byte range. ok is false when the header is
// absent or not a single valid byte range (S3 then ignores it).
func parseRange(v string, size int64) (off, n int64, ok bool, err error) {
	spec, found := strings.CutPrefix(v, "bytes=")
	if v == "" || !found || strings.Contains(spec, ",") {
		return 0, 0, false, nil
	}
	a, b, found := strings.Cut(strings.TrimSpace(spec), "-")
	if !found {
		return 0, 0, false, nil
	}
	switch {
	case a == "":
		suffix, perr := strconv.ParseInt(b, 10, 64)
		if perr != nil || suffix < 0 {
			return 0, 0, false, nil
		}
		if suffix == 0 {
			return 0, 0, false, s3err.InvalidRange
		}
		suffix = min(suffix, size)
		return size - suffix, suffix, true, nil
	default:
		start, perr := strconv.ParseInt(a, 10, 64)
		if perr != nil || start < 0 {
			return 0, 0, false, nil
		}
		end := size - 1
		if b != "" {
			e, perr := strconv.ParseInt(b, 10, 64)
			if perr != nil || e < start {
				return 0, 0, false, nil
			}
			end = min(e, size-1)
		}
		if start >= size {
			return 0, 0, false, s3err.InvalidRange
		}
		return start, end - start + 1, true, nil
	}
}

// setObjectHeaders writes the stored attributes of an object.
func setObjectHeaders(h http.Header, info store.ObjectInfo) {
	h.Set("ETag", `"`+info.ETag+`"`)
	h.Set("Last-Modified", info.ModTime.UTC().Format(http.TimeFormat))
	ct := info.ContentType
	if ct == "" {
		ct = "binary/octet-stream"
	}
	h.Set("Content-Type", ct)
	h.Set("Accept-Ranges", "bytes")
	for k, v := range info.UserMeta {
		// Lowercase, as S3 sends it: clients keep the name as received.
		h["x-amz-meta-"+k] = []string{v}
	}
	for k, v := range info.Headers {
		h.Set(k, v)
	}
}

// responseOverrides are the query parameters a signed GET may use to
// override response headers.
var responseOverrides = map[string]string{
	"response-content-type":        "Content-Type",
	"response-content-language":    "Content-Language",
	"response-expires":             "Expires",
	"response-cache-control":       "Cache-Control",
	"response-content-disposition": "Content-Disposition",
	"response-content-encoding":    "Content-Encoding",
}

func (s *Server) getObject(w http.ResponseWriter, r *http.Request, req *request, head bool) {
	q := r.URL.Query()
	if v := q.Get("versionId"); v != "" && v != "null" {
		s.writeError(w, r, req, s3err.InvalidArgument.With("Invalid version id specified"))
		return
	}
	o, err := s.store.OpenObject(req.bucket, req.key)
	if err != nil {
		s.writeError(w, r, req, err)
		return
	}
	defer o.Close()
	info := o.Info
	h := w.Header()
	if err := checkPreconditions(r.Header, info, true); err != nil {
		if errors.Is(err, s3err.NotModified) {
			h.Set("ETag", `"`+info.ETag+`"`)
			h.Set("Last-Modified", info.ModTime.UTC().Format(http.TimeFormat))
		}
		s.writeError(w, r, req, err)
		return
	}

	off, n := int64(0), info.Size
	partial := false
	if pn := q.Get("partNumber"); pn != "" {
		num, err := strconv.Atoi(pn)
		if err != nil || num < 1 || num > store.MaxPartNumber {
			s.writeError(w, r, req, s3err.InvalidArgument.With("Part number must be an integer between 1 and 10000, inclusive"))
			return
		}
		if num > len(info.Parts) {
			s.writeError(w, r, req, s3err.InvalidRange.With("The requested partnumber is not satisfiable"))
			return
		}
		for _, p := range info.Parts[:num-1] {
			off += p.Size
		}
		n = info.Parts[num-1].Size
		partial = len(info.Parts) > 1 || info.Parts[0].Number != 1
		if len(info.Parts) > 1 {
			h.Set("X-Amz-Mp-Parts-Count", strconv.Itoa(len(info.Parts)))
		}
	} else if v := r.Header.Get("Range"); v != "" {
		var ok bool
		off, n, ok, err = parseRange(v, info.Size)
		if err != nil {
			h.Set("Content-Range", fmt.Sprintf("bytes */%d", info.Size))
			s.writeError(w, r, req, err)
			return
		}
		if !ok {
			off, n = 0, info.Size
		}
		partial = ok
	}

	setObjectHeaders(h, info)
	for p, hdr := range responseOverrides {
		if v := q.Get(p); v != "" {
			h.Set(hdr, v)
		}
	}
	if info.Checksum != "" && !partial && strings.EqualFold(r.Header.Get("X-Amz-Checksum-Mode"), "ENABLED") {
		h.Set(checksumHeader(info.ChecksumAlgorithm), info.Checksum)
		h.Set("X-Amz-Checksum-Type", info.ChecksumType)
	}
	h.Set("Content-Length", strconv.FormatInt(n, 10))
	status := http.StatusOK
	if partial {
		status = http.StatusPartialContent
		h.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", off, off+n-1, info.Size))
	}
	w.WriteHeader(status)
	if head || n == 0 {
		return
	}
	if err := o.WriteRange(r.Context(), w, off, n); err != nil {
		if !errors.Is(err, context.Canceled) {
			s.log.Error("read failed after the response started", "id", req.id, "bucket", req.bucket, "key", req.key, "err", err)
		}
		// The status line is gone; cut the connection so the client sees
		// a short body rather than a complete-looking wrong one.
		panic(http.ErrAbortHandler)
	}
}

func (s *Server) deleteObject(w http.ResponseWriter, r *http.Request, req *request) {
	if v := r.URL.Query().Get("versionId"); v != "" && v != "null" {
		s.writeError(w, r, req, s3err.InvalidArgument.With("Invalid version id specified"))
		return
	}
	if err := s.store.DeleteObject(req.bucket, req.key); err != nil {
		s.writeError(w, r, req, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// parseCopySource parses x-amz-copy-source: "[/]bucket/key[?versionId=…]",
// URL-encoded.
func parseCopySource(v string) (bucket, key string, err error) {
	src, version, _ := strings.Cut(v, "?")
	src, perr := url.PathUnescape(src)
	if perr != nil {
		return "", "", s3err.InvalidArgument.With("Copy Source must mention the source bucket and key: sourcebucket/sourcekey")
	}
	src = strings.TrimPrefix(src, "/")
	bucket, key, ok := strings.Cut(src, "/")
	if !ok || bucket == "" || key == "" {
		return "", "", s3err.InvalidArgument.With("Copy Source must mention the source bucket and key: sourcebucket/sourcekey")
	}
	if vid, ok := strings.CutPrefix(version, "versionId="); ok && vid != "null" {
		return "", "", s3err.InvalidArgument.With("Invalid version id specified")
	}
	return bucket, key, nil
}

// openCopySource opens the source of a copy and checks its conditions.
func (s *Server) openCopySource(r *http.Request) (*store.Object, error) {
	sb, sk, err := parseCopySource(r.Header.Get("X-Amz-Copy-Source"))
	if err != nil {
		return nil, err
	}
	o, err := s.store.OpenObject(sb, sk)
	if err != nil {
		return nil, err
	}
	if err := copySourcePreconditions(r.Header, o.Info); err != nil {
		o.Close()
		return nil, err
	}
	return o, nil
}

// streamFrom returns a reader of bytes [off, off+n) of an open object.
func streamFrom(ctx context.Context, o *store.Object, off, n int64) io.ReadCloser {
	pr, pw := io.Pipe()
	go func() {
		pw.CloseWithError(o.WriteRange(ctx, pw, off, n))
	}()
	return pr
}

func (s *Server) copyObject(w http.ResponseWriter, r *http.Request, req *request) {
	src, err := s.openCopySource(r)
	if err != nil {
		s.writeError(w, r, req, err)
		return
	}
	defer src.Close()
	if src.Info.Size > store.MaxPutSize {
		s.writeError(w, r, req, s3err.InvalidRequest.With("The specified copy source is larger than the maximum allowable size for a copy source: %d", int64(store.MaxPutSize)))
		return
	}
	directive := r.Header.Get("X-Amz-Metadata-Directive")
	var opts store.PutOptions
	switch directive {
	case "", "COPY":
		opts = store.PutOptions{ContentType: src.Info.ContentType, UserMeta: src.Info.UserMeta, Headers: src.Info.Headers}
		if !storageClasses[r.Header.Get("X-Amz-Storage-Class")] {
			s.writeError(w, r, req, s3err.InvalidStorageClass)
			return
		}
		if src.Info.Bucket == req.bucket && src.Info.Key == req.key {
			s.writeError(w, r, req, s3err.InvalidRequest.With("This copy request is illegal because it is trying to copy an object to itself without changing the object's metadata, storage class, website redirect location or encryption attributes."))
			return
		}
	case "REPLACE":
		if opts, err = objectAttributes(r); err != nil {
			s.writeError(w, r, req, err)
			return
		}
	default:
		s.writeError(w, r, req, s3err.InvalidArgument.With("Unknown metadata directive."))
		return
	}
	if opts.Conditions, err = writeConditions(r); err != nil {
		s.writeError(w, r, req, err)
		return
	}
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	body := streamFrom(ctx, src, 0, src.Info.Size)
	defer body.Close()
	info, err := s.store.PutObject(ctx, req.bucket, req.key, body, opts)
	if err != nil {
		s.writeError(w, r, req, err)
		return
	}
	writeXML(w, http.StatusOK, copyObjectResult{NS: s3NS, LastModified: iso8601(info.ModTime), ETag: `"` + info.ETag + `"`})
}
