// Package s3api serves the Amazon S3 REST API on top of the store.
package s3api

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/useless-husband/strata/internal/s3err"
	"github.com/useless-husband/strata/internal/sigv4"
	"github.com/useless-husband/strata/internal/store"
)

// Config configures a Server.
type Config struct {
	Store  *store.Store
	Region string
	// Credentials maps access keys to secret keys. Every key has full
	// access; strata has no IAM or bucket policies.
	Credentials map[string]string
	// Domains enables virtual-hosted-style requests: a Host of
	// "<bucket>.<domain>" addresses the bucket.
	Domains     []string
	Logger      *slog.Logger
	LogRequests bool
	Version     string
	Now         func() time.Time // for tests
}

// Server is an http.Handler implementing the S3 API.
type Server struct {
	cfg      Config
	store    *store.Store
	verifier *sigv4.Verifier
	metrics  *metrics
	log      *slog.Logger
	hostID   string
}

// New returns a Server.
func New(cfg Config) *Server {
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.DiscardHandler)
	}
	if cfg.Region == "" {
		cfg.Region = "us-east-1"
	}
	s := &Server{cfg: cfg, store: cfg.Store, log: cfg.Logger, metrics: newMetrics()}
	s.verifier = &sigv4.Verifier{
		Region: cfg.Region,
		Secret: func(ak string) (string, bool) {
			sk, ok := cfg.Credentials[ak]
			return sk, ok
		},
		Now:     cfg.Now,
		Domains: cfg.Domains,
	}
	h := sha256.Sum256([]byte("strata:" + cfg.Store.Deployment()))
	s.hostID = hex.EncodeToString(h[:])
	return s
}

func ownerFor(accessKey string) owner {
	h := sha256.Sum256([]byte("strata-owner:" + accessKey))
	return owner{ID: hex.EncodeToString(h[:]), DisplayName: accessKey}
}

// request is the per-request state shared by the handlers.
type request struct {
	id     string
	api    string
	bucket string
	key    string
	auth   *sigv4.Auth
	start  time.Time
}

type ctxKey struct{}

// statusWriter records the status and size of a response.
type statusWriter struct {
	http.ResponseWriter
	status  int
	written int64
}

func (w *statusWriter) WriteHeader(code int) {
	if w.status == 0 {
		w.status = code
	}
	w.ResponseWriter.WriteHeader(code)
}

func (w *statusWriter) Write(p []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	n, err := w.ResponseWriter.Write(p)
	w.written += int64(n)
	return n, err
}

func (w *statusWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

func newRequestID() string {
	var b [8]byte
	rand.Read(b[:])
	return strings.ToUpper(hex.EncodeToString(b[:]))
}

// countingReader counts request body bytes for the metrics.
type countingReader struct {
	r interface {
		Read([]byte) (int, error)
		Close() error
	}
	n int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}

func (c *countingReader) Close() error { return c.r.Close() }

// ServeHTTP handles one request.
func (s *Server) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	req := &request{id: newRequestID(), start: time.Now(), api: "Unknown"}
	w := &statusWriter{ResponseWriter: rw}
	h := w.Header()
	h.Set("X-Amz-Request-Id", req.id)
	h.Set("X-Amz-Id-2", s.hostID)
	h.Set("Server", "strata")
	body := &countingReader{r: r.Body}
	r.Body = body
	s.metrics.inflight.Add(1)
	defer func() {
		s.metrics.inflight.Add(-1)
		if p := recover(); p != nil {
			if p == http.ErrAbortHandler {
				s.finish(req, w, body.n, r, "aborted")
				panic(p)
			}
			s.log.Error("panic serving request", "id", req.id, "panic", fmt.Sprint(p))
			if w.status == 0 {
				s.writeError(w, r, req, s3err.InternalError)
			}
		}
		s.finish(req, w, body.n, r, "")
	}()
	r = r.WithContext(context.WithValue(r.Context(), ctxKey{}, req))
	s.route(w, r, req)
}

func (s *Server) finish(req *request, w *statusWriter, in int64, r *http.Request, note string) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	d := time.Since(req.start)
	s.metrics.observe(req.api, w.status, d, in, w.written)
	if s.cfg.LogRequests {
		attrs := []any{"id", req.id, "api", req.api, "method", r.Method, "bucket", req.bucket, "key", req.key,
			"status", w.status, "in", in, "out", w.written, "ms", float64(d.Microseconds()) / 1000, "remote", remoteIP(r)}
		if req.auth != nil && (r.Method == http.MethodPut || r.Method == http.MethodPost) {
			attrs = append(attrs, "payload", req.auth.Mode.String())
		}
		if note != "" {
			attrs = append(attrs, "note", note)
		}
		s.log.Info("request", attrs...)
	}
}

func remoteIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

// writeError sends an S3 error response. HEAD responses carry no body.
func (s *Server) writeError(w http.ResponseWriter, r *http.Request, req *request, err error) {
	e := s3err.From(err)
	if e == s3err.InternalError && !errors.Is(err, s3err.InternalError) {
		s.log.Error("internal error", "id", req.id, "api", req.api, "bucket", req.bucket, "key", req.key, "err", err)
	} else if errors.Is(err, s3err.InternalError) || errors.Is(err, s3err.SlowDown) {
		s.log.Warn("request failed", "id", req.id, "api", req.api, "bucket", req.bucket, "key", req.key, "err", err)
	}
	if e.Status == http.StatusNotModified {
		w.WriteHeader(http.StatusNotModified)
		return
	}
	if r.Method == http.MethodHead {
		w.WriteHeader(e.Status)
		return
	}
	resource := r.URL.Path
	writeXML(w, e.Status, errorResponse{
		Code: e.Code, Message: e.Message, Key: req.key, BucketName: req.bucket,
		Resource: resource, RequestID: req.id, HostID: s.hostID,
	})
}

// splitPath extracts the bucket and key from a request, path-style or
// virtual-hosted-style.
func (s *Server) splitPath(r *http.Request) (bucket, key string) {
	host := r.Host
	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	}
	for _, d := range s.cfg.Domains {
		if b, ok := strings.CutSuffix(host, "."+d); ok && b != "" {
			return b, strings.TrimPrefix(r.URL.Path, "/")
		}
	}
	p := strings.TrimPrefix(r.URL.Path, "/")
	bucket, key, _ = strings.Cut(p, "/")
	return bucket, key
}

// subresources that strata does not implement. A request naming one gets
// NotImplemented rather than being mistaken for a plain object operation.
var unsupported = []string{"policy", "cors", "lifecycle", "tagging", "website", "logging", "notification",
	"replication", "encryption", "object-lock", "retention", "legal-hold", "requestPayment", "accelerate",
	"inventory", "analytics", "metrics", "intelligent-tiering", "ownershipControls", "publicAccessBlock",
	"torrent", "restore", "select", "attributes", "policyStatus", "renameObject"}

func (s *Server) route(w http.ResponseWriter, r *http.Request, req *request) {
	if strings.HasPrefix(r.URL.Path, "/-/") {
		s.serveInternal(w, r, req)
		return
	}
	req.bucket, req.key = s.splitPath(r)
	q := r.URL.Query()

	auth, err := s.verifier.Verify(r)
	if err != nil {
		req.api = "Auth"
		s.writeError(w, r, req, err)
		return
	}
	req.auth = auth
	if len(req.key) > store.MaxKeyLength {
		req.api = "Validate"
		s.writeError(w, r, req, s3err.KeyTooLong)
		return
	}

	for _, sub := range unsupported {
		if q.Has(sub) {
			req.api = "Unsupported"
			s.writeError(w, r, req, s3err.NotImplemented.With("The %s subresource is not implemented by strata", sub))
			return
		}
	}

	switch {
	case req.bucket == "":
		if r.Method == http.MethodGet {
			req.api = "ListBuckets"
			s.listBuckets(w, r, req)
			return
		}
	case req.key == "":
		s.routeBucket(w, r, req, q)
		return
	default:
		s.routeObject(w, r, req, q)
		return
	}
	req.api = "Unknown"
	s.writeError(w, r, req, s3err.MethodNotAllowed)
}

func (s *Server) routeBucket(w http.ResponseWriter, r *http.Request, req *request, q url.Values) {
	switch r.Method {
	case http.MethodGet:
		switch {
		case q.Has("location"):
			req.api = "GetBucketLocation"
			s.getBucketLocation(w, r, req)
		case q.Has("versioning"):
			req.api = "GetBucketVersioning"
			s.getBucketVersioning(w, r, req)
		case q.Has("acl"):
			req.api = "GetBucketAcl"
			s.getACL(w, r, req)
		case q.Has("uploads"):
			req.api = "ListMultipartUploads"
			s.listMultipartUploads(w, r, req)
		case q.Has("versions"):
			req.api = "ListObjectVersions"
			s.listObjectVersions(w, r, req)
		case q.Get("list-type") == "2":
			req.api = "ListObjectsV2"
			s.listObjectsV2(w, r, req)
		default:
			req.api = "ListObjects"
			s.listObjectsV1(w, r, req)
		}
	case http.MethodPut:
		switch {
		case q.Has("versioning"):
			req.api = "PutBucketVersioning"
			s.writeError(w, r, req, s3err.NotImplemented.With("Object versioning is not implemented by strata"))
		case q.Has("acl"):
			req.api = "PutBucketAcl"
			s.putACL(w, r, req)
		default:
			req.api = "CreateBucket"
			s.createBucket(w, r, req)
		}
	case http.MethodDelete:
		req.api = "DeleteBucket"
		s.deleteBucket(w, r, req)
	case http.MethodHead:
		req.api = "HeadBucket"
		s.headBucket(w, r, req)
	case http.MethodPost:
		if q.Has("delete") {
			req.api = "DeleteObjects"
			s.deleteObjects(w, r, req)
			return
		}
		req.api = "PostObject"
		s.writeError(w, r, req, s3err.NotImplemented.With("Browser-based POST uploads are not implemented by strata"))
	default:
		s.writeError(w, r, req, s3err.MethodNotAllowed)
	}
}

func (s *Server) routeObject(w http.ResponseWriter, r *http.Request, req *request, q url.Values) {
	switch r.Method {
	case http.MethodGet:
		switch {
		case q.Has("uploadId"):
			req.api = "ListParts"
			s.listParts(w, r, req)
		case q.Has("acl"):
			req.api = "GetObjectAcl"
			s.getACL(w, r, req)
		default:
			req.api = "GetObject"
			s.getObject(w, r, req, false)
		}
	case http.MethodHead:
		req.api = "HeadObject"
		s.getObject(w, r, req, true)
	case http.MethodPut:
		switch {
		case q.Has("partNumber") && q.Has("uploadId"):
			if r.Header.Get("X-Amz-Copy-Source") != "" {
				req.api = "UploadPartCopy"
				s.uploadPartCopy(w, r, req)
			} else {
				req.api = "UploadPart"
				s.uploadPart(w, r, req)
			}
		case q.Has("acl"):
			req.api = "PutObjectAcl"
			s.putACL(w, r, req)
		case r.Header.Get("X-Amz-Copy-Source") != "":
			req.api = "CopyObject"
			s.copyObject(w, r, req)
		default:
			req.api = "PutObject"
			s.putObject(w, r, req)
		}
	case http.MethodDelete:
		if q.Has("uploadId") {
			req.api = "AbortMultipartUpload"
			s.abortMultipartUpload(w, r, req)
			return
		}
		req.api = "DeleteObject"
		s.deleteObject(w, r, req)
	case http.MethodPost:
		switch {
		case q.Has("uploads"):
			req.api = "CreateMultipartUpload"
			s.createMultipartUpload(w, r, req)
		case q.Has("uploadId"):
			req.api = "CompleteMultipartUpload"
			s.completeMultipartUpload(w, r, req)
		default:
			req.api = "PostObject"
			s.writeError(w, r, req, s3err.MethodNotAllowed)
		}
	default:
		s.writeError(w, r, req, s3err.MethodNotAllowed)
	}
}
