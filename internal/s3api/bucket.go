package s3api

import (
	"encoding/base64"
	"errors"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/useless-husband/strata/internal/s3err"
	"github.com/useless-husband/strata/internal/sigv4"
	"github.com/useless-husband/strata/internal/store"
)

// listBuckets implements ListBuckets, with the optional max-buckets,
// continuation-token, prefix and bucket-region parameters.
func (s *Server) listBuckets(w http.ResponseWriter, r *http.Request, req *request) {
	q := r.URL.Query()
	maxBuckets := 10000
	if v := q.Get("max-buckets"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > 10000 {
			s.writeError(w, r, req, s3err.InvalidArgument.With("max-buckets must be an integer between 1 and 10000"))
			return
		}
		maxBuckets = n
	}
	after := ""
	if t := q.Get("continuation-token"); t != "" {
		b, err := base64.RawURLEncoding.DecodeString(t)
		if err != nil {
			s.writeError(w, r, req, s3err.InvalidArgument.With("The continuation token provided is incorrect"))
			return
		}
		after = string(b)
	}
	prefix := q.Get("prefix")
	res := listAllMyBucketsResult{NS: s3NS, Owner: ownerFor(req.auth.AccessKey), Prefix: prefix}
	if region := q.Get("bucket-region"); region != "" && region != s.cfg.Region {
		writeXML(w, http.StatusOK, res)
		return
	}
	for _, b := range s.store.ListBuckets() {
		if b.Name <= after || !strings.HasPrefix(b.Name, prefix) {
			continue
		}
		if len(res.Buckets) == maxBuckets {
			last := res.Buckets[len(res.Buckets)-1].Name
			res.ContinuationToken = base64.RawURLEncoding.EncodeToString([]byte(last))
			break
		}
		res.Buckets = append(res.Buckets, bucketXML{Name: b.Name, CreationDate: iso8601(b.Created), BucketRegion: s.cfg.Region})
	}
	writeXML(w, http.StatusOK, res)
}

// checkCannedACL accepts the only access policy strata has: private.
func checkCannedACL(r *http.Request) error {
	switch r.Header.Get("X-Amz-Acl") {
	case "", "private", "bucket-owner-full-control":
	default:
		return s3err.NotImplemented.With("Only the private canned ACL is supported by strata")
	}
	for name := range r.Header {
		if strings.HasPrefix(strings.ToLower(name), "x-amz-grant-") {
			return s3err.NotImplemented.With("ACL grants are not supported by strata")
		}
	}
	return nil
}

func (s *Server) createBucket(w http.ResponseWriter, r *http.Request, req *request) {
	if err := checkCannedACL(r); err != nil {
		s.writeError(w, r, req, err)
		return
	}
	if strings.EqualFold(r.Header.Get("X-Amz-Bucket-Object-Lock-Enabled"), "true") {
		s.writeError(w, r, req, s3err.NotImplemented.With("Object Lock is not implemented by strata"))
		return
	}
	if r.ContentLength > 0 {
		var cfg createBucketConfiguration
		if err := readXML(r.Body, &cfg); err != nil {
			s.writeError(w, r, req, err)
			return
		}
		if cfg.LocationConstraint != "" && cfg.LocationConstraint != s.cfg.Region {
			s.writeError(w, r, req, s3err.InvalidArgument.With("The %s location constraint is incompatible for the region specific endpoint this request was sent to.", cfg.LocationConstraint))
			return
		}
	}
	if err := s.store.MakeBucket(req.bucket); err != nil {
		// In us-east-1, S3 answers 200 to re-creating a bucket you own
		// (and leaves it as it is); elsewhere it is an error.
		if !(errors.Is(err, s3err.BucketAlreadyOwnedByYou) && s.cfg.Region == "us-east-1") {
			s.writeError(w, r, req, err)
			return
		}
	}
	w.Header().Set("Location", "/"+req.bucket)
	w.WriteHeader(http.StatusOK)
}

func (s *Server) deleteBucket(w http.ResponseWriter, r *http.Request, req *request) {
	if err := s.store.DeleteBucket(req.bucket); err != nil {
		s.writeError(w, r, req, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) headBucket(w http.ResponseWriter, r *http.Request, req *request) {
	if _, err := s.store.HeadBucket(req.bucket); err != nil {
		s.writeError(w, r, req, err)
		return
	}
	w.Header().Set("X-Amz-Bucket-Region", s.cfg.Region)
	w.WriteHeader(http.StatusOK)
}

func (s *Server) getBucketLocation(w http.ResponseWriter, r *http.Request, req *request) {
	if _, err := s.store.HeadBucket(req.bucket); err != nil {
		s.writeError(w, r, req, err)
		return
	}
	loc := s.cfg.Region
	if loc == "us-east-1" {
		loc = "" // S3 reports the original region as an empty constraint
	}
	writeXML(w, http.StatusOK, locationConstraint{NS: s3NS, Value: loc})
}

func (s *Server) getBucketVersioning(w http.ResponseWriter, r *http.Request, req *request) {
	if _, err := s.store.HeadBucket(req.bucket); err != nil {
		s.writeError(w, r, req, err)
		return
	}
	writeXML(w, http.StatusOK, versioningConfiguration{NS: s3NS})
}

// getACL reports the fixed ACL every bucket and object has: the owner has
// full control.
func (s *Server) getACL(w http.ResponseWriter, r *http.Request, req *request) {
	if req.key != "" {
		if _, err := s.store.StatObject(req.bucket, req.key); err != nil {
			s.writeError(w, r, req, err)
			return
		}
	} else if _, err := s.store.HeadBucket(req.bucket); err != nil {
		s.writeError(w, r, req, err)
		return
	}
	own := ownerFor(req.auth.AccessKey)
	var g grant
	g.Grantee.XMLNS = "http://www.w3.org/2001/XMLSchema-instance"
	g.Grantee.Type = "CanonicalUser"
	g.Grantee.ID, g.Grantee.DisplayName = own.ID, own.DisplayName
	g.Permission = "FULL_CONTROL"
	writeXML(w, http.StatusOK, accessControlPolicy{NS: s3NS, Owner: own, Grants: []grant{g}})
}

// putACL accepts setting the private canned ACL (what every resource
// already has) and refuses anything else.
func (s *Server) putACL(w http.ResponseWriter, r *http.Request, req *request) {
	if r.ContentLength > 0 || r.Header.Get("X-Amz-Acl") == "" {
		s.writeError(w, r, req, s3err.NotImplemented.With("Only the private canned ACL is supported by strata"))
		return
	}
	if err := checkCannedACL(r); err != nil {
		s.writeError(w, r, req, err)
		return
	}
	if _, err := s.store.HeadBucket(req.bucket); err != nil {
		s.writeError(w, r, req, err)
		return
	}
	w.WriteHeader(http.StatusOK)
}

// encoder applies encoding-type=url to keys and prefixes in listings.
// Everything but unreserved characters and '/' is percent-encoded, which
// both unquote and unquote_plus decoders (botocore uses the latter) read
// back exactly.
func encoder(q url.Values) (string, func(string) string, error) {
	switch q.Get("encoding-type") {
	case "":
		return "", func(s string) string { return s }, nil
	case "url":
		return "url", func(s string) string { return sigv4.URIEncode(s, true) }, nil
	}
	return "", nil, s3err.InvalidArgument.With("Invalid Encoding Method specified in Request")
}

func parseMaxKeys(q url.Values, name string, def int) (int, error) {
	v := q.Get(name)
	if v == "" {
		return def, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 0 {
		return 0, s3err.InvalidArgument.With("Provided %s not an integer or within integer range", name)
	}
	return min(n, def), nil
}

func (s *Server) listObjectsV2(w http.ResponseWriter, r *http.Request, req *request) {
	q := r.URL.Query()
	enc, encode, err := encoder(q)
	if err != nil {
		s.writeError(w, r, req, err)
		return
	}
	maxKeys, err := parseMaxKeys(q, "max-keys", 1000)
	if err != nil {
		s.writeError(w, r, req, err)
		return
	}
	opt := store.ListOptions{Prefix: q.Get("prefix"), Delimiter: q.Get("delimiter"), After: q.Get("start-after"), MaxKeys: maxKeys}
	token := q.Get("continuation-token")
	if token != "" {
		after, err := base64.RawURLEncoding.DecodeString(token)
		if err != nil || len(after) == 0 {
			s.writeError(w, r, req, s3err.InvalidArgument.With("The continuation token provided is incorrect"))
			return
		}
		opt.After = string(after)
	}
	res, err := s.store.ListObjects(req.bucket, opt)
	if err != nil {
		s.writeError(w, r, req, err)
		return
	}
	out := listBucketResultV2{
		NS: s3NS, Name: req.bucket, Prefix: encode(opt.Prefix), MaxKeys: maxKeys,
		Delimiter: encode(opt.Delimiter), EncodingType: enc, IsTruncated: res.IsTruncated,
		StartAfter: encode(q.Get("start-after")),
		KeyCount:   len(res.Objects) + len(res.CommonPrefixes),
	}
	if q.Has("continuation-token") {
		out.ContinuationToken = &token // echoed even when empty
	}
	if res.IsTruncated {
		out.NextContinuationToken = base64.RawURLEncoding.EncodeToString([]byte(res.NextMarker))
	}
	var own *owner
	if q.Get("fetch-owner") == "true" {
		o := ownerFor(req.auth.AccessKey)
		own = &o
	}
	for _, o := range res.Objects {
		out.Contents = append(out.Contents, objectXML{Key: encode(o.Key), LastModified: iso8601(o.ModTime),
			ETag: `"` + o.ETag + `"`, Size: o.Size, StorageClass: "STANDARD", Owner: own})
	}
	for _, p := range res.CommonPrefixes {
		out.CommonPrefixes = append(out.CommonPrefixes, commonPrefixXML{Prefix: encode(p)})
	}
	writeXML(w, http.StatusOK, out)
}

func (s *Server) listObjectsV1(w http.ResponseWriter, r *http.Request, req *request) {
	q := r.URL.Query()
	enc, encode, err := encoder(q)
	if err != nil {
		s.writeError(w, r, req, err)
		return
	}
	maxKeys, err := parseMaxKeys(q, "max-keys", 1000)
	if err != nil {
		s.writeError(w, r, req, err)
		return
	}
	opt := store.ListOptions{Prefix: q.Get("prefix"), Delimiter: q.Get("delimiter"), After: q.Get("marker"), MaxKeys: maxKeys}
	res, err := s.store.ListObjects(req.bucket, opt)
	if err != nil {
		s.writeError(w, r, req, err)
		return
	}
	// With encoding-type=url S3 encodes Marker, NextMarker, Delimiter and
	// the keys of a v1 listing, but not Prefix (clients decode accordingly).
	out := listBucketResultV1{
		NS: s3NS, Name: req.bucket, Prefix: opt.Prefix, Marker: encode(opt.After), MaxKeys: maxKeys,
		Delimiter: encode(opt.Delimiter), EncodingType: enc, IsTruncated: res.IsTruncated,
	}
	// S3 returns NextMarker only when a delimiter is given; without one,
	// clients continue from the last key.
	if res.IsTruncated && opt.Delimiter != "" {
		out.NextMarker = encode(res.NextMarker)
	}
	own := ownerFor(req.auth.AccessKey)
	for _, o := range res.Objects {
		out.Contents = append(out.Contents, objectXML{Key: encode(o.Key), LastModified: iso8601(o.ModTime),
			ETag: `"` + o.ETag + `"`, Size: o.Size, StorageClass: "STANDARD", Owner: &own})
	}
	for _, p := range res.CommonPrefixes {
		out.CommonPrefixes = append(out.CommonPrefixes, commonPrefixXML{Prefix: encode(p)})
	}
	writeXML(w, http.StatusOK, out)
}

// listObjectVersions lists the current versions: strata does not keep
// object history, so every object has the single version "null", as in an
// S3 bucket where versioning was never enabled.
func (s *Server) listObjectVersions(w http.ResponseWriter, r *http.Request, req *request) {
	q := r.URL.Query()
	enc, encode, err := encoder(q)
	if err != nil {
		s.writeError(w, r, req, err)
		return
	}
	maxKeys, err := parseMaxKeys(q, "max-keys", 1000)
	if err != nil {
		s.writeError(w, r, req, err)
		return
	}
	opt := store.ListOptions{Prefix: q.Get("prefix"), Delimiter: q.Get("delimiter"), After: q.Get("key-marker"), MaxKeys: maxKeys}
	res, err := s.store.ListObjects(req.bucket, opt)
	if err != nil {
		s.writeError(w, r, req, err)
		return
	}
	out := listVersionsResult{NS: s3NS, Name: req.bucket, Prefix: encode(opt.Prefix), KeyMarker: encode(opt.After),
		VersionIDMarker: q.Get("version-id-marker"), MaxKeys: maxKeys, Delimiter: encode(opt.Delimiter),
		EncodingType: enc, IsTruncated: res.IsTruncated}
	if res.IsTruncated {
		out.NextKeyMarker, out.NextVersionIDMarker = encode(res.NextMarker), "null"
	}
	own := ownerFor(req.auth.AccessKey)
	for _, o := range res.Objects {
		out.Versions = append(out.Versions, versionXML{Key: encode(o.Key), VersionID: "null", IsLatest: true,
			LastModified: iso8601(o.ModTime), ETag: `"` + o.ETag + `"`, Size: o.Size, StorageClass: "STANDARD", Owner: own})
	}
	for _, p := range res.CommonPrefixes {
		out.CommonPrefixes = append(out.CommonPrefixes, commonPrefixXML{Prefix: encode(p)})
	}
	writeXML(w, http.StatusOK, out)
}

// maxDeleteObjects is S3's limit on keys per DeleteObjects request.
const maxDeleteObjects = 1000

func (s *Server) deleteObjects(w http.ResponseWriter, r *http.Request, req *request) {
	if _, err := s.store.HeadBucket(req.bucket); err != nil {
		s.writeError(w, r, req, err)
		return
	}
	body, err := s.readSmallBody(r, req, false)
	if err != nil {
		s.writeError(w, r, req, err)
		return
	}
	var dr deleteRequest
	if err := readXML(strings.NewReader(string(body)), &dr); err != nil {
		s.writeError(w, r, req, err)
		return
	}
	if len(dr.Objects) == 0 || len(dr.Objects) > maxDeleteObjects {
		s.writeError(w, r, req, s3err.MalformedXML)
		return
	}
	res := deleteResult{NS: s3NS}
	for _, o := range dr.Objects {
		var err error
		switch {
		case o.VersionID != "" && o.VersionID != "null":
			err = s3err.InvalidArgument.With("Invalid version id specified")
		case len(o.Key) > store.MaxKeyLength:
			err = s3err.KeyTooLong
		default:
			err = s.store.DeleteObjectIf(req.bucket, o.Key, trimETag(o.ETag))
		}
		if err != nil {
			e := s3err.From(err)
			res.Errors = append(res.Errors, deleteErrorXML{Key: o.Key, Code: e.Code, Message: e.Message})
			continue
		}
		if !dr.Quiet {
			res.Deleted = append(res.Deleted, deletedXML{Key: o.Key, VersionID: o.VersionID})
		}
	}
	writeXML(w, http.StatusOK, res)
}
