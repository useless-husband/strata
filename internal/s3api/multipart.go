package s3api

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/useless-husband/strata/internal/checksum"
	"github.com/useless-husband/strata/internal/s3err"
	"github.com/useless-husband/strata/internal/store"
)

func (s *Server) createMultipartUpload(w http.ResponseWriter, r *http.Request, req *request) {
	opts, err := objectAttributes(r)
	if err != nil {
		s.writeError(w, r, req, err)
		return
	}
	algo := strings.ToUpper(r.Header.Get("X-Amz-Checksum-Algorithm"))
	typ := strings.ToUpper(r.Header.Get("X-Amz-Checksum-Type"))
	switch {
	case algo == "" && typ != "":
		s.writeError(w, r, req, s3err.InvalidRequest.With("The x-amz-checksum-type header can only be used with the x-amz-checksum-algorithm header."))
		return
	case algo != "" && !checksum.Valid(algo):
		s.writeError(w, r, req, s3err.InvalidRequest.With("Checksum algorithm %s is not supported", algo))
		return
	case algo != "" && typ == "":
		typ = checksum.DefaultType(algo)
	}
	if algo != "" && !checksum.TypeAllowed(algo, typ) {
		s.writeError(w, r, req, s3err.InvalidRequest.With("The %s checksum type cannot be used with the %s checksum algorithm.", typ, algo))
		return
	}
	up, err := s.store.NewMultipartUpload(req.bucket, req.key, opts, algo, typ)
	if err != nil {
		s.writeError(w, r, req, err)
		return
	}
	if algo != "" {
		w.Header().Set("X-Amz-Checksum-Algorithm", algo)
		w.Header().Set("X-Amz-Checksum-Type", typ)
	}
	writeXML(w, http.StatusOK, initiateMultipartUploadResult{NS: s3NS, Bucket: req.bucket, Key: req.key, UploadID: up.UploadID})
}

func partNumber(r *http.Request) (int, error) {
	n, err := strconv.Atoi(r.URL.Query().Get("partNumber"))
	if err != nil || n < 1 || n > store.MaxPartNumber {
		return 0, s3err.TooManyParts
	}
	return n, nil
}

func (s *Server) uploadPart(w http.ResponseWriter, r *http.Request, req *request) {
	n, err := partNumber(r)
	if err == nil {
		err = checkContentMD5(r)
	}
	if err != nil {
		s.writeError(w, r, req, err)
		return
	}
	uploadID := r.URL.Query().Get("uploadId")
	if _, err := s.store.GetUpload(req.bucket, req.key, uploadID); err != nil {
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
	info, err := s.store.PutObjectPart(r.Context(), req.bucket, req.key, uploadID, n, body,
		store.PartOptions{Precommit: body.precommit(r)})
	if err != nil {
		s.writeError(w, r, req, err)
		return
	}
	w.Header().Set("ETag", `"`+info.ETag+`"`)
	if body.ck != nil {
		w.Header().Set(checksumHeader(body.ckAlgo), info.Checksum)
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Server) uploadPartCopy(w http.ResponseWriter, r *http.Request, req *request) {
	n, err := partNumber(r)
	if err != nil {
		s.writeError(w, r, req, err)
		return
	}
	uploadID := r.URL.Query().Get("uploadId")
	if _, err := s.store.GetUpload(req.bucket, req.key, uploadID); err != nil {
		s.writeError(w, r, req, err)
		return
	}
	src, err := s.openCopySource(r)
	if err != nil {
		s.writeError(w, r, req, err)
		return
	}
	defer src.Close()
	off, length := int64(0), src.Info.Size
	if v := r.Header.Get("X-Amz-Copy-Source-Range"); v != "" {
		var ok bool
		// Unlike a GET range, a copy range must be explicit and inside
		// the source object.
		first, last, ok := parseCopyRange(v)
		if !ok {
			s.writeError(w, r, req, s3err.InvalidArgument.With("The x-amz-copy-source-range value must be of the form bytes=first-last where first and last are the zero-based offsets of the first and last bytes to copy"))
			return
		}
		if last >= src.Info.Size {
			s.writeError(w, r, req, s3err.InvalidRange.With("The requested range is not satisfiable: bytes %d-%d of an object of %d bytes", first, last, src.Info.Size))
			return
		}
		off, length = first, last-first+1
	}
	if length > store.MaxPutSize {
		s.writeError(w, r, req, s3err.InvalidRequest.With("The specified copy range is larger than the maximum part size"))
		return
	}
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	body := streamFrom(ctx, src, off, length)
	defer body.Close()
	info, err := s.store.PutObjectPart(ctx, req.bucket, req.key, uploadID, n, body, store.PartOptions{})
	if err != nil {
		s.writeError(w, r, req, err)
		return
	}
	writeXML(w, http.StatusOK, copyPartResult{NS: s3NS, LastModified: iso8601(src.Info.ModTime), ETag: `"` + info.ETag + `"`})
}

func (s *Server) completeMultipartUpload(w http.ResponseWriter, r *http.Request, req *request) {
	uploadID := r.URL.Query().Get("uploadId")
	cond, err := writeConditions(r)
	if err != nil {
		s.writeError(w, r, req, err)
		return
	}
	if _, err := s.store.HeadBucket(req.bucket); err != nil {
		s.writeError(w, r, req, err)
		return
	}
	// The x-amz-checksum-* header of this request is the checksum of the
	// object being assembled, not of the XML body.
	data, err := s.readSmallBody(r, req, true)
	if err != nil {
		s.writeError(w, r, req, err)
		return
	}
	var cmu completeMultipartUpload
	if err := readXML(strings.NewReader(string(data)), &cmu); err != nil {
		if errors.Is(err, s3err.MissingRequestBody) {
			err = s3err.MalformedXML
		}
		s.writeError(w, r, req, err)
		return
	}
	up, err := s.store.GetUpload(req.bucket, req.key, uploadID)
	if err != nil && !errors.Is(err, s3err.NoSuchUpload) {
		s.writeError(w, r, req, err)
		return
	}
	parts := make([]store.CompletePart, len(cmu.Parts))
	for i, p := range cmu.Parts {
		parts[i] = store.CompletePart{Number: p.PartNumber, ETag: trimETag(p.ETag), Checksum: p.checksum(up.ChecksumAlgorithm)}
	}
	opt := store.CompleteOptions{Conditions: cond}
	if up.ChecksumAlgorithm != "" {
		opt.Checksum = r.Header.Get(checksumHeader(up.ChecksumAlgorithm))
	}
	info, err := s.store.CompleteMultipartUpload(r.Context(), req.bucket, req.key, uploadID, parts, opt)
	if err != nil {
		s.writeError(w, r, req, err)
		return
	}
	res := completeMultipartUploadResult{
		NS: s3NS, Location: fmt.Sprintf("/%s/%s", req.bucket, req.key),
		Bucket: req.bucket, Key: req.key, ETag: `"` + info.ETag + `"`,
	}
	if info.Checksum != "" {
		res.setChecksum(info.ChecksumAlgorithm, info.Checksum)
		res.ChecksumType = info.ChecksumType
	}
	writeXML(w, http.StatusOK, res)
}

func (s *Server) abortMultipartUpload(w http.ResponseWriter, r *http.Request, req *request) {
	if err := s.store.AbortMultipartUpload(req.bucket, req.key, r.URL.Query().Get("uploadId")); err != nil {
		s.writeError(w, r, req, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) listParts(w http.ResponseWriter, r *http.Request, req *request) {
	q := r.URL.Query()
	uploadID := q.Get("uploadId")
	maxParts, err := parseMaxKeys(q, "max-parts", 1000)
	if err != nil {
		s.writeError(w, r, req, err)
		return
	}
	marker := 0
	if v := q.Get("part-number-marker"); v != "" {
		if marker, err = strconv.Atoi(v); err != nil || marker < 0 {
			s.writeError(w, r, req, s3err.InvalidArgument.With("Provided part-number-marker not an integer or within integer range"))
			return
		}
	}
	res, err := s.store.ListParts(req.bucket, req.key, uploadID, marker, maxParts)
	if err != nil {
		s.writeError(w, r, req, err)
		return
	}
	own := ownerFor(req.auth.AccessKey)
	out := listPartsResult{NS: s3NS, Bucket: req.bucket, Key: req.key, UploadID: uploadID, Initiator: own, Owner: own,
		StorageClass: "STANDARD", PartNumberMarker: marker, NextPartNumberMarker: res.NextPartNumberMarker,
		MaxParts: maxParts, IsTruncated: res.IsTruncated}
	up, _ := s.store.GetUpload(req.bucket, req.key, uploadID)
	if up.ChecksumAlgorithm != "" {
		out.ChecksumAlgorithm, out.ChecksumType = up.ChecksumAlgorithm, up.ChecksumType
	}
	for i, p := range res.Parts {
		px := partXML{PartNumber: p.Number, LastModified: iso8601(res.ModTimes[i]), ETag: `"` + p.ETag + `"`, Size: p.Size}
		if p.Checksum != "" {
			px.setChecksum(up.ChecksumAlgorithm, p.Checksum)
		}
		out.Parts = append(out.Parts, px)
	}
	writeXML(w, http.StatusOK, out)
}

func (s *Server) listMultipartUploads(w http.ResponseWriter, r *http.Request, req *request) {
	q := r.URL.Query()
	enc, encode, err := encoder(q)
	if err != nil {
		s.writeError(w, r, req, err)
		return
	}
	maxUploads, err := parseMaxKeys(q, "max-uploads", 1000)
	if err != nil {
		s.writeError(w, r, req, err)
		return
	}
	opt := store.ListUploadsOptions{Prefix: q.Get("prefix"), Delimiter: q.Get("delimiter"),
		KeyMarker: q.Get("key-marker"), UploadIDMarker: q.Get("upload-id-marker"), MaxUploads: maxUploads}
	res, err := s.store.ListMultipartUploads(req.bucket, opt)
	if err != nil {
		s.writeError(w, r, req, err)
		return
	}
	own := ownerFor(req.auth.AccessKey)
	out := listMultipartUploadsResult{NS: s3NS, Bucket: req.bucket, KeyMarker: encode(opt.KeyMarker),
		UploadIDMarker: opt.UploadIDMarker, NextKeyMarker: encode(res.NextKeyMarker), NextUploadIDMarker: res.NextUploadIDMarker,
		Delimiter: encode(opt.Delimiter), Prefix: encode(opt.Prefix), EncodingType: enc, MaxUploads: maxUploads, IsTruncated: res.IsTruncated}
	for _, u := range res.Uploads {
		out.Uploads = append(out.Uploads, uploadXML{Key: encode(u.Key), UploadID: u.UploadID, Initiator: own, Owner: own,
			StorageClass: "STANDARD", Initiated: iso8601(u.Initiated)})
	}
	for _, p := range res.CommonPrefixes {
		out.CommonPrefixes = append(out.CommonPrefixes, commonPrefixXML{Prefix: encode(p)})
	}
	writeXML(w, http.StatusOK, out)
}

// parseCopyRange parses x-amz-copy-source-range: "bytes=first-last".
func parseCopyRange(v string) (first, last int64, ok bool) {
	spec, found := strings.CutPrefix(strings.TrimSpace(v), "bytes=")
	if !found {
		return 0, 0, false
	}
	a, b, found := strings.Cut(spec, "-")
	if !found {
		return 0, 0, false
	}
	first, err1 := strconv.ParseInt(a, 10, 64)
	last, err2 := strconv.ParseInt(b, 10, 64)
	if err1 != nil || err2 != nil || first < 0 || last < first {
		return 0, 0, false
	}
	return first, last, true
}
