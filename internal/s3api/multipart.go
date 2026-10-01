package s3api

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"

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
	if algo != "" && newChecksum(algo) == nil {
		s.writeError(w, r, req, s3err.InvalidRequest.With("Checksum algorithm %s is not supported", algo))
		return
	}
	up, err := s.store.NewMultipartUpload(req.bucket, req.key, opts, algo)
	if err != nil {
		s.writeError(w, r, req, err)
		return
	}
	if algo != "" {
		w.Header().Set("X-Amz-Checksum-Algorithm", algo)
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
		off, length, ok, err = parseRange(v, src.Info.Size)
		if err != nil || !ok || strings.HasPrefix(strings.TrimPrefix(v, "bytes="), "-") {
			s.writeError(w, r, req, s3err.InvalidArgument.With("The x-amz-copy-source-range value must be of the form bytes=first-last where first and last are the zero-based offsets of the first and last bytes to copy"))
			return
		}
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
	if _, err := s.store.GetUpload(req.bucket, req.key, uploadID); err != nil {
		s.writeError(w, r, req, err)
		return
	}
	data, err := s.readSmallBody(r, req)
	if err != nil {
		s.writeError(w, r, req, err)
		return
	}
	var cmu completeMultipartUpload
	if err := readXML(strings.NewReader(string(data)), &cmu); err != nil {
		s.writeError(w, r, req, err)
		return
	}
	parts := make([]store.CompletePart, len(cmu.Parts))
	for i, p := range cmu.Parts {
		parts[i] = store.CompletePart{Number: p.PartNumber, ETag: trimETag(p.ETag)}
	}
	info, err := s.store.CompleteMultipartUpload(r.Context(), req.bucket, req.key, uploadID, parts, cond)
	if err != nil {
		s.writeError(w, r, req, err)
		return
	}
	writeXML(w, http.StatusOK, completeMultipartUploadResult{
		NS: s3NS, Location: fmt.Sprintf("/%s/%s", req.bucket, req.key),
		Bucket: req.bucket, Key: req.key, ETag: `"` + info.ETag + `"`,
	})
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
	for i, p := range res.Parts {
		out.Parts = append(out.Parts, partXML{PartNumber: p.Number, LastModified: iso8601(res.ModTimes[i]),
			ETag: `"` + p.ETag + `"`, Size: p.Size})
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
