// Package sigv4 implements AWS Signature Version 4 as Amazon S3 uses it:
// verification of header-signed and presigned requests, decoding of
// aws-chunked streaming bodies with per-chunk and trailer signatures, and a
// signer for strata's own clients and tests.
//
// S3 differs from other AWS services in two details handled here: the path
// is URI-encoded once and never normalised, and the payload hash comes from
// the x-amz-content-sha256 header, which may name a streaming mode instead of
// a hash.
package sigv4

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Algorithm is the only signing algorithm supported.
const Algorithm = "AWS4-HMAC-SHA256"

// Special values of x-amz-content-sha256.
const (
	UnsignedPayload          = "UNSIGNED-PAYLOAD"
	StreamingPayload         = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"
	StreamingPayloadTrailer  = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER"
	StreamingUnsignedTrailer = "STREAMING-UNSIGNED-PAYLOAD-TRAILER"
	streamingECDSA           = "STREAMING-AWS4-ECDSA-P256-SHA256-PAYLOAD"
	streamingECDSATrailer    = "STREAMING-AWS4-ECDSA-P256-SHA256-PAYLOAD-TRAILER"
)

// EmptySHA256 is the hex SHA-256 of the empty string.
const EmptySHA256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

const (
	timeFormat  = "20060102T150405Z"
	shortFormat = "20060102"
)

func hmacSHA256(key []byte, data string) []byte {
	h := hmac.New(sha256.New, key)
	h.Write([]byte(data))
	return h.Sum(nil)
}

func sha256Hex(s string) string {
	h := sha256.Sum256([]byte(s))
	return hex.EncodeToString(h[:])
}

// SigningKey derives the key for one day, region and service.
func SigningKey(secret, date, region, service string) []byte {
	k := hmacSHA256([]byte("AWS4"+secret), date)
	k = hmacSHA256(k, region)
	k = hmacSHA256(k, service)
	return hmacSHA256(k, "aws4_request")
}

// scope is the credential scope "date/region/service/aws4_request".
func scope(date, region, service string) string {
	return date + "/" + region + "/" + service + "/aws4_request"
}

// URIEncode percent-encodes s as SigV4 requires: every byte except the
// unreserved characters A-Z a-z 0-9 - . _ ~ is encoded, with uppercase hex.
// If keepSlash is set, '/' is left alone (for paths).
func URIEncode(s string, keepSlash bool) string {
	const hexUpper = "0123456789ABCDEF"
	var b strings.Builder
	b.Grow(len(s) + 16)
	for i := 0; i < len(s); i++ {
		c := s[i]
		if 'A' <= c && c <= 'Z' || 'a' <= c && c <= 'z' || '0' <= c && c <= '9' ||
			c == '-' || c == '.' || c == '_' || c == '~' || (c == '/' && keepSlash) {
			b.WriteByte(c)
			continue
		}
		b.WriteByte('%')
		b.WriteByte(hexUpper[c>>4])
		b.WriteByte(hexUpper[c&15])
	}
	return b.String()
}

// canonicalURI encodes the (decoded) request path once, as S3 expects.
func canonicalURI(path string) string {
	if path == "" {
		return "/"
	}
	return URIEncode(path, true)
}

// canonicalQuery sorts and encodes the query parameters, leaving out
// X-Amz-Signature (which cannot sign itself).
func canonicalQuery(q url.Values) string {
	type kv struct{ k, v string }
	var pairs []kv
	for k, vs := range q {
		if k == "X-Amz-Signature" {
			continue
		}
		ek := URIEncode(k, false)
		for _, v := range vs {
			pairs = append(pairs, kv{ek, URIEncode(v, false)})
		}
	}
	sort.Slice(pairs, func(i, j int) bool {
		if pairs[i].k != pairs[j].k {
			return pairs[i].k < pairs[j].k
		}
		return pairs[i].v < pairs[j].v
	})
	var b strings.Builder
	for i, p := range pairs {
		if i > 0 {
			b.WriteByte('&')
		}
		b.WriteString(p.k)
		b.WriteByte('=')
		b.WriteString(p.v)
	}
	return b.String()
}

// headerValue returns the value of a header as SigV4 canonicalises it: all
// values joined by commas, each trimmed and with runs of spaces collapsed.
// Host, Content-Length and Transfer-Encoding are taken from where net/http
// moves them.
func headerValue(r *http.Request, name string) (string, bool) {
	switch name {
	case "host":
		return r.Host, r.Host != ""
	case "content-length":
		if v := r.Header.Values("Content-Length"); len(v) > 0 {
			return canonicalValues(v), true
		}
		if r.ContentLength >= 0 && (r.ContentLength > 0 || r.Method == http.MethodPut || r.Method == http.MethodPost) {
			return strconv.FormatInt(r.ContentLength, 10), true
		}
		return "", false
	case "transfer-encoding":
		if len(r.TransferEncoding) > 0 {
			return strings.Join(r.TransferEncoding, ","), true
		}
	}
	v := r.Header.Values(name)
	if len(v) == 0 {
		return "", false
	}
	return canonicalValues(v), true
}

func canonicalValues(vs []string) string {
	out := make([]string, len(vs))
	for i, v := range vs {
		out[i] = strings.Join(strings.Fields(v), " ")
	}
	return strings.Join(out, ",")
}

// canonicalRequest builds the canonical request for the given signed
// headers (lowercase, sorted) and payload hash.
func canonicalRequest(r *http.Request, signed []string, payloadHash string) (string, error) {
	var b strings.Builder
	b.WriteString(r.Method)
	b.WriteByte('\n')
	b.WriteString(canonicalURI(r.URL.Path))
	b.WriteByte('\n')
	b.WriteString(canonicalQuery(r.URL.Query()))
	b.WriteByte('\n')
	for _, h := range signed {
		v, ok := headerValue(r, h)
		if !ok {
			return "", errMissingSignedHeader(h)
		}
		b.WriteString(h)
		b.WriteByte(':')
		b.WriteString(v)
		b.WriteByte('\n')
	}
	b.WriteByte('\n')
	b.WriteString(strings.Join(signed, ";"))
	b.WriteByte('\n')
	b.WriteString(payloadHash)
	return b.String(), nil
}

func stringToSign(t time.Time, scope, canonical string) string {
	return Algorithm + "\n" + t.UTC().Format(timeFormat) + "\n" + scope + "\n" + sha256Hex(canonical)
}

// chunkStringToSign is the string to sign for one aws-chunked chunk.
func chunkStringToSign(t time.Time, scope, prevSig, chunkHash string) string {
	return "AWS4-HMAC-SHA256-PAYLOAD\n" + t.UTC().Format(timeFormat) + "\n" + scope + "\n" +
		prevSig + "\n" + EmptySHA256 + "\n" + chunkHash
}

// trailerStringToSign is the string to sign for the trailing headers.
func trailerStringToSign(t time.Time, scope, prevSig, trailerHash string) string {
	return "AWS4-HMAC-SHA256-TRAILER\n" + t.UTC().Format(timeFormat) + "\n" + scope + "\n" +
		prevSig + "\n" + trailerHash
}
