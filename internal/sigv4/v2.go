package sigv4

import (
	"crypto/hmac"
	"crypto/sha1"
	"encoding/base64"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/useless-husband/strata/internal/s3err"
)

// Signature Version 2 is deprecated by AWS but still the default of boto3
// (1.43) for presigned URLs to a custom endpoint in us-east-1, so strata
// verifies it, in the Authorization header ("AWS key:signature") and in
// the query string (AWSAccessKeyId, Expires, Signature). V2 does not sign
// the payload; bodies are checked only against Content-MD5 when present.

// v2Subresources are the query parameters that are part of the canonical
// resource, per the S3 documentation (and botocore's HmacV1Auth).
var v2Subresources = map[string]bool{
	"accelerate": true, "acl": true, "cors": true, "defaultObjectAcl": true, "location": true, "logging": true,
	"partNumber": true, "policy": true, "requestPayment": true, "torrent": true, "versioning": true,
	"versionId": true, "versions": true, "website": true, "uploads": true, "uploadId": true,
	"response-content-type": true, "response-content-language": true, "response-expires": true,
	"response-cache-control": true, "response-content-disposition": true, "response-content-encoding": true,
	"delete": true, "lifecycle": true, "tagging": true, "restore": true, "storageClass": true,
	"notification": true, "replication": true, "analytics": true, "metrics": true, "inventory": true,
	"select": true, "select-type": true, "object-lock": true,
}

// v2Resource builds CanonicalizedResource: the path exactly as sent
// (prefixed with /bucket for virtual-hosted requests) and the
// subresources, sorted, with decoded values.
func (v *Verifier) v2Resource(r *http.Request) string {
	path, _, _ := strings.Cut(r.RequestURI, "?")
	if path == "" || !strings.HasPrefix(path, "/") {
		path = r.URL.EscapedPath()
	}
	host := r.Host
	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	}
	for _, d := range v.Domains {
		if b, ok := strings.CutSuffix(host, "."+d); ok && b != "" {
			path = "/" + b + path
			break
		}
	}
	var subs []string
	for _, pair := range strings.Split(r.URL.RawQuery, "&") {
		if pair == "" {
			continue
		}
		name, val, hasVal := strings.Cut(pair, "=")
		if n, err := url.QueryUnescape(name); err == nil {
			name = n
		}
		if !v2Subresources[name] {
			continue
		}
		if hasVal {
			if dv, err := url.QueryUnescape(val); err == nil {
				val = dv
			}
			subs = append(subs, name+"="+val)
		} else {
			subs = append(subs, name)
		}
	}
	sort.SliceStable(subs, func(i, j int) bool {
		a, _, _ := strings.Cut(subs[i], "=")
		b, _, _ := strings.Cut(subs[j], "=")
		return a < b
	})
	if len(subs) > 0 {
		path += "?" + strings.Join(subs, "&")
	}
	return path
}

// v2StringToSign builds the V2 string to sign; date is the Date header or,
// for presigned URLs, the Expires value.
func (v *Verifier) v2StringToSign(r *http.Request, date string) string {
	var amz []string
	vals := map[string][]string{}
	for name, vs := range r.Header {
		l := strings.ToLower(name)
		if strings.HasPrefix(l, "x-amz-") {
			for _, x := range vs {
				vals[l] = append(vals[l], strings.TrimSpace(x))
			}
		}
	}
	for name := range vals {
		amz = append(amz, name)
	}
	sort.Strings(amz)
	var b strings.Builder
	b.WriteString(r.Method + "\n")
	b.WriteString(strings.TrimSpace(r.Header.Get("Content-Md5")) + "\n")
	b.WriteString(strings.TrimSpace(r.Header.Get("Content-Type")) + "\n")
	b.WriteString(date + "\n")
	for _, name := range amz {
		b.WriteString(name + ":" + strings.Join(vals[name], ",") + "\n")
	}
	b.WriteString(v.v2Resource(r))
	return b.String()
}

func v2Sign(secret, sts string) string {
	h := hmac.New(sha1.New, []byte(secret))
	h.Write([]byte(sts))
	return base64.StdEncoding.EncodeToString(h.Sum(nil))
}

func (v *Verifier) verifyV2Header(r *http.Request, authz string) (*Auth, error) {
	cred := strings.TrimPrefix(authz, "AWS ")
	i := strings.LastIndex(cred, ":")
	if i <= 0 {
		return nil, s3err.AuthorizationHeaderMalformed.With("The authorization header is malformed; expecting \"AWS AccessKeyId:Signature\"")
	}
	accessKey, sig := cred[:i], cred[i+1:]
	// With x-amz-date the Date line of the string to sign is empty.
	date := r.Header.Get("Date")
	stamp := date
	if xd := r.Header.Get("X-Amz-Date"); xd != "" {
		date, stamp = "", xd
	}
	t, err := http.ParseTime(stamp)
	for _, layout := range []string{time.RFC1123Z, timeFormat} {
		if err == nil {
			break
		}
		if t2, err2 := time.Parse(layout, stamp); err2 == nil {
			t, err = t2, nil
		}
	}
	if err != nil {
		return nil, s3err.AccessDenied.With("AWS authentication requires a valid Date or x-amz-date header")
	}
	if skew := v.now().Sub(t); skew > v.maxSkew() || skew < -v.maxSkew() {
		return nil, s3err.RequestTimeTooSkewed
	}
	secret, ok := v.Secret(accessKey)
	if !ok {
		return nil, s3err.InvalidAccessKeyID
	}
	if !hmac.Equal([]byte(v2Sign(secret, v.v2StringToSign(r, date))), []byte(sig)) {
		return nil, s3err.SignatureDoesNotMatch
	}
	return &Auth{AccessKey: accessKey, Mode: PayloadUnsigned, Time: t, V2: true}, nil
}

func (v *Verifier) verifyV2Query(r *http.Request) (*Auth, error) {
	q := r.URL.Query()
	accessKey, sig, exp := q.Get("AWSAccessKeyId"), q.Get("Signature"), q.Get("Expires")
	if accessKey == "" || sig == "" || exp == "" {
		return nil, s3err.AccessDenied.With("Query-string authentication requires the Signature, Expires and AWSAccessKeyId parameters")
	}
	e, err := strconv.ParseInt(exp, 10, 64)
	if err != nil {
		return nil, s3err.AccessDenied.With("Invalid date (should be seconds since epoch): %s", exp)
	}
	if v.now().Unix() > e {
		return nil, s3err.AccessDenied.With("Request has expired")
	}
	secret, ok := v.Secret(accessKey)
	if !ok {
		return nil, s3err.InvalidAccessKeyID
	}
	if !hmac.Equal([]byte(v2Sign(secret, v.v2StringToSign(r, exp))), []byte(sig)) {
		return nil, s3err.SignatureDoesNotMatch
	}
	return &Auth{AccessKey: accessKey, Presigned: true, Mode: PayloadUnsigned, Time: time.Unix(e, 0), V2: true}, nil
}
