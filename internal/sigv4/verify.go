package sigv4

import (
	"crypto/hmac"
	"encoding/hex"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/useless-husband/strata/internal/s3err"
)

// MaxPresignExpiry is the longest validity S3 accepts for a presigned URL.
const MaxPresignExpiry = 7 * 24 * time.Hour

// DefaultMaxSkew is how far the request time may be from the server clock.
const DefaultMaxSkew = 15 * time.Minute

// PayloadMode says how the request body is protected.
type PayloadMode int

// Payload modes.
const (
	// PayloadSigned: the body's SHA-256 is in x-amz-content-sha256 and is
	// covered by the signature; the server must check it.
	PayloadSigned PayloadMode = iota
	// PayloadUnsigned: UNSIGNED-PAYLOAD; the body is not authenticated.
	PayloadUnsigned
	// PayloadStreaming: aws-chunked with a signature per chunk.
	PayloadStreaming
	// PayloadStreamingTrailer: as PayloadStreaming, plus signed trailers.
	PayloadStreamingTrailer
	// PayloadStreamingUnsignedTrailer: aws-chunked without chunk signatures,
	// with trailers (usually a checksum).
	PayloadStreamingUnsignedTrailer
)

func (m PayloadMode) String() string {
	switch m {
	case PayloadSigned:
		return "signed"
	case PayloadUnsigned:
		return "unsigned"
	case PayloadStreaming:
		return "aws-chunked-signed"
	case PayloadStreamingTrailer:
		return "aws-chunked-signed-trailer"
	case PayloadStreamingUnsignedTrailer:
		return "aws-chunked-unsigned-trailer"
	}
	return "unknown"
}

// Auth is the result of verifying a request.
type Auth struct {
	AccessKey   string
	Presigned   bool
	Mode        PayloadMode
	PayloadHash string // the hex SHA-256 the body must have, for PayloadSigned

	// For aws-chunked bodies.
	Time       time.Time
	Scope      string
	SigningKey []byte
	Signature  string // seed signature: the request's own
}

// Verifier checks SigV4 signatures.
type Verifier struct {
	Region  string
	Service string // "s3" if empty
	// Secret returns the secret key for an access key.
	Secret  func(accessKey string) (string, bool)
	Now     func() time.Time // time.Now if nil
	MaxSkew time.Duration    // DefaultMaxSkew if zero
}

func (v *Verifier) now() time.Time {
	if v.Now != nil {
		return v.Now()
	}
	return time.Now()
}

func (v *Verifier) service() string {
	if v.Service == "" {
		return "s3"
	}
	return v.Service
}

func (v *Verifier) maxSkew() time.Duration {
	if v.MaxSkew == 0 {
		return DefaultMaxSkew
	}
	return v.MaxSkew
}

// IsSigned reports whether the request carries any SigV4 credentials.
func IsSigned(r *http.Request) bool {
	return r.Header.Get("Authorization") != "" || r.URL.Query().Has("X-Amz-Algorithm") ||
		r.URL.Query().Has("X-Amz-Credential")
}

func errMissingSignedHeader(h string) error {
	return s3err.AccessDenied.With("There were headers present in the request which were not signed: the signed header %q is missing", h)
}

// Verify authenticates a request signed with SigV4, in the Authorization
// header or in the query string (presigned URL).
func (v *Verifier) Verify(r *http.Request) (*Auth, error) {
	authz := r.Header.Get("Authorization")
	q := r.URL.Query()
	switch {
	case authz != "" && q.Has("X-Amz-Algorithm"):
		return nil, s3err.InvalidArgument.With("Only one auth mechanism allowed; only the X-Amz-Algorithm query parameter, Signature query string parameter or the Authorization header should be specified")
	case authz != "":
		return v.verifyHeader(r, authz)
	case q.Has("X-Amz-Algorithm"):
		return v.verifyPresigned(r)
	case q.Has("Signature") || q.Has("AWSAccessKeyId"):
		return nil, s3err.NotImplemented.With("Signature Version 2 is not supported; use Signature Version 4")
	}
	return nil, s3err.AccessDenied.With("Anonymous access is not allowed")
}

type credential struct {
	accessKey, date, region, service string
}

func parseCredential(s string, malformed *s3err.Error) (credential, error) {
	// AKID/20130524/us-east-1/s3/aws4_request; the access key itself may
	// not contain '/', so split from the right.
	parts := strings.Split(s, "/")
	if len(parts) < 5 {
		return credential{}, malformed.With("the Credential is mal-formed; expecting \"<YOUR-AKID>/YYYYMMDD/REGION/SERVICE/aws4_request\".")
	}
	n := len(parts)
	c := credential{
		accessKey: strings.Join(parts[:n-4], "/"),
		date:      parts[n-4],
		region:    parts[n-3],
		service:   parts[n-2],
	}
	if parts[n-1] != "aws4_request" {
		return c, malformed.With("the Credential has a terminator %q; expecting \"aws4_request\".", parts[n-1])
	}
	if _, err := time.Parse(shortFormat, c.date); err != nil {
		return c, malformed.With("invalid date %q in the Credential", c.date)
	}
	return c, nil
}

func (v *Verifier) checkScope(c credential, malformed *s3err.Error) error {
	if c.service != v.service() {
		return malformed.With("the Credential has the wrong service %q; expecting %q", c.service, v.service())
	}
	if c.region != v.Region {
		return malformed.With("the region '%s' is wrong; expecting '%s'", c.region, v.Region)
	}
	return nil
}

func parseSignedHeaders(s string) ([]string, error) {
	if s == "" {
		return nil, s3err.AuthorizationHeaderMalformed.With("SignedHeaders is empty")
	}
	h := strings.Split(s, ";")
	hasHost := false
	for i, name := range h {
		if name == "" || name != strings.ToLower(name) || (i > 0 && h[i-1] >= name) {
			return nil, s3err.AuthorizationHeaderMalformed.With("SignedHeaders must be lowercase, sorted and unique")
		}
		if name == "host" {
			hasHost = true
		}
	}
	if !hasHost {
		return nil, s3err.AccessDenied.With("The host header must be signed")
	}
	return h, nil
}

func (v *Verifier) verifyHeader(r *http.Request, authz string) (*Auth, error) {
	malformed := s3err.AuthorizationHeaderMalformed
	if !strings.HasPrefix(authz, Algorithm+" ") {
		if strings.HasPrefix(authz, "AWS ") {
			return nil, s3err.NotImplemented.With("Signature Version 2 is not supported; use Signature Version 4")
		}
		return nil, s3err.AuthorizationHeaderMalformed.With("Unsupported authorization type")
	}
	fields := map[string]string{}
	for _, f := range strings.Split(strings.TrimPrefix(authz, Algorithm+" "), ",") {
		f = strings.TrimSpace(f)
		k, val, ok := strings.Cut(f, "=")
		if !ok {
			return nil, malformed.With("the authorization header has a malformed field %q", f)
		}
		fields[k] = val
	}
	credStr, signedStr, sig := fields["Credential"], fields["SignedHeaders"], fields["Signature"]
	if credStr == "" || signedStr == "" || sig == "" {
		return nil, malformed.With("the authorization header requires Credential, SignedHeaders and Signature")
	}
	cred, err := parseCredential(credStr, malformed)
	if err != nil {
		return nil, err
	}
	if err := v.checkScope(cred, malformed); err != nil {
		return nil, err
	}
	signed, err := parseSignedHeaders(signedStr)
	if err != nil {
		return nil, err
	}

	// Request time: X-Amz-Date, or Date if absent.
	var t time.Time
	if d := r.Header.Get("X-Amz-Date"); d != "" {
		t, err = time.Parse(timeFormat, d)
	} else if d := r.Header.Get("Date"); d != "" {
		t, err = http.ParseTime(d)
	} else {
		return nil, s3err.AccessDenied.With("AWS authentication requires a valid Date or x-amz-date header")
	}
	if err != nil {
		return nil, s3err.AccessDenied.With("AWS authentication requires a valid Date or x-amz-date header")
	}
	if t.UTC().Format(shortFormat) != cred.date {
		return nil, malformed.With("the date in the Credential (%s) does not match the request date", cred.date)
	}
	if skew := v.now().Sub(t); skew > v.maxSkew() || skew < -v.maxSkew() {
		return nil, s3err.RequestTimeTooSkewed
	}

	payload := r.Header.Get("X-Amz-Content-Sha256")
	if payload == "" {
		return nil, s3err.InvalidRequest.With("Missing required header for this request: x-amz-content-sha256")
	}
	auth := &Auth{AccessKey: cred.accessKey, Time: t, Scope: scope(cred.date, cred.region, cred.service), Signature: sig}
	switch payload {
	case UnsignedPayload:
		auth.Mode = PayloadUnsigned
	case StreamingPayload:
		auth.Mode = PayloadStreaming
	case StreamingPayloadTrailer:
		auth.Mode = PayloadStreamingTrailer
	case StreamingUnsignedTrailer:
		auth.Mode = PayloadStreamingUnsignedTrailer
	case streamingECDSA, streamingECDSATrailer:
		return nil, s3err.NotImplemented.With("Signature Version 4A is not supported")
	default:
		if len(payload) != 64 || !isLowerHex(payload) {
			return nil, s3err.InvalidArgument.With("x-amz-content-sha256 must be UNSIGNED-PAYLOAD, a streaming mode, or a valid sha256 value")
		}
		auth.Mode = PayloadSigned
		auth.PayloadHash = payload
	}

	// Every x-amz-* header must be signed, so they cannot be added in
	// transit.
	signedSet := make(map[string]bool, len(signed))
	for _, h := range signed {
		signedSet[h] = true
	}
	for name := range r.Header {
		l := strings.ToLower(name)
		if strings.HasPrefix(l, "x-amz-") && !signedSet[l] {
			return nil, s3err.AccessDenied.With("There were headers present in the request which were not signed: %s", l)
		}
	}

	secret, ok := v.Secret(cred.accessKey)
	if !ok {
		return nil, s3err.InvalidAccessKeyID
	}
	canonical, err := canonicalRequest(r, signed, payload)
	if err != nil {
		return nil, err
	}
	key := SigningKey(secret, cred.date, cred.region, cred.service)
	want := hex.EncodeToString(hmacSHA256(key, stringToSign(t, auth.Scope, canonical)))
	if !hmac.Equal([]byte(want), []byte(sig)) {
		return nil, s3err.SignatureDoesNotMatch
	}
	auth.SigningKey = key
	return auth, nil
}

func (v *Verifier) verifyPresigned(r *http.Request) (*Auth, error) {
	malformed := s3err.AuthorizationQueryMalformed
	q := r.URL.Query()
	if q.Get("X-Amz-Algorithm") != Algorithm {
		return nil, malformed.With("X-Amz-Algorithm only supports \"%s\"", Algorithm)
	}
	for _, p := range []string{"X-Amz-Credential", "X-Amz-Date", "X-Amz-Expires", "X-Amz-SignedHeaders", "X-Amz-Signature"} {
		if q.Get(p) == "" {
			return nil, malformed.With("Query-string authentication version 4 requires the X-Amz-Algorithm, X-Amz-Credential, X-Amz-Signature, X-Amz-Date, X-Amz-SignedHeaders, and X-Amz-Expires parameters.")
		}
	}
	cred, err := parseCredential(q.Get("X-Amz-Credential"), malformed)
	if err != nil {
		return nil, err
	}
	if err := v.checkScope(cred, malformed); err != nil {
		return nil, err
	}
	t, err := time.Parse(timeFormat, q.Get("X-Amz-Date"))
	if err != nil {
		return nil, malformed.With("X-Amz-Date must be in the ISO8601 Long Format \"yyyyMMdd'T'HHmmss'Z'\"")
	}
	if t.UTC().Format(shortFormat) != cred.date {
		return nil, malformed.With("the date in the Credential (%s) does not match X-Amz-Date", cred.date)
	}
	exp, err := strconv.ParseInt(q.Get("X-Amz-Expires"), 10, 64)
	if err != nil || exp < 0 {
		return nil, malformed.With("X-Amz-Expires should be a number")
	}
	if exp < 1 || time.Duration(exp)*time.Second > MaxPresignExpiry {
		return nil, malformed.With("X-Amz-Expires must be less than a week (in seconds) that is 604800")
	}
	now := v.now()
	if t.Sub(now) > v.maxSkew() {
		return nil, s3err.AccessDenied.With("Request is not valid yet")
	}
	if now.After(t.Add(time.Duration(exp) * time.Second)) {
		return nil, s3err.AccessDenied.With("Request has expired")
	}
	signed, err := parseSignedHeaders(q.Get("X-Amz-SignedHeaders"))
	if err != nil {
		return nil, err
	}
	payload := UnsignedPayload
	if p := q.Get("X-Amz-Content-Sha256"); p != "" {
		payload = p
	}
	auth := &Auth{AccessKey: cred.accessKey, Presigned: true, Mode: PayloadUnsigned, Time: t,
		Scope: scope(cred.date, cred.region, cred.service), Signature: q.Get("X-Amz-Signature")}
	if payload != UnsignedPayload {
		if len(payload) != 64 || !isLowerHex(payload) {
			return nil, s3err.InvalidArgument.With("X-Amz-Content-Sha256 must be UNSIGNED-PAYLOAD or a valid sha256 value")
		}
		auth.Mode = PayloadSigned
		auth.PayloadHash = payload
	}
	secret, ok := v.Secret(cred.accessKey)
	if !ok {
		return nil, s3err.InvalidAccessKeyID
	}
	canonical, err := canonicalRequest(r, signed, payload)
	if err != nil {
		return nil, err
	}
	key := SigningKey(secret, cred.date, cred.region, cred.service)
	want := hex.EncodeToString(hmacSHA256(key, stringToSign(t, auth.Scope, canonical)))
	if !hmac.Equal([]byte(want), []byte(auth.Signature)) {
		return nil, s3err.SignatureDoesNotMatch
	}
	auth.SigningKey = key
	return auth, nil
}

func isLowerHex(s string) bool {
	for i := 0; i < len(s); i++ {
		c := s[i]
		if !('0' <= c && c <= '9' || 'a' <= c && c <= 'f') {
			return false
		}
	}
	return true
}
