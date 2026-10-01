// Package s3client is a minimal S3 client used by strata's own tests and
// load generator. Interoperability is tested with the official AWS clients
// (test/awscli.sh, test/interop, test/boto3_test.py); this package exists
// so the crash test and benchmarks control exactly what is sent.
package s3client

import (
	"bytes"
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/useless-husband/strata/internal/sigv4"
)

// Client talks to one endpoint with one key.
type Client struct {
	Endpoint string // http://host:port
	Signer   *sigv4.Signer
	HTTP     *http.Client
}

// New returns a client.
func New(endpoint, accessKey, secretKey, region string) *Client {
	return &Client{
		Endpoint: strings.TrimRight(endpoint, "/"),
		Signer:   &sigv4.Signer{AccessKey: accessKey, SecretKey: secretKey, Region: region},
		HTTP: &http.Client{Transport: &http.Transport{MaxIdleConnsPerHost: 256, MaxConnsPerHost: 0},
			Timeout: 10 * time.Minute},
	}
}

// Error is a non-2xx response.
type Error struct {
	Status int
	Code   string
	Body   string
}

func (e *Error) Error() string { return fmt.Sprintf("HTTP %d %s: %.200s", e.Status, e.Code, e.Body) }

// StatusOf returns the HTTP status of an *Error, or 0.
func StatusOf(err error) int {
	if e, ok := err.(*Error); ok {
		return e.Status
	}
	return 0
}

func objectPath(bucket, key string) string {
	if key == "" {
		return "/" + bucket
	}
	return "/" + bucket + "/" + sigv4.URIEncode(key, true)
}

// Do sends a request with body (may be nil) and returns the response with
// its body read.
func (c *Client) Do(ctx context.Context, method, bucket, key string, query url.Values, body []byte, header http.Header) (*http.Response, []byte, error) {
	u := c.Endpoint + objectPath(bucket, key)
	if len(query) > 0 {
		u += "?" + strings.ReplaceAll(query.Encode(), "+", "%20")
	}
	req, err := http.NewRequestWithContext(ctx, method, u, nil)
	if err != nil {
		return nil, nil, err
	}
	for k, v := range header {
		req.Header[k] = v
	}
	c.Signer.SignBytes(req, body)
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return nil, nil, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return resp, nil, err
	}
	if resp.StatusCode >= 300 && resp.StatusCode != http.StatusNotModified {
		var e struct {
			Code string `xml:"Code"`
		}
		xml.Unmarshal(data, &e)
		return resp, data, &Error{Status: resp.StatusCode, Code: e.Code, Body: string(data)}
	}
	return resp, data, nil
}

// PutObject uploads data and returns the ETag without quotes.
func (c *Client) PutObject(ctx context.Context, bucket, key string, data []byte) (string, error) {
	resp, _, err := c.Do(ctx, "PUT", bucket, key, nil, data, nil)
	if err != nil {
		return "", err
	}
	return strings.Trim(resp.Header.Get("ETag"), `"`), nil
}

// GetObject downloads an object.
func (c *Client) GetObject(ctx context.Context, bucket, key string) ([]byte, http.Header, error) {
	resp, data, err := c.Do(ctx, "GET", bucket, key, nil, nil, nil)
	if err != nil {
		return nil, nil, err
	}
	return data, resp.Header, nil
}

// GetObjectStream downloads an object into w without buffering it.
func (c *Client) GetObjectStream(ctx context.Context, bucket, key string, w io.Writer) (int64, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", c.Endpoint+objectPath(bucket, key), nil)
	if err != nil {
		return 0, err
	}
	c.Signer.SignBytes(req, nil)
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		b, _ := io.ReadAll(resp.Body)
		return 0, &Error{Status: resp.StatusCode, Body: string(b)}
	}
	return io.Copy(w, resp.Body)
}

// HeadObject returns the headers of an object.
func (c *Client) HeadObject(ctx context.Context, bucket, key string) (http.Header, error) {
	resp, _, err := c.Do(ctx, "HEAD", bucket, key, nil, nil, nil)
	if err != nil {
		return nil, err
	}
	return resp.Header, nil
}

// DeleteObject deletes a key.
func (c *Client) DeleteObject(ctx context.Context, bucket, key string) error {
	_, _, err := c.Do(ctx, "DELETE", bucket, key, nil, nil, nil)
	return err
}

// CreateBucket creates a bucket.
func (c *Client) CreateBucket(ctx context.Context, bucket string) error {
	_, _, err := c.Do(ctx, "PUT", bucket, "", nil, nil, nil)
	return err
}

// Object is one entry of a listing.
type Object struct {
	Key  string `xml:"Key"`
	Size int64  `xml:"Size"`
	ETag string `xml:"ETag"`
}

// ListPage is one ListObjectsV2 response.
type ListPage struct {
	Contents       []Object `xml:"Contents"`
	CommonPrefixes []struct {
		Prefix string `xml:"Prefix"`
	} `xml:"CommonPrefixes"`
	IsTruncated           bool   `xml:"IsTruncated"`
	NextContinuationToken string `xml:"NextContinuationToken"`
	KeyCount              int    `xml:"KeyCount"`
}

// ListPage fetches one page of ListObjectsV2.
func (c *Client) ListPage(ctx context.Context, bucket, prefix, delimiter, token string, maxKeys int) (*ListPage, error) {
	q := url.Values{"list-type": {"2"}}
	if prefix != "" {
		q.Set("prefix", prefix)
	}
	if delimiter != "" {
		q.Set("delimiter", delimiter)
	}
	if token != "" {
		q.Set("continuation-token", token)
	}
	if maxKeys > 0 {
		q.Set("max-keys", strconv.Itoa(maxKeys))
	}
	_, data, err := c.Do(ctx, "GET", bucket, "", q, nil, nil)
	if err != nil {
		return nil, err
	}
	var p ListPage
	if err := xml.Unmarshal(data, &p); err != nil {
		return nil, err
	}
	return &p, nil
}

// ListAll lists every key with a prefix.
func (c *Client) ListAll(ctx context.Context, bucket, prefix string) ([]Object, error) {
	var out []Object
	token := ""
	for {
		p, err := c.ListPage(ctx, bucket, prefix, "", token, 1000)
		if err != nil {
			return nil, err
		}
		out = append(out, p.Contents...)
		if !p.IsTruncated {
			return out, nil
		}
		token = p.NextContinuationToken
	}
}

// MultipartUpload uploads data in parts of partSize bytes and returns the
// ETag of the assembled object.
func (c *Client) MultipartUpload(ctx context.Context, bucket, key string, data []byte, partSize int) (string, error) {
	_, body, err := c.Do(ctx, "POST", bucket, key, url.Values{"uploads": {""}}, nil, nil)
	if err != nil {
		return "", err
	}
	var init struct {
		UploadID string `xml:"UploadId"`
	}
	if err := xml.Unmarshal(body, &init); err != nil {
		return "", err
	}
	var b bytes.Buffer
	b.WriteString("<CompleteMultipartUpload>")
	for n, off := 1, 0; off < len(data) || n == 1; n, off = n+1, off+partSize {
		part := data[off:min(off+partSize, len(data))]
		q := url.Values{"partNumber": {strconv.Itoa(n)}, "uploadId": {init.UploadID}}
		resp, _, err := c.Do(ctx, "PUT", bucket, key, q, part, nil)
		if err != nil {
			return "", err
		}
		fmt.Fprintf(&b, "<Part><PartNumber>%d</PartNumber><ETag>%s</ETag></Part>", n, resp.Header.Get("ETag"))
		if off+partSize >= len(data) {
			break
		}
	}
	b.WriteString("</CompleteMultipartUpload>")
	_, body, err = c.Do(ctx, "POST", bucket, key, url.Values{"uploadId": {init.UploadID}}, b.Bytes(), nil)
	if err != nil {
		return "", err
	}
	var res struct {
		ETag string `xml:"ETag"`
	}
	xml.Unmarshal(body, &res)
	return strings.Trim(res.ETag, `"`), nil
}
