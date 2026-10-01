// Package interop tests strata with official and widely used S3 clients:
// the AWS SDK for Go v2 and minio-go. It is a separate module so that
// strata itself has no dependencies.
//
// The tests start the strata binary (STRATA_BIN, or built from ../..) on
// an ephemeral port with 4 data and 2 parity disks.
package interop

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/minio/minio-go/v7"
	miniocreds "github.com/minio/minio-go/v7/pkg/credentials"
)

const (
	accessKey = "interop-access"
	secretKey = "interop-secret-key"
	region    = "us-east-1"
)

var (
	endpoint    string // http://127.0.0.1:port
	hostPort    string
	tlsEndpoint string // a second server, over HTTPS
	tlsClient   *http.Client
	serverLog   = &syncBuffer{}
)

type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func TestMain(m *testing.M) {
	os.Setenv("AWS_CONFIG_FILE", os.DevNull)
	os.Setenv("AWS_SHARED_CREDENTIALS_FILE", os.DevNull)
	os.Setenv("AWS_EC2_METADATA_DISABLED", "true")
	dir, err := os.MkdirTemp("", "strata-interop")
	if err != nil {
		panic(err)
	}
	bin := os.Getenv("STRATA_BIN")
	if bin == "" {
		bin = filepath.Join(dir, "strata")
		// Built from the main module's root: this test module does not
		// contain cmd/strata.
		build := exec.Command("go", "build", "-o", bin, "./cmd/strata")
		build.Dir = filepath.Join("..", "..")
		if out, err := build.CombinedOutput(); err != nil {
			fmt.Fprintf(os.Stderr, "building strata: %v\n%s", err, out)
			os.Exit(1)
		}
	}
	var procs []*exec.Cmd
	start := func(name string, extra ...string) string {
		addrFile := filepath.Join(dir, name+".addr")
		args := []string{"server", "--address", "127.0.0.1:0", "--address-file", addrFile,
			"--access-key", accessKey, "--secret-key", secretKey, "--sync", "none", "--log-requests",
			"--data", "4", "--parity", "2"}
		args = append(args, extra...)
		for i := 0; i < 6; i++ {
			args = append(args, filepath.Join(dir, name, fmt.Sprintf("disk%d", i)))
		}
		cmd := exec.Command(bin, args...)
		cmd.Stdout, cmd.Stderr = serverLog, serverLog
		if err := cmd.Start(); err != nil {
			panic(err)
		}
		procs = append(procs, cmd)
		for i := 0; i < 200; i++ {
			if b, err := os.ReadFile(addrFile); err == nil && len(b) > 0 {
				return string(b)
			}
			time.Sleep(25 * time.Millisecond)
		}
		return ""
	}
	hostPort = start("plain")
	certFile, keyFile, pool := selfSignedCert(dir)
	tlsHostPort := start("tls", "--tls-cert", certFile, "--tls-key", keyFile)
	tlsEndpoint = "https://" + tlsHostPort
	tlsClient = &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool}}}
	code := 1
	if hostPort != "" && tlsHostPort != "" {
		endpoint = "http://" + hostPort
		code = m.Run()
	} else {
		fmt.Fprintln(os.Stderr, "strata did not start:\n"+serverLog.String())
	}
	for _, cmd := range procs {
		cmd.Process.Signal(syscall.SIGTERM)
		cmd.Wait()
	}
	if code != 0 {
		fmt.Fprintln(os.Stderr, "server log (last 4 KiB):")
		s := serverLog.String()
		fmt.Fprintln(os.Stderr, s[max(0, len(s)-4096):])
	}
	os.RemoveAll(dir)
	os.Exit(code)
}

// selfSignedCert writes a throwaway certificate for 127.0.0.1.
func selfSignedCert(dir string) (certFile, keyFile string, pool *x509.CertPool) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		panic(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "strata interop"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(24 * time.Hour),
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, KeyUsage: x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		panic(err)
	}
	kb, _ := x509.MarshalECPrivateKey(key)
	certFile, keyFile = filepath.Join(dir, "tls.crt"), filepath.Join(dir, "tls.key")
	os.WriteFile(certFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600)
	os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: kb}), 0o600)
	cert, _ := x509.ParseCertificate(der)
	pool = x509.NewCertPool()
	pool.AddCert(cert)
	return certFile, keyFile, pool
}

func randomBytes(n int) []byte {
	b := make([]byte, n)
	rand.Read(b)
	return b
}

// --- AWS SDK for Go v2 ---

func sdkClient(t *testing.T) *s3.Client { return sdkClientFor(t, endpoint, http.DefaultClient) }

func sdkClientFor(t *testing.T, url string, hc *http.Client) *s3.Client {
	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion(region), config.WithHTTPClient(hc),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(accessKey, secretKey, "")))
	if err != nil {
		t.Fatal(err)
	}
	return s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(url)
		o.UsePathStyle = true
	})
}

func httpStatus(err error) int {
	var re *smithyhttp.ResponseError
	if errors.As(err, &re) {
		return re.HTTPStatusCode()
	}
	return 0
}

func apiCode(err error) string {
	var ae smithy.APIError
	if errors.As(err, &ae) {
		return ae.ErrorCode()
	}
	return ""
}

func TestGoSDKv2(t *testing.T) {
	ctx := context.Background()
	c := sdkClient(t)
	bucket := "sdk-go-v2"
	if _, err := c.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatal(err)
	}
	// In us-east-1 (the region these tests use) S3 answers 200 to
	// re-creating a bucket you own; other regions answer
	// BucketAlreadyOwnedByYou.
	if _, err := c.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatalf("second CreateBucket in us-east-1: %v", err)
	}
	if _, err := c.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String("no-such-bucket")}); httpStatus(err) != 404 {
		t.Fatalf("HeadBucket of a missing bucket: %v", err)
	}

	t.Run("put and get with checksums and metadata", func(t *testing.T) {
		data := randomBytes(300_000)
		for _, algo := range []types.ChecksumAlgorithm{"", types.ChecksumAlgorithmCrc32, types.ChecksumAlgorithmCrc32c,
			types.ChecksumAlgorithmCrc64nvme, types.ChecksumAlgorithmSha1, types.ChecksumAlgorithmSha256} {
			key := "obj-" + strings.ToLower(string(algo))
			_, err := c.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(key),
				Body: bytes.NewReader(data), ContentType: aws.String("application/x-test"),
				Metadata: map[string]string{"origin": "sdk-v2"}, ChecksumAlgorithm: algo})
			if err != nil {
				t.Fatalf("PutObject %s: %v", algo, err)
			}
			out, err := c.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key),
				ChecksumMode: types.ChecksumModeEnabled})
			if err != nil {
				t.Fatalf("GetObject %s: %v", algo, err)
			}
			got, err := io.ReadAll(out.Body) // the SDK validates the checksum while reading
			out.Body.Close()
			if err != nil || !bytes.Equal(got, data) {
				t.Fatalf("GetObject %s: %v, %d bytes", algo, err, len(got))
			}
			if aws.ToString(out.ContentType) != "application/x-test" || out.Metadata["origin"] != "sdk-v2" {
				t.Fatalf("attributes: %v %v", aws.ToString(out.ContentType), out.Metadata)
			}
		}
	})

	t.Run("range, head, conditional", func(t *testing.T) {
		out, err := c.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String("obj-"), Range: aws.String("bytes=10-19")})
		if err != nil {
			t.Fatal(err)
		}
		got, _ := io.ReadAll(out.Body)
		out.Body.Close()
		if len(got) != 10 || aws.ToString(out.ContentRange) != "bytes 10-19/300000" {
			t.Fatalf("range: %d bytes, %s", len(got), aws.ToString(out.ContentRange))
		}
		head, err := c.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String("obj-")})
		if err != nil {
			t.Fatal(err)
		}
		_, err = c.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String("obj-"), IfNoneMatch: head.ETag})
		if httpStatus(err) != 304 {
			t.Fatalf("If-None-Match: %v", err)
		}
		_, err = c.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String("obj-"), IfMatch: aws.String(`"nope"`)})
		if httpStatus(err) != 412 {
			t.Fatalf("If-Match: %v", err)
		}
		_, err = c.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String("obj-"),
			Body: bytes.NewReader([]byte("x")), IfNoneMatch: aws.String("*")})
		if httpStatus(err) != 412 {
			t.Fatalf("conditional PUT on an existing key: %v", err)
		}
		_, err = c.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String("missing")})
		var nsk *types.NoSuchKey
		if !errors.As(err, &nsk) {
			t.Fatalf("missing key: %v", err)
		}
	})

	t.Run("list with paginator, prefix and delimiter", func(t *testing.T) {
		for _, k := range []string{"dir/a", "dir/b", "dir/sub/c", "other/d", "top"} {
			if _, err := c.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String("list/" + k),
				Body: strings.NewReader(k)}); err != nil {
				t.Fatal(err)
			}
		}
		var keys, prefixes []string
		p := s3.NewListObjectsV2Paginator(c, &s3.ListObjectsV2Input{Bucket: aws.String(bucket),
			Prefix: aws.String("list/"), Delimiter: aws.String("/"), MaxKeys: aws.Int32(1)})
		pages := 0
		for p.HasMorePages() {
			out, err := p.NextPage(ctx)
			if err != nil {
				t.Fatal(err)
			}
			pages++
			for _, o := range out.Contents {
				keys = append(keys, aws.ToString(o.Key))
			}
			for _, cp := range out.CommonPrefixes {
				prefixes = append(prefixes, aws.ToString(cp.Prefix))
			}
		}
		if strings.Join(keys, ",") != "list/top" || strings.Join(prefixes, ",") != "list/dir/,list/other/" || pages != 3 {
			t.Fatalf("keys %v prefixes %v pages %d", keys, prefixes, pages)
		}
	})

	t.Run("copy and batch delete", func(t *testing.T) {
		_, err := c.CopyObject(ctx, &s3.CopyObjectInput{Bucket: aws.String(bucket), Key: aws.String("copied"),
			CopySource: aws.String(bucket + "/list/top")})
		if err != nil {
			t.Fatal(err)
		}
		out, err := c.DeleteObjects(ctx, &s3.DeleteObjectsInput{Bucket: aws.String(bucket), Delete: &types.Delete{
			Objects: []types.ObjectIdentifier{{Key: aws.String("copied")}, {Key: aws.String("list/top")}, {Key: aws.String("never-existed")}}}})
		if err != nil {
			t.Fatal(err)
		}
		if len(out.Deleted) != 3 || len(out.Errors) != 0 {
			t.Fatalf("DeleteObjects: %d deleted, %d errors", len(out.Deleted), len(out.Errors))
		}
	})

	t.Run("multipart upload and ranged download with the transfer manager", func(t *testing.T) {
		data := randomBytes(23 << 20)
		up := manager.NewUploader(c, func(u *manager.Uploader) { u.PartSize = 5 << 20; u.Concurrency = 4 })
		res, err := up.Upload(ctx, &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String("big"), Body: bytes.NewReader(data)})
		if err != nil {
			t.Fatal(err)
		}
		if !strings.HasSuffix(aws.ToString(res.ETag), `-5"`) {
			t.Fatalf("multipart ETag %s", aws.ToString(res.ETag))
		}
		buf := manager.NewWriteAtBuffer(nil)
		down := manager.NewDownloader(c, func(d *manager.Downloader) { d.PartSize = 3 << 20; d.Concurrency = 4 })
		n, err := down.Download(ctx, buf, &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String("big")})
		if err != nil || n != int64(len(data)) || !bytes.Equal(buf.Bytes(), data) {
			t.Fatalf("download: %v, %d bytes", err, n)
		}
		parts, err := c.ListMultipartUploads(ctx, &s3.ListMultipartUploadsInput{Bucket: aws.String(bucket)})
		if err != nil || len(parts.Uploads) != 0 {
			t.Fatalf("uploads left behind: %v %d", err, len(parts.Uploads))
		}
	})

	t.Run("presigned GET and PUT", func(t *testing.T) {
		pc := s3.NewPresignClient(c)
		put, err := pc.PresignPutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String("presigned")},
			s3.WithPresignExpires(time.Minute))
		if err != nil {
			t.Fatal(err)
		}
		req, _ := http.NewRequest("PUT", put.URL, strings.NewReader("presigned body"))
		resp, err := http.DefaultClient.Do(req)
		if err != nil || resp.StatusCode != 200 {
			t.Fatalf("presigned PUT: %v %v", err, resp)
		}
		resp.Body.Close()
		get, err := pc.PresignGetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String("presigned")})
		if err != nil {
			t.Fatal(err)
		}
		resp, err = http.Get(get.URL)
		if err != nil {
			t.Fatal(err)
		}
		got, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if string(got) != "presigned body" {
			t.Fatalf("presigned GET: %d %q", resp.StatusCode, got)
		}
	})
}

// TestGoSDKv2OverTLS uploads an unseekable stream, which the SDK only
// allows over TLS: it sends an aws-chunked body with an unsigned payload
// and a trailing checksum.
func TestGoSDKv2OverTLS(t *testing.T) {
	ctx := context.Background()
	c := sdkClientFor(t, tlsEndpoint, tlsClient)
	if _, err := c.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String("tls-bucket")}); err != nil {
		t.Fatal(err)
	}
	data := randomBytes(3 << 20)
	for _, algo := range []types.ChecksumAlgorithm{types.ChecksumAlgorithmCrc32, types.ChecksumAlgorithmCrc64nvme, types.ChecksumAlgorithmSha256} {
		key := "stream-" + strings.ToLower(string(algo))
		_, err := c.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String("tls-bucket"), Key: aws.String(key),
			Body: io.NopCloser(bytes.NewReader(data)), ContentLength: aws.Int64(int64(len(data))), ChecksumAlgorithm: algo})
		if err != nil {
			t.Fatalf("%s: %v", algo, err)
		}
		out, err := c.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String("tls-bucket"), Key: aws.String(key), ChecksumMode: types.ChecksumModeEnabled})
		if err != nil {
			t.Fatal(err)
		}
		got, err := io.ReadAll(out.Body)
		out.Body.Close()
		if err != nil || !bytes.Equal(got, data) {
			t.Fatalf("%s: read back %d bytes: %v", algo, len(got), err)
		}
	}
}

// --- minio-go ---

func TestMinioGo(t *testing.T) {
	ctx := context.Background()
	c, err := minio.New(hostPort, &minio.Options{
		Creds: miniocreds.NewStaticV4(accessKey, secretKey, ""), Secure: false, Region: region,
		BucketLookup: minio.BucketLookupPath,
	})
	if err != nil {
		t.Fatal(err)
	}
	bucket := "minio-go"
	if err := c.MakeBucket(ctx, bucket, minio.MakeBucketOptions{Region: region}); err != nil {
		t.Fatal(err)
	}
	small := randomBytes(1 << 20)
	if _, err := c.PutObject(ctx, bucket, "small", bytes.NewReader(small), int64(len(small)),
		minio.PutObjectOptions{ContentType: "image/x-test", UserMetadata: map[string]string{"camera": "z"}}); err != nil {
		t.Fatal(err)
	}
	// Unknown length: minio-go buffers parts and uses a multipart upload.
	big := randomBytes(40 << 20)
	if _, err := c.PutObject(ctx, bucket, "big", io.NopCloser(bytes.NewReader(big)), -1,
		minio.PutObjectOptions{PartSize: 16 << 20}); err != nil {
		t.Fatal(err)
	}
	for key, want := range map[string][]byte{"small": small, "big": big} {
		o, err := c.GetObject(ctx, bucket, key, minio.GetObjectOptions{})
		if err != nil {
			t.Fatal(err)
		}
		got, err := io.ReadAll(o)
		o.Close()
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("%s: %v, %d bytes", key, err, len(got))
		}
	}
	st, err := c.StatObject(ctx, bucket, "small", minio.StatObjectOptions{})
	if err != nil || st.ContentType != "image/x-test" || st.UserMetadata["Camera"] != "z" {
		t.Fatalf("StatObject: %v %+v", err, st)
	}
	// Seeking reads use ranged GETs.
	o, _ := c.GetObject(ctx, bucket, "big", minio.GetObjectOptions{})
	o.Seek(30<<20, io.SeekStart)
	chunk := make([]byte, 4096)
	if _, err := io.ReadFull(o, chunk); err != nil || !bytes.Equal(chunk, big[30<<20:30<<20+4096]) {
		t.Fatalf("seek+read: %v", err)
	}
	o.Close()
	if _, err := c.CopyObject(ctx, minio.CopyDestOptions{Bucket: bucket, Object: "copy"},
		minio.CopySrcOptions{Bucket: bucket, Object: "small"}); err != nil {
		t.Fatal(err)
	}
	var keys []string
	for obj := range c.ListObjects(ctx, bucket, minio.ListObjectsOptions{Recursive: true}) {
		if obj.Err != nil {
			t.Fatal(obj.Err)
		}
		keys = append(keys, obj.Key)
	}
	sort.Strings(keys)
	if strings.Join(keys, ",") != "big,copy,small" {
		t.Fatalf("ListObjects: %v", keys)
	}
	u, err := c.PresignedGetObject(ctx, bucket, "small", time.Minute, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.Get(u.String())
	if err != nil {
		t.Fatal(err)
	}
	got, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if !bytes.Equal(got, small) {
		t.Fatalf("presigned GET: %d", resp.StatusCode)
	}
	errs := c.RemoveObjects(ctx, bucket, func() <-chan minio.ObjectInfo {
		ch := make(chan minio.ObjectInfo, 3)
		for _, k := range keys {
			ch <- minio.ObjectInfo{Key: k}
		}
		close(ch)
		return ch
	}(), minio.RemoveObjectsOptions{})
	for e := range errs {
		t.Fatalf("RemoveObjects: %v", e.Err)
	}
	if err := c.RemoveBucket(ctx, bucket); err != nil {
		t.Fatal(err)
	}
}

// TestSignedChunksSeen checks, from the server's access log, which payload
// modes the clients above used. minio-go sends aws-chunked bodies with
// per-chunk signatures over plain HTTP; this is the real-client evidence
// for strata's chunk signature verification.
func TestSignedChunksSeen(t *testing.T) {
	re := regexp.MustCompile(`api=(PutObject|UploadPart) .*status=200.*payload=([a-z-]+)`)
	modes := map[string]int{}
	for _, m := range re.FindAllStringSubmatch(serverLog.String(), -1) {
		modes[m[2]]++
	}
	t.Logf("successful uploads by payload mode: %v", modes)
	if modes["aws-chunked-signed"]+modes["aws-chunked-signed-trailer"] == 0 {
		t.Fatal("no upload used per-chunk signatures")
	}
	if modes["aws-chunked-unsigned-trailer"] == 0 {
		t.Fatal("no upload used an unsigned aws-chunked body with a trailer")
	}
}
