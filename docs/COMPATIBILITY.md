# S3 compatibility

What strata implements, how it compares with S3 on edge cases, and what an independent conformance suite says.

## Implemented

| Area | Operations |
|---|---|
| Authentication | Signature V4 in the `Authorization` header and in presigned URLs; payloads signed (SHA-256), unsigned, or aws-chunked with per-chunk signatures, with signed trailers, or unsigned with trailers. Signature V2 (header and presigned), because boto3 still presigns with it by default. |
| Buckets | CreateBucket, DeleteBucket, HeadBucket, ListBuckets (with `max-buckets`, `continuation-token`, `prefix`), GetBucketLocation, GetBucketVersioning (always unversioned), GetBucketAcl (fixed owner-only ACL) |
| Objects | PutObject, GetObject, HeadObject, DeleteObject, DeleteObjects, CopyObject (COPY/REPLACE metadata, copy-source conditions), GetObjectAttributes, GetObjectAcl; ranged GETs, `partNumber` GETs, `response-*` overrides, user metadata and the standard stored headers |
| Conditions | `If-Match`, `If-None-Match`, `If-Modified-Since`, `If-Unmodified-Since` on GET and HEAD; `If-None-Match: *` and `If-Match` on PUT and CompleteMultipartUpload; `If-Match` on DeleteObject and per-key ETags in DeleteObjects |
| Listing | ListObjectsV2 and ListObjects (prefix, delimiter, `start-after`, continuation tokens, markers, `max-keys`, `encoding-type=url`, `fetch-owner`), ListObjectVersions (one "null" version per key) |
| Multipart | CreateMultipartUpload, UploadPart, UploadPartCopy, CompleteMultipartUpload (idempotent retries), AbortMultipartUpload, ListParts, ListMultipartUploads |
| Checksums | `x-amz-checksum-` CRC32, CRC32C, CRC64NVME, SHA1, SHA256, as headers or trailers, checked before commit and returned with `x-amz-checksum-mode: ENABLED`; multipart COMPOSITE and FULL_OBJECT checksums |

Not implemented, answered with `501 NotImplemented`: ACLs other than private, bucket policies, IAM/STS, versioning, object lock, server-side encryption, tagging, lifecycle, CORS, website hosting, logging, notifications, replication, public access blocks, ownership controls, browser POST uploads, SelectObjectContent, Signature V4A.

## Edge cases against documented S3 behaviour

Each row is checked by the named test (`internal/s3api` unless noted).

| Request | S3 (documentation) | strata | Test |
|---|---|---|---|
| `If-Match` true and `If-Unmodified-Since` false | 200 | 200 | `TestConditionalRequests` |
| `If-None-Match` false and `If-Modified-Since` true | 304 | 304 | `TestConditionalRequests` |
| `If-Modified-Since` equal to Last-Modified | 304 (second resolution) | 304 | `TestConditionalRequests` |
| Unparseable date in a condition | ignored | ignored | `TestConditionalRequests` |
| `Range: bytes=-10`, `bytes=99990-`, end past the object | suffix / open / clamped, 206 | same | `TestRangeRequests` |
| `Range` starting at or past the end | 416 `InvalidRange`, `Content-Range: bytes */size` | same | `TestRangeRequests` |
| Several ranges, or a malformed range | the whole object, 200 | same | `TestRangeRequests` |
| Any range of an empty object | 416 | 416 | `TestRangeRequests` |
| DELETE of a key that does not exist | 204 | 204 | `TestObjectAPI` |
| Keys `a//b`, `../up`, `trailing/`, `a+b`, `%41` | distinct keys, no normalisation | same | `TestObjectAPI` |
| Listing order | UTF-8 binary order | same (`z/` before `ü`) | `TestListingAPI` |
| `encoding-type=url` | keys and prefixes encoded and `EncodingType` echoed; in ListObjects v1 not `Prefix` | same | `TestListingAPI` |
| ListObjects v1 `NextMarker` | only with a delimiter | same | `TestListingAPI` |
| Empty `continuation-token` | treated as absent, echoed | same | `TestS3BehavioursFromConformanceSuite` |
| CopyObject onto itself with the COPY directive | 400 `InvalidRequest` | same | `TestCopyAPI` |
| ETag of a copy of a multipart object | MD5 of the data | same | `TestModelRandomOperations` |
| Multipart ETag | MD5 of the part MD5s, `-N` | same | `TestMultipartAPI` |
| Complete with parts out of order / unknown ETag / a part under 5 MiB that is not last | `InvalidPartOrder` / `InvalidPart` / `EntityTooSmall` | same | `TestMultipartAPI`, `internal/store` |
| Complete retried after success | 200 with the same result | same (until restart) | `TestMultipartAPI` |
| `partNumber=1` of a single-PUT object; `partNumber` past the last part | the whole object; 400 `InvalidPart` | same | `TestS3BehavioursFromConformanceSuite` |
| UploadPartCopy range past the source | `InvalidRange` | same | `TestS3BehavioursFromConformanceSuite` |
| Re-creating a bucket you own in us-east-1 | 200, bucket unchanged | same | `TestBucketsAPI` |
| `Content-MD5` or `x-amz-checksum-*` mismatch | 400 `BadDigest`, nothing stored | same | `TestObjectAPI`, `TestPayloadVerification` |
| Signed payload hash mismatch | 400 `XAmzContentSHA256Mismatch`, nothing stored | same | `TestPayloadVerification` |
| Unsigned `x-amz-*` header | 403 `AccessDenied` | same | `internal/sigv4` |
| Request time off by more than 15 minutes | 403 `RequestTimeTooSkewed` | same | `TestAuthAPI` |
| Presigned URL past its expiry, or with negative `X-Amz-Expires` | 403 `AccessDenied` | same | `TestAuthAPI`, `internal/sigv4` |
| `X-Amz-Expires` over 7 days | 400 `AuthorizationQueryParametersError` | same | `internal/sigv4` |
| Bucket name rules (length, case, IP form, `..`, `xn--`) | `InvalidBucketName` | same | `internal/store` |
| Key over 1,024 bytes; user metadata over 2 KB | `KeyTooLongError`; `MetadataTooLarge` | same | `TestObjectAPI` |
| `Content-Encoding: aws-chunked, gzip` | stored as `gzip` | same | `TestS3BehavioursFromConformanceSuite` |

## ceph/s3-tests

[ceph/s3-tests](https://github.com/ceph/s3-tests) is the conformance suite of Ceph's RADOS Gateway: 838 boto3 tests in `s3tests/functional/test_s3.py`, covering most of the S3 API, many of them RGW-specific. [`test/s3tests/run.sh`](../test/s3tests/run.sh) clones it at commit `5522d1c`, starts strata (4+2) on an ephemeral port with three users, and runs the whole file:

```sh
test/s3tests/run.sh            # about 3 minutes
```

**Result: 250 passed, 494 failed, 94 skipped** (2026-10-02). The suite's own skips are bucket-logging extensions (74) and storage-class and cloud-tier tests that need configuration strata does not have.

| Failures | Count |
|---|---:|
| Features strata does not implement, answered with 501: ACLs (84), versioning (69), bucket policy (58), SSE headers (89), object lock (32), lifecycle (17), bucket encryption (16), public access block (9), tagging (10), ownership controls (6), policy status (3), logging (2), torrent (1), others (4) | 400 |
| Tests of the same features that fail before reaching a 501: browser POST uploads (24), SSE (22), ACLs and per-user ownership (8), CORS (5), lifecycle (6), object lock (7), tagging (4), policy (2), logging (1) | 79 |
| RGW extensions (`X-RGW-Object-Count`, unordered listings, account usage, `x-amz-delete-marker: false` on unversioned buckets) | 6 |
| Marked `fails_on_aws`, or directory-bucket-only conditions (anonymous ListBuckets, `x-amz-if-match-size` and `-last-modified-time` on DELETE) | 5 |
| **Differences in implemented features** | **4** |

The four differences:

- `test_bucket_get_location`: the test creates a bucket with `LocationConstraint=us-east-1` and expects that string back. S3 reports `us-east-1` buckets with an empty constraint, as strata does (S3 also rejects that constraint at creation).
- `test_multipart_resend_first_finishes_last`: completes an upload listing part 1 twice. strata rejects repeated part numbers as `InvalidPartOrder`, as the S3 documentation requires ascending order; RGW accepts them.
- `test_object_set_get_unicode_metadata` (marked `fails_on_rgw`): non-ASCII user metadata. strata stores and returns the bytes as sent; S3 returns such values MIME-encoded.
- `test_object_read_unreadable` (marked `fails_on_rgw`): expects 400 for a key containing a C1 control character; strata accepts any valid UTF-8 key.

Running the suite was worth it: the first run passed 214 tests and found real differences in implemented features, now fixed (commit "fix(s3api): S3 behaviours found by running ceph/s3-tests"): paginated ListBuckets, us-east-1 bucket re-creation, the error for an empty CompleteMultipartUpload, idempotent completion retries, multipart checksums (composite and full-object, including CRC combination), GetObjectAttributes, `partNumber` semantics, UploadPartCopy range checks, conditional deletes, `BadDigest` for malformed checksums, empty continuation tokens, v1 `Prefix` encoding, Content-Encoding normalisation, negative presign expiry and HTTP chunked uploads.

## Real clients

The AWS CLI v2, the AWS SDK for Go v2, boto3 and minio-go are tested in CI against a running strata ([`test/awscli.sh`](../test/awscli.sh), [`test/interop`](../test/interop), [`test/boto3_test.py`](../test/boto3_test.py)); see the README for what each covers. Notes from making them work:

- AWS CLI 2.37 (botocore) signs every payload's SHA-256 over plain HTTP and switches to aws-chunked bodies with unsigned payloads and a trailing CRC64NVME over HTTPS; it never sends per-chunk signatures. minio-go does, over HTTP.
- botocore keeps user-metadata names as the server sends them, so `x-amz-meta-*` headers must go out in lowercase (Go canonicalises header names unless told otherwise).
- boto3 1.43 presigns with Signature V2 by default for a custom endpoint in us-east-1.
- The AWS SDK for Go v2 refuses to upload an unseekable stream over plain HTTP; over TLS it uses aws-chunked with a trailer.
