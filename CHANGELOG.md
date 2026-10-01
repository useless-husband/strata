# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

### Added
- Reed–Solomon erasure coding over GF(2^8) with a systematic Cauchy matrix,
  an arm64 NEON kernel and a portable table-driven kernel.
- AWS Signature Version 4: header and presigned authentication, aws-chunked
  bodies with per-chunk signatures, signed and unsigned trailers.
- Storage engine: striped, erasure-coded objects over N directories with a
  CRC32C per shard block, atomic commits, degraded reads, on-read and
  background healing, deep scrubbing, reformatting of wiped disks.
- S3 API: buckets, objects, copy, batch delete, ranged and conditional
  requests, ListObjects v1/v2, multipart uploads, checksums.
- `strata server`, `heal`, `scrub`, `info`; Prometheus metrics; structured logs.
- Group-committed drive-cache flushes (one `F_FULLFSYNC` per device per commit
  phase, shared by concurrent commits) and inline storage of objects up to
  128 KiB; 4 KiB durable PUTs went from 24/s to 224/s with 16 clients.
- Parallel shard writes; start-up quorum counting from directory listings.
- Signature Version 2 verification (boto3's default for presigned URLs).
- Multipart checksums (COMPOSITE and FULL_OBJECT with CRC combination),
  GetObjectAttributes, paginated ListBuckets, conditional DELETE, idempotent
  CompleteMultipartUpload retries, and other fixes found with ceph/s3-tests.
- Tests with the AWS CLI, the AWS SDK for Go v2, boto3, minio-go and
  ceph/s3-tests; a kill -9 crash test; a model-based random test.

### Fixed
- A heal racing a DELETE or overwrite could report the object as lost.
- CompleteMultipartUpload and DeleteBucket could deadlock.
