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
