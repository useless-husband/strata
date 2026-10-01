# strata

**A self-hosted object store that speaks the Amazon S3 API and keeps data on several disks with Reed–Solomon erasure coding — the coding, the S3 signature checks and the storage engine all written from scratch in Go, with no dependencies.**

Point the official AWS tools at it unchanged (`aws s3 cp`, the AWS SDKs, boto3, minio-go). Give it *k + m* directories that stand in for disks: every object is cut into stripes, each stripe is coded into *k* data and *m* parity shards, and each disk holds one shard of every stripe with a checksum per block. Any *k* disks are enough to read everything. Lost or silently corrupted shards are found on read, by a background scrubber or by `strata heal`, and rebuilt. New objects become visible atomically, so a crash never exposes half an object.

[繁體中文說明](README.zh-TW.md) · [Design](docs/DESIGN.md) · [Benchmarks](docs/BENCHMARKS.md) · [S3 compatibility](docs/COMPATIBILITY.md) · [導讀（給初學者）](docs/導讀.zh-TW.md)

## What it looks like

Six directories as disks, 4 data + 2 parity. Upload with the AWS CLI, then delete two of the disks outright ([`test/demo.sh`](test/demo.sh), unedited output except the temporary paths):

```
$ aws --endpoint-url http://127.0.0.1:52588 s3 cp video.bin s3://demo/media/video.bin
$ aws --endpoint-url http://127.0.0.1:52588 s3 cp note.txt s3://demo/note.txt

$ du -sh disk1 disk2 disk3 disk4 disk5 disk6        # 64 MB object, 1.5× overhead
 15M	disk1
 15M	disk2
 15M	disk3
 15M	disk4
 15M	disk5
 15M	disk6

$ rm -rf disk2 disk5                                 # lose two disks

$ aws --endpoint-url http://127.0.0.1:52588 s3 cp s3://demo/media/video.bin video.back
$ cmp video.bin video.back                           # identical
$ aws --endpoint-url http://127.0.0.1:52588 s3 cp s3://demo/note.txt -
hello, erasure coding

# The reads were served by reconstruction and queued the objects for healing;
# the wiped disks were noticed, reformatted and refilled.
$ curl -s http://127.0.0.1:52588/-/metrics | grep -E '^strata_(degraded_stripes|missing_shards|healed_objects|disks_replaced)_total'
strata_missing_shards_total 16
strata_degraded_stripes_total 62
strata_healed_objects_total 2
strata_disks_replaced_total 2

$ du -sh disk2 disk5
 15M	disk2
 15M	disk5

$ strata scrub --endpoint http://127.0.0.1:52588    # read and verify every block
checked 2 objects in 1 buckets in 18ms (91.6 MiB read)
damaged: 0   repairable: 0   lost: 0   stale versions removed: 0

# Bit rot: flip one bit in the middle of a shard file on disk3, then read again.
$ cmp video.bin video.back2                          # identical
$ curl -s http://127.0.0.1:52588/-/metrics | grep -E '^strata_(corrupt_blocks|healed_objects)_total'
strata_corrupt_blocks_total 2
strata_healed_objects_total 3
```

The flipped block failed its CRC32C, the stripe was served from the other shards, and the object was rewritten.

## Quick start

```sh
go install github.com/useless-husband/strata/cmd/strata@latest     # Go 1.26+; or: make build

export STRATA_ACCESS_KEY=admin STRATA_SECRET_KEY=change-me-please
strata server --address 127.0.0.1:9000 /srv/strata/disk{1...6}      # 6 disks → 4 data + 2 parity

export AWS_ACCESS_KEY_ID=admin AWS_SECRET_ACCESS_KEY=change-me-please AWS_DEFAULT_REGION=us-east-1
aws --endpoint-url http://127.0.0.1:9000 s3 mb s3://photos
aws --endpoint-url http://127.0.0.1:9000 s3 sync ~/Pictures s3://photos/
```

| Command | What it does |
|---|---|
| `strata server DISK...` | Serve the S3 API. `--data`/`--parity` choose the code (default: a third of the disks are parity), `--sync full\|fsync\|none`, `--tls-cert/--tls-key`, `--scrub-interval`, `--domain` for virtual-hosted-style buckets. Every flag has a `STRATA_*` variable. |
| `strata heal` | Repair missing or damaged shards (`--deep` verifies every block). Offline on the disks, or `--endpoint URL` against a running server. |
| `strata scrub` | Read and verify every block; repair nothing; exit status 1 if anything is damaged. |
| `strata info` | Layout, object counts, integrity counters and disk space. |

The server also serves, without authentication, `/-/health` (503 when fewer disks than a write needs are online) and Prometheus metrics at `/-/metrics`: requests and latency histograms per S3 API, bytes in and out, corrupt blocks, missing shards, degraded stripes, healed and lost objects, scrub progress, per-disk state and free space.

## How it works

```
 PUT ─► SigV4 check ─► body verifier ────────► 1 MiB stripes ─► Reed–Solomon k+m ─► CRC32C per block
         (header,       (SHA-256, aws-chunked     │                (Cauchy matrix,      │
          presigned,     chunk signatures,        │                 NEON TBL kernel)    ▼
          V2 for boto3)  checksum trailers,       │                          disk1 … diskN/.strata/tmp/<op>/
                         Content-MD5, MD5 ETag)   │                          shard files, synced
                                                  ▼                                     │
                                  all checks pass at end of stream ─────── rename into place, metadata last,
                                                                           on ≥ max(k, m+1) disks ─► visible
```

- **Layout.** Each disk holds `buckets/<bucket>/objects/<hh>/<hash>/<version>.meta` and `<version>/part.N`. The object directory is named by a hash of the key, so S3 keys with `..`, `//` or 1,024 bytes of UTF-8 never meet the file system's rules. The shard of disk *d* is a rotation chosen from the key, so parity is spread over all disks. Objects up to 128 KiB are stored inline: each disk's shard file is the payload of its metadata file, one file per object per disk.
- **Shard files** are `[crc32c][block]` per stripe; the CRC is seeded with the stripe number and shard index, so a block that lands at the wrong offset or on the wrong disk fails too. Ranged reads touch only the blocks they need.
- **Commit.** A new version is staged under `.strata/tmp` on every disk, synced, and renamed into place, data first and metadata last; the metadata rename is each disk's commit point. A version counts only if *k* disks hold its metadata, and a write needs max(*k*, *m*+1) disks, so a crash at any moment leaves the old version or the new one, and a deleted version can never resurface. On macOS the drive's write cache is flushed with `F_FULLFSYNC`, once per device per commit phase and shared by concurrent commits (group commit).
- **Reads** verify every block and, on a missing file or checksum mismatch, read other shards (data first, then parity) until they have *k*, reconstruct, and queue the object for healing. A GET keeps the version it opened alive (a lease) even if the key is overwritten mid-download.
- **Healing** rebuilds damaged copies from any *k* intact blocks per stripe, found on any disk, and installs them with the same staging and rename protocol. Sweeps run after a wiped disk is detected; `--scrub-interval` runs periodic deep scrubs.
- **Listing** is served from an in-memory ordered index (a one-level B+ tree) rebuilt from the metadata at start-up; delimiter listings skip each common prefix in one seek instead of walking its keys.

[docs/DESIGN.md](docs/DESIGN.md) explains the hard parts (quorums and visibility, crash consistency, leases, healing without blocking readers) and the alternatives that were rejected.

## What is verified, and how

| Claim | Evidence (all in CI) |
|---|---|
| The Reed–Solomon code is MDS and correct | Every *k*-subset of rows is invertible for all codes with *k*+*m* ≤ 12 (8,166 subsets); every erasure pattern of up to *m* shards recovers bit-exact for 8 codes; parity matches a byte-by-byte reference; NEON and portable kernels agree for all 256 constants; field axioms checked over all 2²⁴ triples; fuzzing. `go test ./internal/gf ./internal/rs` |
| SigV4 is implemented as AWS specifies it | The header, presigned and 64 KiB + 1 KiB aws-chunked examples from the S3 API reference verify byte for byte, as do the Signature V2 examples; tampering, truncation and chunk reordering are rejected. `go test ./internal/sigv4` |
| The real AWS CLI works | [`test/awscli.sh`](test/awscli.sh): mb, cp of a small file and a 300 MiB file (multipart), ls with prefixes, sync up/down with `--delete`, presign + curl, s3api metadata/copy/range/If-None-Match, rm, rb, rb --force — once over HTTP (signed payloads) and once over HTTPS (aws-chunked bodies with a trailing CRC64NVME). |
| The official SDKs work | [`test/interop`](test/interop): AWS SDK for Go v2 (all checksum algorithms with validation on read, paginators, transfer manager multipart up/download, presigning, unseekable streams over TLS) and minio-go (which sends per-chunk-signed aws-chunked bodies). [`test/boto3_test.py`](test/boto3_test.py): boto3 incl. s3transfer and V2/V4 presigned URLs. |
| Losing up to *m* disks loses nothing | Every combination of *m* wiped disks for 2+1, 2+2, 4+2 and 3+3: all objects read back bit-exact, heal, deep scrub clean, then lose *m* other disks and restart. `go test -run SurvivesLosing ./internal/store` |
| Bit rot is detected and repaired | Random bit flips in up to two shard files per object; reads bit-exact, on-read heal, deep scrub clean; corrupt metadata copies are healed too. `go test -run 'BitRot|CorruptMetadata' ./internal/store` |
| A crash never exposes a partial object | `kill -9` of the server (by PID) during concurrent PUTs, multipart uploads and DELETEs, 5 rounds in CI (75 rounds over three seeds locally): every key holds one complete version — its last acknowledged state or a later attempt — listings agree with GETs, and a final deep scrub is clean. `make crash` |
| Conformance, measured by someone else's suite | [ceph/s3-tests](https://github.com/ceph/s3-tests) (838 tests, Ceph RGW's suite): **250 passed, 494 failed, 94 skipped**. 479 of the failures exercise features strata does not implement (ACLs, versioning, policies, encryption, object lock, …), 11 are RGW extensions or fail on AWS too, and 4 are differences in implemented features, explained in [COMPATIBILITY.md](docs/COMPATIBILITY.md). `test/s3tests/run.sh` (not in CI: needs network) |
| The API behaves like S3 under random use | A seeded model-based test drives random operations against strata and an in-memory model of S3 semantics and compares every response, restarting the server mid-run. `go test -run Model ./internal/s3api` |

## Benchmarks

Measured on an Apple M5 (10 cores, 16 GB) under macOS 27 with Go 1.27, **on a machine shared with other work**; all six "disks" are directories on one internal SSD, so these numbers say nothing about a multi-drive server. Method, full tables and the measurements behind the design choices: [docs/BENCHMARKS.md](docs/BENCHMARKS.md).

Reed–Solomon, one core, 1 MiB stripes, MB/s of object data (`make bench-rs`), with [klauspost/reedsolomon](https://github.com/klauspost/reedsolomon) measured the same way on the same machine for reference:

| | 4+2 encode | 8+4 encode | 4+2 rebuild 1 data shard | 4+2 rebuild 2 | 8+4 rebuild 4 |
|---|---:|---:|---:|---:|---:|
| strata, NEON `TBL` kernel | 23,600 | 11,500 | 46,500 | 23,200 | 11,300 |
| strata, portable table kernel | 2,020 | 940 | 4,090 | 2,030 | 940 |
| klauspost/reedsolomon v1.14.2, one goroutine | 10,300 | 11,100 | 15,900 | 10,100 | 11,200 |

S3 over HTTP on 4+2 with `--sync full` (data on stable storage before the response), [`tools/s3bench`](tools/s3bench), client on the same host:

| object size | 1 client PUT | 16 clients PUT | 1 client GET | 16 clients GET |
|---:|---:|---:|---:|---:|
| 4 KiB | 105/s, p50 9.1 ms | 224/s | 3,600/s, p50 275 µs | 6,400/s |
| 1 MiB | 70 MiB/s | 109 MiB/s | 1.9 GB/s | 6.8 GB/s |
| 64 MiB | 364 MiB/s | 819 MiB/s | 4.7 GB/s | 8.9 GB/s |

Small durable writes are bound by macOS's `F_FULLFSYNC` (about 4 ms per drive-cache flush, during which APFS slows every other file operation); strata shares one flush per device among concurrent commits and stores objects up to 128 KiB inside their metadata files, which took 4 KiB PUTs from 24/s to 224/s at 16 clients. With `--sync none` they run at about 1,700/s.

## Limitations

Written down rather than faked:

- **No IAM, policies, ACLs, versioning, object lock, encryption, tagging, lifecycle, website or event features.** Requests that need them get `501 NotImplemented` (only the private canned ACL is accepted). Every access key has full access.
- **One node.** The disks are directories of one process; there is no clustering, replication between servers or rebalancing. Start-up requires every disk path to be accessible (an empty directory is treated as a replaced disk).
- **Erasure parameters are fixed at format time**, and disks cannot be added later.
- **The listing index lives in memory** and is rebuilt from the disks at start-up (about 160 µs per object here: 20,000 objects open in 4 s): memory and start-up time grow with the number of objects.
- **In-progress multipart uploads are not healed**: completing one needs a write quorum of disks that still hold every part. Completion retries are recognised only until a restart.
- **Healing does not cover the window** in which an acknowledged write that reached exactly the write quorum is followed by a restart with one of those disks missing: until that disk returns, the previous version is served (see DESIGN).
- **The crash test kills the process, not the machine.** It proves the commit protocol; that `F_FULLFSYNC` makes data survive power loss is Apple's guarantee, not something tested here.
- Signature Version 4A, SSE-C, POST policy uploads and SelectObjectContent are not implemented.

## Related work

- **[MinIO](https://github.com/minio/minio)** is the closest design: S3 API, erasure sets over drives, per-shard bitrot hashes, inline healing. It is a production system with distributed mode, IAM, versioning and much more. strata borrows the overall shape (staging + rename commits, quorum reads) and differs in details documented in DESIGN (Cauchy rather than Vandermonde-derived matrices, CRC32C with position seeding rather than HighwayHash, immutable per-version metadata files rather than one rewritten `xl.meta`, an in-memory index for listing instead of directory walks, group-committed drive flushes). Storing small objects inside their metadata follows MinIO's inline data.
- **[klauspost/reedsolomon](https://github.com/klauspost/reedsolomon)** is the Go Reed–Solomon library MinIO uses, with AVX2/AVX-512/GFNI/NEON/SVE kernels; strata's codec is independent and simpler (one kernel shape).
- **[Garage](https://garagehq.deuxfleurs.fr/)**, **[SeaweedFS](https://github.com/seaweedfs/seaweedfs)** and **[Ceph RGW](https://docs.ceph.com/en/latest/radosgw/)** are distributed S3-compatible stores (replication in Garage, erasure coding in SeaweedFS and Ceph).
- **[s3proxy](https://github.com/gaul/s3proxy)** and **[versitygw](https://github.com/versity/versitygw)** translate S3 to other back ends without their own redundancy layer.

strata is a single-binary study of the core mechanisms, small enough to read (about 10,600 lines of non-test Go, comments included, and 5,000 of tests), with each correctness claim tied to a test.

## Build and test

```sh
make build     # ./strata
make test      # unit, integration, durability and model-based tests
make race      # the same with the race detector
make lint      # gofmt, go vet, staticcheck
make awscli    # the real AWS CLI against a server (needs aws v2 and openssl)
make interop   # AWS SDK for Go v2 and minio-go
make boto3     # boto3 in a .venv
make crash     # kill -9 during uploads
make bench     # Reed–Solomon and S3 throughput
```

## License

[MIT](LICENSE)
