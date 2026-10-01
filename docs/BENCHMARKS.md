# Benchmarks

Every number here was measured for this document with the commands shown. Read them with three caveats:

- **One machine, one SSD.** Apple M5 (10 cores), 16 GB, macOS 27.0, Go 1.27.1, internal APFS SSD. All six "disks" are directories on that one SSD, so these numbers say nothing about a server with six drives: the drives' bandwidth is shared, every flush flushes the same device, and the erasure coding buys no parallelism.
- **A shared machine.** Other builds and tests were running at the same time (load averages between 3 and 10 during the runs). Repeat runs varied by up to about 25% for small-object GETs; treat the numbers as an order of magnitude.
- **Client and server on the same host,** over loopback. The client signs every payload's SHA-256 (as the AWS CLI does over HTTP) and the server verifies it.

## Reed–Solomon coding

One goroutine, 1 MiB stripes, throughput in MB/s of object data. `make bench-rs` for strata; [`test/interop/rs_compare_test.go`](../test/interop/rs_compare_test.go) runs [klauspost/reedsolomon](https://github.com/klauspost/reedsolomon) v1.14.2 (the library MinIO uses) with the same shapes, `WithMaxGoroutines(1)`.

```sh
go test -run XXX -bench . -cpu 1 -benchtime 2s ./internal/rs
cd test/interop && go test -run XXX -bench Klauspost -cpu 1 -benchtime 2s .
```

| operation | strata, NEON | strata, portable | klauspost |
|---|---:|---:|---:|
| encode 4+2 | 23,600 | 2,020 | 10,280 |
| encode 8+4 | 11,500 | 940 | 11,110 |
| encode 12+4 | 11,340 | 950 | 8,610 |
| rebuild 1 lost data shard, 4+2 | 46,480 | 4,090 | 15,940 |
| rebuild 2 lost data shards, 4+2 | 23,160 | 2,030 | 10,090 |
| rebuild 4 lost data shards, 8+4 | 11,320 | 940 | 11,170 |

"Rebuild" is `ReconstructData`: only the missing data shards are computed, which is what a degraded read needs.

How to read this: strata's kernel is a straightforward NEON `TBL` loop (two 16-entry tables per coefficient, 64 bytes per iteration) that processes 16 KiB chunks so the output stays in L1 while every input is added into it. On this CPU it is as fast as klauspost's for the larger codes and faster for 4+2; klauspost is far more general (AVX2, AVX-512, GFNI and SVE kernels, multi-goroutine splitting, Leopard codes for hundreds of shards), and on x86 strata has only the portable kernel, which is about ten times slower. Either way, coding is not what limits strata: the storage path below runs at under 1 GB/s.

## Storage engine

No HTTP and no SigV4: `PutObject` and `GetObject` of the store package on 4+2 disks, 64 MiB objects, one stream.

```sh
go test -run XXX -bench 64MiB -benchtime 10x ./internal/store
```

| benchmark | MB/s |
|---|---:|
| PUT, `--sync none` | 703 |
| PUT, `--sync full` | 512 |
| GET, all disks | 8,190 |
| GET, two disks gone (every stripe reconstructed) | 6,090 |

A PUT computes the MD5 ETag, encodes, checksums and writes 96 MB (1.5×) to six files; one writer goroutine per disk overlaps the writes with encoding the next stripe. Before that pipeline (one goroutine writing the six files in turn) the same benchmark ran at 364 MB/s; the profile then was dominated by `write(2)`.

## S3 over HTTP

[`tools/s3bench`](../tools/s3bench) starts strata on fresh directories (4+2, 1 MiB stripes) and runs each cell for 4 seconds; every client overwrites four keys of its own, so disk usage stays bounded.

```sh
make build
go run ./tools/s3bench -strata ./strata                     # --sync full, the default
go run ./tools/s3bench -strata ./strata -sync none -concurrency 1,16
```

### `--sync full` (durable: data is on stable storage before the response)

| size | clients | PUT ops/s | PUT MiB/s | PUT p50 | PUT p99 | GET ops/s | GET MiB/s | GET p50 | GET p99 |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 4 KiB | 1 | 105 | 0.4 | 9.1 ms | 11.5 ms | 3,622 | 14 | 275 µs | 361 µs |
| 4 KiB | 4 | 137 | 0.5 | 29.2 ms | 43.9 ms | 4,427 | 17 | 892 µs | 1.5 ms |
| 4 KiB | 16 | 224 | 0.9 | 69.0 ms | 125.7 ms | 6,379 | 25 | 2.3 ms | 6.0 ms |
| 1 MiB | 1 | 70 | 70 | 14.2 ms | 18.6 ms | 1,904 | 1,904 | 526 µs | 687 µs |
| 1 MiB | 4 | 99 | 99 | 40.3 ms | 53.0 ms | 4,863 | 4,863 | 811 µs | 1.3 ms |
| 1 MiB | 16 | 109 | 109 | 139.8 ms | 321.9 ms | 6,815 | 6,815 | 2.1 ms | 5.9 ms |
| 64 MiB | 1 | 6 | 364 | 176 ms | 179 ms | 74 | 4,707 | 13.6 ms | 14.5 ms |
| 64 MiB | 4 | 11 | 734 | 349 ms | 378 ms | 150 | 9,610 | 25.5 ms | 44.6 ms |
| 64 MiB | 16 | 13 | 819 | 1.15 s | 1.58 s | 139 | 8,927 | 114 ms | 189 ms |

### `--sync none` (survives a process crash, not a power cut)

| size | clients | PUT ops/s | PUT MiB/s | PUT p50 | PUT p99 | GET ops/s | GET MiB/s |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 4 KiB | 1 | 1,333 | 5.2 | 728 µs | 1.1 ms | 3,438 | 13 |
| 4 KiB | 16 | 1,757 | 6.9 | 8.0 ms | 26.4 ms | 6,360 | 25 |
| 1 MiB | 1 | 183 | 183 | 5.4 ms | 7.1 ms | 1,942 | 1,942 |
| 1 MiB | 16 | 315 | 315 | 47.9 ms | 102.7 ms | 6,819 | 6,819 |
| 64 MiB | 1 | 7 | 439 | 143 ms | 177 ms | 75 | 4,776 |
| 64 MiB | 16 | 33 | 2,089 | 460 ms | 818 ms | 140 | 8,946 |

With the real AWS CLI, [`test/awscli.sh`](../test/awscli.sh) reports uploading a 300 MiB file (38 parts, 8 MiB each, the CLI's default 10 concurrent requests) in about 2 seconds with `--sync full`, over HTTP and over HTTPS alike.

## Why small writes cost what they cost

Durability on macOS is the expensive part of a small PUT. [`tools/syncprobe`](../tools/syncprobe) measures the primitives on this machine (`go run ./tools/syncprobe`):

| operation on a fresh 4 KiB file | mean | p50 | p99 |
|---|---:|---:|---:|
| `fsync(2)` | 71 µs | 62 µs | 223 µs |
| `F_FULLFSYNC` | 3.81 ms | 3.80 ms | 4.81 ms |
| `F_FULLFSYNC`, nothing dirty | 2 µs | 1 µs | 76 µs |

`fsync(2)` on macOS only hands the data to the drive; `F_FULLFSYNC` also flushes the drive's volatile cache, and that flush is the 4 ms. A second effect matters as much: while an `F_FULLFSYNC` is in flight, APFS slows every other metadata operation on the volume.

| operation (8 threads) | mean, idle | mean, during F_FULLFSYNC | p99, during F_FULLFSYNC |
|---|---:|---:|---:|
| create | 356 µs | 1.27 ms | 3.91 ms |
| fsync (file) | 79 µs | 387 µs | 3.65 ms |
| rename | 429 µs | 1.85 ms | 4.34 ms |
| fsync (directory) | 118 µs | 1.21 ms | 4.06 ms |
| mkdir | 317 µs | 1.14 ms | 3.87 ms |
| unlink | 121 µs | 1.13 ms | 3.82 ms |

That explains how the small-PUT numbers developed (4 KiB, `--sync full`, ops/s at 1 / 4 / 16 clients):

| version | 1 | 4 | 16 | what changed |
|---|---:|---:|---:|---|
| first | 24 | 24 | 24 | `F_FULLFSYNC` on every file and directory: about 12 drive flushes per PUT, serialised |
| shared barrier | 30 | 41 | 48 | `fsync(2)` per file, then one `F_FULLFSYNC` per device shared by concurrent waiters |
| two-phase commit | 81 | 96 | 100 | all disks push, then one flush; rename, push, one flush: about 2 flushes per PUT |
| inline objects | 111 | 141 | 228 | objects ≤ 128 KiB live inside their metadata files: ~5 instead of ~15 file operations per disk |

After the barrier, flushes were being shared poorly (652 waits needed 583 flushes) because the PUTs were not reaching the barrier together: each one was busy with its ~90 file operations, and every operation stalled behind whatever flush was running. Cutting the number of file operations per PUT was what helped, more than coalescing flushes. Small PUTs are still bound by this; with `--sync none` the same PUTs run at 1,300–1,750 per second. Off macOS, `fsync(2)` already flushes the drive cache and none of this applies; those numbers have not been measured.

## Start-up

Opening a store rebuilds the in-memory listing index from the disks: one directory listing per object per disk and one metadata read per object. 20,000 small objects on 4+2 open in about 4 s, **157 µs per object** (`go test -run XXX -bench BenchmarkOpen -benchtime 3x ./internal/store`). That is linear: a million objects would take minutes, which is why this is listed under the README's limitations. Reading every metadata copy, as the first version did, cost 410 µs per object; eight scan workers were slower than four.
