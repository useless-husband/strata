# Design

This document explains how strata is built and why, for anyone reviewing the code or deciding whether to trust it. The README covers what it does; [BENCHMARKS.md](BENCHMARKS.md) has the measurements referred to here.

## Packages

| Package | Responsibility |
|---|---|
| `internal/gf` | GF(2^8) arithmetic (log/exp and product tables built from a shift-and-add multiplier), matrices, Gauss–Jordan inversion, Cauchy matrices. |
| `internal/rs` | Systematic Reed–Solomon codec: `Encode`, `Reconstruct`, `ReconstructData`, `Verify`; arm64 NEON and portable kernels. |
| `internal/sigv4` | Signature V4 verification (header, presigned), aws-chunked decoding with chunk and trailer signatures, Signature V2 verification, and a signer for strata's own tools. |
| `internal/store` | The storage engine: disk layout and formatting, shard files, atomic commits, reads with reconstruction, healing, scrubbing, the listing index, multipart uploads, durability barriers. |
| `internal/s3api` | HTTP handlers for the S3 REST API, body verification, XML, errors, metrics and admin endpoints. |
| `internal/s3err` | S3 error codes with their HTTP statuses. |
| `internal/s3client` | A minimal client for the crash test and the load generator. |
| `cmd/strata` | `server`, `heal`, `scrub`, `info`. |

`internal/store` knows nothing about HTTP, and `internal/s3api` nothing about files. The store speaks S3 concepts (buckets, keys, ETags, conditional writes) and returns `s3err` errors, because it implements S3 semantics; splitting that further would only add translation code.

## The write path

```
 HTTP body ─► chunk decoder ─► bodyReader ─────────► store.PutObject
              (aws-chunked:     counts bytes,          ├─ ≤ 128 KiB: encode in memory (inline)
               verify each       SHA-256 of payload,   └─ larger: writePart, stripe by stripe:
               chunk's sig       x-amz-checksum-*,          read 1 MiB ─► MD5 ─► split ─► RS encode
               before release,   verdict at EOF             ─► CRC32C per block ─► one writer goroutine
               trailer sig)                                    per disk ─► .strata/tmp/<op>/data/part.1
                                                       Precommit(md5): Content-MD5, checksum ─┐
                                                       commit (key lock) ◄────────────────────┘
                                                         phase 1: write metadata to tmp, push all, flush
                                                         phase 2: rename data dir, rename metadata,
                                                                  push dir, flush
                                                         ≥ max(k, m+1) disks? index ← new version
                                                         old version: metadata now, data after readers
```

Everything the client promised about the body (length, signed SHA-256, chunk signatures, trailing checksum, Content-MD5) is checked by the time the store has read the last byte and before anything is renamed into place. A body that fails any check makes `PutObject` return the error, and the staging directory is deleted; no reader can ever see it.

## Hard problem 1: atomic visibility without a coordinator

There is no transaction log and no leader: each disk is a directory, and a crash can stop a commit after any subset of disks has renamed its files. strata makes this safe with immutable per-version metadata files and two quorum sizes.

**Per-version files.** A version is `<version>.meta` plus (for objects over 128 KiB) a `<version>/` directory of shard files, in the key's object directory on each disk. Versions are never modified after their metadata is renamed into place; a new PUT adds a new version next to the old one, and the old one is deleted only after the new one is committed. (MinIO keeps all versions in one `xl.meta` file that is rewritten on every change; strata's choice means no read-modify-write of metadata and nothing to tear.)

**Visibility rule.** A version is visible if at least *k* disks hold its metadata, and the newest such version (by modification time, then version ID) is current. This is the rule start-up applies to rebuild the index, and the rule healing applies when a disk returns.

**Write quorum.** A write (or delete) must reach *w* = max(*k*, *m*+1) disks before it is acknowledged. This gives two properties:

1. *w* + *k* > *n*, since *w* ≥ *m*+1 = *n*−*k*+1. Any *k* disks a later reader uses overlap every acknowledged write.
2. After a delete reaches *w* disks, at most *n*−*w* ≤ *k*−1 copies of the old metadata remain, so the deleted version can never again be seen on *k* disks, even if every disk that missed the delete comes back.

For 4+2 this is *w* = 4; for 2+2 it is 3, not 2, because otherwise a 2+2 store could acknowledge a delete on two disks and have the other two resurrect the object.

**Monotonic versions.** "Newest" decides which version wins after a crash, so a new version's modification time is `max(now, current + 1 ns)`, taken under the key's lock. A clock that steps back cannot make an older version win.

**The crash cases.** A commit renames, on each disk, first the data directory and then the metadata; the metadata rename is the commit point on that disk.

| The process dies… | Disk state | After restart |
|---|---|---|
| while receiving or encoding | staging files only | `.strata/tmp` is emptied; the old version is current |
| after some metadata renames, fewer than *k* | new version on < *k* disks | not visible; removed at start-up (with every disk present); old version current |
| after ≥ *k* metadata renames | new version visible | current; its missing copies are healed; old version removed |
| after the commit, before deleting the old version | both versions present | newest with quorum (the new one) wins; the old one is removed |

The client gets its 200 only after *w* renames, so an acknowledged write is always in the third or fourth row. [`cmd/strata/kill_test.go`](../cmd/strata/kill_test.go) kills the process with SIGKILL at random moments during concurrent PUTs, multipart uploads and DELETEs and checks every key against the history of what was attempted; it logs how many keys it found half-committed at restart, which shows the kills do land inside commits.

**What it does not cover.** Start-up removes versions that lack a quorum only when every disk is formatted, since a version could otherwise have its other copies on a disk that is not there. There is one remaining window: an acknowledged write that reached exactly *w* disks, followed by a restart with one of those disks missing and replaced by an empty one. The new version is then on fewer than *k* readable disks, so the previous version is served. strata never deletes that newer version, and when its disk returns healing promotes it (`HealResult.Promoted`); but until then reads are stale. Writing to all *n* disks before acknowledging would close the window at the cost of failing writes whenever any disk is down; MinIO makes the same trade.

## Hard problem 2: durability on macOS without paying for it twelve times

On Linux `fsync(2)` makes a file durable. On macOS it only hands the data to the drive, whose volatile cache can still lose it on power failure; `F_FULLFSYNC` also flushes that cache, and costs about 4 ms on this machine (BENCHMARKS.md). The cache flush is a property of the device, not of the file.

strata separates the two halves. `push` is `fsync(2)`; `flush` waits on a **barrier** shared by all disks on the same device (same `st_dev`). The barrier implements group commit: a caller needs a flush that *started* after it arrived (its pushes completed before that); if a flush is running, the caller waits for the next one, which every caller that arrived meanwhile shares. The code is twenty lines in [`barrier.go`](../internal/store/barrier.go), and its test checks the rule with 64 concurrent waiters (served by two flushes).

Commits are then arranged as two phases across all disks: write and push every metadata file (the shard files were pushed when written), flush once; rename everything, push the directories, flush once. Data and metadata are durable before the renames, and the renames are durable before the acknowledgement.

That was not enough. Measurements showed that while an `F_FULLFSYNC` is in flight, APFS slows every other metadata operation on the volume (create, rename, mkdir, unlink, even plain `fsync`) by 1–2 ms on average and up to the length of the flush. A PUT doing about fifteen file operations per disk therefore serialised behind the flushes no matter how well they were shared. The fix was structural: objects up to 128 KiB are stored *inline*, each disk's shard file becoming the payload of its metadata file, so a small PUT creates one file and renames it, per disk. Together these took 4 KiB durable PUTs from 24/s to 224/s with 16 clients.

What this relies on is Apple's documented behaviour of `F_FULLFSYNC` ("asks the drive to flush all buffered data to the permanent storage device"). It has not been tested by cutting power; the crash test kills the process, which leaves the page cache intact.

## Hard problem 3: readers, writers and deletion

Three kinds of activity touch the same key: commits (write, overwrite, delete), reads, and healing. strata keeps them out of each other's way with three mechanisms.

- **Key locks** (1,024 striped `RWMutex`es). A commit holds the write lock only while renaming and updating the index, never while receiving the body, so a slow upload does not block anything. A reader holds the read lock only while it looks the key up, leases the version and reads its metadata (and, for an inline object, the payloads, which live in the metadata files that a commit deletes under the write lock).
- **Leases.** After an overwrite or delete, the old version's metadata is removed at once (under the lock) but its data directories are deleted only when the last reader that leased the version releases it. A download that has started therefore keeps working even if it later needs to open a parity shard to get round a bad block. The deferred deletion runs without the key lock, so it leaves the object directory alone: a concurrent PUT may be about to rename into it.
- **Heal locks**, separate from the key locks. Healing takes the key's read lock to snapshot the index and the disks and lease the version, then rebuilds shards with no lock held, then takes the write lock briefly to install the rebuilt copies, after checking that the version is still current. Because a delete or overwrite removes metadata (and inline data) at once, a heal that finds copies missing re-checks that its version is still current before calling them damaged or the object lost; the self-review found that without this check a heal racing a DELETE reported data loss. Two heals of the same key (a read noticed damage while a sweep was running) are serialised by the heal lock, so shards are never rebuilt twice in parallel.

**Lock order.** Upload mutex, then bucket lock (shared by commits, exclusive for DeleteBucket), then key lock. DeleteBucket aborts the bucket's uploads only after releasing the bucket; an earlier version aborted them while holding it and could deadlock with a completing upload, which a hook-driven test now reproduces.

## Hard problem 4: noticing bit rot

Every shard block (`1 MiB / k` bytes of one stripe) is stored with a CRC32C, and every metadata file starts with a CRC32C of its JSON. Reads verify every block they use.

**Why CRC32C.** The threat is random corruption: a flipped bit, a misdirected or torn write, a sector that returns stale data. CRC32C detects every burst of up to 32 bits and other random damage with probability 1 − 2⁻³² per block; it runs on dedicated instructions on arm64 and amd64, so verifying is far cheaper than decoding; and it is in Go's standard library. HighwayHash (MinIO's choice) and BLAKE3 would add a dependency or a hand-written SIMD implementation for strength that only matters against an adversary, and an adversary who can write the disks can rewrite the checksums too. SHA-256 would cost more than the erasure coding.

**Seeding.** The CRC of block *s* of shard *i* is computed over the stripe number and shard index followed by the data. A block that is intact but in the wrong place (written at the wrong offset, a shard file copied to the wrong disk, a stale file from another version) fails verification.

**Granularity.** One checksum per block, not per file, means a ranged read verifies only the blocks it reads, and a damaged block costs one stripe's reconstruction, not the object.

Reads touch only the data shards they need, so damage to a parity block, or to the zero padding of a short last stripe, is found only by a scrub (`strata scrub`, `--scrub-interval`), which reads every block of every copy.

## The erasure code

strata uses a systematic Reed–Solomon code over GF(2^8) with the primitive polynomial x⁸+x⁴+x³+x²+1 (0x11D). The encoding matrix is the *k*×*k* identity on top of an *m*×*k* Cauchy matrix C[i][j] = 1/(x_i + y_j) with y_j = j and x_i = k + i.

**Why Cauchy.** Every square submatrix of a Cauchy matrix is invertible. Choosing any *k* rows of [I; C] amounts, after removing the identity rows, to a square submatrix of C, so every choice of *k* surviving shards can be decoded: the code is MDS by construction. The common alternative, a Vandermonde matrix made systematic by multiplying by the inverse of its top square, is also MDS, but a naïve "identity on top of Vandermonde rows" is not (a pitfall that made it into published code), and the construction needs more care for no benefit at these sizes. The tests do not take the theory on trust: they check every *k*-subset of rows for every code with *k*+*m* ≤ 12.

**Decoding.** To rebuild data shards, strata takes the first *k* present shards (data first, which keeps the decode matrix closest to the identity), inverts that *k*×*k* submatrix, and multiplies. Inverses are cached by the set of rows used, so a disk that stays missing costs one inversion, not one per stripe.

**Kernel.** The inner operation is `out ^= c × in` for a constant *c* and a buffer. Multiplication by *c* is linear over GF(2), so c·x = c·(x & 0x0f) ⊕ c·(x & 0xf0): two 16-entry tables, and NEON's `TBL` looks up sixteen of them in one instruction. The assembly loop handles 64 bytes per iteration (4 vector registers in, 8 table lookups); the Go wrapper finishes the tail with a 256-entry product table, which is also the portable kernel. Work is done in 16 KiB chunks so the output chunk stays in L1 while every input is added into it. The tests compare the two kernels for all 256 constants at many lengths and alignments.

## SigV4 details that matter

- S3 URI-encodes the path once and does not normalise it, unlike other AWS services: `a//b` and `../x` are keys. strata's router never cleans paths.
- `x-amz-content-sha256` is either the payload's SHA-256, `UNSIGNED-PAYLOAD`, or a streaming mode. A signed hash is checked as the body streams, and a mismatch at the end aborts the upload.
- Every `x-amz-*` header must be signed, so headers like `x-amz-meta-*` cannot be added in transit.
- In aws-chunked bodies each chunk's signature chains from the previous one (starting from the request's own signature), so chunks cannot be reordered, dropped or replayed. A chunk is buffered and verified before any of its bytes are released; with trailers, the trailing checksum header has its own signature. The AWS CLI sends aws-chunked bodies only over HTTPS and without chunk signatures (`STREAMING-UNSIGNED-PAYLOAD-TRAILER`); minio-go sends signed chunks over HTTP. Both are tested with the real clients, and the signed-chunk decoder also reproduces the worked example of the S3 documentation byte for byte.
- boto3 1.43 still uses Signature Version 2 for presigned URLs to a custom endpoint in us-east-1, so strata verifies V2 as well (header and query string), checked against the V2 examples of the S3 developer guide.

## Listing

S3 lists keys in UTF-8 byte order with optional prefix, delimiter and continuation. On disk, object directories are named by a hash of the key, which keeps arbitrary keys away from file-system rules but loses order, so listing is served from an in-memory index: per bucket, a sorted list of sorted leaves of up to 512 entries (a one-level B+ tree). Lookups and inserts are two binary searches; iteration from any key is sequential.

With a delimiter, a common prefix such as `photos/2024/` may hide millions of keys. After emitting it, the listing restarts at the smallest string greater than every string with that prefix (increment the last byte that is not 0xff), so a page costs O(page size × log n) whatever the prefixes hide; a test checks that listing three prefixes over 30,000 keys examines at most ten entries. The continuation token is the last key or prefix returned; resuming from a prefix skips past it the same way.

The price is memory per object and a start-up scan. Version IDs are in the metadata file names, so the scan counts copies from directory listings and reads one metadata copy per version (157 µs per object here; reading every copy cost 410 µs). Corrupt copies are not detected at start-up; reads that meet one queue a heal, and a scrub finds them all.

**Rejected:** directories shaped like the keys (MinIO's layout) make listing a directory walk, but sorting siblings correctly needs care (`a-b` < `a/` < `a0` in byte order), flat namespaces make huge directories, and keys like `..` need escaping. An embedded B-tree on disk (bbolt, Pebble) would make start-up instant but would be the largest dependency of the project and a second source of truth to keep consistent with the shard files.

## Healing

Damage is noticed in six places: a read that meets a missing or corrupt block, a commit that reached fewer than *n* disks, the start-up scan, the disk watcher (every 5 s, and on demand when a heal starts) which reformats a wiped or replaced disk and requests a sweep, periodic scrubs, and `strata heal` / `POST /-/admin/heal`.

Healing one object: snapshot the index entry and every disk's object directory; check each disk's copy (metadata present, shard sizes right, and with `--deep` every block's checksum); rebuild the bad copies stripe by stripe from any *k* blocks that verify, found on any disk (including blocks of a copy that is being replaced, if the good copies are not enough); install them with the same two-phase commit as a PUT; and delete versions older than the current one and data directories that no metadata refers to. Newer versions without a quorum are left alone, because they are either the remains of a failed write (removed at start-up) or a version whose other copies are on a disk that is away.

A healed object reads back bit-exact and passes a deep scrub; the durability tests check this after wiping every combination of *m* disks for four codes, with and without inline objects, and then lose *m* other disks and restart.

Not healed: in-progress multipart uploads. Their parts are erasure-coded like objects, but completing an upload needs a write quorum of disks that still hold every part.

## Multipart uploads

Each part is erasure-coded on its own with the object's shard distribution and stored in `.strata/multipart/<upload>/` as `p<n>.<id>` with `part.<n>.meta` pointing at the current file, so replacing a part never pairs metadata with the wrong data. Completing hard-links the chosen parts into a staged version and commits it like a PUT; the upload is removed only after the commit, so a failed or interrupted completion can be retried. The ETag is the MD5 of the parts' binary MD5s followed by `-N`, as S3 computes it.

## Testing strategy

Each layer is tested against something independent of itself: the field against a shift-and-add multiplier and its axioms; the codec against a byte-by-byte matrix product and exhaustive erasure patterns; SigV4 against AWS's published examples; the store against a brute-force listing model and against wiped disks, flipped bits and simulated crashes; the API against a model of S3 semantics with random operations and restarts; the whole server against the AWS CLI, the AWS SDK for Go v2, boto3 and minio-go, and against `kill -9`. Randomised tests use fixed seeds, print them on failure, and accept `STRATA_SEED`.
