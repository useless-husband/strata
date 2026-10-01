# strata

**一個自架的物件儲存服務：說 Amazon S3 的 API，用 Reed–Solomon 抹除碼把資料分散存在多顆磁碟上。編碼、S3 簽章驗證、儲存引擎全部用 Go 從零寫成，沒有任何相依套件。**

官方的 AWS 工具不用改就能直接用（`aws s3 cp`、AWS SDK、boto3、minio-go）。給它 *k + m* 個目錄當作磁碟：每個物件被切成條帶（stripe），每個條帶編碼成 *k* 個資料分片和 *m* 個同位分片，每顆磁碟存每個條帶的其中一個分片，每個區塊都帶一個檢查碼。任意 *k* 顆磁碟就足以讀出全部資料。遺失或悄悄損壞的分片會在讀取時、背景巡檢（scrub）時或執行 `strata heal` 時被發現並重建。新物件的出現是原子性的，當機永遠不會讓人看到半個物件。

[English](README.md) · [設計](docs/DESIGN.md) · [效能測試](docs/BENCHMARKS.md) · [S3 相容性](docs/COMPATIBILITY.md) · [導讀（給初學者）](docs/導讀.zh-TW.md)

## 實際操作的樣子

六個目錄當磁碟，4 資料 + 2 同位。用 AWS CLI 上傳，然後直接刪掉其中兩顆磁碟（[`test/demo.sh`](test/demo.sh) 的原始輸出，只改了暫存路徑）：

```
$ aws --endpoint-url http://127.0.0.1:52588 s3 cp video.bin s3://demo/media/video.bin
$ aws --endpoint-url http://127.0.0.1:52588 s3 cp note.txt s3://demo/note.txt

$ du -sh disk1 disk2 disk3 disk4 disk5 disk6        # 64 MB 的物件，空間是 1.5 倍
 15M	disk1
 15M	disk2
 15M	disk3
 15M	disk4
 15M	disk5
 15M	disk6

$ rm -rf disk2 disk5                                 # 弄丟兩顆磁碟

$ aws --endpoint-url http://127.0.0.1:52588 s3 cp s3://demo/media/video.bin video.back
$ cmp video.bin video.back                           # 完全相同
$ aws --endpoint-url http://127.0.0.1:52588 s3 cp s3://demo/note.txt -
hello, erasure coding

# 讀取是靠重建資料完成的，並把物件排進修復佇列；
# 被清空的磁碟被偵測到、重新格式化並補回資料。
$ curl -s http://127.0.0.1:52588/-/metrics | grep -E '^strata_(degraded_stripes|missing_shards|healed_objects|disks_replaced)_total'
strata_missing_shards_total 16
strata_degraded_stripes_total 62
strata_healed_objects_total 2
strata_disks_replaced_total 2

$ du -sh disk2 disk5
 15M	disk2
 15M	disk5

$ strata scrub --endpoint http://127.0.0.1:52588    # 讀出並驗證每一個區塊
checked 2 objects in 1 buckets in 18ms (91.6 MiB read)
damaged: 0   repairable: 0   lost: 0   stale versions removed: 0

# 位元腐壞：在 disk3 某個分片檔中間翻轉一個位元，再讀一次。
$ cmp video.bin video.back2                          # 完全相同
$ curl -s http://127.0.0.1:52588/-/metrics | grep -E '^strata_(corrupt_blocks|healed_objects)_total'
strata_corrupt_blocks_total 2
strata_healed_objects_total 3
```

被翻轉的區塊沒通過 CRC32C 檢查，該條帶改由其他分片提供，物件隨後被重寫。

## 快速開始

```sh
go install github.com/useless-husband/strata/cmd/strata@latest     # Go 1.26 以上；或 make build

export STRATA_ACCESS_KEY=admin STRATA_SECRET_KEY=change-me-please
strata server --address 127.0.0.1:9000 /srv/strata/disk{1...6}      # 6 顆磁碟 → 4 資料 + 2 同位

export AWS_ACCESS_KEY_ID=admin AWS_SECRET_ACCESS_KEY=change-me-please AWS_DEFAULT_REGION=us-east-1
aws --endpoint-url http://127.0.0.1:9000 s3 mb s3://photos
aws --endpoint-url http://127.0.0.1:9000 s3 sync ~/Pictures s3://photos/
```

| 指令 | 作用 |
|---|---|
| `strata server DISK...` | 提供 S3 API。`--data`/`--parity` 選擇編碼（預設三分之一的磁碟當同位）、`--sync full\|fsync\|none`、`--tls-cert/--tls-key`、`--scrub-interval`、`--domain`（虛擬主機式 bucket）。每個參數都有對應的 `STRATA_*` 環境變數。 |
| `strata heal` | 修復遺失或損壞的分片（`--deep` 會驗證每個區塊）。可離線直接作用在磁碟上，或用 `--endpoint URL` 對執行中的伺服器操作。 |
| `strata scrub` | 讀出並驗證每個區塊，不做任何修復；有損壞時結束碼為 1。 |
| `strata info` | 版面配置、物件數量、完整性計數與磁碟空間。 |

伺服器另外提供（不需驗證）`/-/health`（可寫入的磁碟不足時回 503）和 `/-/metrics` 的 Prometheus 指標：每個 S3 API 的請求數與延遲分布、進出位元組、損壞區塊、遺失分片、降級條帶、已修復與已遺失的物件、巡檢進度、每顆磁碟的狀態與剩餘空間。

## 運作原理

```
 PUT ─► SigV4 驗證 ─► 內容驗證器 ─────────────► 1 MiB 條帶 ─► Reed–Solomon k+m ─► 每區塊 CRC32C
         (標頭、        (SHA-256、aws-chunked      │              (Cauchy 矩陣、        │
          預簽 URL、     每塊簽章、尾隨檢查碼、     │               NEON TBL 核心)       ▼
          boto3 的 V2)   Content-MD5、MD5 ETag)    │                       disk1 … diskN/.strata/tmp/<op>/
                                                   │                       分片檔，已同步到磁碟
                                                   ▼                                     │
                                   資料流結束時所有檢查都通過 ─────── 改名就位，中繼資料最後；
                                                                     在 ≥ max(k, m+1) 顆磁碟上 ─► 可見
```

- **版面配置。** 每顆磁碟存 `buckets/<bucket>/objects/<hh>/<hash>/<version>.meta` 和 `<version>/part.N`。物件目錄以 key 的雜湊命名，所以含 `..`、`//` 或 1,024 位元組 UTF-8 的 S3 key 永遠不會碰到檔案系統的規則。磁碟 *d* 存哪個分片是依 key 決定的輪轉，所以同位分片平均分散在所有磁碟。128 KiB 以下的物件內嵌儲存：每顆磁碟的分片檔直接放在它的中繼資料檔裡，每個物件每顆磁碟只有一個檔案。
- **分片檔**每個條帶是 `[crc32c][區塊]`；CRC 以條帶編號和分片索引為種子，所以放錯位置或放錯磁碟的區塊也會驗證失敗。範圍讀取只碰需要的區塊。
- **提交。** 新版本先在每顆磁碟的 `.strata/tmp` 暫存、同步到磁碟，再改名就位，資料先、中繼資料最後；中繼資料改名就是該磁碟的提交點。一個版本只有在 *k* 顆磁碟上都有中繼資料時才算數，而一次寫入需要 max(*k*, *m*+1) 顆磁碟，所以任何時刻當機都只會留下舊版或新版，被刪掉的版本也永遠不會復活。在 macOS 上用 `F_FULLFSYNC` 清空磁碟的寫入快取，每個提交階段每個裝置只做一次，並由同時進行的提交共用（群組提交）。
- **讀取**會驗證每個區塊；遇到檔案遺失或檢查碼不符時，改讀其他分片（先資料、再同位）直到湊滿 *k* 個，重建後把物件排進修復佇列。GET 會讓它開啟的版本一直有效（租約），即使下載途中 key 被覆寫也不受影響。
- **修復**從任何磁碟上、每個條帶任意 *k* 個完好的區塊重建損壞的副本，再用同樣的暫存加改名流程安裝。偵測到磁碟被清空後會掃描全部物件；`--scrub-interval` 會定期深度巡檢。
- **列出物件**由記憶體中的有序索引（一層的 B+ 樹）提供，啟動時從中繼資料重建；有分隔字元的列表一次跳過整個共同前綴，不必走過底下的每個 key。

[docs/DESIGN.md](docs/DESIGN.md) 說明困難的部分（法定數量與可見性、當機一致性、租約、不阻擋讀取的修復）以及被否決的替代做法。

## 驗證了什麼、怎麼驗證

| 宣稱 | 證據（全部在 CI 中執行） |
|---|---|
| Reed–Solomon 編碼是 MDS 且正確 | *k*+*m* ≤ 12 的所有編碼中，每一種選 *k* 列的組合都可逆（8,166 組）；8 種編碼下，最多 *m* 個分片的每一種遺失組合都能一位元不差地還原；同位分片與逐位元組的參考實作相符；NEON 和可攜核心在全部 256 個常數下一致；域公理在全部 2²⁴ 組三元組上檢查；模糊測試。`go test ./internal/gf ./internal/rs` |
| SigV4 依照 AWS 的規格實作 | S3 API 文件中的標頭簽章、預簽 URL、64 KiB + 1 KiB aws-chunked 範例逐位元組驗證通過，Signature V2 範例也是；竄改、截斷、調換分塊順序都會被拒絕。`go test ./internal/sigv4` |
| 真正的 AWS CLI 能用 | [`test/awscli.sh`](test/awscli.sh)：mb、上傳小檔與 300 MiB 檔（分段上傳）、依前綴 ls、雙向 sync 加 `--delete`、presign + curl、s3api 的中繼資料／複製／範圍／If-None-Match、rm、rb、rb --force——HTTP 跑一次（簽署內容），HTTPS 再跑一次（aws-chunked 加尾隨 CRC64NVME）。 |
| 官方 SDK 能用 | [`test/interop`](test/interop)：AWS SDK for Go v2（所有檢查碼演算法並在讀取時驗證、分頁器、transfer manager 分段上下傳、預簽、TLS 上的不可回轉資料流）與 minio-go（會送每塊都簽章的 aws-chunked 內容）。[`test/boto3_test.py`](test/boto3_test.py)：boto3，包括 s3transfer 與 V2/V4 預簽 URL。 |
| 少掉最多 *m* 顆磁碟不會遺失資料 | 2+1、2+2、4+2、3+3（後兩者分別測內嵌與檔案兩種小物件存法）下，清空每一種 *m* 顆磁碟的組合：所有物件一位元不差地讀回、修復、深度巡檢乾淨，再弄丟另外 *m* 顆並重新啟動。`go test -run SurvivesLosing ./internal/store` |
| 位元腐壞會被偵測並修復 | 每個物件在最多兩個分片檔中隨機翻轉位元；讀取一位元不差、讀取時修復、深度巡檢乾淨；損壞的中繼資料副本也會被修復。`go test -run 'BitRot|CorruptMetadata' ./internal/store` |
| 當機絕不會露出寫到一半的物件 | 在並行的 PUT、分段上傳、DELETE 進行中用 `kill -9`（依 PID）殺掉伺服器，CI 跑 5 輪（本機用三個種子跑了 75 輪）：每個 key 都恰好是一個完整版本——最後一次被確認的狀態或之後的某次嘗試——列表與 GET 一致，最後的深度巡檢乾淨。`make crash` |
| 用別人寫的測試套件衡量相容性 | [ceph/s3-tests](https://github.com/ceph/s3-tests)（Ceph RGW 的 838 個測試）：**通過 250、失敗 494、略過 94**。失敗中有 479 個是 strata 沒實作的功能（ACL、版本控制、政策、加密、物件鎖定……），11 個是 RGW 專屬擴充或在 AWS 上也會失敗，4 個是已實作功能的行為差異，說明見 [COMPATIBILITY.md](docs/COMPATIBILITY.md)。`test/s3tests/run.sh`（需要網路，不在 CI 中） |
| 隨機操作下 API 行為和 S3 一樣 | 固定種子的模型測試對 strata 和一個記憶體中的 S3 語意模型做相同的隨機操作，比對每個回應，中途還會重新啟動伺服器。`go test -run Model ./internal/s3api` |

## 效能測試

在 Apple M5（10 核、16 GB）、macOS 27、Go 1.27 上量測，**這台機器同時在跑其他工作**；六顆「磁碟」都是同一顆內建 SSD 上的目錄，所以這些數字不代表多顆硬碟的伺服器。方法、完整表格以及支撐設計決策的量測：[docs/BENCHMARKS.md](docs/BENCHMARKS.md)。

Reed–Solomon，單核、1 MiB 條帶，單位是每秒處理的物件資料 MB（`make bench-rs`），並以相同方式在同一台機器上量測 [klauspost/reedsolomon](https://github.com/klauspost/reedsolomon) 作為參考：

| | 4+2 編碼 | 8+4 編碼 | 4+2 重建 1 個資料分片 | 4+2 重建 2 個 | 8+4 重建 4 個 |
|---|---:|---:|---:|---:|---:|
| strata，NEON `TBL` 核心 | 23,600 | 11,500 | 46,500 | 23,200 | 11,300 |
| strata，可攜查表核心 | 2,020 | 940 | 4,090 | 2,030 | 940 |
| klauspost/reedsolomon v1.14.2，單一 goroutine | 10,300 | 11,100 | 15,900 | 10,100 | 11,200 |

4+2、`--sync full`（回應前資料已寫到穩定儲存）下透過 HTTP 的 S3 效能，[`tools/s3bench`](tools/s3bench)，用戶端在同一台主機：

| 物件大小 | 1 個用戶端 PUT | 16 個用戶端 PUT | 1 個用戶端 GET | 16 個用戶端 GET |
|---:|---:|---:|---:|---:|
| 4 KiB | 105/s，p50 9.1 ms | 224/s | 3,600/s，p50 275 µs | 6,400/s |
| 1 MiB | 70 MiB/s | 109 MiB/s | 1.9 GB/s | 6.8 GB/s |
| 64 MiB | 364 MiB/s | 819 MiB/s | 4.7 GB/s | 8.9 GB/s |

小檔的持久寫入受限於 macOS 的 `F_FULLFSYNC`（每次清空磁碟快取約 4 ms，期間 APFS 上的其他檔案操作都會變慢）；strata 讓同時進行的提交共用每個裝置的一次清空，並把 128 KiB 以下的物件存進它的中繼資料檔，16 個用戶端時 4 KiB PUT 從每秒 24 個提升到 224 個。用 `--sync none` 時約每秒 1,700 個。

## 限制

寫清楚，而不是假裝有：

- **沒有 IAM、政策、ACL、版本控制、物件鎖定、加密、標籤、生命週期、網站或事件功能。** 需要這些的請求會得到 `501 NotImplemented`（只接受 private 這個預設 ACL）。每把存取金鑰都有完整權限。
- **單一節點。** 磁碟是同一個行程裡的目錄；沒有叢集、沒有伺服器之間的複寫或重新平衡。啟動時每個磁碟路徑都必須可存取（空目錄會被當成換上的新磁碟）。
- **抹除碼參數在格式化時就固定**，之後也不能加磁碟。
- **列表索引放在記憶體中**，啟動時從磁碟重建（這台機器上每個物件約 160 µs：2 萬個物件約 4 秒開啟）：記憶體與啟動時間隨物件數量成長。
- **進行中的分段上傳不會被修復**：要完成一個上傳，需要達到寫入法定數量、且仍保有每個分段的磁碟。完成請求的重試只在重新啟動前會被認得。
- **修復不涵蓋這個空窗**：某次已確認的寫入剛好只寫到寫入法定數量那麼多顆磁碟，接著重新啟動時其中一顆不見了——在那顆磁碟回來之前，讀到的是前一個版本（見 DESIGN）。
- **當機測試殺的是行程，不是整台機器。** 它證明了提交協定；`F_FULLFSYNC` 能讓資料撐過斷電是 Apple 的保證，這裡沒有測試。
- 沒有實作 Signature Version 4A、SSE-C、POST policy 上傳和 SelectObjectContent。

## 相關專案

- **[MinIO](https://github.com/minio/minio)** 是設計最接近的：S3 API、跨磁碟的抹除碼集合、每個分片的位元腐壞雜湊、讀取時修復。它是正式的產品系統，有分散式模式、IAM、版本控制等等。strata 借用了整體架構（暫存加改名的提交、法定數量讀取），細節上的差異寫在 DESIGN 裡（用 Cauchy 而非由 Vandermonde 衍生的矩陣、用帶位置種子的 CRC32C 而非 HighwayHash、每個版本一個不可變的中繼資料檔而非一個不斷重寫的 `xl.meta`、用記憶體索引而非走訪目錄來列表、群組提交的磁碟快取清空）。小物件內嵌在中繼資料裡則是沿用 MinIO 的 inline data 做法。
- **[klauspost/reedsolomon](https://github.com/klauspost/reedsolomon)** 是 MinIO 使用的 Go Reed–Solomon 函式庫，有 AVX2/AVX-512/GFNI/NEON/SVE 核心；strata 的編碼器是獨立寫的，也更簡單（只有一種核心形狀）。
- **[Garage](https://garagehq.deuxfleurs.fr/)**、**[SeaweedFS](https://github.com/seaweedfs/seaweedfs)** 和 **[Ceph RGW](https://docs.ceph.com/en/latest/radosgw/)** 是分散式的 S3 相容儲存（Garage 用複寫，SeaweedFS 和 Ceph 用抹除碼）。
- **[s3proxy](https://github.com/gaul/s3proxy)** 和 **[versitygw](https://github.com/versity/versitygw)** 把 S3 轉接到其他後端，本身沒有冗餘層。

strata 是一個研究核心機制的單一執行檔，小到可以讀完（約 10,600 行非測試的 Go，含註解，另有約 5,000 行測試），每個正確性宣稱都對應到一個測試。

## 建置與測試

```sh
make build     # ./strata
make test      # 單元、整合、耐久性與模型測試
make race      # 同上，加上競爭偵測器
make lint      # gofmt、go vet、staticcheck
make awscli    # 用真正的 AWS CLI 對伺服器測試（需要 aws v2 與 openssl）
make interop   # AWS SDK for Go v2 與 minio-go
make boto3     # 在 .venv 中用 boto3 測試
make crash     # 上傳途中 kill -9
make bench     # Reed–Solomon 與 S3 吞吐量
```

## 授權

[MIT](LICENSE)
