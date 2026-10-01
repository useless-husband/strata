// Command s3bench measures S3 PUT and GET throughput and latency.
//
// By default it starts a strata server (the binary given with -strata) on
// fresh directories under -dir, so the numbers include the whole path:
// SigV4 verification, MD5, erasure coding, CRC32C, file writes, commit and
// fsync. Each cell runs workers for a fixed time; every worker overwrites a
// small set of its own keys so disk usage stays bounded.
//
//	go run ./tools/s3bench -strata ./strata
//	go run ./tools/s3bench -endpoint http://127.0.0.1:9000 -access-key K -secret-key S
package main

import (
	"context"
	"crypto/rand"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/useless-husband/strata/internal/s3client"
)

func parseSize(s string) (int, error) {
	s = strings.TrimSpace(s)
	mult := 1
	for suffix, m := range map[string]int{"KiB": 1 << 10, "MiB": 1 << 20, "GiB": 1 << 30} {
		if v, ok := strings.CutSuffix(s, suffix); ok {
			s, mult = v, m
			break
		}
	}
	n, err := strconv.Atoi(s)
	return n * mult, err
}

func human(n int) string {
	switch {
	case n >= 1<<20 && n%(1<<20) == 0:
		return fmt.Sprintf("%d MiB", n>>20)
	case n >= 1<<10 && n%(1<<10) == 0:
		return fmt.Sprintf("%d KiB", n>>10)
	}
	return fmt.Sprintf("%d B", n)
}

type result struct {
	ops     int
	bytes   int64
	elapsed time.Duration
	lat     []time.Duration
	errs    int
}

func (r *result) pct(p float64) time.Duration {
	if len(r.lat) == 0 {
		return 0
	}
	sort.Slice(r.lat, func(i, j int) bool { return r.lat[i] < r.lat[j] })
	return r.lat[min(len(r.lat)-1, int(float64(len(r.lat))*p))]
}

// run starts conc workers calling op for d and merges their results.
func run(conc int, d time.Duration, op func(worker, i int) (int64, error)) *result {
	var mu sync.Mutex
	total := &result{}
	var wg sync.WaitGroup
	start := time.Now()
	deadline := start.Add(d)
	for w := 0; w < conc; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var local result
			for i := 0; time.Now().Before(deadline); i++ {
				t := time.Now()
				n, err := op(w, i)
				if err != nil {
					local.errs++
					if local.errs < 3 {
						log.Printf("worker %d: %v", w, err)
					}
					continue
				}
				local.lat = append(local.lat, time.Since(t))
				local.ops++
				local.bytes += n
			}
			mu.Lock()
			total.ops += local.ops
			total.bytes += local.bytes
			total.errs += local.errs
			total.lat = append(total.lat, local.lat...)
			mu.Unlock()
		}()
	}
	wg.Wait()
	total.elapsed = time.Since(start)
	return total
}

func startServer(bin, dir, syncMode string, k, m int) (*exec.Cmd, string, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, "", err
	}
	addrFile := filepath.Join(dir, "addr")
	os.Remove(addrFile)
	args := []string{"server", "--address", "127.0.0.1:0", "--address-file", addrFile,
		"--access-key", "bench", "--secret-key", "bench-secret", "--sync", syncMode,
		"--data", strconv.Itoa(k), "--parity", strconv.Itoa(m), "--log-level", "warn"}
	for i := 0; i < k+m; i++ {
		args = append(args, filepath.Join(dir, fmt.Sprintf("disk%d", i)))
	}
	cmd := exec.Command(bin, args...)
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		return nil, "", err
	}
	for i := 0; i < 400; i++ {
		if b, err := os.ReadFile(addrFile); err == nil && len(b) > 0 {
			return cmd, "http://" + string(b), nil
		}
		time.Sleep(25 * time.Millisecond)
	}
	cmd.Process.Kill()
	return nil, "", fmt.Errorf("server did not start")
}

func main() {
	bin := flag.String("strata", "", "strata binary to start (on fresh directories under -dir)")
	endpoint := flag.String("endpoint", "", "benchmark a running server instead")
	access := flag.String("access-key", "bench", "access key for -endpoint")
	secret := flag.String("secret-key", "bench-secret", "secret key for -endpoint")
	dir := flag.String("dir", "", "where the started server keeps its disks (default: a temporary directory)")
	syncMode := flag.String("sync", "full", "sync mode of the started server")
	k := flag.Int("data", 4, "data shards of the started server")
	m := flag.Int("parity", 2, "parity shards of the started server")
	sizesFlag := flag.String("sizes", "4KiB,1MiB,64MiB", "object sizes")
	concFlag := flag.String("concurrency", "1,4,16", "numbers of concurrent clients")
	dur := flag.Duration("duration", 4*time.Second, "time per measurement")
	keysPer := flag.Int("keys", 4, "keys per worker (overwritten in turn)")
	flag.Parse()

	var sizes, concs []int
	for _, s := range strings.Split(*sizesFlag, ",") {
		n, err := parseSize(s)
		if err != nil {
			log.Fatalf("bad size %q", s)
		}
		sizes = append(sizes, n)
	}
	for _, s := range strings.Split(*concFlag, ",") {
		n, err := strconv.Atoi(strings.TrimSpace(s))
		if err != nil || n < 1 {
			log.Fatalf("bad concurrency %q", s)
		}
		concs = append(concs, n)
	}

	url := *endpoint
	if url == "" {
		if *bin == "" {
			log.Fatal("give -strata BINARY or -endpoint URL")
		}
		if *dir == "" {
			d, err := os.MkdirTemp("", "s3bench")
			if err != nil {
				log.Fatal(err)
			}
			defer os.RemoveAll(d)
			*dir = d
		}
		cmd, u, err := startServer(*bin, *dir, *syncMode, *k, *m)
		if err != nil {
			log.Fatal(err)
		}
		defer func() {
			cmd.Process.Signal(syscall.SIGTERM)
			cmd.Wait()
		}()
		url = u
		*access, *secret = "bench", "bench-secret"
	}
	c := s3client.New(url, *access, *secret, "us-east-1")
	ctx := context.Background()
	if err := c.CreateBucket(ctx, "bench"); err != nil && s3client.StatusOf(err) != 409 {
		log.Fatal(err)
	}

	fmt.Printf("s3bench: %s, %d+%d, sync=%s, %s per cell, Go %s, %d CPUs\n\n", url, *k, *m, *syncMode, *dur, runtime.Version(), runtime.NumCPU())
	fmt.Println("| size | clients | op | ops/s | MiB/s | p50 | p99 | errors |")
	fmt.Println("|---:|---:|---|---:|---:|---:|---:|---:|")
	for _, size := range sizes {
		payload := make([]byte, size)
		rand.Read(payload)
		for _, conc := range concs {
			key := func(w, i int) string { return fmt.Sprintf("s%d/c%d/w%d/k%d", size, conc, w, i%*keysPer) }
			written := make([]int, conc) // keys each worker has written
			put := run(conc, *dur, func(w, i int) (int64, error) {
				_, err := c.PutObject(ctx, "bench", key(w, i), payload)
				if err == nil {
					written[w] = min(*keysPer, written[w]+1)
				}
				return int64(size), err
			})
			report(size, conc, "PUT", put)
			for w := range written {
				for ; written[w] == 0; written[w]++ {
					if _, err := c.PutObject(ctx, "bench", key(w, 0), payload); err != nil {
						log.Fatal(err)
					}
				}
			}
			get := run(conc, *dur, func(w, i int) (int64, error) {
				n, err := c.GetObjectStream(ctx, "bench", key(w, i%written[w]), io.Discard)
				if err == nil && n != int64(size) {
					err = fmt.Errorf("short read: %d of %d", n, size)
				}
				return n, err
			})
			report(size, conc, "GET", get)
		}
	}
}

func report(size, conc int, op string, r *result) {
	secs := r.elapsed.Seconds()
	fmt.Printf("| %s | %d | %s | %.0f | %.1f | %s | %s | %d |\n", human(size), conc, op,
		float64(r.ops)/secs, float64(r.bytes)/secs/(1<<20), fmtDur(r.pct(0.50)), fmtDur(r.pct(0.99)), r.errs)
}

func fmtDur(d time.Duration) string {
	switch {
	case d >= time.Second:
		return fmt.Sprintf("%.2f s", d.Seconds())
	case d >= time.Millisecond:
		return fmt.Sprintf("%.1f ms", float64(d.Microseconds())/1000)
	}
	return fmt.Sprintf("%d µs", d.Microseconds())
}
