// Command strata is a self-hosted object store that speaks the Amazon S3
// API and keeps data on several directories with Reed–Solomon erasure
// coding.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/useless-husband/strata/internal/rs"
	"github.com/useless-husband/strata/internal/s3api"
	"github.com/useless-husband/strata/internal/sigv4"
	"github.com/useless-husband/strata/internal/store"
)

var version = "dev"

const usage = `strata — an S3-compatible object store with erasure coding

Usage:
  strata server  [flags] DISK...    serve the S3 API
  strata heal    [flags] DISK...    repair missing or damaged shards
  strata scrub   [flags] DISK...    verify every block, repair nothing
  strata info    [flags] DISK...    show the layout and contents
  strata version

DISK arguments may use {a...b} ranges: /srv/strata/disk{1...6}.
Every flag can also be set with an environment variable, shown in the
flag help. Run "strata <command> -h" for the flags of a command.
`

func main() {
	if len(os.Args) < 2 {
		fmt.Fprint(os.Stderr, usage)
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "server":
		err = runServer(os.Args[2:])
	case "heal":
		err = runHeal(os.Args[2:], false)
	case "scrub":
		err = runHeal(os.Args[2:], true)
	case "info":
		err = runInfo(os.Args[2:])
	case "version", "-version", "--version":
		fmt.Printf("strata %s (Reed-Solomon kernel: %s)\n", version, rs.Kernel())
	case "help", "-h", "--help":
		fmt.Print(usage)
	default:
		fmt.Fprintf(os.Stderr, "strata: unknown command %q\n\n%s", os.Args[1], usage)
		os.Exit(2)
	}
	if err != nil {
		var ec exitCode
		if errors.As(err, &ec) {
			os.Exit(int(ec))
		}
		fmt.Fprintln(os.Stderr, "strata:", err)
		os.Exit(1)
	}
}

type exitCode int

func (e exitCode) Error() string { return "exit " + strconv.Itoa(int(e)) }

func env(name, def string) string {
	if v, ok := os.LookupEnv(name); ok {
		return v
	}
	return def
}

func envInt(name string, def int) int {
	if v, ok := os.LookupEnv(name); ok {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func envDuration(name string, def time.Duration) time.Duration {
	if v, ok := os.LookupEnv(name); ok {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}

var rangeRE = regexp.MustCompile(`\{(\d+)\.\.\.(\d+)\}`)

// expandDisks expands "{a...b}" ranges and comma-separated lists.
func expandDisks(args []string) ([]string, error) {
	var out []string
	for _, a := range args {
		for _, p := range strings.Split(a, ",") {
			p = strings.TrimSpace(p)
			if p == "" {
				continue
			}
			m := rangeRE.FindStringSubmatchIndex(p)
			if m == nil {
				out = append(out, p)
				continue
			}
			lo, _ := strconv.Atoi(p[m[2]:m[3]])
			hi, _ := strconv.Atoi(p[m[4]:m[5]])
			if hi < lo || hi-lo > 255 {
				return nil, fmt.Errorf("bad range in %q", p)
			}
			width := m[3] - m[2]
			for i := lo; i <= hi; i++ {
				out = append(out, p[:m[0]]+fmt.Sprintf("%0*d", width, i)+p[m[1]:])
			}
		}
	}
	return out, nil
}

// storeFlags are shared by every command that opens the disks.
type storeFlags struct {
	data, parity int
	blockSize    int
	sync         string
}

func (f *storeFlags) register(fs *flag.FlagSet) {
	fs.IntVar(&f.data, "data", envInt("STRATA_DATA", 0), "data shards per stripe, k (default: disks minus parity) [STRATA_DATA]")
	fs.IntVar(&f.parity, "parity", envInt("STRATA_PARITY", -1), "parity shards per stripe, m (default: a third of the disks, at least 1) [STRATA_PARITY]")
	fs.IntVar(&f.blockSize, "block-size", envInt("STRATA_BLOCK_SIZE", store.DefaultBlockSize), "data bytes per stripe; fixed when the disks are first formatted [STRATA_BLOCK_SIZE]")
	fs.StringVar(&f.sync, "sync", env("STRATA_SYNC", "full"), "durability of writes: full (F_FULLFSYNC on macOS), fsync, or none [STRATA_SYNC]")
}

func (f *storeFlags) config(fs *flag.FlagSet, logger *slog.Logger) (store.Config, error) {
	args := fs.Args()
	if len(args) == 0 {
		if v := os.Getenv("STRATA_DISKS"); v != "" {
			args = []string{v}
		}
	}
	disks, err := expandDisks(args)
	if err != nil {
		return store.Config{}, err
	}
	if len(disks) == 0 {
		return store.Config{}, errors.New("no disks given: pass directories as arguments or set STRATA_DISKS")
	}
	n := len(disks)
	parity := f.parity
	if parity < 0 {
		parity = 0
		if n > 1 {
			parity = max(1, n/3)
		}
	}
	data := f.data
	if data == 0 {
		data = n - parity
	}
	mode, err := store.ParseSyncMode(f.sync)
	if err != nil {
		return store.Config{}, err
	}
	return store.Config{Disks: disks, DataShards: data, ParityShards: parity, BlockSize: f.blockSize, Sync: mode, Logger: logger}, nil
}

func newLogger(format, level string) (*slog.Logger, error) {
	var lv slog.Level
	if err := lv.UnmarshalText([]byte(level)); err != nil {
		return nil, fmt.Errorf("bad log level %q", level)
	}
	opts := &slog.HandlerOptions{Level: lv}
	switch format {
	case "text":
		return slog.New(slog.NewTextHandler(os.Stderr, opts)), nil
	case "json":
		return slog.New(slog.NewJSONHandler(os.Stderr, opts)), nil
	}
	return nil, fmt.Errorf("bad log format %q (want text or json)", format)
}

// loadCredentials reads "ACCESS_KEY SECRET_KEY" lines.
func loadCredentials(path string, into map[string]string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for line := 1; sc.Scan(); line++ {
		t := strings.TrimSpace(sc.Text())
		if t == "" || strings.HasPrefix(t, "#") {
			continue
		}
		parts := strings.Fields(t)
		if len(parts) != 2 {
			return fmt.Errorf("%s:%d: want \"ACCESS_KEY SECRET_KEY\"", path, line)
		}
		into[parts[0]] = parts[1]
	}
	return sc.Err()
}

func runServer(args []string) error {
	fs := flag.NewFlagSet("server", flag.ExitOnError)
	var sf storeFlags
	sf.register(fs)
	addr := fs.String("address", env("STRATA_ADDRESS", "127.0.0.1:9000"), "listen address; port 0 picks a free port [STRATA_ADDRESS]")
	addrFile := fs.String("address-file", env("STRATA_ADDRESS_FILE", ""), "write the bound address to this file once listening [STRATA_ADDRESS_FILE]")
	region := fs.String("region", env("STRATA_REGION", "us-east-1"), "region in SigV4 credential scopes [STRATA_REGION]")
	accessKey := fs.String("access-key", env("STRATA_ACCESS_KEY", ""), "access key [STRATA_ACCESS_KEY]")
	secretKey := fs.String("secret-key", env("STRATA_SECRET_KEY", ""), "secret key [STRATA_SECRET_KEY]")
	credFile := fs.String("credentials-file", env("STRATA_CREDENTIALS_FILE", ""), "file of \"ACCESS_KEY SECRET_KEY\" lines, for more keys [STRATA_CREDENTIALS_FILE]")
	domains := fs.String("domain", env("STRATA_DOMAIN", ""), "comma-separated domains for virtual-hosted-style requests (<bucket>.<domain>) [STRATA_DOMAIN]")
	scrub := fs.Duration("scrub-interval", envDuration("STRATA_SCRUB_INTERVAL", 0), "verify every block of every object this often; 0 disables [STRATA_SCRUB_INTERVAL]")
	healWorkers := fs.Int("heal-workers", envInt("STRATA_HEAL_WORKERS", 2), "objects healed concurrently [STRATA_HEAL_WORKERS]")
	logFormat := fs.String("log-format", env("STRATA_LOG_FORMAT", "text"), "text or json [STRATA_LOG_FORMAT]")
	logLevel := fs.String("log-level", env("STRATA_LOG_LEVEL", "info"), "debug, info, warn or error [STRATA_LOG_LEVEL]")
	logRequests := fs.Bool("log-requests", env("STRATA_LOG_REQUESTS", "false") == "true", "log every request [STRATA_LOG_REQUESTS]")
	tlsCert := fs.String("tls-cert", env("STRATA_TLS_CERT", ""), "serve HTTPS with this certificate [STRATA_TLS_CERT]")
	tlsKey := fs.String("tls-key", env("STRATA_TLS_KEY", ""), "key of the certificate [STRATA_TLS_KEY]")
	fs.Parse(args)

	logger, err := newLogger(*logFormat, *logLevel)
	if err != nil {
		return err
	}
	creds := map[string]string{}
	if *accessKey != "" || *secretKey != "" {
		if *accessKey == "" || len(*secretKey) < 8 {
			return errors.New("set both an access key and a secret key of at least 8 characters")
		}
		creds[*accessKey] = *secretKey
	}
	if *credFile != "" {
		if err := loadCredentials(*credFile, creds); err != nil {
			return err
		}
	}
	if len(creds) == 0 {
		return errors.New("no credentials: set --access-key and --secret-key (or STRATA_ACCESS_KEY and STRATA_SECRET_KEY)")
	}
	cfg, err := sf.config(fs, logger)
	if err != nil {
		return err
	}
	cfg.ScrubInterval = *scrub
	cfg.HealWorkers = *healWorkers
	st, err := store.Open(cfg)
	if err != nil {
		return err
	}
	defer st.Close()

	var doms []string
	for _, d := range strings.Split(*domains, ",") {
		if d = strings.TrimSpace(d); d != "" {
			doms = append(doms, d)
		}
	}
	api := s3api.New(s3api.Config{Store: st, Region: *region, Credentials: creds, Domains: doms,
		Logger: logger, LogRequests: *logRequests, Version: version})
	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		return err
	}
	scheme := "http"
	if *tlsCert != "" {
		scheme = "https"
	}
	bound := ln.Addr().String()
	if *addrFile != "" {
		if err := os.WriteFile(*addrFile+".tmp", []byte(bound), 0o644); err != nil {
			return err
		}
		if err := os.Rename(*addrFile+".tmp", *addrFile); err != nil {
			return err
		}
	}
	srv := &http.Server{Handler: api, ReadHeaderTimeout: 30 * time.Second, IdleTimeout: 2 * time.Minute,
		ErrorLog: slog.NewLogLogger(logger.Handler(), slog.LevelWarn)}
	logger.Info("strata listening", "url", scheme+"://"+bound, "region", *region, "version", version, "keys", len(creds))

	errc := make(chan error, 1)
	go func() {
		if *tlsCert != "" {
			errc <- srv.ServeTLS(ln, *tlsCert, *tlsKey)
		} else {
			errc <- srv.Serve(ln)
		}
	}()
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	select {
	case err := <-errc:
		return err
	case s := <-sig:
		logger.Info("shutting down", "signal", s.String())
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return srv.Shutdown(ctx)
}

// remoteFlags let admin commands talk to a running server instead of
// opening the disks.
type remoteFlags struct {
	endpoint, accessKey, secretKey, region string
}

func (r *remoteFlags) register(fs *flag.FlagSet) {
	fs.StringVar(&r.endpoint, "endpoint", env("STRATA_ENDPOINT", ""), "URL of a running strata server, instead of opening the disks [STRATA_ENDPOINT]")
	fs.StringVar(&r.accessKey, "access-key", env("STRATA_ACCESS_KEY", ""), "access key for --endpoint [STRATA_ACCESS_KEY]")
	fs.StringVar(&r.secretKey, "secret-key", env("STRATA_SECRET_KEY", ""), "secret key for --endpoint [STRATA_SECRET_KEY]")
	fs.StringVar(&r.region, "region", env("STRATA_REGION", "us-east-1"), "region for --endpoint [STRATA_REGION]")
}

func (r *remoteFlags) call(method, path string, q map[string]string, out any) error {
	req, err := http.NewRequest(method, strings.TrimRight(r.endpoint, "/")+path, nil)
	if err != nil {
		return err
	}
	qv := req.URL.Query()
	for k, v := range q {
		if v != "" {
			qv.Set(k, v)
		}
	}
	req.URL.RawQuery = qv.Encode()
	signer := &sigv4.Signer{AccessKey: r.accessKey, SecretKey: r.secretKey, Region: r.region}
	signer.SignBytes(req, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("%s %s: %s: %s", method, path, resp.Status, strings.TrimSpace(string(body)))
	}
	return json.Unmarshal(body, out)
}

func printJSON(v any) {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	enc.Encode(v)
}

// runHeal implements "heal" and "scrub". Scrub reads and verifies every
// block and repairs nothing; it exits with status 1 if it finds damage.
func runHeal(args []string, scrub bool) error {
	name := "heal"
	if scrub {
		name = "scrub"
	}
	fs := flag.NewFlagSet(name, flag.ExitOnError)
	var sf storeFlags
	var rf remoteFlags
	sf.register(fs)
	rf.register(fs)
	deep := fs.Bool("deep", scrub, "read and verify every block (otherwise check metadata and file sizes)")
	dryRun := fs.Bool("dry-run", scrub, "report problems without repairing them")
	bucket := fs.String("bucket", "", "heal only this object (with --key)")
	key := fs.String("key", "", "heal only this object (with --bucket)")
	jsonOut := fs.Bool("json", false, "print the result as JSON")
	fs.Parse(args)
	if scrub {
		*deep = true
	}

	var res any
	damaged := false
	if rf.endpoint != "" {
		q := map[string]string{"deep": strconv.FormatBool(*deep), "dry-run": strconv.FormatBool(*dryRun), "bucket": *bucket, "key": *key}
		if *bucket != "" {
			var r store.HealResult
			if err := rf.call("POST", "/-/admin/heal", q, &r); err != nil {
				return err
			}
			res, damaged = r, len(r.BadDisks) > 0 || r.Lost
		} else {
			var r store.SweepResult
			if err := rf.call("POST", "/-/admin/heal", q, &r); err != nil {
				return err
			}
			res, damaged = r, r.Damaged > 0 || r.Lost > 0
		}
	} else {
		logger, _ := newLogger("text", "warn")
		cfg, err := sf.config(fs, logger)
		if err != nil {
			return err
		}
		cfg.DiskCheckInterval = -1
		cfg.ReadOnly = *dryRun
		st, err := store.Open(cfg)
		if err != nil {
			return err
		}
		defer st.Close()
		opt := store.HealOptions{Deep: *deep, DryRun: *dryRun}
		ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
		defer stop()
		if *bucket != "" {
			r, err := st.HealObject(ctx, *bucket, *key, opt)
			if err != nil {
				return err
			}
			res, damaged = r, len(r.BadDisks) > 0 || r.Lost
		} else {
			r, err := st.Sweep(ctx, opt)
			if err != nil {
				return err
			}
			res, damaged = r, r.Damaged > 0 || r.Lost > 0
		}
	}
	if *jsonOut {
		printJSON(res)
	} else {
		printHeal(res, *dryRun)
	}
	if damaged && *dryRun {
		return exitCode(1)
	}
	return nil
}

func printHeal(v any, dryRun bool) {
	switch r := v.(type) {
	case store.SweepResult:
		verb := "healed"
		if dryRun {
			verb = "repairable"
		}
		fmt.Printf("checked %d objects in %d buckets in %s (%s read)\n", r.Objects, r.Buckets, r.Duration.Round(time.Millisecond), humanBytes(r.BytesRead))
		fmt.Printf("damaged: %d   %s: %d   lost: %d   stale versions removed: %d\n", r.Damaged, verb, r.Healed, r.Lost, r.StaleRemoved)
		for _, o := range r.LostObjects {
			fmt.Printf("  LOST %s\n", o)
		}
	case store.HealResult:
		fmt.Printf("%s/%s version %s: bad disks %v, healed %v, lost %v\n", r.Bucket, r.Key, r.VersionID, r.BadDisks, r.Healed, r.Lost)
	}
}

func humanBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for m := n / unit; m >= unit; m /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(n)/float64(div), "KMGTPE"[exp])
}

func runInfo(args []string) error {
	fs := flag.NewFlagSet("info", flag.ExitOnError)
	var sf storeFlags
	var rf remoteFlags
	sf.register(fs)
	rf.register(fs)
	jsonOut := fs.Bool("json", false, "print as JSON")
	fs.Parse(args)
	var st store.StatsSnapshot
	if rf.endpoint != "" {
		if err := rf.call("GET", "/-/admin/info", nil, &st); err != nil {
			return err
		}
	} else {
		logger, _ := newLogger("text", "warn")
		cfg, err := sf.config(fs, logger)
		if err != nil {
			return err
		}
		cfg.ReadOnly = true
		cfg.DiskCheckInterval = -1
		s, err := store.Open(cfg)
		if err != nil {
			return err
		}
		defer s.Close()
		st = s.Stats()
	}
	if *jsonOut {
		printJSON(st)
		return nil
	}
	fmt.Printf("layout:    %d data + %d parity shards per stripe; reads need %d disks, writes %d\n",
		st.DataShards, st.ParityShards, st.ReadQuorum, st.WriteQuorum)
	fmt.Printf("contents:  %d buckets, %d objects, %d multipart uploads in progress\n", st.Buckets, st.Objects, st.MultipartUploads)
	fmt.Printf("integrity: %d corrupt blocks and %d missing shards seen, %d objects healed, %d lost\n",
		st.CorruptBlocks, st.MissingShards, st.HealedObjects, st.LostObjects)
	for _, d := range st.Disks {
		state := "online"
		if !d.Online {
			state = "OFFLINE"
		}
		fmt.Printf("disk %-3d %-8s %s  (file system %s free of %s)\n", d.Index, state, d.Path, humanBytes(int64(d.FreeBytes)), humanBytes(int64(d.TotalBytes)))
	}
	return nil
}
