package main

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/useless-husband/strata/internal/s3client"
	"github.com/useless-husband/strata/internal/store"
)

const (
	killAccess = "kill-test"
	killSecret = "kill-test-secret"
)

func testSeed(t testing.TB) uint64 {
	if s := os.Getenv("STRATA_SEED"); s != "" {
		v, err := strconv.ParseUint(s, 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		return v
	}
	return 20261001
}

var (
	buildOnce sync.Once
	builtBin  string
	buildErr  error
)

// strataBinary returns STRATA_BIN or builds the command once.
func strataBinary(t testing.TB) string {
	if b := os.Getenv("STRATA_BIN"); b != "" {
		return b
	}
	buildOnce.Do(func() {
		dir, err := os.MkdirTemp("", "strata-bin")
		if err != nil {
			buildErr = err
			return
		}
		builtBin = filepath.Join(dir, "strata")
		out, err := exec.Command("go", "build", "-o", builtBin, ".").CombinedOutput()
		if err != nil {
			buildErr = fmt.Errorf("go build: %v\n%s", err, out)
		}
	})
	if buildErr != nil {
		t.Fatal(buildErr)
	}
	return builtBin
}

type proc struct {
	cmd  *exec.Cmd
	addr string
	log  *bytes.Buffer
}

func startStrata(t testing.TB, bin string, disks []string, extra ...string) *proc {
	t.Helper()
	addrFile := filepath.Join(t.TempDir(), "addr")
	args := append([]string{"server", "--address", "127.0.0.1:0", "--address-file", addrFile,
		"--access-key", killAccess, "--secret-key", killSecret}, extra...)
	args = append(args, disks...)
	cmd := exec.Command(bin, args...)
	var log bytes.Buffer
	sw := &syncWriter{w: &log}
	cmd.Stdout, cmd.Stderr = sw, sw
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	p := &proc{cmd: cmd, log: &log}
	t.Cleanup(func() { p.kill() })
	for i := 0; i < 200; i++ {
		if b, err := os.ReadFile(addrFile); err == nil && len(b) > 0 {
			p.addr = string(b)
			return p
		}
		time.Sleep(25 * time.Millisecond)
	}
	p.kill()
	t.Fatalf("server did not start:\n%s", log.String())
	return nil
}

func (p *proc) logString() string {
	sw := p.cmd.Stderr.(*syncWriter)
	sw.mu.Lock()
	defer sw.mu.Unlock()
	return sw.w.String()
}

type syncWriter struct {
	mu sync.Mutex
	w  *bytes.Buffer
}

func (s *syncWriter) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.w.Write(p)
}

// kill sends SIGKILL to the server's PID: no shutdown code runs.
func (p *proc) kill() {
	if p.cmd.ProcessState != nil {
		return
	}
	p.cmd.Process.Signal(syscall.SIGKILL)
	p.cmd.Wait()
}

// keyState is one attempted state of a key: content, or deletion.
type keyState struct {
	deleted bool
	sum     [32]byte // SHA-256 of the content
	size    int
	etag    string
	acked   bool
}

// keyHistory is what one writer did to one key, in order. Only one
// goroutine writes a key, so at most one operation is in flight at a time.
type keyHistory struct {
	mu   sync.Mutex
	vers []keyState // vers[0] is the state before the first round: absent
}

// candidates returns the states a key may be in: the last acknowledged
// one and every later attempt (each may or may not have committed before
// the kill).
func (h *keyHistory) candidates() []keyState {
	last := 0
	for i, v := range h.vers {
		if v.acked {
			last = i
		}
	}
	return h.vers[last:]
}

func multipartETag(data []byte, partSize int) string {
	h := md5.New()
	n := 0
	for off := 0; off < len(data); off += partSize {
		s := md5.Sum(data[off:min(off+partSize, len(data))])
		h.Write(s[:])
		n++
	}
	return hex.EncodeToString(h.Sum(nil)) + "-" + strconv.Itoa(n)
}

// TestKillDuringUploads runs concurrent writers against a strata process,
// kills it with SIGKILL at a random moment, restarts it, and checks that
// every key holds exactly one complete version it was ever given: the last
// acknowledged one or a later attempt. Partial objects must never be
// listed or served, and acknowledged writes and deletes must survive.
//
// A process kill leaves the page cache intact, so this tests the commit
// protocol (staging, renames, quorum), not fsync; the server runs with
// --sync none to make more of the run fall inside commits.
func TestKillDuringUploads(t *testing.T) {
	if testing.Short() {
		t.Skip("starts and kills a server repeatedly")
	}
	seed := testSeed(t)
	rounds := 5
	if v := os.Getenv("STRATA_KILL_ROUNDS"); v != "" {
		rounds, _ = strconv.Atoi(v)
	}
	bin := strataBinary(t)
	root := t.TempDir()
	var disks []string
	for i := 0; i < 6; i++ {
		disks = append(disks, filepath.Join(root, fmt.Sprintf("disk%d", i)))
	}
	const writers, keysPer = 8, 3
	const partSize = store.MinPartSize
	hist := map[string]*keyHistory{}
	for w := 0; w < writers; w++ {
		for k := 0; k < keysPer; k++ {
			hist[fmt.Sprintf("w%d/key%d", w, k)] = &keyHistory{vers: []keyState{{deleted: true, acked: true}}}
		}
	}
	rng := rand.New(rand.NewPCG(seed, 1))
	ackedTotal, killedInFlight, incomplete := 0, 0, 0
	needHealRE := regexp.MustCompile(`needHeal=(\d+)`)

	for round := 0; round <= rounds; round++ {
		p := startStrata(t, bin, disks, "--sync", "none", "--data", "4", "--parity", "2", "--block-size", "65536")
		c := s3client.New("http://"+p.addr, killAccess, killSecret, "us-east-1")
		ctx := context.Background()
		if round == 0 {
			if err := c.CreateBucket(ctx, "crash"); err != nil {
				t.Fatal(err)
			}
		} else {
			verifyAgainstHistory(t, c, hist, disks, seed, round)
			// Keys found at start-up with copies missing on some disks:
			// commits the kill interrupted after their quorum.
			if m := needHealRE.FindStringSubmatch(p.logString()); m != nil {
				n, _ := strconv.Atoi(m[1])
				incomplete += n
			}
		}
		if round == rounds {
			// Final round: repair what the crashes left incomplete and
			// check that a deep scrub then finds nothing wrong.
			var sweep store.SweepResult
			adminPost(t, c, "/-/admin/heal", url.Values{}, &sweep)
			var scrub store.SweepResult
			adminPost(t, c, "/-/admin/heal", url.Values{"deep": {"true"}, "dry-run": {"true"}}, &scrub)
			if scrub.Damaged != 0 || scrub.Lost != 0 {
				t.Fatalf("seed %d: after healing, a deep scrub found %d damaged and %d lost objects", seed, scrub.Damaged, scrub.Lost)
			}
			t.Logf("seed %d: %d rounds, %d acknowledged operations, %d in flight at a kill, %d keys found incompletely committed at restart; final deep scrub of %d objects clean",
				seed, rounds, ackedTotal, killedInFlight, incomplete, scrub.Objects)
			p.kill()
			break
		}

		wctx, stop := context.WithCancel(ctx)
		var wg sync.WaitGroup
		var mu sync.Mutex
		for w := 0; w < writers; w++ {
			wg.Add(1)
			wr := rand.New(rand.NewPCG(seed, uint64(round*100+w+2)))
			go func() {
				defer wg.Done()
				for wctx.Err() == nil {
					key := fmt.Sprintf("w%d/key%d", w, wr.IntN(keysPer))
					h := hist[key]
					var v keyState
					var data []byte
					multipart := false
					if wr.IntN(8) == 0 {
						v.deleted = true
					} else {
						size := []int{0, 1, 100, 65535, 65536, 65537, 300_000, 2_000_000}[wr.IntN(8)]
						if wr.IntN(10) == 0 {
							size, multipart = partSize+wr.IntN(partSize), true
						}
						data = make([]byte, size)
						for i := range data {
							data[i] = byte(wr.Uint32())
						}
						h.mu.Lock()
						seq := len(h.vers)
						h.mu.Unlock()
						copy(data, fmt.Sprintf("%s|%d|", key, seq)) // self-describing
						v.sum, v.size = sha256.Sum256(data), size
						m := md5.Sum(data)
						v.etag = hex.EncodeToString(m[:])
						if multipart {
							v.etag = multipartETag(data, partSize)
						}
					}
					h.mu.Lock()
					h.vers = append(h.vers, v)
					idx := len(h.vers) - 1
					h.mu.Unlock()
					var err error
					switch {
					case v.deleted:
						err = c.DeleteObject(wctx, "crash", key)
					case multipart:
						var etag string
						etag, err = c.MultipartUpload(wctx, "crash", key, data, partSize)
						if err == nil && etag != v.etag {
							t.Errorf("multipart ETag %s, want %s", etag, v.etag)
						}
					default:
						_, err = c.PutObject(wctx, "crash", key, data)
					}
					if err != nil {
						if s := s3client.StatusOf(err); s != 0 {
							t.Errorf("seed %d: %s: unexpected S3 error before the kill: %v", seed, key, err)
						}
						mu.Lock()
						killedInFlight++
						mu.Unlock()
						return // the server is gone
					}
					h.mu.Lock()
					h.vers[idx].acked = true
					h.mu.Unlock()
					mu.Lock()
					ackedTotal++
					mu.Unlock()
				}
			}()
		}
		time.Sleep(time.Duration(150+rng.IntN(600)) * time.Millisecond)
		p.kill()
		stop()
		wg.Wait()
		if t.Failed() {
			t.Fatalf("server log:\n%s", p.logString())
		}
	}
}

func adminPost(t *testing.T, c *s3client.Client, path string, q url.Values, out any) {
	t.Helper()
	req, _ := http.NewRequest("POST", c.Endpoint+path+"?"+q.Encode(), nil)
	c.Signer.SignBytes(req, nil)
	resp, err := c.HTTP.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("%s: %s", path, resp.Status)
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		t.Fatal(err)
	}
}

// verifyAgainstHistory checks every key after a restart and collapses each
// history to the state observed, which is now known to be durable.
func verifyAgainstHistory(t *testing.T, c *s3client.Client, hist map[string]*keyHistory, disks []string, seed uint64, round int) {
	t.Helper()
	ctx := context.Background()
	listed, err := c.ListAll(ctx, "crash", "")
	if err != nil {
		t.Fatal(err)
	}
	byKey := map[string]s3client.Object{}
	for _, o := range listed {
		if hist[o.Key] == nil {
			t.Fatalf("seed %d round %d: unknown key %q listed", seed, round, o.Key)
		}
		byKey[o.Key] = o
	}
	for key, h := range hist {
		cands := h.candidates()
		o, isListed := byKey[key]
		data, _, getErr := c.GetObject(ctx, "crash", key)
		var observed *keyState
		if !isListed {
			if s3client.StatusOf(getErr) != 404 {
				t.Fatalf("seed %d round %d: %s is not listed but GET returned %v", seed, round, key, getErr)
			}
			for i := range cands {
				if cands[i].deleted {
					observed = &cands[i]
					break
				}
			}
			if observed == nil {
				t.Fatalf("seed %d round %d: %s is missing, but its last acknowledged state was a %d-byte object", seed, round, key, cands[0].size)
			}
		} else {
			if getErr != nil {
				t.Fatalf("seed %d round %d: %s is listed but GET failed: %v", seed, round, key, getErr)
			}
			sum := sha256.Sum256(data)
			for i := range cands {
				if !cands[i].deleted && cands[i].sum == sum {
					observed = &cands[i]
					break
				}
			}
			if observed == nil {
				t.Fatalf("seed %d round %d: %s holds %d bytes that match none of its %d candidate versions (torn or stale write)", seed, round, key, len(data), len(cands))
			}
			if o.Size != int64(len(data)) || strings.Trim(o.ETag, `"`) != observed.etag {
				t.Fatalf("seed %d round %d: %s listed as %d bytes, ETag %s; content is %d bytes, ETag %s", seed, round, key, o.Size, o.ETag, len(data), observed.etag)
			}
		}
		v := *observed
		v.acked = true
		h.vers = []keyState{v}
	}
	for _, d := range disks {
		entries, _ := os.ReadDir(filepath.Join(d, ".strata", "tmp"))
		if len(entries) != 0 {
			t.Fatalf("seed %d round %d: %s has %d staging leftovers after start-up", seed, round, d, len(entries))
		}
	}
}
