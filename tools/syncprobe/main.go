//go:build darwin

// Command syncprobe measures what durability costs on this Mac's file
// system: fsync(2) versus F_FULLFSYNC, and how long ordinary metadata
// operations take while another thread runs F_FULLFSYNC. These are the
// measurements behind strata's group-commit barrier and inline objects
// (docs/BENCHMARKS.md).
//
//	go run ./tools/syncprobe [-dir DIR]
package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

func fullSync(f *os.File) {
	syscall.Syscall(syscall.SYS_FCNTL, f.Fd(), syscall.F_FULLFSYNC, 0)
}

type stats struct {
	mu  sync.Mutex
	lat map[string][]time.Duration
}

func (s *stats) add(name string, d time.Duration) {
	s.mu.Lock()
	s.lat[name] = append(s.lat[name], d)
	s.mu.Unlock()
}

func (s *stats) print(order []string) {
	for _, name := range order {
		l := s.lat[name]
		sort.Slice(l, func(i, j int) bool { return l[i] < l[j] })
		var sum time.Duration
		for _, d := range l {
			sum += d
		}
		fmt.Printf("| %s | %d | %v | %v | %v |\n", name, len(l), (sum / time.Duration(len(l))).Round(time.Microsecond),
			l[len(l)/2].Round(time.Microsecond), l[len(l)*99/100].Round(time.Microsecond))
	}
}

// ops runs a mix of metadata operations from 8 goroutines.
func ops(dir string, st *stats) {
	data := make([]byte, 4096)
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 40; i++ {
				p := filepath.Join(dir, fmt.Sprintf("w%d-%d", w, i))
				t := time.Now()
				f, _ := os.Create(p)
				st.add("create", time.Since(t))
				f.Write(data)
				t = time.Now()
				syscall.Fsync(int(f.Fd()))
				st.add("fsync (file)", time.Since(t))
				f.Close()
				t = time.Now()
				os.Rename(p, p+".r")
				st.add("rename", time.Since(t))
				t = time.Now()
				d, _ := os.Open(dir)
				syscall.Fsync(int(d.Fd()))
				d.Close()
				st.add("fsync (directory)", time.Since(t))
				t = time.Now()
				os.Mkdir(p+".d", 0o755)
				st.add("mkdir", time.Since(t))
				t = time.Now()
				os.Remove(p + ".r")
				st.add("unlink", time.Since(t))
			}
		}()
	}
	wg.Wait()
}

func main() {
	base := flag.String("dir", os.TempDir(), "directory on the file system to probe")
	flag.Parse()
	dir, err := os.MkdirTemp(*base, "syncprobe")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir)
	data := make([]byte, 4096)

	fmt.Println("## Cost of one sync of a freshly written 4 KiB file")
	fmt.Println()
	fmt.Println("| operation | n | mean | p50 | p99 |")
	fmt.Println("|---|---:|---:|---:|---:|")
	st := &stats{lat: map[string][]time.Duration{}}
	for i := 0; i < 100; i++ {
		f, _ := os.Create(filepath.Join(dir, fmt.Sprint("a", i)))
		f.Write(data)
		t := time.Now()
		syscall.Fsync(int(f.Fd()))
		st.add("fsync(2)", time.Since(t))
		f.Write(data)
		t = time.Now()
		fullSync(f)
		st.add("F_FULLFSYNC", time.Since(t))
		f.Close()
	}
	for i := 0; i < 100; i++ {
		d, _ := os.Open(dir)
		t := time.Now()
		fullSync(d)
		st.add("F_FULLFSYNC, nothing dirty", time.Since(t))
		d.Close()
	}
	st.print([]string{"fsync(2)", "F_FULLFSYNC", "F_FULLFSYNC, nothing dirty"})

	for _, busy := range []bool{false, true} {
		fmt.Println()
		if busy {
			fmt.Println("## Metadata operations (8 threads) while another thread loops on F_FULLFSYNC")
		} else {
			fmt.Println("## Metadata operations (8 threads), file system otherwise idle")
		}
		fmt.Println()
		fmt.Println("| operation | n | mean | p50 | p99 |")
		fmt.Println("|---|---:|---:|---:|---:|")
		sub, _ := os.MkdirTemp(dir, "ops")
		var stop atomic.Bool
		var flushes atomic.Int64
		done := make(chan struct{})
		if busy {
			go func() {
				defer close(done)
				for i := 0; !stop.Load(); i++ {
					f, _ := os.Create(filepath.Join(sub, fmt.Sprint("bg", i)))
					f.Write(data)
					fullSync(f)
					f.Close()
					flushes.Add(1)
				}
			}()
			time.Sleep(50 * time.Millisecond)
		} else {
			close(done)
		}
		st := &stats{lat: map[string][]time.Duration{}}
		ops(sub, st)
		stop.Store(true)
		<-done
		st.print([]string{"create", "fsync (file)", "rename", "fsync (directory)", "mkdir", "unlink"})
		if busy {
			fmt.Printf("\n(%d F_FULLFSYNC calls ran meanwhile)\n", flushes.Load())
		}
	}
}
