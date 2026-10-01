package s3api

import (
	"fmt"
	"io"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/useless-husband/strata/internal/rs"
	"github.com/useless-husband/strata/internal/store"
)

// metrics are written in the Prometheus text exposition format by hand;
// the format is simple and a client library would be strata's largest
// dependency.
type metrics struct {
	inflight atomic.Int64
	bytesIn  atomic.Int64
	bytesOut atomic.Int64
	started  time.Time

	mu       sync.Mutex
	requests map[[2]string]uint64 // api, status code
	latency  map[string]*histogram
}

var latencyBuckets = []float64{0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60}

type histogram struct {
	counts []uint64 // per bucket, not cumulative
	sum    float64
	count  uint64
}

func newMetrics() *metrics {
	return &metrics{requests: map[[2]string]uint64{}, latency: map[string]*histogram{}, started: time.Now()}
}

func (m *metrics) observe(api string, status int, d time.Duration, in, out int64) {
	m.bytesIn.Add(in)
	m.bytesOut.Add(out)
	sec := d.Seconds()
	m.mu.Lock()
	m.requests[[2]string{api, strconv.Itoa(status)}]++
	h := m.latency[api]
	if h == nil {
		h = &histogram{counts: make([]uint64, len(latencyBuckets))}
		m.latency[api] = h
	}
	for i, b := range latencyBuckets {
		if sec <= b {
			h.counts[i]++
			break
		}
	}
	h.sum += sec
	h.count++
	m.mu.Unlock()
}

func escapeLabel(s string) string {
	return strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`).Replace(s)
}

type promWriter struct {
	w io.Writer
}

func (p promWriter) header(name, typ, help string) {
	fmt.Fprintf(p.w, "# HELP %s %s\n# TYPE %s %s\n", name, help, name, typ)
}

func (p promWriter) value(name string, v float64, labels ...string) {
	fmt.Fprint(p.w, name)
	if len(labels) > 0 {
		fmt.Fprint(p.w, "{")
		for i := 0; i+1 < len(labels); i += 2 {
			if i > 0 {
				fmt.Fprint(p.w, ",")
			}
			fmt.Fprintf(p.w, `%s="%s"`, labels[i], escapeLabel(labels[i+1]))
		}
		fmt.Fprint(p.w, "}")
	}
	fmt.Fprintf(p.w, " %s\n", strconv.FormatFloat(v, 'g', -1, 64))
}

func (p promWriter) single(name, typ, help string, v float64) {
	p.header(name, typ, help)
	p.value(name, v)
}

// writeMetrics writes every metric.
func (s *Server) writeMetrics(w io.Writer) {
	p := promWriter{w}
	m := s.metrics
	p.header("strata_build_info", "gauge", "Build information.")
	p.value("strata_build_info", 1, "version", s.cfg.Version, "goversion", runtime.Version(), "rs_kernel", rs.Kernel())

	m.mu.Lock()
	keys := make([][2]string, 0, len(m.requests))
	for k := range m.requests {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i][0]+keys[i][1] < keys[j][0]+keys[j][1] })
	p.header("strata_requests_total", "counter", "S3 requests by API and HTTP status.")
	for _, k := range keys {
		p.value("strata_requests_total", float64(m.requests[k]), "api", k[0], "code", k[1])
	}
	apis := make([]string, 0, len(m.latency))
	for a := range m.latency {
		apis = append(apis, a)
	}
	sort.Strings(apis)
	p.header("strata_request_duration_seconds", "histogram", "S3 request latency by API, from the first byte of the request to the end of the response.")
	for _, a := range apis {
		h := m.latency[a]
		var cum uint64
		for i, b := range latencyBuckets {
			cum += h.counts[i]
			p.value("strata_request_duration_seconds_bucket", float64(cum), "api", a, "le", strconv.FormatFloat(b, 'g', -1, 64))
		}
		p.value("strata_request_duration_seconds_bucket", float64(h.count), "api", a, "le", "+Inf")
		p.value("strata_request_duration_seconds_sum", h.sum, "api", a)
		p.value("strata_request_duration_seconds_count", float64(h.count), "api", a)
	}
	m.mu.Unlock()
	p.single("strata_http_received_bytes_total", "counter", "Request body bytes received.", float64(m.bytesIn.Load()))
	p.single("strata_http_sent_bytes_total", "counter", "Response body bytes sent.", float64(m.bytesOut.Load()))
	p.single("strata_inflight_requests", "gauge", "Requests being served.", float64(m.inflight.Load()))

	st := s.store.Stats()
	p.single("strata_buckets", "gauge", "Number of buckets.", float64(st.Buckets))
	p.single("strata_objects", "gauge", "Number of objects.", float64(st.Objects))
	p.single("strata_multipart_uploads", "gauge", "Multipart uploads in progress.", float64(st.MultipartUploads))
	p.single("strata_objects_written_total", "counter", "Object versions committed.", float64(st.ObjectsWritten))
	p.single("strata_objects_deleted_total", "counter", "Objects deleted.", float64(st.ObjectsDeleted))
	p.single("strata_erasure_data_shards", "gauge", "Data shards per stripe (k).", float64(st.DataShards))
	p.single("strata_erasure_parity_shards", "gauge", "Parity shards per stripe (m).", float64(st.ParityShards))
	p.single("strata_corrupt_blocks_total", "counter", "Shard blocks that failed their checksum.", float64(st.CorruptBlocks))
	p.single("strata_missing_shards_total", "counter", "Shard files found missing.", float64(st.MissingShards))
	p.single("strata_degraded_stripes_total", "counter", "Stripes served by Reed-Solomon reconstruction.", float64(st.DegradedStripes))
	p.single("strata_read_quorum_failures_total", "counter", "Reads that found fewer than k intact shards.", float64(st.ReadFailures))
	p.single("strata_healed_objects_total", "counter", "Objects whose damaged copies were rewritten.", float64(st.HealedObjects))
	p.single("strata_healed_shards_total", "counter", "Shard copies rewritten by healing.", float64(st.HealedShards))
	p.single("strata_lost_objects_total", "counter", "Heals that found too few intact shards (data loss).", float64(st.LostObjects))
	p.single("strata_disks_replaced_total", "counter", "Disks found wiped or replaced and reformatted.", float64(st.DisksReplaced))
	p.single("strata_heal_queue_length", "gauge", "Objects waiting to be healed.", float64(st.HealQueue))
	p.single("strata_scrubs_total", "counter", "Deep scrubs started.", float64(st.Scrubs))
	p.single("strata_scrubbed_objects_total", "counter", "Objects verified by deep scrubs.", float64(st.ScrubbedObjects))
	p.single("strata_scrubbed_bytes_total", "counter", "Bytes read and verified by deep scrubs.", float64(st.ScrubbedBytes))
	diskMetric := func(name, typ, help string, f func(store.DiskStatus) float64) {
		p.header(name, typ, help)
		for _, d := range st.Disks {
			p.value(name, f(d), "disk", strconv.Itoa(d.Index), "path", d.Path)
		}
	}
	diskMetric("strata_disk_online", "gauge", "1 if the disk is usable.", func(d store.DiskStatus) float64 {
		if d.Online {
			return 1
		}
		return 0
	})
	diskMetric("strata_disk_errors_total", "counter", "I/O errors on the disk.", func(d store.DiskStatus) float64 { return float64(d.Errors) })
	diskMetric("strata_disk_total_bytes", "gauge", "Size of the file system holding the disk.", func(d store.DiskStatus) float64 { return float64(d.TotalBytes) })
	diskMetric("strata_disk_free_bytes", "gauge", "Free space on the file system holding the disk.", func(d store.DiskStatus) float64 { return float64(d.FreeBytes) })

	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	p.single("go_goroutines", "gauge", "Number of goroutines.", float64(runtime.NumGoroutine()))
	p.single("go_memstats_heap_alloc_bytes", "gauge", "Heap bytes allocated and in use.", float64(ms.HeapAlloc))
	p.single("process_start_time_seconds", "gauge", "Start time of the process.", float64(m.started.Unix()))
}
