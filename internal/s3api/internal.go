package s3api

import (
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/useless-husband/strata/internal/s3err"
	"github.com/useless-husband/strata/internal/store"
)

// serveInternal handles strata's own endpoints under /-/, a prefix that is
// not a valid bucket name:
//
//	GET  /-/health        liveness and write availability (no auth)
//	GET  /-/metrics       Prometheus metrics (no auth)
//	GET  /-/admin/info    store status as JSON (SigV4)
//	POST /-/admin/heal    heal one object (?bucket=&key=) or everything;
//	                      ?deep=true verifies every block, ?dry-run=true
//	                      only reports (SigV4)
func (s *Server) serveInternal(w http.ResponseWriter, r *http.Request, req *request) {
	switch r.URL.Path {
	case "/-/health":
		req.api = "Health"
		st := s.store.Stats()
		online := 0
		for _, d := range st.Disks {
			if d.Online {
				online++
			}
		}
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		if online < st.WriteQuorum {
			w.WriteHeader(http.StatusServiceUnavailable)
			w.Write([]byte("degraded: " + strconv.Itoa(online) + " disks online, writes need " + strconv.Itoa(st.WriteQuorum) + "\n"))
			return
		}
		w.Write([]byte("ok\n"))
		return
	case "/-/metrics":
		req.api = "Metrics"
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		s.writeMetrics(w)
		return
	}
	req.api = "Admin"
	auth, err := s.verifier.Verify(r)
	if err != nil {
		s.writeError(w, r, req, err)
		return
	}
	req.auth = auth
	q := r.URL.Query()
	switch {
	case r.URL.Path == "/-/admin/info" && r.Method == http.MethodGet:
		writeJSON(w, s.store.Stats())
	case r.URL.Path == "/-/admin/heal" && r.Method == http.MethodPost:
		opt := store.HealOptions{Deep: q.Get("deep") == "true", DryRun: q.Get("dry-run") == "true"}
		if q.Get("bucket") != "" && q.Get("key") != "" {
			res, err := s.store.HealObject(r.Context(), q.Get("bucket"), q.Get("key"), opt)
			if err != nil {
				s.writeError(w, r, req, err)
				return
			}
			writeJSON(w, res)
			return
		}
		res, err := s.store.Sweep(r.Context(), opt)
		if err != nil {
			s.writeError(w, r, req, err)
			return
		}
		writeJSON(w, res)
	default:
		s.writeError(w, r, req, s3err.MethodNotAllowed)
	}
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	enc.Encode(v)
}
