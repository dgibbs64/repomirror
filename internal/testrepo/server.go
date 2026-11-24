package testrepo

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"path"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// Fault changes how the server answers requests for one path.
type Fault int

const (
	// FaultNone serves the file normally.
	FaultNone Fault = iota
	// FaultMissing answers 404 Not Found.
	FaultMissing
	// FaultCorrupt serves the file with its last byte changed.
	FaultCorrupt
	// FaultTruncate sends half of the file and then drops the connection.
	FaultTruncate
	// FaultServerError answers 500 Internal Server Error.
	FaultServerError
)

// Server serves an in-memory file tree over HTTP.
type Server struct {
	// URL is the base URL of the server, without a trailing slash.
	URL string

	srv    *httptest.Server
	mu     sync.Mutex
	files  map[string][]byte
	faults map[string]Fault
	hits   map[string]int
	ranges map[string][]string
}

// NewServer starts a server that is closed when the test ends.
func NewServer(t testing.TB) *Server {
	t.Helper()
	s := &Server{
		files:  map[string][]byte{},
		faults: map[string]Fault{},
		hits:   map[string]int{},
		ranges: map[string][]string{},
	}
	s.srv = httptest.NewServer(http.HandlerFunc(s.handle))
	s.URL = s.srv.URL
	t.Cleanup(s.srv.Close)
	return s
}

func clean(p string) string {
	return strings.TrimPrefix(path.Clean("/"+p), "/")
}

// Put serves data at p.
func (s *Server) Put(p string, data []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.files[clean(p)] = data
}

// Delete stops serving p.
func (s *Server) Delete(p string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.files, clean(p))
}

// Publish replaces every file under prefix with files, whose keys are
// relative to prefix.
func (s *Server) Publish(prefix string, files map[string][]byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	root := clean(prefix)
	for p := range s.files {
		if root == "" || p == root || strings.HasPrefix(p, root+"/") {
			delete(s.files, p)
		}
	}
	for rel, data := range files {
		s.files[clean(root+"/"+rel)] = data
	}
}

// SetFault makes requests for p fail as described by f.
func (s *Server) SetFault(p string, f Fault) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if f == FaultNone {
		delete(s.faults, clean(p))
		return
	}
	s.faults[clean(p)] = f
}

// Hits returns how many requests were made for p.
func (s *Server) Hits(p string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.hits[clean(p)]
}

// TotalHits returns how many requests were made for paths under prefix.
func (s *Server) TotalHits(prefix string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	root := clean(prefix)
	n := 0
	for p, c := range s.hits {
		if strings.HasPrefix(p, root) {
			n += c
		}
	}
	return n
}

// Body returns the data currently served at p.
func (s *Server) Body(p string) []byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.files[clean(p)]
}

// Ranges returns the Range headers sent with requests for p.
func (s *Server) Ranges(p string) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.ranges[clean(p)]...)
}

func (s *Server) lookup(r *http.Request) ([]byte, bool, Fault) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p := clean(r.URL.Path)
	s.hits[p]++
	if rg := r.Header.Get("Range"); rg != "" {
		s.ranges[p] = append(s.ranges[p], rg)
	}
	data, ok := s.files[p]
	return data, ok, s.faults[p]
}

func (s *Server) handle(w http.ResponseWriter, r *http.Request) {
	data, ok, fault := s.lookup(r)
	switch {
	case fault == FaultMissing || !ok:
		http.NotFound(w, r)
		return
	case fault == FaultServerError:
		http.Error(w, "injected failure", http.StatusInternalServerError)
		return
	case fault == FaultTruncate:
		truncate(w, data)
		return
	case fault == FaultCorrupt && len(data) > 0:
		bad := append([]byte(nil), data...)
		bad[len(bad)-1] ^= 0xff
		data = bad
	}
	http.ServeContent(w, r, path.Base(r.URL.Path), time.Time{}, bytes.NewReader(data))
}

func truncate(w http.ResponseWriter, data []byte) {
	w.Header().Set("Content-Length", strconv.Itoa(len(data)))
	w.WriteHeader(http.StatusOK)
	if _, err := w.Write(data[:len(data)/2]); err != nil {
		return
	}
	if f, ok := w.(http.Flusher); ok {
		f.Flush()
	}
	panic(http.ErrAbortHandler)
}
