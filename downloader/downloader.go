// Package downloader provides a resumable, checksum-verified HTTP file downloader.
package downloader

import (
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

const transferActivityInterval = 20 * time.Second
const checksumCopyBufferSize = 1024 * 1024

var checksumCopyBufPool = sync.Pool{
	New: func() any {
		return make([]byte, checksumCopyBufferSize)
	},
}

// Client wraps an http.Client with retry and resume logic.
type Client struct {
	HTTP    *http.Client
	Retries int
	DryRun  bool // if true, DownloadFile creates dirs and logs but skips actual downloads
}

// New returns a Client with sensible defaults.
func New() *Client {
	return &Client{
		HTTP: &http.Client{
			// Use a finite request timeout so a stalled transfer eventually retries.
			Timeout: 45 * time.Minute,
			Transport: &http.Transport{
				DialContext: (&net.Dialer{
					Timeout:   30 * time.Second,
					KeepAlive: 30 * time.Second,
				}).DialContext,
				TLSHandshakeTimeout:   15 * time.Second,
				ResponseHeaderTimeout: 30 * time.Second,
				ExpectContinueTimeout: 1 * time.Second,
				IdleConnTimeout:       90 * time.Second,
			},
		},
		Retries: 3,
	}
}

// FetchBytes fetches url and returns the response body as a byte slice.
func (c *Client) FetchBytes(url string) ([]byte, error) {
	var lastErr error
	for attempt := 0; attempt <= c.Retries; attempt++ {
		resp, err := c.HTTP.Get(url) // #nosec G107 – URL comes from user config
		if err != nil {
			lastErr = err
			continue
		}
		if resp.StatusCode != http.StatusOK {
			resp.Body.Close()
			return nil, fmt.Errorf("HTTP %d fetching %s", resp.StatusCode, url)
		}
		reader := &transferReader{
			r:          resp.Body,
			url:        url,
			lastReport: time.Now(),
			nextReport: time.Now().Add(transferActivityInterval),
			total:      resp.ContentLength,
		}
		data, err := io.ReadAll(reader)
		resp.Body.Close()
		if err != nil {
			lastErr = err
			continue
		}
		return data, nil
	}
	return nil, fmt.Errorf("fetching %s after %d retries: %w", url, c.Retries, lastErr)
}

// DownloadFile downloads url to destPath (no progress tracking).
func (c *Client) DownloadFile(url, destPath, algo, expected string) error {
	return c.DownloadFileP(url, destPath, algo, expected, nil)
}

// DownloadFileP is like DownloadFile but reports byte progress to prog (may be nil).
// Skips the download if the file already exists with a matching checksum.
// In dry-run mode the directory is created but no download occurs.
func (c *Client) DownloadFileP(url, destPath, algo, expected string, prog *Counter) error {
	if c.DryRun {
		if err := os.MkdirAll(filepath.Dir(destPath), 0o755); err != nil {
			return fmt.Errorf("mkdir %s: %w", filepath.Dir(destPath), err)
		}
		log.Printf("[dry-run] would download: %s", url)
		return nil
	}

	info, statErr := os.Stat(destPath)

	// If the file exists and checksum matches, skip. A small sidecar cache
	// avoids re-hashing unchanged files on every run.
	if expected != "" && statErr == nil {
		if checksumCacheHit(destPath, algo, expected, info) {
			_ = os.Remove(resumeMarkerPath(destPath))
			return nil
		}
		if ok, _ := checksumMatchP(destPath, algo, expected, prog); ok {
			_ = writeChecksumCache(destPath, algo, expected, info)
			_ = os.Remove(resumeMarkerPath(destPath))
			return nil
		}
	} else if statErr == nil {
		// No checksum provided; skip if file already exists.
		_ = os.Remove(resumeMarkerPath(destPath))
		return nil
	}

	if err := os.MkdirAll(filepath.Dir(destPath), 0o755); err != nil {
		return fmt.Errorf("mkdir %s: %w", filepath.Dir(destPath), err)
	}

	var lastErr error
	for attempt := 0; attempt <= c.Retries; attempt++ {
		if err := c.downloadOnce(url, destPath, algo, expected, prog); err != nil {
			lastErr = err
			continue
		}
		return nil
	}
	return fmt.Errorf("downloading %s: %w", url, lastErr)
}

func (c *Client) downloadOnce(url, destPath, algo, expected string, prog *Counter) error {
	// Support resuming: check existing partial file size.
	var startByte int64
	if info, err := os.Stat(destPath); err == nil {
		if info.Size() > 0 && trustedResumeMarkerExists(destPath, url) {
			startByte = info.Size()
		} else {
			startByte = 0
		}
	}

	if err := writeResumeMarker(destPath, url); err != nil {
		return err
	}

	req, err := http.NewRequest(http.MethodGet, url, nil) // #nosec G107
	if err != nil {
		return err
	}
	if startByte > 0 {
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-", startByte))
	}

	resp, err := c.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK:
		// Server doesn't support range requests; start fresh.
		startByte = 0
	case http.StatusPartialContent:
		// Server honours range; we can append.
	case http.StatusRequestedRangeNotSatisfiable:
		// File is already complete on disk. Verify checksum if we have one.
		if expected != "" {
			ok, err := checksumMatchP(destPath, algo, expected, prog)
			if err != nil {
				return err
			}
			if !ok {
				// Corrupt; delete and retry from scratch.
				os.Remove(destPath)
				startByte = 0
				return c.downloadOnce(url, destPath, algo, expected, prog)
			}
		}
		return nil
	default:
		return fmt.Errorf("HTTP %d for %s", resp.StatusCode, url)
	}

	flag := os.O_CREATE | os.O_WRONLY
	if startByte > 0 {
		flag |= os.O_APPEND
	} else {
		flag |= os.O_TRUNC
	}
	f, err := openFileWithRetry(destPath, flag, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()

	var total int64 = -1
	if resp.ContentLength >= 0 {
		total = startByte + resp.ContentLength
	}
	src := &transferReader{
		r:           resp.Body,
		c:           prog,
		url:         url,
		display:     shortURLForLog(url),
		lastReport:  time.Now(),
		nextReport:  time.Now().Add(transferActivityInterval),
		transferred: startByte,
		total:       total,
	}
	if prog != nil {
		// Keep the current filename visible immediately, even before first bytes arrive.
		prog.SetActiveProgress(src.display, src.transferred, src.total)
	}
	if _, err := io.Copy(f, src); err != nil {
		return err
	}

	// Verify checksum after writing.
	if expected != "" {
		ok, err := checksumMatchP(destPath, algo, expected, prog)
		if err != nil {
			return err
		}
		if !ok {
			os.Remove(destPath)
			_ = os.Remove(checksumCachePath(destPath))
			_ = os.Remove(resumeMarkerPath(destPath))
			return fmt.Errorf("checksum mismatch for %s", destPath)
		}
		if info, err := os.Stat(destPath); err == nil {
			_ = writeChecksumCache(destPath, algo, expected, info)
		}
	}
	_ = os.Remove(resumeMarkerPath(destPath))
	return nil
}

// SafeJoin joins root+relativePath and rejects any traversal outside root.
func SafeJoin(root, relativePath string) (string, error) {
	if filepath.IsAbs(relativePath) {
		return "", fmt.Errorf("absolute path not allowed: %s", relativePath)
	}
	cleanRel := filepath.Clean(relativePath)
	if cleanRel == "." || cleanRel == "" {
		return root, nil
	}
	if cleanRel == ".." || strings.HasPrefix(cleanRel, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("path traversal not allowed: %s", relativePath)
	}
	joined := filepath.Join(root, cleanRel)
	rootClean := filepath.Clean(root)
	rel, err := filepath.Rel(rootClean, joined)
	if err != nil {
		return "", err
	}
	if rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("path escapes destination root: %s", relativePath)
	}
	return joined, nil
}

// checksumMatch returns true if the file at path matches the expected hex digest.
func checksumMatch(path, algo, expected string) (bool, error) {
	return checksumMatchP(path, algo, expected, nil)
}

func checksumMatchP(path, algo, expected string, prog *Counter) (bool, error) {
	f, err := os.Open(path) // #nosec G304 – path derived from config
	if err != nil {
		return false, err
	}
	defer f.Close()

	var total int64 = -1
	if info, err := f.Stat(); err == nil {
		total = info.Size()
	}
	display := filepath.Base(path)
	if display == "" || display == "." || display == string(filepath.Separator) {
		display = path
	}
	if len(display) > 96 {
		display = display[:48] + "..." + display[len(display)-32:]
	}
	if prog != nil {
		prog.SetActiveValidation(display, 0, total)
		defer prog.ClearActiveProgress(display)
	}

	var h hash.Hash
	switch strings.ToLower(algo) {
	case "sha256":
		h = sha256.New()
	case "sha512":
		h = sha512.New()
	case "sha1":
		h = sha1.New() // #nosec G401 – used only for legacy repo metadata verification
	case "md5":
		h = md5.New() // #nosec G401 – used only for legacy repo metadata verification
	default:
		return false, fmt.Errorf("unknown checksum algorithm: %s", algo)
	}

	reader := io.Reader(f)
	if prog != nil {
		reader = &hashProgressReader{r: f, c: prog, display: display, total: total}
	}

	buf := checksumCopyBufPool.Get().([]byte)
	defer checksumCopyBufPool.Put(buf)

	if _, err := io.CopyBuffer(h, reader, buf); err != nil {
		return false, err
	}
	actual := hex.EncodeToString(h.Sum(nil))
	return strings.EqualFold(actual, expected), nil
}

type hashProgressReader struct {
	r           io.Reader
	c           *Counter
	display     string
	transferred int64
	total       int64
}

func (hr *hashProgressReader) Read(p []byte) (int, error) {
	n, err := hr.r.Read(p)
	if n > 0 {
		hr.transferred += int64(n)
		hr.c.SetActiveValidation(hr.display, hr.transferred, hr.total)
	}
	return n, err
}

func checksumCachePath(path string) string {
	return path + ".repomirror.checksum"
}

func resumeMarkerPath(path string) string {
	return path + ".repomirror.resume"
}

func trustedResumeMarkerExists(path, url string) bool {
	b, err := os.ReadFile(resumeMarkerPath(path))
	if err != nil {
		return false
	}
	stored := strings.TrimSpace(string(b))
	return stored == url
}

func writeResumeMarker(path, url string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	if strings.TrimSpace(url) == "" {
		return errors.New("empty URL for resume marker")
	}
	return os.WriteFile(resumeMarkerPath(path), []byte(url+"\n"), 0o644)
}

func checksumCacheHit(path, algo, expected string, info os.FileInfo) bool {
	b, err := os.ReadFile(checksumCachePath(path))
	if err != nil {
		return false
	}
	parts := strings.Split(strings.TrimSpace(string(b)), "\n")
	if len(parts) != 4 {
		return false
	}
	if !strings.EqualFold(strings.TrimSpace(parts[0]), strings.TrimSpace(algo)) {
		return false
	}
	if !strings.EqualFold(strings.TrimSpace(parts[1]), strings.TrimSpace(expected)) {
		return false
	}
	size, err := strconv.ParseInt(strings.TrimSpace(parts[2]), 10, 64)
	if err != nil || size != info.Size() {
		return false
	}
	mtime, err := strconv.ParseInt(strings.TrimSpace(parts[3]), 10, 64)
	if err != nil || mtime != info.ModTime().UnixNano() {
		return false
	}
	return true
}

func writeChecksumCache(path, algo, expected string, info os.FileInfo) error {
	content := strings.Join([]string{
		strings.ToLower(strings.TrimSpace(algo)),
		strings.ToLower(strings.TrimSpace(expected)),
		strconv.FormatInt(info.Size(), 10),
		strconv.FormatInt(info.ModTime().UnixNano(), 10),
	}, "\n") + "\n"
	return os.WriteFile(checksumCachePath(path), []byte(content), 0o644)
}

// transferReader reports periodic activity for long-running transfers.
type transferReader struct {
	r           io.Reader
	c           *Counter
	url         string
	display     string
	transferred int64
	total       int64
	lastReport  time.Time
	nextReport  time.Time
}

func (tr *transferReader) Read(p []byte) (int, error) {
	n, err := tr.r.Read(p)
	if n > 0 {
		tr.transferred += int64(n)
		if tr.c != nil {
			tr.c.AddBytes(int64(n))
			tr.c.SetActiveProgress(tr.display, tr.transferred, tr.total)
		}
		now := time.Now()
		if now.After(tr.nextReport) {
			tr.report(now)
			tr.nextReport = now.Add(transferActivityInterval)
		}
	}
	return n, err
}

func (tr *transferReader) report(now time.Time) {
	// When a repo counter is active, progress is already shown on the live line.
	// Extra log lines here can interleave and make terminal output look messy.
	if tr.c != nil {
		return
	}

	elapsed := now.Sub(tr.lastReport)
	if elapsed <= 0 {
		elapsed = time.Second
	}
	if tr.total > 0 {
		pct := float64(tr.transferred) / float64(tr.total) * 100
		log.Printf("[dl] active %s: %s/%s (%.1f%%)", shortURLForLog(tr.url), fmtBytes(float64(tr.transferred)), fmtBytes(float64(tr.total)), pct)
		return
	}
	log.Printf("[dl] active %s: %s transferred", shortURLForLog(tr.url), fmtBytes(float64(tr.transferred)))
}

func shortURLForLog(raw string) string {
	name := path.Base(raw)
	if name == "." || name == "/" || name == "" {
		return raw
	}
	if len(name) > 96 {
		return name[:48] + "..." + name[len(name)-32:]
	}
	return name
}

func openFileWithRetry(path string, flag int, perm os.FileMode) (*os.File, error) {
	var lastErr error
	for attempt := 0; attempt < 5; attempt++ {
		f, err := os.OpenFile(path, flag, perm)
		if err == nil {
			return f, nil
		}
		lastErr = err
		if !isTransientMntPathError(path, err) {
			return nil, err
		}
		_ = os.MkdirAll(filepath.Dir(path), 0o755)
		time.Sleep(time.Duration(100*(attempt+1)) * time.Millisecond)
	}
	return nil, lastErr
}

func isTransientMntPathError(path string, err error) bool {
	if !strings.HasPrefix(filepath.Clean(path), string(filepath.Separator)+"mnt"+string(filepath.Separator)) {
		return false
	}
	var pe *os.PathError
	if !errors.As(err, &pe) {
		return false
	}
	return errors.Is(pe.Err, syscall.EINVAL) || errors.Is(pe.Err, syscall.EIO) || errors.Is(pe.Err, syscall.EPERM)
}
