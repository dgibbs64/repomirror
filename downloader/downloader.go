// Package downloader provides a resumable, checksum-verified HTTP file downloader.
package downloader

import (
	"context"
	"crypto/md5"  //nolint:gosec
	"crypto/sha1" //nolint:gosec
	"crypto/sha256"
	"crypto/sha512"
	"database/sql"
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
	"strings"
	"sync"
	"syscall"
	"time"

	"golang.org/x/time/rate"
	_ "modernc.org/sqlite" // register sqlite driver
)

const transferActivityInterval = 20 * time.Second

// permanentError wraps an error that should not be retried (e.g. HTTP 4xx).
type permanentError struct{ err error }

func (e permanentError) Error() string { return e.err.Error() }
func (e permanentError) Unwrap() error { return e.err }

const checksumCopyBufferSize = 1024 * 1024

var checksumCopyBufPool = sync.Pool{
	New: func() any {
		return make([]byte, checksumCopyBufferSize)
	},
}

var (
	stateDBOnce sync.Once
	stateDB     *sql.DB
	stateDBErr  error
)

// Client wraps an http.Client with retry and resume logic.
type Client struct {
	HTTP      *http.Client
	Retries   int
	DryRun    bool // if true, DownloadFile creates dirs but skips actual downloads
	Verbose   bool // if true, dry-run logs every file it would download
	UserAgent string
	limiter   *rate.Limiter
}

const rateLimitBurst = 256 * 1024

// SetRateLimit caps the combined transfer rate of all downloads made by c.
// A value of zero or less removes the limit.
func (c *Client) SetRateLimit(bytesPerSecond int64) {
	if bytesPerSecond <= 0 {
		c.limiter = nil
		return
	}
	burst := max(int(bytesPerSecond), rateLimitBurst)
	c.limiter = rate.NewLimiter(rate.Limit(bytesPerSecond), burst)
}

func (c *Client) newRequest(url string) (*http.Request, error) {
	req, err := http.NewRequest(http.MethodGet, url, nil) // #nosec G107 – URL comes from user config
	if err != nil {
		return nil, err
	}
	if c.UserAgent != "" {
		req.Header.Set("User-Agent", c.UserAgent)
	}
	return req, nil
}

func retryDelay(attempt int) time.Duration {
	return time.Duration(1<<min(attempt, 5)) * time.Second
}

func httpStatusError(code int, url string) error {
	err := fmt.Errorf("HTTP %d for %s", code, url)
	if code >= 400 && code < 500 && code != http.StatusTooManyRequests && code != http.StatusRequestTimeout {
		return permanentError{err}
	}
	return err
}

func waitForTokens(l *rate.Limiter, n int) {
	for n > 0 {
		chunk := min(n, l.Burst())
		_ = l.WaitN(context.Background(), chunk) //nolint:errcheck
		n -= chunk
	}
}

// NormalizeAlgo maps checksum type names used by repository metadata to the
// names understood by the downloader. "sha" is the createrepo name for SHA-1.
func NormalizeAlgo(algo string) string {
	a := strings.ToLower(strings.TrimSpace(algo))
	a = strings.ReplaceAll(a, "-", "")
	switch a {
	case "sha":
		return "sha1"
	case "md5sum":
		return "md5"
	}
	return a
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
		if attempt > 0 {
			time.Sleep(retryDelay(attempt))
		}
		req, err := c.newRequest(url)
		if err != nil {
			return nil, err
		}
		resp, err := c.HTTP.Do(req)
		if err != nil {
			lastErr = err
			continue
		}
		if resp.StatusCode != http.StatusOK {
			_ = resp.Body.Close()
			lastErr = httpStatusError(resp.StatusCode, url)
			if errors.As(lastErr, new(permanentError)) {
				return nil, lastErr
			}
			continue
		}
		reader := &transferReader{
			r:          resp.Body,
			url:        url,
			lim:        c.limiter,
			lastReport: time.Now(),
			nextReport: time.Now().Add(transferActivityInterval),
			total:      resp.ContentLength,
		}
		data, err := io.ReadAll(reader)
		_ = resp.Body.Close()
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
	algo = NormalizeAlgo(algo)
	if c.DryRun {
		if err := os.MkdirAll(filepath.Dir(destPath), 0o755); err != nil {
			return fmt.Errorf("mkdir %s: %w", filepath.Dir(destPath), err)
		}
		if c.Verbose {
			log.Printf("[dry-run] would download: %s", url)
		}
		return nil
	}

	info, statErr := os.Stat(destPath)

	// If the file exists and checksum matches, skip. A small sidecar cache
	// avoids re-hashing unchanged files on every run.
	if expected != "" && statErr == nil {
		if checksumCacheHit(destPath, algo, expected, info) {
			_ = clearResumeMarker(destPath) //nolint:errcheck
			return nil
		}
		if ok, _ := checksumMatchP(destPath, algo, expected, prog); ok { //nolint:errcheck
			_ = writeChecksumCache(destPath, algo, expected, info) //nolint:errcheck
			_ = clearResumeMarker(destPath)                        //nolint:errcheck
			return nil
		}
	} else if statErr == nil {
		// No checksum provided; skip if file already exists.
		_ = clearResumeMarker(destPath) //nolint:errcheck
		return nil
	}

	if err := os.MkdirAll(filepath.Dir(destPath), 0o755); err != nil {
		return fmt.Errorf("mkdir %s: %w", filepath.Dir(destPath), err)
	}

	var lastErr error
	for attempt := 0; attempt <= c.Retries; attempt++ {
		if attempt > 0 {
			time.Sleep(retryDelay(attempt))
		}
		if err := c.downloadOnce(url, destPath, algo, expected, prog); err != nil {
			lastErr = err
			if errors.As(err, new(permanentError)) {
				break
			}
			continue
		}
		return nil
	}
	return fmt.Errorf("downloading %s: %w", url, lastErr)
}

func (c *Client) downloadOnce(url, destPath, algo, expected string, prog *Counter) error { //nolint:gocyclo
	writePath := destPath
	if useTempWritePath(destPath) {
		writePath = destPath + PartSuffix
	}

	// Support resuming: check existing partial file size.
	var startByte int64
	if info, err := os.Stat(writePath); err == nil {
		if info.Size() > 0 && trustedResumeMarkerExists(writePath, url) {
			startByte = info.Size()
		} else {
			startByte = 0
		}
	}

	if err := writeResumeMarker(writePath, url); err != nil {
		// Resume marker is best-effort; log and continue rather than aborting the download.
		log.Printf("[dl] warning: could not write resume marker for %s: %v", shortURLForLog(url), err)
	}

	req, err := c.newRequest(url)
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
	defer func() {
		if resp != nil && resp.Body != nil {
			_ = resp.Body.Close() //nolint:errcheck
		}
	}()

	switch resp.StatusCode {
	case http.StatusOK:
		// Server doesn't support range requests; start fresh.
		startByte = 0
	case http.StatusPartialContent:
		// Server honours range; we can append.
	case http.StatusRequestedRangeNotSatisfiable:
		if startByte == 0 {
			return httpStatusError(resp.StatusCode, url)
		}
		if expected != "" {
			ok, checkErr := checksumMatchP(writePath, algo, expected, prog)
			if checkErr != nil {
				return checkErr
			}
			if !ok {
				os.Remove(writePath)             //nolint:errcheck
				_ = clearResumeMarker(writePath) //nolint:errcheck
				return c.downloadOnce(url, destPath, algo, expected, prog)
			}
		}
		return finalizeDownload(writePath, destPath, algo, expected)
	default:
		return httpStatusError(resp.StatusCode, url)
	}

	flag := os.O_CREATE | os.O_WRONLY
	if startByte > 0 {
		flag |= os.O_APPEND
	} else {
		flag |= os.O_TRUNC
	}
	f, err := openFileWithRetry(writePath, flag, 0o644)
	if err != nil && isTransientMntPathError(writePath, err) {
		// Some drvfs/9p paths reject specific open flag combinations intermittently.
		// Retry with minimal flags, then apply truncate/seek explicitly.
		f2, err2 := openFileWithRetry(writePath, os.O_CREATE|os.O_WRONLY, 0o644)
		if err2 == nil {
			if startByte > 0 {
				if _, seekErr := f2.Seek(0, io.SeekEnd); seekErr != nil {
					_ = f2.Close() //nolint:errcheck
					return seekErr
				}
			} else {
				if truncErr := f2.Truncate(0); truncErr != nil {
					_ = f2.Close() //nolint:errcheck
					return truncErr
				}
				if _, seekErr := f2.Seek(0, io.SeekStart); seekErr != nil {
					_ = f2.Close() //nolint:errcheck
					return seekErr
				}
			}
			f = f2
			err = nil
		}
	}
	if err != nil {
		if writePath != destPath && isTransientMntPathError(writePath, err) {
			// Some /mnt filesystems reject temp-file open patterns intermittently;
			// retry directly against destination path.
			_ = resp.Body.Close() //nolint:errcheck
			writePath = destPath
			startByte = 0
			req, reqErr := c.newRequest(url)
			if reqErr != nil {
				return reqErr
			}
			resp2, doErr := c.HTTP.Do(req)
			if doErr != nil {
				return doErr
			}
			if resp2.StatusCode != http.StatusOK {
				_ = resp2.Body.Close() //nolint:errcheck
				return httpStatusError(resp2.StatusCode, url)
			}
			resp = resp2
			f, err = openFileWithRetry(writePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
			if err != nil {
				_ = resp.Body.Close() //nolint:errcheck
				return err
			}
		} else {
			return err
		}
	}
	defer func() { _ = f.Close() }()

	var total int64 = -1
	if resp.ContentLength >= 0 {
		total = startByte + resp.ContentLength
	}
	src := &transferReader{
		r:           resp.Body,
		c:           prog,
		lim:         c.limiter,
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
		// If the file vanished mid-write (e.g. quarantined by antivirus), give a clear message.
		if _, statErr := os.Stat(writePath); os.IsNotExist(statErr) {
			return fmt.Errorf("file was removed during download (antivirus quarantine?): %s", writePath)
		}
		return err
	}

	// Check the file still exists before checksum — antivirus can quarantine it immediately after write.
	if _, statErr := os.Stat(writePath); os.IsNotExist(statErr) {
		return fmt.Errorf("file was removed after download (antivirus quarantine?): %s", writePath)
	}

	// Verify checksum after writing.
	if expected != "" {
		ok, err := checksumMatchP(writePath, algo, expected, prog)
		if err != nil {
			return err
		}
		if !ok {
			_ = os.Remove(writePath)
			_ = clearChecksumCache(destPath) //nolint:errcheck
			_ = clearResumeMarker(writePath) //nolint:errcheck
			return fmt.Errorf("checksum mismatch for %s", writePath)
		}
	}

	return finalizeDownload(writePath, destPath, algo, expected)
}

func finalizeDownload(writePath, destPath, algo, expected string) error {
	if writePath != destPath {
		_ = os.Remove(destPath) //nolint:errcheck
		if err := os.Rename(writePath, destPath); err != nil {
			return fmt.Errorf("finalize %s: %w", destPath, err)
		}
	}

	if expected != "" {
		if info, err := os.Stat(destPath); err == nil {
			_ = writeChecksumCache(destPath, algo, expected, info) //nolint:errcheck
		}
	}
	_ = clearResumeMarker(writePath) //nolint:errcheck
	_ = clearResumeMarker(destPath)  //nolint:errcheck
	return nil
}

// FileMatches reports whether path exists and has the expected checksum.
func FileMatches(path, algo, expected string) bool {
	algo = NormalizeAlgo(algo)
	info, err := os.Stat(path)
	if err != nil || !info.Mode().IsRegular() || expected == "" {
		return false
	}
	if checksumCacheHit(path, algo, expected, info) {
		return true
	}
	ok, _ := checksumMatchP(path, algo, expected, nil) //nolint:errcheck
	if ok {
		_ = writeChecksumCache(path, algo, expected, info) //nolint:errcheck
	}
	return ok
}

// BytesMatch reports whether data has the expected checksum.
func BytesMatch(data []byte, algo, expected string) bool {
	h, err := newHash(NormalizeAlgo(algo))
	if err != nil {
		return false
	}
	h.Write(data)
	return strings.EqualFold(hex.EncodeToString(h.Sum(nil)), strings.TrimSpace(expected))
}

// ForgetPath drops any cached checksum or resume state recorded for path.
func ForgetPath(path string) {
	_ = clearChecksumCache(path) //nolint:errcheck
	_ = clearResumeMarker(path)  //nolint:errcheck
}

// MovePath renames src to dst and moves its cached checksum state with it.
func MovePath(src, dst string) error {
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return err
	}
	if err := os.Rename(src, dst); err != nil {
		return err
	}
	ForgetPath(src)
	ForgetPath(dst)
	return nil
}

// WriteFileAtomic writes data to path via a temporary file and rename.
func WriteFileAtomic(path string, data []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	tmp := path + TempSuffix
	if err := os.WriteFile(tmp, data, 0o644); err != nil { //nolint:gosec
		_ = os.Remove(tmp) //nolint:errcheck
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp) //nolint:errcheck
		return err
	}
	ForgetPath(path)
	return nil
}

// Temporary file suffixes written by repomirror. Files ending in any of
// these are never part of a finished mirror.
const (
	PartSuffix  = ".repomirror.part"
	TempSuffix  = ".repomirror.tmp"
	StageSuffix = ".repomirror.new"
)

// IsTempFile reports whether path is a repomirror temporary file.
func IsTempFile(path string) bool {
	for _, s := range []string{PartSuffix, TempSuffix, StageSuffix} {
		if strings.HasSuffix(path, s) {
			return true
		}
	}
	return false
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

func checksumMatchP(path, algo, expected string, prog *Counter) (bool, error) {
	f, err := os.Open(path) // #nosec G304 – path derived from config
	if err != nil {
		return false, err
	}
	defer func() { _ = f.Close() }()

	var total int64 = -1
	if info, statErr := f.Stat(); statErr == nil {
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

	h, err := newHash(NormalizeAlgo(algo))
	if err != nil {
		return false, err
	}

	reader := io.Reader(f)
	if prog != nil {
		reader = &hashProgressReader{r: f, c: prog, display: display, total: total}
	}

	buf := checksumCopyBufPool.Get().([]byte)
	defer checksumCopyBufPool.Put(buf) //nolint:staticcheck

	if _, err := io.CopyBuffer(h, reader, buf); err != nil {
		return false, err
	}
	actual := hex.EncodeToString(h.Sum(nil))
	return strings.EqualFold(actual, expected), nil
}

func newHash(algo string) (hash.Hash, error) {
	switch algo {
	case "sha256":
		return sha256.New(), nil
	case "sha224":
		return sha256.New224(), nil
	case "sha384":
		return sha512.New384(), nil
	case "sha512":
		return sha512.New(), nil
	case "sha1":
		return sha1.New(), nil // #nosec G401 – used only for legacy repo metadata verification
	case "md5":
		return md5.New(), nil // #nosec G401 – used only for legacy repo metadata verification
	}
	return nil, fmt.Errorf("unknown checksum algorithm: %s", algo)
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

func trustedResumeMarkerExists(path, url string) bool {
	if db, err := getStateDB(); err == nil {
		var stored string
		err = db.QueryRow("SELECT url FROM resume_markers WHERE path = ?", filepath.Clean(path)).Scan(&stored)
		if err == nil && strings.TrimSpace(stored) == url {
			return true
		}
	}
	return false
}

func writeResumeMarker(path, url string) error {
	if strings.TrimSpace(url) == "" {
		return errors.New("empty URL for resume marker")
	}
	if db, err := getStateDB(); err == nil {
		_, err = db.Exec(
			`INSERT INTO resume_markers(path, url, updated_at)
			 VALUES(?, ?, strftime('%s','now'))
			 ON CONFLICT(path) DO UPDATE SET url=excluded.url, updated_at=excluded.updated_at`,
			filepath.Clean(path),
			url,
		)
		if err == nil {
			return nil
		}
	}
	return fmt.Errorf("state db unavailable while writing resume marker")
}

func checksumCacheHit(path, algo, expected string, info os.FileInfo) bool {
	if db, err := getStateDB(); err == nil {
		var sAlgo, sExpected string
		var sSize, sMtime int64
		err = db.QueryRow(
			"SELECT algo, expected, size, mtime FROM checksum_cache WHERE path = ?",
			filepath.Clean(path),
		).Scan(&sAlgo, &sExpected, &sSize, &sMtime)
		if err == nil {
			if strings.EqualFold(strings.TrimSpace(sAlgo), strings.TrimSpace(algo)) &&
				strings.EqualFold(strings.TrimSpace(sExpected), strings.TrimSpace(expected)) &&
				sSize == info.Size() &&
				sMtime == info.ModTime().UnixNano() {
				return true
			}
		}
	}
	return false
}

func writeChecksumCache(path, algo, expected string, info os.FileInfo) error {
	if db, err := getStateDB(); err == nil {
		_, err = db.Exec(
			`INSERT INTO checksum_cache(path, algo, expected, size, mtime, updated_at)
			 VALUES(?, ?, ?, ?, ?, strftime('%s','now'))
			 ON CONFLICT(path) DO UPDATE SET
			   algo=excluded.algo,
			   expected=excluded.expected,
			   size=excluded.size,
			   mtime=excluded.mtime,
			   updated_at=excluded.updated_at`,
			filepath.Clean(path),
			strings.ToLower(strings.TrimSpace(algo)),
			strings.ToLower(strings.TrimSpace(expected)),
			info.Size(),
			info.ModTime().UnixNano(),
		)
		if err == nil {
			return nil
		}
	}
	return fmt.Errorf("state db unavailable while writing checksum cache")
}

// transferReader reports periodic activity for long-running transfers.
type transferReader struct {
	r           io.Reader
	c           *Counter
	lim         *rate.Limiter
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
		if tr.lim != nil {
			waitForTokens(tr.lim, n)
		}
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

func (tr *transferReader) report(_ time.Time) {
	// When a repo counter is active, progress is already shown on the live line.
	// Extra log lines here can interleave and make terminal output look messy.
	if tr.c != nil {
		return
	}

	if tr.total > 0 {
		pct := float64(tr.transferred) / float64(tr.total) * 100
		log.Printf("[dl] active %s: %s/%s (%.1f%%)", shortURLForLog(tr.url), FmtBytes(float64(tr.transferred)), FmtBytes(float64(tr.total)), pct)
		return
	}
	log.Printf("[dl] active %s: %s transferred", shortURLForLog(tr.url), FmtBytes(float64(tr.transferred)))
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

func sidecarBaseDir() string {
	if d, err := os.UserCacheDir(); err == nil && strings.TrimSpace(d) != "" {
		return filepath.Join(d, "repomirror", "state")
	}
	return filepath.Join(os.TempDir(), "repomirror-state")
}

func clearResumeMarker(path string) error {
	db, err := getStateDB()
	if err != nil {
		return err
	}
	_, err = db.Exec("DELETE FROM resume_markers WHERE path = ?", filepath.Clean(path))
	return err
}

func clearChecksumCache(path string) error {
	db, err := getStateDB()
	if err != nil {
		return err
	}
	_, err = db.Exec("DELETE FROM checksum_cache WHERE path = ?", filepath.Clean(path))
	return err
}

func stateDBPath() string {
	return filepath.Join(sidecarBaseDir(), "state.db")
}

func getStateDB() (*sql.DB, error) {
	stateDBOnce.Do(func() {
		dbPath := stateDBPath()
		if err := os.MkdirAll(filepath.Dir(dbPath), 0o755); err != nil {
			stateDBErr = err
			return
		}
		dsn := dbPath + "?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)"
		db, err := sql.Open("sqlite", dsn)
		if err != nil {
			stateDBErr = err
			return
		}
		if _, err := db.Exec(`
			CREATE TABLE IF NOT EXISTS checksum_cache (
				path TEXT PRIMARY KEY,
				algo TEXT NOT NULL,
				expected TEXT NOT NULL,
				size INTEGER NOT NULL,
				mtime INTEGER NOT NULL,
				updated_at INTEGER NOT NULL
			);
			CREATE TABLE IF NOT EXISTS resume_markers (
				path TEXT PRIMARY KEY,
				url TEXT NOT NULL,
				updated_at INTEGER NOT NULL
			);
		`); err != nil {
			_ = db.Close() //nolint:errcheck
			stateDBErr = err
			return
		}
		stateDB = db
	})
	if stateDBErr != nil {
		return nil, stateDBErr
	}
	return stateDB, nil
}

func openFileWithRetry(path string, flag int, perm os.FileMode) (*os.File, error) {
	var lastErr error
	for attempt := 0; attempt < 20; attempt++ {
		f, err := os.OpenFile(path, flag, perm)
		if err == nil {
			return f, nil
		}
		lastErr = err
		if !isTransientMntPathError(path, err) {
			return nil, err
		}
		if info, statErr := os.Lstat(path); statErr == nil && info.IsDir() {
			// A stale directory at a file path can trigger EINVAL on drvfs mounts.
			_ = os.Remove(path) //nolint:errcheck
		}
		_ = os.MkdirAll(filepath.Dir(path), 0o755) //nolint:errcheck
		time.Sleep(time.Duration(150*(attempt+1)) * time.Millisecond)
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

// useTempWritePath returns true when the downloader should write to a .repomirror.part
// staging file and rename on success, rather than writing directly to destPath.
// /mnt/* paths (WSL drvfs/9p) are excluded because those mounts can return EINVAL
// intermittently when opening certain temp file paths.
func useTempWritePath(path string) bool {
	clean := filepath.Clean(path)
	prefix := string(filepath.Separator) + "mnt" + string(filepath.Separator)
	return !strings.HasPrefix(clean, prefix)
}
