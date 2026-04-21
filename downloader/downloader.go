// Package downloader provides a resumable, checksum-verified HTTP file downloader.
package downloader

import (
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/hex"
	"fmt"
	"hash"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

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
			Timeout: 0, // no per-request timeout; large files can be slow
			Transport: &http.Transport{
				ResponseHeaderTimeout: 30 * time.Second,
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
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("HTTP %d fetching %s", resp.StatusCode, url)
		}
		data, err := io.ReadAll(resp.Body)
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

	// If the file exists and checksum matches, skip.
	if expected != "" {
		if ok, _ := checksumMatch(destPath, algo, expected); ok {
			return nil
		}
	} else if _, err := os.Stat(destPath); err == nil {
		// No checksum provided; skip if file already exists.
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
		startByte = info.Size()
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
			ok, err := checksumMatch(destPath, algo, expected)
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
	f, err := os.OpenFile(destPath, flag, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()

	var src io.Reader = resp.Body
	if prog != nil {
		src = &countingReader{r: resp.Body, c: prog}
	}
	if _, err := io.Copy(f, src); err != nil {
		return err
	}

	// Verify checksum after writing.
	if expected != "" {
		ok, err := checksumMatch(destPath, algo, expected)
		if err != nil {
			return err
		}
		if !ok {
			os.Remove(destPath)
			return fmt.Errorf("checksum mismatch for %s", destPath)
		}
	}
	return nil
}

// checksumMatch returns true if the file at path matches the expected hex digest.
func checksumMatch(path, algo, expected string) (bool, error) {
	f, err := os.Open(path) // #nosec G304 – path derived from config
	if err != nil {
		return false, err
	}
	defer f.Close()

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

	if _, err := io.Copy(h, f); err != nil {
		return false, err
	}
	actual := hex.EncodeToString(h.Sum(nil))
	return strings.EqualFold(actual, expected), nil
}
