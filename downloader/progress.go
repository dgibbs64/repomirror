package downloader

import (
	"fmt"
	"io"
	"log"
	"os"
	"sync/atomic"
	"time"
)

// Counter tracks download progress for one repo mirror run.
type Counter struct {
	name     string
	repoType string // "rpm" or "deb"
	total    int64
	done     atomic.Int64
	bytes    atomic.Int64
	start    time.Time
}

// NewCounter creates a Counter for a repo with the given total package count.
func NewCounter(name, repoType string, total int) *Counter {
	return &Counter{
		name:     name,
		repoType: repoType,
		total:    int64(total),
		start:    time.Now(),
	}
}

// AddBytes records n bytes transferred over the network.
func (c *Counter) AddBytes(n int64) { c.bytes.Add(n) }

// Done records one package as complete (downloaded or already cached).
func (c *Counter) Done() { c.done.Add(1) }

// Log overwrites the current terminal line with a live progress update.
// It writes directly to stderr with \r so no newline is emitted.
func (c *Counter) Log() {
	done := c.done.Load()
	total := c.total
	bytes := c.bytes.Load()
	elapsed := time.Since(c.start).Seconds()
	if elapsed < 0.001 {
		elapsed = 0.001
	}
	pct := float64(done) / float64(total) * 100
	speed := float64(bytes) / elapsed

	var eta string
	if done > 0 && done < total {
		remaining := float64(total-done) * (elapsed / float64(done))
		eta = fmt.Sprintf(" ETA %s", fmtDuration(time.Duration(remaining)*time.Second))
	}

	line := fmt.Sprintf("[%s] %s: %d/%d (%.1f%%) %s/s%s",
		c.repoType, c.name, done, total, pct, fmtBytes(speed), eta)
	// %-80s pads/truncates so we always overwrite the full previous line.
	fmt.Fprintf(os.Stderr, "\r%-80s", line)
}

// Finish clears the live progress line and prints a final summary via the
// standard logger (which adds a timestamp and newline).
func (c *Counter) Finish() {
	// Erase the in-place progress line.
	fmt.Fprintf(os.Stderr, "\r%-80s\r", "")
	elapsed := time.Since(c.start)
	bytes := c.bytes.Load()
	var avgStr string
	if s := elapsed.Seconds(); s > 0 {
		avgStr = fmt.Sprintf(" avg %s/s", fmtBytes(float64(bytes)/s))
	}
	log.Printf("[%s] %s: done — %s in %s%s",
		c.repoType, c.name, fmtBytes(float64(bytes)), fmtDuration(elapsed), avgStr)
}

// StartLogger refreshes the progress line every interval until stop is closed.
func (c *Counter) StartLogger(interval time.Duration, stop <-chan struct{}) {
	go func() {
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-t.C:
				c.Log()
			case <-stop:
				return
			}
		}
	}()
}

func fmtBytes(n float64) string {
	switch {
	case n >= 1e9:
		return fmt.Sprintf("%.1f GB", n/1e9)
	case n >= 1e6:
		return fmt.Sprintf("%.1f MB", n/1e6)
	case n >= 1e3:
		return fmt.Sprintf("%.1f KB", n/1e3)
	default:
		return fmt.Sprintf("%.0f B", n)
	}
}

func fmtDuration(d time.Duration) string {
	d = d.Round(time.Second)
	h := int(d.Hours())
	m := int(d.Minutes()) % 60
	s := int(d.Seconds()) % 60
	if h > 0 {
		return fmt.Sprintf("%dh%02dm%02ds", h, m, s)
	}
	if m > 0 {
		return fmt.Sprintf("%dm%02ds", m, s)
	}
	return fmt.Sprintf("%ds", s)
}

// countingReader wraps an io.Reader and records each read's byte count into a Counter.
type countingReader struct {
	r io.Reader
	c *Counter
}

func (cr *countingReader) Read(p []byte) (int, error) {
	n, err := cr.r.Read(p)
	if n > 0 {
		cr.c.AddBytes(int64(n))
	}
	return n, err
}
