package downloader

import (
	"fmt"
	"io"
	"log"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"
	"unsafe"
)

type termTheme struct {
	color   bool
	unicode bool
}

var progressTheme = detectTermTheme()
var ansiRE = regexp.MustCompile(`\x1b\[[0-9;]*m`)

func detectTermTheme() termTheme {
	unicode := supportsUnicode()
	if os.Getenv("NO_COLOR") != "" {
		return termTheme{color: false, unicode: unicode}
	}
	if os.Getenv("TERM") == "dumb" {
		return termTheme{color: false, unicode: unicode}
	}
	fi, err := os.Stderr.Stat()
	if err != nil {
		return termTheme{color: false, unicode: unicode}
	}
	if (fi.Mode() & os.ModeCharDevice) == 0 {
		return termTheme{color: false, unicode: unicode}
	}
	return termTheme{color: true, unicode: unicode}
}

func supportsUnicode() bool {
	check := strings.ToUpper(os.Getenv("LC_ALL") + " " + os.Getenv("LC_CTYPE") + " " + os.Getenv("LANG"))
	return strings.Contains(check, "UTF-8") || strings.Contains(check, "UTF8")
}

func (t termTheme) paint(code, s string) string {
	if !t.color {
		return s
	}
	return "\033[" + code + "m" + s + "\033[0m"
}

func (t termTheme) typeBadge(repoType string) string {
	label := strings.ToUpper(repoType)
	if !t.color {
		return "[" + label + "]"
	}
	if repoType == "rpm" {
		// RHEL-style red badge.
		return t.paint("97;41", " RPM ")
	}
	if repoType == "deb" {
		// Ubuntu-style warm amber badge.
		return t.paint("30;43", " DEB ")
	}
	return t.paint("30;47", " "+label+" ")
}

func (t termTheme) icon(repoType string) string {
	_ = repoType
	if !t.unicode {
		return "pkg"
	}
	return "📥"
}

// Counter tracks download progress for one repo mirror run.
type Counter struct {
	name     string
	repoType string // "rpm" or "deb"
	total    int64
	done     atomic.Int64
	bytes    atomic.Int64
	active   atomic.Value // string
	activeOp atomic.Value // "download" | "validate"
	activeTx atomic.Int64
	activeSz atomic.Int64 // -1 when unknown
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

// SetActiveProgress records the currently active package/file transfer.
func (c *Counter) SetActiveProgress(name string, transferred, total int64) {
	c.active.Store(name)
	c.activeOp.Store("download")
	c.activeTx.Store(transferred)
	c.activeSz.Store(total)
}

// SetActiveValidation records the currently active checksum validation.
func (c *Counter) SetActiveValidation(name string, validated, total int64) {
	c.active.Store(name)
	c.activeOp.Store("validate")
	c.activeTx.Store(validated)
	c.activeSz.Store(total)
}

// ClearActiveProgress clears active transfer details if the name matches.
func (c *Counter) ClearActiveProgress(name string) {
	v := c.active.Load()
	if v == nil {
		return
	}
	current, ok := v.(string)
	if !ok || current != name {
		return
	}
	c.active.Store("")
	c.activeOp.Store("")
	c.activeTx.Store(0)
	c.activeSz.Store(-1)
}

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
		eta = fmt.Sprintf(" eta %s", fmtDuration(time.Duration(remaining)*time.Second))
	}

	activeOp := ""
	if v := c.activeOp.Load(); v != nil {
		if op, ok := v.(string); ok {
			activeOp = op
		}
	}

	rateText := progressTheme.paint("36", fmtBytes(speed)+"/s")
	if activeOp == "validate" && bytes == 0 {
		rateText = progressTheme.paint("35", "validating")
	}

	base := fmt.Sprintf("%s %s %s %d/%d %s %s%s",
		progressTheme.typeBadge(c.repoType),
		progressTheme.icon(c.repoType),
		progressTheme.paint("1", c.name+":"),
		done, total,
		progressTheme.paint("33", fmt.Sprintf("(%.1f%%)", pct)),
		rateText,
		progressTheme.paint("2", eta))

	line := base

	if v := c.active.Load(); v != nil {
		if activeName, ok := v.(string); ok && activeName != "" {
			activeTx := c.activeTx.Load()
			activeSz := c.activeSz.Load()
			op := "download"
			if ov := c.activeOp.Load(); ov != nil {
				if s, ok := ov.(string); ok && s != "" {
					op = s
				}
			}

			fileIcon := progressTheme.icon(c.repoType)
			actionLabel := "downloading"
			if op == "validate" {
				actionLabel = "validating"
				if progressTheme.unicode {
					fileIcon = "🔎"
				} else {
					fileIcon = "chk"
				}
			}
			if activeSz > 0 {
				activePct := float64(activeTx) / float64(activeSz) * 100
				line += fmt.Sprintf(" | %s %s %s %s/%s %s", fileIcon,
					progressTheme.paint("2", actionLabel),
					progressTheme.paint("2", activeName),
					fmtBytes(float64(activeTx)), fmtBytes(float64(activeSz)),
					progressTheme.paint("33", fmt.Sprintf("(%.1f%%)", activePct)))
			} else {
				line += fmt.Sprintf(" | %s %s %s %s", fileIcon,
					progressTheme.paint("2", actionLabel),
					progressTheme.paint("2", activeName),
					fmtBytes(float64(activeTx)))
			}
		}
	}

	width := termWidth()
	line = fitToWidth(line, width)
	// Clear the entire terminal line before rendering updated progress.
	fmt.Fprintf(os.Stderr, "\r\033[K%s", line)
}

// Finish clears the live progress line and prints a final summary via the
// standard logger (which adds a timestamp and newline).
func (c *Counter) Finish() {
	// Erase the in-place progress line.
	fmt.Fprintf(os.Stderr, "\r\033[K")
	elapsed := time.Since(c.start)
	bytes := c.bytes.Load()
	if bytes == 0 {
		if c.total > 0 {
			log.Printf("%s %s %s done (up-to-date; %d files already present)",
				progressTheme.typeBadge(c.repoType),
				progressTheme.icon(c.repoType),
				c.name+":",
				c.total)
			return
		}
		log.Printf("%s %s %s done (nothing to mirror)",
			progressTheme.typeBadge(c.repoType),
			progressTheme.icon(c.repoType),
			c.name+":")
		return
	}

	avgStr := ""
	if s := elapsed.Seconds(); s > 0 {
		avgStr = fmt.Sprintf(" avg %s/s", fmtBytes(float64(bytes)/s))
	}
	log.Printf("%s %s %s done %s in %s%s",
		progressTheme.typeBadge(c.repoType),
		progressTheme.icon(c.repoType),
		c.name+":",
		fmtBytes(float64(bytes)), fmtDuration(elapsed), avgStr)
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

func termWidth() int {
	if s := os.Getenv("COLUMNS"); s != "" {
		if n, err := strconv.Atoi(s); err == nil && n > 20 {
			return n
		}
	}
	ws, err := getWinSize()
	if err == nil && ws > 20 {
		return ws
	}
	return 120
}

func getWinSize() (int, error) {
	type winsize struct {
		Row    uint16
		Col    uint16
		Xpixel uint16
		Ypixel uint16
	}
	ws := &winsize{}
	_, _, errno := syscall.Syscall(syscall.SYS_IOCTL, os.Stderr.Fd(), uintptr(syscall.TIOCGWINSZ), uintptr(unsafe.Pointer(ws)))
	if errno != 0 {
		return 0, errno
	}
	return int(ws.Col), nil
}

func fitToWidth(s string, width int) string {
	if width <= 0 {
		return s
	}
	// Keep a small safety margin because some terminals render emoji wide.
	target := width - 4
	if target < 20 {
		target = 20
	}
	if visualLen(s) <= target {
		return s
	}
	if idx := strings.Index(s, " | "); idx > 0 {
		s = s[:idx]
		if visualLen(s) <= target {
			return s
		}
	}
	// If still too long, fall back to plain (no ANSI) truncated text to avoid
	// cutting escape sequences mid-stream.
	plain := stripANSI(s)
	return trimVisual(plain, target)
}

func visualLen(s string) int {
	return len([]rune(stripANSI(s)))
}

func stripANSI(s string) string {
	return ansiRE.ReplaceAllString(s, "")
}

func trimVisual(s string, max int) string {
	if max <= 1 {
		return s
	}
	r := []rune(s)
	if len(r) <= max {
		return s
	}
	return string(r[:max-1]) + "…"
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
