package main

import (
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"repomirror/downloader"
)

const (
	statusOK      = "ok"
	statusFailed  = "FAILED"
	statusSkipped = "skipped"
)

const (
	colorBold  = "1"
	colorDim   = "2"
	colorGreen = "32"
	colorRed   = "1;31"
)

type repoReport struct {
	Type     string
	Name     string
	Status   string
	Packages int
	Failed   int
	Bytes    int64
	Duration time.Duration
	Err      error
}

type cleanupReport struct {
	Path    string
	Skipped string
	Files   int
	Bytes   int64
	Err     error
}

type runReport struct {
	DryRun  bool
	Prune   bool
	Style   reportStyle
	Repos   []repoReport
	Cleanup []cleanupReport
	Errors  []string
}

func (r *runReport) add(rep repoReport) {
	r.Repos = append(r.Repos, rep)
	if rep.Err != nil {
		r.Errors = append(r.Errors, rep.Err.Error())
	}
}

func (r *runReport) addCleanup(c cleanupReport) { r.Cleanup = append(r.Cleanup, c) }

func (r *runReport) addError(msg string) { r.Errors = append(r.Errors, msg) }

type reportStyle struct {
	Unicode bool
	Color   bool
}

func detectReportStyle() reportStyle {
	return reportStyle{
		Unicode: downloader.SupportsUnicode(),
		Color:   os.Getenv("NO_COLOR") == "" && downloader.IsInteractiveStderr(),
	}
}

func (s reportStyle) paint(code, text string) string {
	if !s.Color || code == "" {
		return text
	}
	return "\033[" + code + "m" + text + "\033[0m"
}

func (s reportStyle) good(text string) string { return s.pick("✓ ", "") + text }

func (s reportStyle) bad(text string) string { return s.pick("✗ ", "") + text }

func (s reportStyle) skip(text string) string { return s.pick("– ", "") + text }

func (s reportStyle) heading(w io.Writer, text string) {
	fmt.Fprintf(w, "\n%s\n", s.paint(colorBold, text))
}

func (s reportStyle) pick(unicode, ascii string) string {
	if s.Unicode {
		return unicode
	}
	return ascii
}

type cell struct {
	text  string
	color string
}

type column struct {
	title string
	right bool
}

func (s reportStyle) table(w io.Writer, cols []column, rows, footer [][]cell) {
	widths := make([]int, len(cols))
	for i, c := range cols {
		widths[i] = displayWidth(c.title)
	}
	for _, row := range append(append([][]cell{}, rows...), footer...) {
		for i, c := range row {
			widths[i] = max(widths[i], displayWidth(c.text))
		}
	}

	h, v := s.pick("─", "-"), s.pick("│", "|")
	border := func(left, mid, right string) {
		parts := make([]string, len(widths))
		for i, wd := range widths {
			parts[i] = strings.Repeat(h, wd+2)
		}
		fmt.Fprintln(w, s.paint(colorDim, left+strings.Join(parts, mid)+right))
	}
	line := func(cells []cell, right func(int) bool) {
		var b strings.Builder
		b.WriteString(s.paint(colorDim, v))
		for i, c := range cells {
			pad := strings.Repeat(" ", widths[i]-displayWidth(c.text))
			text := s.paint(c.color, c.text)
			if right(i) {
				text = pad + text
			} else {
				text += pad
			}
			b.WriteString(" " + text + " " + s.paint(colorDim, v))
		}
		fmt.Fprintln(w, b.String())
	}

	header := make([]cell, len(cols))
	for i, c := range cols {
		header[i] = cell{text: c.title, color: colorBold}
	}
	border(s.pick("╭", "+"), s.pick("┬", "+"), s.pick("╮", "+"))
	right := func(i int) bool { return cols[i].right }
	line(header, right)
	border(s.pick("├", "+"), s.pick("┼", "+"), s.pick("┤", "+"))
	for _, row := range rows {
		line(row, right)
	}
	if len(footer) > 0 {
		border(s.pick("├", "+"), s.pick("┼", "+"), s.pick("┤", "+"))
		for _, row := range footer {
			line(row, right)
		}
	}
	border(s.pick("╰", "+"), s.pick("┴", "+"), s.pick("╯", "+"))
}

type tally struct {
	ok, failed, skipped int
	packages, failedPkg int
	bytes               int64
	cleanFiles          int
	cleanBytes          int64
}

func removeVerb(dryRun bool) string {
	if dryRun {
		return "would remove"
	}
	return "removed"
}

func (r *runReport) write(w io.Writer, elapsed time.Duration) {
	title := "Sync report"
	if r.DryRun {
		title += " (dry run)"
	}
	r.Style.heading(w, title)

	var t tally
	r.writeRepos(w, &t, elapsed)
	r.writeCleanup(w, &t)
	r.writeSummary(w, &t, elapsed)
	r.writeErrors(w)
	fmt.Fprintln(w)
}

func (r *runReport) repoRow(rep repoReport, t *tally) []cell {
	s := r.Style
	t.bytes += rep.Bytes
	t.packages += rep.Packages
	t.failedPkg += rep.Failed
	typ := cell{text: strings.ToUpper(rep.Type)}
	name := cell{text: rep.Name}
	var status cell
	switch rep.Status {
	case statusOK:
		t.ok++
		status = cell{text: s.good(statusOK), color: colorGreen}
	case statusFailed:
		t.failed++
		status = cell{text: s.bad(statusFailed), color: colorRed}
	default:
		t.skipped++
		dash := cell{text: "-", color: colorDim}
		return []cell{typ, name, {text: s.skip(statusSkipped), color: colorDim}, dash, dash, dash, dash}
	}
	return []cell{typ, name, status,
		{text: fmtCount(rep.Packages)}, failedCell(rep.Failed),
		{text: r.fmtBytes(rep.Bytes)}, {text: downloader.FmtDuration(rep.Duration)}}
}

func (r *runReport) writeRepos(w io.Writer, t *tally, elapsed time.Duration) {
	if len(r.Repos) == 0 {
		fmt.Fprintln(w, "No repositories configured.")
		return
	}
	cols := []column{
		{title: "Type"}, {title: "Repo"}, {title: "Status"},
		{title: "Packages", right: true}, {title: "Failed", right: true},
		{title: "Downloaded", right: true}, {title: "Time", right: true},
	}
	rows := make([][]cell, 0, len(r.Repos))
	for _, rep := range r.Repos {
		rows = append(rows, r.repoRow(rep, t))
	}
	total := []cell{{}, {text: "Total", color: colorBold}, {},
		{text: fmtCount(t.packages), color: colorBold}, failedCell(t.failedPkg),
		{text: r.fmtBytes(t.bytes), color: colorBold}, {text: downloader.FmtDuration(elapsed), color: colorBold}}
	r.Style.table(w, cols, rows, [][]cell{total})
}

func (r *runReport) cleanupRow(c cleanupReport, t *tally) []cell {
	s := r.Style
	path := cell{text: c.Path}
	dash := cell{text: "-", color: colorDim}
	switch {
	case c.Err != nil:
		return []cell{path, {text: s.bad("error"), color: colorRed}, dash, dash}
	case c.Skipped != "":
		return []cell{path, {text: s.skip("skipped: " + c.Skipped), color: colorDim}, dash, dash}
	case c.Files == 0:
		return []cell{path, {text: s.good("nothing to remove"), color: colorGreen}, {text: "0"}, {text: "0"}}
	}
	t.cleanFiles += c.Files
	t.cleanBytes += c.Bytes
	return []cell{path, {text: s.good(removeVerb(r.DryRun)), color: colorGreen},
		{text: fmtCount(c.Files)}, {text: downloader.FmtBytes(float64(c.Bytes))}}
}

func (r *runReport) writeCleanup(w io.Writer, t *tally) {
	if len(r.Cleanup) == 0 {
		return
	}
	heading := "Temporary file cleanup"
	if r.Prune {
		heading = "Prune"
	}
	if r.DryRun {
		heading += " (dry run)"
	}
	r.Style.heading(w, heading)
	cols := []column{{title: "Path"}, {title: "Result"}, {title: "Files", right: true}, {title: "Size", right: true}}
	rows := make([][]cell, 0, len(r.Cleanup))
	for _, c := range r.Cleanup {
		rows = append(rows, r.cleanupRow(c, t))
	}
	r.Style.table(w, cols, rows, nil)
}

func (r *runReport) writeSummary(w io.Writer, t *tally, elapsed time.Duration) {
	s := r.Style
	failedText := fmt.Sprintf("%d failed", t.failed)
	if t.failed > 0 {
		failedText = s.paint(colorRed, failedText)
	}
	parts := []string{
		fmt.Sprintf("%d repo(s)", len(r.Repos)),
		s.paint(colorGreen, fmt.Sprintf("%d ok", t.ok)),
		failedText,
		fmt.Sprintf("%d skipped", t.skipped),
	}
	if !r.DryRun {
		parts = append(parts, downloader.FmtBytes(float64(t.bytes))+" downloaded")
	}
	if t.cleanFiles > 0 {
		parts = append(parts, fmt.Sprintf("%s %s file(s), %s", removeVerb(r.DryRun), fmtCount(t.cleanFiles), downloader.FmtBytes(float64(t.cleanBytes))))
	}
	parts = append(parts, s.paint(colorBold, "total time "+downloader.FmtDuration(elapsed)))
	fmt.Fprintln(w)
	fmt.Fprintln(w, strings.Join(parts, s.pick(" · ", ", ")))
}

func (r *runReport) writeErrors(w io.Writer) {
	if len(r.Errors) == 0 {
		return
	}
	s := r.Style
	fmt.Fprintln(w)
	fmt.Fprintln(w, s.paint(colorRed, fmt.Sprintf("Errors (%d)", len(r.Errors))))
	for _, msg := range r.Errors {
		fmt.Fprintf(w, "  %s %s\n", s.paint("31", s.pick("✗", "-")), msg)
	}
}

func failedCell(n int) cell {
	if n > 0 {
		return cell{text: fmtCount(n), color: colorRed}
	}
	return cell{text: "0"}
}

func (r *runReport) fmtBytes(n int64) string {
	if r.DryRun {
		return "-"
	}
	if n == 0 {
		return "0"
	}
	return downloader.FmtBytes(float64(n))
}

func fmtCount(n int) string {
	s := strconv.Itoa(n)
	if n < 0 || len(s) <= 3 {
		return s
	}
	var b strings.Builder
	lead := len(s) % 3
	if lead > 0 {
		b.WriteString(s[:lead])
	}
	for i := lead; i < len(s); i += 3 {
		if b.Len() > 0 {
			b.WriteByte(',')
		}
		b.WriteString(s[i : i+3])
	}
	return b.String()
}

var wideRanges = [][2]rune{
	{0x1100, 0x115F}, {0x2E80, 0x303E}, {0x3041, 0x33FF}, {0x3400, 0x4DBF},
	{0x4E00, 0x9FFF}, {0xA000, 0xA4CF}, {0xAC00, 0xD7A3}, {0xF900, 0xFAFF},
	{0xFE30, 0xFE4F}, {0xFF00, 0xFF60}, {0xFFE0, 0xFFE6}, {0x1F300, 0x1F64F},
	{0x1F900, 0x1F9FF}, {0x20000, 0x3FFFD},
}

// displayWidth returns the number of terminal cells text occupies.
func displayWidth(text string) int {
	n := 0
	for _, r := range text {
		n++
		for _, wr := range wideRanges {
			if r >= wr[0] && r <= wr[1] {
				n++
				break
			}
		}
	}
	return n
}
