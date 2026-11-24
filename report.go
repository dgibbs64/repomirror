package main

import (
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"repomirror/downloader"
)

const (
	statusOK      = "ok"
	statusFailed  = "FAILED"
	statusSkipped = "skipped"
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

func (s reportStyle) table(w io.Writer, cols []column, rows [][]cell) {
	widths := make([]int, len(cols))
	for i, c := range cols {
		widths[i] = utf8.RuneCountInString(c.title)
	}
	for _, row := range rows {
		for i, c := range row {
			widths[i] = max(widths[i], utf8.RuneCountInString(c.text))
		}
	}

	h, v := s.pick("─", "-"), s.pick("│", "|")
	border := func(left, mid, right string) {
		parts := make([]string, len(widths))
		for i, wd := range widths {
			parts[i] = strings.Repeat(h, wd+2)
		}
		fmt.Fprintln(w, s.paint("2", left+strings.Join(parts, mid)+right))
	}
	line := func(cells []cell, right func(int) bool) {
		var b strings.Builder
		b.WriteString(s.paint("2", v))
		for i, c := range cells {
			pad := strings.Repeat(" ", widths[i]-utf8.RuneCountInString(c.text))
			text := s.paint(c.color, c.text)
			if right(i) {
				text = pad + text
			} else {
				text += pad
			}
			b.WriteString(" " + text + " " + s.paint("2", v))
		}
		fmt.Fprintln(w, b.String())
	}

	header := make([]cell, len(cols))
	for i, c := range cols {
		header[i] = cell{text: c.title, color: "1"}
	}
	border(s.pick("╭", "+"), s.pick("┬", "+"), s.pick("╮", "+"))
	right := func(i int) bool { return cols[i].right }
	line(header, right)
	border(s.pick("├", "+"), s.pick("┼", "+"), s.pick("┤", "+"))
	for _, row := range rows {
		line(row, right)
	}
	border(s.pick("╰", "+"), s.pick("┴", "+"), s.pick("╯", "+"))
}

func (r *runReport) write(w io.Writer, elapsed time.Duration) {
	s := r.Style
	title := "Sync report"
	if r.DryRun {
		title += " (dry run)"
	}
	fmt.Fprintf(w, "\n%s\n", s.paint("1", title))

	var ok, failed, skipped int
	var bytes int64
	if len(r.Repos) == 0 {
		fmt.Fprintln(w, "No repositories configured.")
	} else {
		cols := []column{
			{title: "Type"}, {title: "Repo"}, {title: "Status"},
			{title: "Packages", right: true}, {title: "Failed", right: true},
			{title: "Downloaded", right: true}, {title: "Time", right: true},
		}
		rows := make([][]cell, 0, len(r.Repos))
		for _, rep := range r.Repos {
			bytes += rep.Bytes
			typ := cell{text: strings.ToUpper(rep.Type)}
			name := cell{text: rep.Name}
			switch rep.Status {
			case statusOK:
				ok++
				rows = append(rows, []cell{typ, name, {text: s.pick("✔ ", "") + statusOK, color: "32"},
					{text: fmtCount(rep.Packages)}, failedCell(rep.Failed),
					{text: r.fmtBytes(rep.Bytes)}, {text: downloader.FmtDuration(rep.Duration)}})
			case statusFailed:
				failed++
				rows = append(rows, []cell{typ, name, {text: s.pick("✘ ", "") + statusFailed, color: "1;31"},
					{text: fmtCount(rep.Packages)}, failedCell(rep.Failed),
					{text: r.fmtBytes(rep.Bytes)}, {text: downloader.FmtDuration(rep.Duration)}})
			default:
				skipped++
				dash := cell{text: "-", color: "2"}
				rows = append(rows, []cell{typ, name, {text: s.pick("– ", "") + statusSkipped, color: "2"},
					dash, dash, dash, dash})
			}
		}
		s.table(w, cols, rows)
	}

	var cleanFiles int
	var cleanBytes int64
	if len(r.Cleanup) > 0 {
		heading := "Cleanup (temporary files)"
		if r.Prune {
			heading = "Prune"
		}
		if r.DryRun {
			heading += ", dry run"
		}
		fmt.Fprintf(w, "\n%s\n", s.paint("1", heading))
		cols := []column{{title: "Path"}, {title: "Result"}, {title: "Files", right: true}, {title: "Size", right: true}}
		rows := make([][]cell, 0, len(r.Cleanup))
		for _, c := range r.Cleanup {
			path := cell{text: c.Path}
			dash := cell{text: "-", color: "2"}
			switch {
			case c.Err != nil:
				rows = append(rows, []cell{path, {text: s.pick("✘ ", "") + "error", color: "1;31"}, dash, dash})
			case c.Skipped != "":
				rows = append(rows, []cell{path, {text: s.pick("– ", "") + "skipped: " + c.Skipped, color: "2"}, dash, dash})
			case c.Files == 0:
				rows = append(rows, []cell{path, {text: s.pick("✔ ", "") + "nothing to remove", color: "32"},
					{text: "0"}, {text: "0"}})
			default:
				verb := "removed"
				if r.DryRun {
					verb = "would remove"
				}
				cleanFiles += c.Files
				cleanBytes += c.Bytes
				rows = append(rows, []cell{path, {text: s.pick("✔ ", "") + verb, color: "32"},
					{text: fmtCount(c.Files)}, {text: downloader.FmtBytes(float64(c.Bytes))}})
			}
		}
		s.table(w, cols, rows)
	}

	sep := s.pick(" · ", ", ")
	failedText := fmt.Sprintf("%d failed", failed)
	if failed > 0 {
		failedText = s.paint("1;31", failedText)
	}
	parts := []string{
		fmt.Sprintf("%d repo(s)", len(r.Repos)),
		s.paint("32", fmt.Sprintf("%d ok", ok)),
		failedText,
		fmt.Sprintf("%d skipped", skipped),
	}
	if !r.DryRun {
		parts = append(parts, downloader.FmtBytes(float64(bytes))+" downloaded")
	}
	if cleanFiles > 0 {
		verb := "removed"
		if r.DryRun {
			verb = "would remove"
		}
		parts = append(parts, fmt.Sprintf("%s %s file(s), %s", verb, fmtCount(cleanFiles), downloader.FmtBytes(float64(cleanBytes))))
	}
	parts = append(parts, "took "+downloader.FmtDuration(elapsed))
	fmt.Fprintf(w, "\n%s\n", strings.Join(parts, sep))

	if len(r.Errors) > 0 {
		fmt.Fprintf(w, "\n%s\n", s.paint("1;31", fmt.Sprintf("Errors (%d)", len(r.Errors))))
		for _, msg := range r.Errors {
			fmt.Fprintf(w, "  %s %s\n", s.paint("31", s.pick("✘", "-")), msg)
		}
	}
	fmt.Fprintln(w)
}

func failedCell(n int) cell {
	if n > 0 {
		return cell{text: fmtCount(n), color: "1;31"}
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
