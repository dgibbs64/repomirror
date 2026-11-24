package main

import (
	"fmt"
	"io"
	"strings"
	"text/tabwriter"
	"time"

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

type runReport struct {
	DryRun     bool
	Prune      bool
	Repos      []repoReport
	CleanFiles int
	CleanBytes int64
	Errors     []string
}

func (r *runReport) add(rep repoReport) {
	r.Repos = append(r.Repos, rep)
	if rep.Err != nil {
		r.Errors = append(r.Errors, rep.Err.Error())
	}
}

func (r *runReport) addError(msg string) { r.Errors = append(r.Errors, msg) }

func (r *runReport) write(w io.Writer, elapsed time.Duration) {
	title := "Sync report"
	if r.DryRun {
		title += " (dry run)"
	}
	fmt.Fprintf(w, "\n%s\n%s\n", title, strings.Repeat("=", len(title)))

	var ok, failed, skipped int
	var bytes int64
	if len(r.Repos) == 0 {
		fmt.Fprintln(w, "No repositories configured.")
	} else {
		tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
		fmt.Fprintln(tw, "TYPE\tREPO\tSTATUS\tPACKAGES\tFAILED\tDOWNLOADED\tTIME")
		for _, rep := range r.Repos {
			switch rep.Status {
			case statusOK:
				ok++
			case statusFailed:
				failed++
			default:
				skipped++
			}
			bytes += rep.Bytes
			if rep.Status == statusSkipped {
				fmt.Fprintf(tw, "%s\t%s\t%s\t-\t-\t-\t-\n", rep.Type, rep.Name, rep.Status)
				continue
			}
			fmt.Fprintf(tw, "%s\t%s\t%s\t%d\t%d\t%s\t%s\n",
				rep.Type, rep.Name, rep.Status, rep.Packages, rep.Failed,
				r.fmtBytes(rep.Bytes), downloader.FmtDuration(rep.Duration))
		}
		_ = tw.Flush()
	}

	fmt.Fprintln(w)
	if r.CleanFiles > 0 {
		verb := "Removed"
		if r.DryRun {
			verb = "Would remove"
		}
		what := "stale temporary file(s)"
		if r.Prune {
			what = "unreferenced file(s)"
		}
		fmt.Fprintf(w, "%s %d %s (%s)\n", verb, r.CleanFiles, what, downloader.FmtBytes(float64(r.CleanBytes)))
	}
	fmt.Fprintf(w, "%d repo(s): %d ok, %d failed, %d skipped. ", len(r.Repos), ok, failed, skipped)
	if r.DryRun {
		fmt.Fprintf(w, "Took %s.\n", downloader.FmtDuration(elapsed))
	} else {
		fmt.Fprintf(w, "Downloaded %s in %s.\n", r.fmtBytes(bytes), downloader.FmtDuration(elapsed))
	}

	if len(r.Errors) > 0 {
		fmt.Fprintf(w, "\nErrors (%d):\n", len(r.Errors))
		for _, msg := range r.Errors {
			fmt.Fprintf(w, "  - %s\n", msg)
		}
	}
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
