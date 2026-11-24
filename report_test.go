package main

import (
	"bytes"
	"errors"
	"strings"
	"testing"
	"time"
)

func sampleReport(style reportStyle) *runReport {
	r := &runReport{Prune: true, Style: style}
	r.add(newRepoReport("deb", "ubuntu", 12000, 0, 3_500_000_000, 5*time.Minute+2*time.Second, nil))
	r.add(newRepoReport("rpm", "rocky-9-baseos", 800, 3, 10_000_000, 30*time.Second,
		errors.New("[rpm] rocky-9-baseos: 3 package(s) failed to download; repomd.xml not updated")))
	r.add(repoReport{Type: "rpm", Name: "epel", Status: statusSkipped})
	r.addCleanup(cleanupReport{Path: "ubuntu", Files: 12, Bytes: 40_000_000})
	r.addCleanup(cleanupReport{Path: "rocky/9/BaseOS", Skipped: "sync had errors"})
	r.addCleanup(cleanupReport{Path: "docker", Err: errors.New("permission denied")})
	r.addError("[prune] docker: permission denied")
	return r
}

func assertContains(t *testing.T, out string, wants []string) {
	t.Helper()
	for _, want := range wants {
		if !strings.Contains(out, want) {
			t.Errorf("report missing %q\n%s", want, out)
		}
	}
}

func TestReportUnicode(t *testing.T) {
	var buf bytes.Buffer
	sampleReport(reportStyle{Unicode: true}).write(&buf, 6*time.Minute)
	out := buf.String()
	assertContains(t, out, []string{
		"Sync report\n╭──────┬────────────────┬───────────┬──────────┬────────┬────────────┬───────╮\n",
		"│ Type │ Repo           │ Status    │ Packages │ Failed │ Downloaded │  Time │\n",
		"│ DEB  │ ubuntu         │ ✔ ok      │   12,000 │      0 │     3.5 GB │ 5m02s │\n",
		"│ RPM  │ rocky-9-baseos │ ✘ FAILED  │      800 │      3 │    10.0 MB │   30s │\n",
		"│ RPM  │ epel           │ – skipped │        - │      - │          - │     - │\n",
		"├──────┼────────────────┼───────────┼──────────┼────────┼────────────┼───────┤\n│      │ Total          │           │   12,800 │      3 │     3.5 GB │ 6m00s │\n╰",
		"\nPrune\n",
		"│ ubuntu         │ ✔ removed                  │    12 │ 40.0 MB │\n",
		"│ rocky/9/BaseOS │ – skipped: sync had errors │     - │       - │\n",
		"│ docker         │ ✘ error                    │     - │       - │\n",
		"3 repo(s) · 1 ok · 1 failed · 1 skipped · 3.5 GB downloaded · removed 12 file(s), 40.0 MB · total time 6m00s\n",
		"Errors (2)\n",
		"  ✘ [rpm] rocky-9-baseos: 3 package(s) failed to download; repomd.xml not updated\n",
		"  ✘ [prune] docker: permission denied\n",
	})
	if strings.Contains(out, "\033[") {
		t.Errorf("colour codes written with colour disabled\n%s", out)
	}
}

func TestReportASCII(t *testing.T) {
	var buf bytes.Buffer
	sampleReport(reportStyle{}).write(&buf, 6*time.Minute)
	out := buf.String()
	assertContains(t, out, []string{
		"+------+----------------+---------+----------+--------+------------+-------+\n",
		"| DEB  | ubuntu         | ok      |   12,000 |      0 |     3.5 GB | 5m02s |\n",
		"| RPM  | rocky-9-baseos | FAILED  |      800 |      3 |    10.0 MB |   30s |\n",
		"|      | Total          |         |   12,800 |      3 |     3.5 GB | 6m00s |\n",
		"| docker         | error                    |     - |       - |\n",
		"3 repo(s), 1 ok, 1 failed, 1 skipped, 3.5 GB downloaded, removed 12 file(s), 40.0 MB, total time 6m00s\n",
		"  - [prune] docker: permission denied\n",
	})
	for _, r := range out {
		if r > 127 {
			t.Fatalf("ASCII report contains non-ASCII rune %q\n%s", r, out)
		}
	}
}

func TestReportDryRun(t *testing.T) {
	r := &runReport{DryRun: true, Style: reportStyle{Unicode: true}}
	r.add(newRepoReport("deb", "ubuntu", 1200, 0, 0, time.Second, nil))
	r.addCleanup(cleanupReport{Path: "ubuntu", Files: 2, Bytes: 1000})
	r.addCleanup(cleanupReport{Path: "debian"})

	var buf bytes.Buffer
	r.write(&buf, time.Second)
	out := buf.String()
	assertContains(t, out, []string{
		"Sync report (dry run)\n",
		"│      │ Total  │        │    1,200 │      0 │          - │   1s │\n",
		"│ DEB  │ ubuntu │ ✔ ok   │    1,200 │      0 │          - │   1s │\n",
		"Temporary file cleanup (dry run)\n",
		"│ ubuntu │ ✔ would remove      │     2 │ 1.0 KB │\n",
		"│ debian │ ✔ nothing to remove │     0 │      0 │\n",
		"1 repo(s) · 1 ok · 0 failed · 0 skipped · would remove 2 file(s), 1.0 KB · total time 1s\n",
	})
	if strings.Contains(out, "Errors") {
		t.Errorf("report with no errors should not list errors\n%s", out)
	}
}

func TestReportColor(t *testing.T) {
	var buf bytes.Buffer
	sampleReport(reportStyle{Unicode: true, Color: true}).write(&buf, time.Minute)
	assertContains(t, buf.String(), []string{"\033[32m✔ ok\033[0m", "\033[1;31m✘ FAILED\033[0m"})
}

func TestReportNoRepos(t *testing.T) {
	var buf bytes.Buffer
	(&runReport{}).write(&buf, time.Second)
	assertContains(t, buf.String(), []string{"No repositories configured.\n", "0 repo(s), 0 ok, 0 failed, 0 skipped"})
}

func TestFmtCount(t *testing.T) {
	for n, want := range map[int]string{0: "0", 999: "999", 1000: "1,000", 12000: "12,000", 1234567: "1,234,567"} {
		if got := fmtCount(n); got != want {
			t.Errorf("fmtCount(%d) = %q, want %q", n, got, want)
		}
	}
}
