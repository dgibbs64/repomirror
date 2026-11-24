package main

import (
	"bytes"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestReportWrite(t *testing.T) {
	r := &runReport{Prune: true}
	r.add(newRepoReport("deb", "ubuntu", 1200, 0, 3_500_000_000, 5*time.Minute+2*time.Second, nil))
	r.add(newRepoReport("rpm", "rocky-9-baseos", 800, 3, 10_000_000, 30*time.Second,
		errors.New("[rpm] rocky-9-baseos: 3 package(s) failed to download; repomd.xml not updated")))
	r.add(repoReport{Type: "rpm", Name: "epel", Status: statusSkipped})
	r.CleanFiles = 12
	r.CleanBytes = 40_000_000
	r.addError("generating client configs: disk full")

	var buf bytes.Buffer
	r.write(&buf, 6*time.Minute)
	out := buf.String()

	for _, want := range []string{
		"Sync report\n===========",
		"TYPE  REPO            STATUS   PACKAGES  FAILED  DOWNLOADED  TIME",
		"deb   ubuntu          ok       1200      0       3.5 GB      5m02s",
		"rpm   rocky-9-baseos  FAILED   800       3       10.0 MB     30s",
		"rpm   epel            skipped  -         -       -           -",
		"Removed 12 unreferenced file(s) (40.0 MB)",
		"3 repo(s): 1 ok, 1 failed, 1 skipped. Downloaded 3.5 GB in 6m00s.",
		"Errors (2):",
		"  - [rpm] rocky-9-baseos: 3 package(s) failed to download; repomd.xml not updated",
		"  - generating client configs: disk full",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("report missing %q\n%s", want, out)
		}
	}
}

func TestReportWriteDryRun(t *testing.T) {
	r := &runReport{DryRun: true}
	r.add(newRepoReport("deb", "ubuntu", 1200, 0, 0, time.Second, nil))
	r.CleanFiles = 2
	r.CleanBytes = 1000

	var buf bytes.Buffer
	r.write(&buf, time.Second)
	out := buf.String()

	for _, want := range []string{
		"Sync report (dry run)",
		"deb   ubuntu  ok      1200      0       -           1s",
		"Would remove 2 stale temporary file(s) (1.0 KB)",
		"1 repo(s): 1 ok, 0 failed, 0 skipped. Took 1s.",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("report missing %q\n%s", want, out)
		}
	}
	if strings.Contains(out, "Errors") {
		t.Errorf("dry-run report with no errors should not list errors\n%s", out)
	}
}
