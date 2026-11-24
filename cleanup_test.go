package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"repomirror/downloader"
	"repomirror/internal/testrepo"
	"repomirror/rpm"
)

func TestMain(m *testing.M) { os.Exit(testrepo.RunIsolated(m)) }

var (
	cleanAlpha = testrepo.RPMPackage{Href: "Packages/a/alpha-1.0-1.noarch.rpm", Arch: "noarch", Data: bytes.Repeat([]byte("a"), 4096)}
	cleanBeta  = testrepo.RPMPackage{Href: "Packages/b/beta-1.0-1.noarch.rpm", Arch: "noarch", Data: bytes.Repeat([]byte("b"), 2048)}
	cleanDelta = testrepo.RPMPackage{Href: "Packages/d/delta-1.0-1.noarch.rpm", Arch: "noarch", Data: bytes.Repeat([]byte("d"), 1024)}
)

type cleanFixture struct {
	t     *testing.T
	srv   *testrepo.Server
	repo  testrepo.RPMRepo
	files map[string][]byte
	out   string
	dest  string
}

func newCleanFixture(t *testing.T) *cleanFixture {
	t.Helper()
	key := testrepo.NewKey(t, "clean-test")
	srv := testrepo.NewServer(t)
	srv.Put("keys/test.key", key.Armored)
	out := t.TempDir()
	f := &cleanFixture{
		t:    t,
		srv:  srv,
		repo: testrepo.RPMRepo{Packages: []testrepo.RPMPackage{cleanAlpha, cleanBeta}, Key: key},
		out:  out,
		dest: repoDestDir(out, "", "el9"),
	}
	f.publish()
	return f
}

func (f *cleanFixture) publish() {
	f.files = f.repo.Files(f.t)
	f.srv.Publish("el9", f.files)
}

func (f *cleanFixture) sync(paths *pathTracker) error {
	dl := downloader.New()
	dl.Retries = 0
	res, err := rpm.Mirror(rpm.Options{
		Name:             "el9",
		BaseURL:          f.srv.URL + "/el9",
		GPGKey:           f.srv.URL + "/keys/test.key",
		DestDir:          f.dest,
		Workers:          2,
		VerifySignatures: true,
	}, dl)
	paths.record(f.dest, res.Referenced, err == nil && res.Complete)
	return err
}

func (f *cleanFixture) mustSync(paths *pathTracker) {
	f.t.Helper()
	if err := f.sync(paths); err != nil {
		f.t.Fatalf("sync: %v", err)
	}
}

func (f *cleanFixture) local(rel string) string {
	return filepath.Join(f.dest, filepath.FromSlash(rel))
}

func (f *cleanFixture) exists(rel string) bool {
	_, err := os.Stat(f.local(rel))
	return err == nil
}

func (f *cleanFixture) clean(paths *pathTracker, prune, dryRun bool) *runReport {
	report := &runReport{Prune: prune, DryRun: dryRun}
	cleanMirrorPaths(paths, f.out, prune, dryRun, report)
	return report
}

func primaryOf(files map[string][]byte) string {
	for p := range files {
		if strings.Contains(p, "-primary.xml") {
			return p
		}
	}
	return ""
}

func TestCleanPrunesFilesUpstreamDropped(t *testing.T) {
	f := newCleanFixture(t)
	f.mustSync(newPathTracker())
	oldPrimary := primaryOf(f.files)

	f.repo.Packages = []testrepo.RPMPackage{cleanAlpha}
	f.publish()
	paths := newPathTracker()
	f.mustSync(paths)
	report := f.clean(paths, true, false)

	for _, rel := range []string{cleanBeta.Href, oldPrimary, "Packages/b"} {
		if f.exists(rel) {
			t.Errorf("%s not pruned", rel)
		}
	}
	for _, rel := range []string{cleanAlpha.Href, primaryOf(f.files), testrepo.RPMRepomd, testrepo.RPMRepomd + ".asc", "gpg-keys/test.key"} {
		if !f.exists(rel) {
			t.Errorf("%s pruned although referenced", rel)
		}
	}
	if len(report.Cleanup) != 1 || report.Cleanup[0].Files != 2 || report.Cleanup[0].Path != "el9" {
		t.Errorf("cleanup report = %+v", report.Cleanup)
	}
}

func TestCleanSkipsPathWhenSyncFailed(t *testing.T) {
	f := newCleanFixture(t)
	f.mustSync(newPathTracker())

	f.repo.Packages = []testrepo.RPMPackage{cleanAlpha, cleanDelta}
	f.publish()
	f.srv.SetFault("el9/"+cleanDelta.Href, testrepo.FaultMissing)
	paths := newPathTracker()
	if err := f.sync(paths); err == nil {
		t.Fatal("sync succeeded with a missing package")
	}
	report := f.clean(paths, true, false)
	if !f.exists(cleanBeta.Href) {
		t.Error("prune ran after a failed sync")
	}
	if len(report.Cleanup) != 1 || report.Cleanup[0].Skipped != "sync had errors" {
		t.Errorf("cleanup report = %+v", report.Cleanup)
	}
}

func TestCleanRemovesAbandonedPartialFiles(t *testing.T) {
	f := newCleanFixture(t)
	f.srv.SetFault("el9/"+cleanBeta.Href, testrepo.FaultTruncate)
	paths := newPathTracker()
	if err := f.sync(paths); err == nil {
		t.Fatal("sync succeeded with a truncated download")
	}
	f.clean(paths, false, false)
	if !f.exists(cleanBeta.Href + downloader.PartSuffix) {
		t.Fatal("partial file removed after a failed sync")
	}

	f.srv.SetFault("el9/"+cleanBeta.Href, testrepo.FaultNone)
	f.repo.Packages = []testrepo.RPMPackage{cleanAlpha}
	f.publish()
	extra := f.local("extra/manual.rpm")
	if err := os.MkdirAll(filepath.Dir(extra), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(extra, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	paths = newPathTracker()
	f.mustSync(paths)
	report := f.clean(paths, false, false)
	if f.exists(cleanBeta.Href + downloader.PartSuffix) {
		t.Error("abandoned partial file kept")
	}
	if !f.exists("extra/manual.rpm") || !f.exists(cleanAlpha.Href) {
		t.Error("temp-only clean removed a regular file")
	}
	if len(report.Cleanup) != 1 || report.Cleanup[0].Files != 2 {
		t.Errorf("cleanup report = %+v", report.Cleanup)
	}
}

func TestCleanDryRunRemovesNothing(t *testing.T) {
	f := newCleanFixture(t)
	f.mustSync(newPathTracker())
	f.repo.Packages = []testrepo.RPMPackage{cleanAlpha}
	f.publish()
	paths := newPathTracker()
	f.mustSync(paths)
	report := f.clean(paths, true, true)
	if !f.exists(cleanBeta.Href) {
		t.Error("dry run removed a file")
	}
	if len(report.Cleanup) != 1 || report.Cleanup[0].Files != 2 {
		t.Errorf("cleanup report = %+v", report.Cleanup)
	}
}

func TestCleanSkipReasons(t *testing.T) {
	out := t.TempDir()
	write := func(rel string) string {
		p := filepath.Join(out, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(p), 0o750); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(rel), 0o600); err != nil {
			t.Fatal(err)
		}
		return p
	}
	keepOuter := write("ubuntu/pool/a.deb")
	staleOuter := write("ubuntu/stale.deb")
	nestedFile := write("ubuntu/nested/pool/b.deb")
	disabledFile := write("disabled/c.rpm")
	emptyFile := write("empty/d.rpm")
	rootFile := write("root.txt")

	paths := newPathTracker()
	paths.record(filepath.Join(out, "ubuntu"), map[string]bool{keepOuter: true}, true)
	paths.record(filepath.Join(out, "ubuntu", "nested"), map[string]bool{}, true)
	paths.disabled(filepath.Join(out, "disabled"))
	paths.record(filepath.Join(out, "empty"), nil, true)
	paths.record(out, map[string]bool{}, true)

	report := &runReport{Prune: true}
	cleanMirrorPaths(paths, out, true, false, report)

	if _, err := os.Stat(staleOuter); !os.IsNotExist(err) {
		t.Error("stale file in the outer repo kept")
	}
	for _, p := range []string{keepOuter, nestedFile, disabledFile, emptyFile, rootFile} {
		if _, err := os.Stat(p); err != nil {
			t.Errorf("%s removed", p)
		}
	}
	want := map[string]string{
		"ubuntu":        "",
		"ubuntu/nested": "nothing referenced",
		"disabled":      "a repo is disabled",
		"empty":         "nothing referenced",
		".":             "not a subdirectory of output_dir",
	}
	if len(report.Cleanup) != len(want) {
		t.Fatalf("cleanup report = %+v", report.Cleanup)
	}
	for _, c := range report.Cleanup {
		if reason, ok := want[c.Path]; !ok || reason != c.Skipped {
			t.Errorf("cleanup entry %+v, want skipped %q", c, reason)
		}
	}
}

func TestPathTracker(t *testing.T) {
	paths := newPathTracker()
	paths.record("/out/a/", map[string]bool{"/out/a/x/../y": true}, true)
	paths.record("/out/a", map[string]bool{"/out/a/z": true}, false)
	s := paths.get("/out/a")
	if len(paths.order) != 1 || s.complete || !s.keep["/out/a/y"] || !s.keep["/out/a/z"] {
		t.Errorf("tracker state = %+v %+v", paths.order, s)
	}
}

func TestMainHelpers(t *testing.T) {
	if repoDestDir("/out", "", "name") != filepath.Join("/out", "name") || repoDestDir("/out", "a/b", "name") != filepath.Join("/out", "a", "b") {
		t.Error("repoDestDir")
	}
	if displayName("n", "p") != "n" || displayName("", "p") != "p" {
		t.Error("displayName")
	}
	if relPath("/out", "/out/a/b") != "a/b" || relPath("/out", "/elsewhere") != "/elsewhere" {
		t.Error("relPath")
	}
	if rep := newRepoReport("rpm", "r", 1, 0, 0, time.Second, os.ErrNotExist); rep.Status != statusFailed {
		t.Errorf("failed repo status = %v", rep.Status)
	}

	dir := t.TempDir()
	if got := defaultConfigPath(dir); got != filepath.Join(dir, "mirrors.yml") {
		t.Errorf("default with no config = %s", got)
	}
	for _, name := range []string{"mirrors.yaml", "mirrors.yml"} {
		if err := os.WriteFile(filepath.Join(dir, name), nil, 0o600); err != nil {
			t.Fatal(err)
		}
		if got := defaultConfigPath(dir); got != filepath.Join(dir, name) {
			t.Errorf("default with %s = %s", name, got)
		}
	}
}

func TestLockOutputTree(t *testing.T) {
	dir := t.TempDir()
	unlock, err := lockOutputTree(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = lockOutputTree(dir); err == nil || !strings.Contains(err.Error(), "already running") {
		t.Errorf("second lock err = %v", err)
	}
	unlock()
	if _, err = os.Stat(filepath.Join(dir, ".repomirror.lock")); !os.IsNotExist(err) {
		t.Error("lock file left behind")
	}
	unlock2, err := lockOutputTree(dir)
	if err != nil {
		t.Fatalf("relock: %v", err)
	}
	unlock2()
	if _, err = lockOutputTree(filepath.Join(dir, "missing")); err == nil {
		t.Error("lock in a missing directory succeeded")
	}
}
