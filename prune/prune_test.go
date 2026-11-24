package prune

import (
	"os"
	"path/filepath"
	"sort"
	"testing"

	"repomirror/downloader"
	"repomirror/internal/testrepo"
)

func TestMain(m *testing.M) { os.Exit(testrepo.RunIsolated(m)) }

type tree struct {
	t    *testing.T
	root string
}

func newTree(t *testing.T, files ...string) *tree {
	t.Helper()
	tr := &tree{t: t, root: filepath.Join(t.TempDir(), "repo")}
	for _, f := range files {
		p := tr.abs(f)
		if err := os.MkdirAll(filepath.Dir(p), 0o750); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(f), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return tr
}

func (tr *tree) abs(rel string) string { return filepath.Join(tr.root, filepath.FromSlash(rel)) }

func (tr *tree) keep(rels ...string) map[string]bool {
	m := map[string]bool{}
	for _, r := range rels {
		m[tr.abs(r)] = true
	}
	return m
}

func (tr *tree) files() []string {
	tr.t.Helper()
	var out []string
	err := filepath.WalkDir(tr.root, func(p string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() {
			rel, relErr := filepath.Rel(tr.root, p)
			if relErr != nil {
				return relErr
			}
			out = append(out, filepath.ToSlash(rel))
		}
		return nil
	})
	if err != nil {
		tr.t.Fatal(err)
	}
	sort.Strings(out)
	return out
}

func (tr *tree) assertFiles(want ...string) {
	tr.t.Helper()
	sort.Strings(want)
	got := tr.files()
	if len(got) != len(want) {
		tr.t.Fatalf("files = %v, want %v", got, want)
	}
	for i := range got {
		if got[i] != want[i] {
			tr.t.Fatalf("files = %v, want %v", got, want)
		}
	}
}

func TestRunRemovesUnreferencedFiles(t *testing.T) {
	tr := newTree(t,
		"repodata/repomd.xml", "repodata/new-primary.xml.gz", "repodata/old-primary.xml.gz",
		"Packages/a/alpha.rpm", "Packages/b/beta.rpm", "Packages/a/alpha.rpm"+downloader.PartSuffix)
	st, err := Run(tr.root, Options{Keep: tr.keep("repodata/repomd.xml", "repodata/new-primary.xml.gz", "Packages/a/alpha.rpm"), Label: "[test]"})
	if err != nil {
		t.Fatal(err)
	}
	tr.assertFiles("repodata/repomd.xml", "repodata/new-primary.xml.gz", "Packages/a/alpha.rpm")
	if st.Files != 3 || st.Dirs != 1 {
		t.Errorf("stats = %+v, want 3 files and 1 dir", st)
	}
	if _, err := os.Stat(tr.abs("Packages/b")); !os.IsNotExist(err) {
		t.Error("empty directory kept")
	}
	if _, err := os.Stat(tr.root); err != nil {
		t.Error("root removed")
	}
}

func TestRunTempOnly(t *testing.T) {
	tr := newTree(t, "a.rpm", "b.rpm"+downloader.PartSuffix, "c"+downloader.TempSuffix, "d"+downloader.StageSuffix, "sub/e.rpm")
	st, err := Run(tr.root, Options{TempOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	tr.assertFiles("a.rpm", "sub/e.rpm")
	if st.Files != 3 || st.Dirs != 0 {
		t.Errorf("stats = %+v", st)
	}
}

func TestRunDryRun(t *testing.T) {
	var files []string
	for i := 0; i < maxListed+5; i++ {
		files = append(files, filepath.ToSlash(filepath.Join("pool", string(rune('a'+i))+".deb")))
	}
	tr := newTree(t, files...)
	st, err := Run(tr.root, Options{DryRun: true, Keep: tr.keep(files[0])})
	if err != nil {
		t.Fatal(err)
	}
	if st.Files != len(files)-1 || st.Bytes == 0 {
		t.Errorf("stats = %+v", st)
	}
	tr.assertFiles(files...)
}

func TestRunSkipsNestedReposAndSymlinks(t *testing.T) {
	tr := newTree(t, "ubuntu/pool/a.deb", "ubuntu/stale.deb", "ubuntu/nested/pool/b.deb", "outside.txt")
	if err := os.Symlink(tr.abs("outside.txt"), tr.abs("ubuntu/link")); err != nil {
		t.Fatal(err)
	}
	root := tr.abs("ubuntu")
	st, err := Run(root, Options{Keep: tr.keep("ubuntu/pool/a.deb"), Skip: []string{tr.abs("ubuntu/nested")}})
	if err != nil {
		t.Fatal(err)
	}
	tr.assertFiles("outside.txt", "ubuntu/link", "ubuntu/nested/pool/b.deb", "ubuntu/pool/a.deb")
	if st.Files != 1 {
		t.Errorf("stats = %+v", st)
	}
}

func TestRunMissingRoot(t *testing.T) {
	st, err := Run(filepath.Join(t.TempDir(), "missing"), Options{})
	if err != nil || st != (Stats{}) {
		t.Errorf("Run on missing root = %+v, %v", st, err)
	}
	file := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(file, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Run(file, Options{}); err == nil {
		t.Error("Run on a regular file succeeded")
	}
}

func TestIsUnder(t *testing.T) {
	roots := []string{"/srv/mirror/ubuntu/"}
	if !isUnder("/srv/mirror/ubuntu", roots) || !isUnder("/srv/mirror/ubuntu/pool", roots) || isUnder("/srv/mirror/ubuntu2", roots) {
		t.Error("isUnder")
	}
}
