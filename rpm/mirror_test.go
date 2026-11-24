package rpm

import (
	"bytes"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"repomirror/downloader"
	"repomirror/internal/testrepo"
)

func TestMain(m *testing.M) { os.Exit(testrepo.RunIsolated(m)) }

const (
	repoPrefix = "el9"
	keyFile    = "keys/RPM-GPG-KEY-test"
	localKey   = "gpg-keys/RPM-GPG-KEY-test"
	archX86    = "x86_64"
	archARM    = "aarch64"
	archNoarch = "noarch"
)

func payload(seed string, n int) []byte {
	return bytes.Repeat([]byte(seed), n/len(seed)+1)[:n]
}

var (
	pkgAlpha = testrepo.RPMPackage{Href: "Packages/a/alpha-1.0-1.x86_64.rpm", Arch: archX86, Data: payload("alpha", 4096)}
	pkgBeta  = testrepo.RPMPackage{Href: "Packages/b/beta-1.0-1.aarch64.rpm", Arch: archARM, Data: payload("beta", 3000)}
	pkgGamma = testrepo.RPMPackage{Href: "Packages/g/gamma-1.0-1.noarch.rpm", Arch: archNoarch, Data: payload("gamma", 1500)}
	pkgDelta = testrepo.RPMPackage{Href: "Packages/d/delta-1.0-1.x86_64.rpm", Arch: archX86, Data: payload("delta", 2000)}
)

type fixture struct {
	t     *testing.T
	srv   *testrepo.Server
	key   *testrepo.Key
	repo  testrepo.RPMRepo
	files map[string][]byte
	dest  string
	opts  Options
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	key := testrepo.NewKey(t, "rpm-test")
	srv := testrepo.NewServer(t)
	srv.Put(keyFile, key.Armored)
	f := &fixture{
		t:    t,
		srv:  srv,
		key:  key,
		repo: testrepo.RPMRepo{Packages: []testrepo.RPMPackage{pkgAlpha, pkgBeta, pkgGamma}, Key: key},
		dest: t.TempDir(),
	}
	f.opts = Options{
		Name:             "el9-test",
		BaseURL:          srv.URL + "/" + repoPrefix + "/",
		GPGKey:           srv.URL + "/" + keyFile,
		DestDir:          f.dest,
		Workers:          2,
		VerifySignatures: true,
	}
	f.publish()
	return f
}

func (f *fixture) publish() map[string][]byte {
	f.files = f.repo.Files(f.t)
	f.srv.Publish(repoPrefix, f.files)
	return f.files
}

func client() *downloader.Client {
	dl := downloader.New()
	dl.Retries = 0
	return dl
}

func (f *fixture) mirror() (*Result, error) {
	return Mirror(f.opts, client())
}

func (f *fixture) mustMirror() *Result {
	f.t.Helper()
	res, err := f.mirror()
	if err != nil {
		f.t.Fatalf("Mirror: %v", err)
	}
	if !res.Complete {
		f.t.Fatal("Mirror returned without error but Complete is false")
	}
	return res
}

func (f *fixture) local(rel string) string {
	return filepath.Join(f.dest, filepath.FromSlash(rel))
}

func (f *fixture) upstream(rel string) string {
	return repoPrefix + "/" + rel
}

func (f *fixture) read(rel string) []byte {
	f.t.Helper()
	data, err := os.ReadFile(f.local(rel))
	if err != nil {
		f.t.Fatalf("read %s: %v", rel, err)
	}
	return data
}

func (f *fixture) assertFile(rel string, want []byte) {
	f.t.Helper()
	if got := f.read(rel); !bytes.Equal(got, want) {
		f.t.Errorf("%s: got %d bytes, want %d", rel, len(got), len(want))
	}
}

func (f *fixture) assertMissing(rel string) {
	f.t.Helper()
	if _, err := os.Stat(f.local(rel)); !os.IsNotExist(err) {
		f.t.Errorf("%s exists, want missing (err=%v)", rel, err)
	}
}

func primaryHref(files map[string][]byte) string {
	for p := range files {
		if strings.Contains(p, "-primary.xml") {
			return p
		}
	}
	return ""
}

func regularFiles(t *testing.T, root string) []string {
	t.Helper()
	var out []string
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.Type().IsRegular() {
			rel, relErr := filepath.Rel(root, p)
			if relErr != nil {
				return relErr
			}
			out = append(out, filepath.ToSlash(rel))
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func TestMirrorFirstSync(t *testing.T) {
	f := newFixture(t)
	files := f.files
	res := f.mustMirror()

	var total int64
	for _, p := range f.repo.Packages {
		f.assertFile(p.Href, p.Data)
		total += int64(len(p.Data))
		if !res.Referenced[f.local(p.Href)] {
			t.Errorf("%s not referenced", p.Href)
		}
	}
	if res.Packages != 3 || res.Failed != 0 || res.Bytes != total {
		t.Errorf("result counts = %d packages, %d failed, %d bytes; want 3, 0, %d", res.Packages, res.Failed, res.Bytes, total)
	}
	f.assertFile(testrepo.RPMRepomd, files[testrepo.RPMRepomd])
	f.assertFile(testrepo.RPMRepomd+".asc", files[testrepo.RPMRepomd+".asc"])
	f.assertFile(primaryHref(files), files[primaryHref(files)])
	if got := f.read(localKey); !bytes.Equal(bytes.TrimSpace(got), bytes.TrimSpace(f.key.Armored)) {
		t.Errorf("stored key differs from upstream key")
	}
	for _, rel := range []string{testrepo.RPMRepomd, testrepo.RPMRepomd + ".asc", primaryHref(files), localKey} {
		if !res.Referenced[f.local(rel)] {
			t.Errorf("%s not referenced", rel)
		}
	}
	for _, p := range regularFiles(t, f.dest) {
		if downloader.IsTempFile(p) {
			t.Errorf("temporary file left behind: %s", p)
		}
	}
}

func TestMirrorSecondRunDownloadsNothing(t *testing.T) {
	f := newFixture(t)
	files := f.files
	f.mustMirror()

	watched := []string{primaryHref(files)}
	for _, p := range f.repo.Packages {
		watched = append(watched, p.Href)
	}
	before := map[string]int{}
	for _, rel := range watched {
		before[rel] = f.srv.Hits(f.upstream(rel))
	}

	res := f.mustMirror()
	if res.Bytes != 0 {
		t.Errorf("second run downloaded %d package bytes, want 0", res.Bytes)
	}
	for _, rel := range watched {
		if got := f.srv.Hits(f.upstream(rel)); got != before[rel] {
			t.Errorf("%s fetched again: %d requests, was %d", rel, got, before[rel])
		}
	}
}

func TestMirrorPackageNotFound(t *testing.T) {
	f := newFixture(t)
	f.srv.SetFault(f.upstream(pkgBeta.Href), testrepo.FaultMissing)

	res, err := f.mirror()
	if err == nil || !strings.Contains(err.Error(), "1 package(s) failed") {
		t.Fatalf("err = %v, want one failed package", err)
	}
	if res.Complete || res.Failed != 1 {
		t.Errorf("Complete=%v Failed=%d, want false and 1", res.Complete, res.Failed)
	}
	f.assertFile(pkgAlpha.Href, pkgAlpha.Data)
	f.assertMissing(pkgBeta.Href)
	f.assertMissing(testrepo.RPMRepomd)
}

func TestMirrorChecksumMismatch(t *testing.T) {
	f := newFixture(t)
	f.srv.SetFault(f.upstream(pkgAlpha.Href), testrepo.FaultCorrupt)

	res, err := f.mirror()
	if err == nil || res.Failed != 1 {
		t.Fatalf("err = %v, Failed = %d; want a failure", err, res.Failed)
	}
	f.assertMissing(pkgAlpha.Href)
	f.assertMissing(pkgAlpha.Href + downloader.PartSuffix)
	f.assertMissing(testrepo.RPMRepomd)
}

func TestMirrorRejectsBadSignature(t *testing.T) {
	f := newFixture(t)
	f.repo.Key = testrepo.NewKey(t, "intruder")
	f.publish()

	_, err := f.mirror()
	if err == nil || !strings.Contains(err.Error(), "signature check failed") {
		t.Fatalf("err = %v, want signature failure", err)
	}
	if n := f.srv.Hits(f.upstream(pkgAlpha.Href)); n != 0 {
		t.Errorf("package fetched %d times after a bad signature", n)
	}
	f.assertMissing(testrepo.RPMRepomd)
}

func TestMirrorUnsignedRepomdIsAccepted(t *testing.T) {
	f := newFixture(t)
	f.mustMirror()
	f.assertFile(testrepo.RPMRepomd+".asc", f.files[testrepo.RPMRepomd+".asc"])

	f.repo.Key = nil
	files := f.publish()
	f.mustMirror()
	f.assertFile(testrepo.RPMRepomd, files[testrepo.RPMRepomd])
	f.assertMissing(testrepo.RPMRepomd + ".asc")
}

func TestMirrorKeyUnavailable(t *testing.T) {
	f := newFixture(t)
	f.srv.Delete(keyFile)

	if _, err := f.mirror(); err == nil || !strings.Contains(err.Error(), "GPG key") {
		t.Fatalf("err = %v, want GPG key error", err)
	}
	f.opts.VerifySignatures = false
	f.mustMirror()
	f.assertFile(pkgAlpha.Href, pkgAlpha.Data)
}

func TestMirrorUsesStoredKeyWhenFetchFails(t *testing.T) {
	f := newFixture(t)
	f.mustMirror()
	f.srv.Delete(keyFile)
	f.mustMirror()
}

func TestMirrorArchFilter(t *testing.T) {
	f := newFixture(t)
	f.opts.Arches = []string{archX86}
	res := f.mustMirror()

	f.assertFile(pkgAlpha.Href, pkgAlpha.Data)
	f.assertFile(pkgGamma.Href, pkgGamma.Data)
	f.assertMissing(pkgBeta.Href)
	if res.Packages != 2 || res.Referenced[f.local(pkgBeta.Href)] {
		t.Errorf("Packages = %d, beta referenced = %v; want 2 and false", res.Packages, res.Referenced[f.local(pkgBeta.Href)])
	}
}

func TestMirrorPrimaryFormats(t *testing.T) {
	cases := []struct {
		name        string
		compression string
		sqlite      bool
		mode        string
	}{
		{name: "xz", compression: testrepo.Xz},
		{name: "zstd", compression: testrepo.Zst},
		{name: "uncompressed", compression: testrepo.None},
		{name: "sqlite", sqlite: true, mode: primaryMetaSQLite},
		{name: "sqlite available but xml forced", sqlite: true, mode: primaryMetaXML},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t)
			f.repo.Compression = tc.compression
			f.repo.SQLite = tc.sqlite
			f.opts.PrimaryMetadata = tc.mode
			f.publish()
			res := f.mustMirror()
			if res.Packages != 3 {
				t.Fatalf("Packages = %d, want 3", res.Packages)
			}
			for _, p := range f.repo.Packages {
				f.assertFile(p.Href, p.Data)
			}
		})
	}
}

func TestMirrorFailedSyncKeepsPublishedMetadata(t *testing.T) {
	f := newFixture(t)
	oldFiles := f.files
	f.mustMirror()
	oldRepomd := f.read(testrepo.RPMRepomd)

	f.repo.Packages = append(f.repo.Packages, pkgDelta)
	newFiles := f.publish()
	f.srv.SetFault(f.upstream(pkgDelta.Href), testrepo.FaultMissing)
	if _, err := f.mirror(); err == nil {
		t.Fatal("Mirror succeeded with a missing package")
	}
	if !bytes.Equal(f.read(testrepo.RPMRepomd), oldRepomd) {
		t.Fatal("repomd.xml was replaced by a failed sync")
	}
	f.assertFile(primaryHref(oldFiles), oldFiles[primaryHref(oldFiles)])
	f.assertMissing(primaryHref(newFiles))

	f.srv.SetFault(f.upstream(pkgDelta.Href), testrepo.FaultNone)
	f.mustMirror()
	f.assertFile(testrepo.RPMRepomd, newFiles[testrepo.RPMRepomd])
	f.assertFile(primaryHref(newFiles), newFiles[primaryHref(newFiles)])
	f.assertMissing(primaryHref(newFiles) + downloader.StageSuffix)
}

func TestMirrorResumesInterruptedDownload(t *testing.T) {
	f := newFixture(t)
	up := f.upstream(pkgAlpha.Href)
	f.srv.SetFault(up, testrepo.FaultTruncate)
	if _, err := f.mirror(); err == nil {
		t.Fatal("Mirror succeeded with a truncated download")
	}
	half := int64(len(pkgAlpha.Data) / 2)
	info, err := os.Stat(f.local(pkgAlpha.Href + downloader.PartSuffix))
	if err != nil || info.Size() != half {
		t.Fatalf("partial file: %v, want %d bytes", err, half)
	}

	f.srv.SetFault(up, testrepo.FaultNone)
	res := f.mustMirror()
	f.assertFile(pkgAlpha.Href, pkgAlpha.Data)
	f.assertMissing(pkgAlpha.Href + downloader.PartSuffix)
	if got := f.srv.Ranges(up); len(got) != 1 || got[0] != "bytes=2048-" {
		t.Errorf("Range requests = %v, want [bytes=2048-]", got)
	}
	if want := int64(len(pkgAlpha.Data)) - half; res.Bytes != want {
		t.Errorf("resumed run downloaded %d bytes, want %d", res.Bytes, want)
	}
}

func TestMirrorDryRunWritesNoFiles(t *testing.T) {
	for _, sqlite := range []bool{false, true} {
		f := newFixture(t)
		f.repo.SQLite = sqlite
		f.publish()
		f.opts.PrimaryMetadata = primaryMetaSQLite
		dl := client()
		dl.DryRun = true
		res, err := Mirror(f.opts, dl)
		if err != nil || !res.Complete || res.Packages != 3 {
			t.Fatalf("sqlite=%v: dry run: err=%v complete=%v packages=%d", sqlite, err, res.Complete, res.Packages)
		}
		if files := regularFiles(t, f.dest); len(files) != 0 {
			t.Errorf("sqlite=%v: dry run wrote files: %v", sqlite, files)
		}
		if n := f.srv.Hits(f.upstream(pkgAlpha.Href)); n != 0 {
			t.Errorf("sqlite=%v: dry run fetched a package %d times", sqlite, n)
		}
		if info, err := os.Stat(f.local("Packages/a")); err != nil || !info.IsDir() {
			t.Errorf("sqlite=%v: package directory not created: %v", sqlite, err)
		}
	}
}

func TestMirrorUpstreamDropsPackage(t *testing.T) {
	f := newFixture(t)
	oldFiles := f.files
	f.mustMirror()

	f.repo.Packages = []testrepo.RPMPackage{pkgAlpha, pkgGamma}
	f.publish()
	res := f.mustMirror()
	if res.Referenced[f.local(pkgBeta.Href)] || res.Referenced[f.local(primaryHref(oldFiles))] {
		t.Error("dropped package or old primary still referenced")
	}
	if !res.Referenced[f.local(pkgAlpha.Href)] {
		t.Error("kept package not referenced")
	}
}

func TestMirrorMetadataErrors(t *testing.T) {
	cases := []struct {
		name  string
		setup func(f *fixture)
		want  string
	}{
		{"no source", func(f *fixture) { f.opts.BaseURL = "" }, "resolve sources"},
		{"repomd missing", func(f *fixture) { f.srv.Delete(f.upstream(testrepo.RPMRepomd)) }, "fetch repomd.xml"},
		{"repomd invalid", func(f *fixture) { f.srv.Put(f.upstream(testrepo.RPMRepomd), []byte("not xml")) }, "not valid repository metadata"},
		{"repomd empty", func(f *fixture) { f.srv.Put(f.upstream(testrepo.RPMRepomd), []byte("<repomd></repomd>")) }, "lists no metadata"},
		{"no primary", func(f *fixture) {
			f.srv.Put(f.upstream(testrepo.RPMRepomd), []byte(`<repomd><data type="other"><location href="repodata/other.xml"/></data></repomd>`))
			f.srv.Put(f.upstream("repodata/other.xml"), []byte("x"))
		}, "no primary metadata"},
		{"unsafe href", func(f *fixture) {
			f.srv.Put(f.upstream(testrepo.RPMRepomd), []byte(`<repomd><data type="primary"><location href="../../escape.xml"/></data></repomd>`))
		}, "metadata file(s) failed"},
		{"primary missing", func(f *fixture) {
			f.srv.SetFault(f.upstream(primaryHref(f.files)), testrepo.FaultMissing)
		}, "metadata file(s) failed"},
		{"primary corrupt", func(f *fixture) {
			f.srv.Put(f.upstream(testrepo.RPMRepomd), []byte(`<repomd><data type="primary"><location href="repodata/primary.xml"/></data></repomd>`))
			f.srv.Put(f.upstream("repodata/primary.xml"), []byte("<metadata><package>"))
		}, "parse primary metadata"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t)
			f.opts.VerifySignatures = false
			tc.setup(f)
			_, err := f.mirror()
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want %q", err, tc.want)
			}
			f.assertMissing(testrepo.RPMRepomd)
		})
	}
}
