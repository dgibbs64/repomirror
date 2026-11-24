package deb

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
	repoPrefix = "debian"
	keyFile    = "keys/debian-test.asc"
	localKey   = "gpg-keys/debian-test.asc"
	suite      = "stable"
	comp       = "main"
	archAMD    = "amd64"
	archARM    = "arm64"
	dist       = "dists/stable/"
	amdIndex   = "main/binary-amd64/Packages"
)

func payload(seed string, n int) []byte {
	return bytes.Repeat([]byte(seed), n/len(seed)+1)[:n]
}

var (
	pkgHelloAMD = testrepo.DEBPackage{Name: "hello", Arch: archAMD, Data: payload("hello-amd64", 2500)}
	pkgHelloARM = testrepo.DEBPackage{Name: "hello", Arch: archARM, Data: payload("hello-arm64", 2400)}
	pkgCommon   = testrepo.DEBPackage{Name: "common", Arch: testrepo.ArchAll, Data: payload("common", 1200)}
	pkgTools    = testrepo.DEBPackage{Name: "tools", Arch: archAMD, Data: payload("tools", 4096)}
	pkgExtra    = testrepo.DEBPackage{Name: "extra", Arch: archAMD, Data: payload("extra", 900)}
)

func fname(p testrepo.DEBPackage) string { return p.Filename(comp) }

type fixture struct {
	t     *testing.T
	srv   *testrepo.Server
	key   *testrepo.Key
	repo  testrepo.DEBRepo
	files map[string][]byte
	dest  string
	opts  Options
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	key := testrepo.NewKey(t, "deb-test")
	srv := testrepo.NewServer(t)
	srv.Put(keyFile, key.Armored)
	f := &fixture{
		t:   t,
		srv: srv,
		key: key,
		repo: testrepo.DEBRepo{
			Suite:     suite,
			Component: comp,
			Arches:    []string{archAMD, archARM},
			Packages:  []testrepo.DEBPackage{pkgHelloAMD, pkgHelloARM, pkgCommon, pkgTools},
			ByHash:    true,
			Key:       key,
		},
		dest: t.TempDir(),
	}
	f.opts = Options{
		Name:             "debian-test",
		MirrorURL:        srv.URL + "/" + repoPrefix,
		GPGKey:           srv.URL + "/" + keyFile,
		DestDir:          f.dest,
		Suites:           []string{suite},
		Components:       []string{comp},
		Arches:           []string{archAMD},
		Workers:          2,
		VerifySignatures: true,
	}
	f.publish()
	return f
}

func (f *fixture) publish() {
	f.files = f.repo.Files(f.t)
	f.srv.Publish(repoPrefix, f.files)
}

func client() *downloader.Client {
	dl := downloader.New()
	dl.Retries = 0
	return dl
}

func (f *fixture) mirror() (*Result, error) { return Mirror(f.opts, client()) }

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

func (f *fixture) local(rel string) string { return filepath.Join(f.dest, filepath.FromSlash(rel)) }

func (f *fixture) upstream(rel string) string { return repoPrefix + "/" + rel }

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

func regularFiles(t *testing.T, root string) []string {
	t.Helper()
	var out []string
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.Type().IsRegular() {
			out = append(out, p)
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
	res := f.mustMirror()

	want := []testrepo.DEBPackage{pkgHelloAMD, pkgCommon, pkgTools}
	var total int64
	for _, p := range want {
		f.assertFile(fname(p), p.Data)
		total += int64(len(p.Data))
	}
	f.assertMissing(fname(pkgHelloARM))
	if res.Packages != len(want) || res.Failed != 0 || res.Bytes != total {
		t.Errorf("result counts = %d packages, %d failed, %d bytes; want %d, 0, %d", res.Packages, res.Failed, res.Bytes, len(want), total)
	}
	for _, rel := range []string{dist + "Release", dist + "Release.gpg", dist + "InRelease", dist + amdIndex + ".gz"} {
		f.assertFile(rel, f.files[rel])
		if !res.Referenced[f.local(rel)] {
			t.Errorf("%s not referenced", rel)
		}
	}
	gz := f.files[dist+amdIndex+".gz"]
	for _, rel := range []string{
		dist + testrepo.ByHashPath(amdIndex+".gz", "SHA256", testrepo.SHA256(gz)),
		dist + testrepo.ByHashPath(amdIndex+".gz", "MD5Sum", testrepo.MD5(gz)),
	} {
		f.assertFile(rel, gz)
		if !res.Referenced[f.local(rel)] {
			t.Errorf("%s not referenced", rel)
		}
	}
	f.assertMissing(dist + "main/binary-arm64")
	if got := f.read(localKey); !bytes.Equal(bytes.TrimSpace(got), bytes.TrimSpace(f.key.Armored)) {
		t.Error("stored key differs from upstream key")
	}
	if n := f.srv.Hits(f.upstream(dist + amdIndex + ".gz")); n != 0 {
		t.Errorf("canonical Packages.gz fetched %d times although by-hash is available", n)
	}
	for _, p := range regularFiles(t, f.dest) {
		if downloader.IsTempFile(p) {
			t.Errorf("temporary file left behind: %s", p)
		}
	}
}

func TestMirrorSecondRunDownloadsNothing(t *testing.T) {
	f := newFixture(t)
	f.mustMirror()
	before := f.srv.TotalHits(f.upstream("pool")) + f.srv.TotalHits(f.upstream(dist+"main"))

	res := f.mustMirror()
	if res.Bytes != 0 {
		t.Errorf("second run downloaded %d package bytes, want 0", res.Bytes)
	}
	if after := f.srv.TotalHits(f.upstream("pool")) + f.srv.TotalHits(f.upstream(dist+"main")); after != before {
		t.Errorf("packages or indexes fetched again: %d requests, was %d", after, before)
	}
}

func TestMirrorAllArches(t *testing.T) {
	f := newFixture(t)
	f.opts.Arches = []string{archAMD, archARM}
	res := f.mustMirror()
	for _, p := range f.repo.Packages {
		f.assertFile(fname(p), p.Data)
	}
	if res.Packages != 4 {
		t.Errorf("Packages = %d, want 4", res.Packages)
	}
}

func TestMirrorPackageNotFound(t *testing.T) {
	f := newFixture(t)
	f.srv.SetFault(f.upstream(fname(pkgCommon)), testrepo.FaultMissing)

	res, err := f.mirror()
	if err == nil || !strings.Contains(err.Error(), "1 package(s) failed") {
		t.Fatalf("err = %v, want one failed package", err)
	}
	if res.Complete || res.Failed != 1 {
		t.Errorf("Complete=%v Failed=%d", res.Complete, res.Failed)
	}
	f.assertFile(fname(pkgHelloAMD), pkgHelloAMD.Data)
	f.assertMissing(dist + "Release")
	f.assertMissing(dist + "InRelease")
	f.assertMissing(dist + amdIndex + ".gz")
}

func TestMirrorChecksumMismatch(t *testing.T) {
	f := newFixture(t)
	f.srv.SetFault(f.upstream(fname(pkgTools)), testrepo.FaultCorrupt)

	if res, err := f.mirror(); err == nil || res.Failed != 1 {
		t.Fatalf("err = %v; want a failed package", err)
	}
	f.assertMissing(fname(pkgTools))
	f.assertMissing(fname(pkgTools) + downloader.PartSuffix)
	f.assertMissing(dist + "InRelease")
}

func TestMirrorSignatures(t *testing.T) {
	cases := []struct {
		name    string
		setup   func(f *fixture)
		verify  bool
		ok      bool
		present []string
		absent  []string
	}{
		{name: "signed by another key", setup: func(f *fixture) { f.repo.Key = testrepo.NewKey(f.t, "intruder"); f.publish() }, verify: true},
		{name: "unsigned with verification", setup: func(f *fixture) { f.repo.Key = nil; f.publish() }, verify: true},
		{name: "unsigned without verification", setup: func(f *fixture) { f.repo.Key = nil; f.publish() }, ok: true,
			present: []string{"Release"}, absent: []string{"InRelease", "Release.gpg"}},
		{name: "Release and Release.gpg only", setup: func(f *fixture) { f.repo.NoInRelease = true; f.publish() }, verify: true, ok: true,
			present: []string{"Release", "Release.gpg"}, absent: []string{"InRelease"}},
		{name: "valid InRelease with bad Release.gpg", setup: func(f *fixture) {
			f.publish()
			f.srv.Put(f.upstream(dist+"Release.gpg"), testrepo.NewKey(f.t, "intruder").DetachSign(f.t, f.files[dist+"Release"]))
		}, verify: true, ok: true, present: []string{"InRelease"}, absent: []string{"Release", "Release.gpg"}},
		{name: "Release only with bad Release.gpg", setup: func(f *fixture) {
			f.repo.NoInRelease = true
			f.publish()
			f.srv.Put(f.upstream(dist+"Release.gpg"), testrepo.NewKey(f.t, "intruder").DetachSign(f.t, f.files[dist+"Release"]))
		}, verify: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t)
			tc.setup(f)
			f.opts.VerifySignatures = tc.verify
			res, err := f.mirror()
			if tc.ok != (err == nil) {
				t.Fatalf("err = %v, want success %v", err, tc.ok)
			}
			if !tc.ok {
				if res.Packages != 0 || f.srv.TotalHits(f.upstream("pool")) != 0 {
					t.Errorf("packages fetched after a signature failure")
				}
				f.assertMissing(dist + "Release")
				f.assertMissing(dist + "InRelease")
				return
			}
			for _, name := range tc.present {
				f.assertFile(dist+name, f.srv.Body(f.upstream(dist+name)))
			}
			for _, name := range tc.absent {
				f.assertMissing(dist + name)
			}
		})
	}
}

func TestMirrorIndexCompressions(t *testing.T) {
	cases := []struct {
		name   string
		exts   []string
		byHash bool
	}{
		{"xz only", []string{testrepo.Xz}, true},
		{"uncompressed only", []string{testrepo.None}, false},
		{"gz and xz without by-hash", []string{testrepo.Gz, testrepo.Xz}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t)
			f.repo.Compressions = tc.exts
			f.repo.ByHash = tc.byHash
			f.publish()
			res := f.mustMirror()
			if res.Packages != 3 {
				t.Fatalf("Packages = %d, want 3", res.Packages)
			}
			for _, ext := range tc.exts {
				f.assertFile(dist+amdIndex+ext, f.files[dist+amdIndex+ext])
			}
			if !tc.byHash {
				f.assertMissing(dist + "main/binary-amd64/by-hash")
			}
		})
	}
}

func TestMirrorFlatRepository(t *testing.T) {
	f := newFixture(t)
	f.repo.Flat = true
	f.publish()
	f.opts.Suites = []string{"/"}
	f.opts.Components = nil
	res := f.mustMirror()
	f.assertFile("Release", f.files["Release"])
	f.assertFile("InRelease", f.files["InRelease"])
	f.assertFile("Packages.gz", f.files["Packages.gz"])
	f.assertFile(fname(pkgHelloAMD), pkgHelloAMD.Data)
	f.assertFile(fname(pkgCommon), pkgCommon.Data)
	f.assertMissing(fname(pkgHelloARM))
	if res.Packages != 3 {
		t.Errorf("Packages = %d, want 3", res.Packages)
	}
}

func TestMirrorFailedSyncKeepsPublishedMetadata(t *testing.T) {
	f := newFixture(t)
	f.mustMirror()
	oldFiles := f.files
	oldRelease := f.read(dist + "Release")
	oldInRelease := f.read(dist + "InRelease")

	f.repo.Packages = append(f.repo.Packages, pkgExtra)
	f.publish()
	f.srv.SetFault(f.upstream(fname(pkgExtra)), testrepo.FaultMissing)
	if _, err := f.mirror(); err == nil {
		t.Fatal("Mirror succeeded with a missing package")
	}
	if !bytes.Equal(f.read(dist+"Release"), oldRelease) || !bytes.Equal(f.read(dist+"InRelease"), oldInRelease) {
		t.Fatal("Release files were replaced by a failed sync")
	}
	f.assertFile(dist+amdIndex+".gz", oldFiles[dist+amdIndex+".gz"])

	f.srv.SetFault(f.upstream(fname(pkgExtra)), testrepo.FaultNone)
	f.mustMirror()
	f.assertFile(dist+"InRelease", f.files[dist+"InRelease"])
	f.assertFile(dist+amdIndex+".gz", f.files[dist+amdIndex+".gz"])
	f.assertFile(fname(pkgExtra), pkgExtra.Data)
}

func TestMirrorPublishesHealthySuites(t *testing.T) {
	f := newFixture(t)
	f.opts.Suites = []string{suite, "missing"}
	res, err := f.mirror()
	if err == nil || !strings.Contains(err.Error(), "1 suite(s)") || res.Complete {
		t.Fatalf("err = %v, Complete = %v; want one failed suite", err, res.Complete)
	}
	f.assertFile(dist+"InRelease", f.files[dist+"InRelease"])
	f.assertFile(fname(pkgTools), pkgTools.Data)
}

func TestMirrorResumesInterruptedDownload(t *testing.T) {
	f := newFixture(t)
	up := f.upstream(fname(pkgTools))
	f.srv.SetFault(up, testrepo.FaultTruncate)
	if _, err := f.mirror(); err == nil {
		t.Fatal("Mirror succeeded with a truncated download")
	}
	half := int64(len(pkgTools.Data) / 2)
	info, err := os.Stat(f.local(fname(pkgTools) + downloader.PartSuffix))
	if err != nil || info.Size() != half {
		t.Fatalf("partial file: %v, want %d bytes", err, half)
	}

	f.srv.SetFault(up, testrepo.FaultNone)
	f.mustMirror()
	f.assertFile(fname(pkgTools), pkgTools.Data)
	f.assertMissing(fname(pkgTools) + downloader.PartSuffix)
	if got := f.srv.Ranges(up); len(got) != 1 || got[0] != "bytes=2048-" {
		t.Errorf("Range requests = %v, want [bytes=2048-]", got)
	}
}

func TestMirrorDryRunWritesNoFiles(t *testing.T) {
	f := newFixture(t)
	dl := client()
	dl.DryRun = true
	res, err := Mirror(f.opts, dl)
	if err != nil || !res.Complete || res.Packages != 3 {
		t.Fatalf("dry run: err=%v complete=%v packages=%d", err, res.Complete, res.Packages)
	}
	if files := regularFiles(t, f.dest); len(files) != 0 {
		t.Errorf("dry run wrote files: %v", files)
	}
	if n := f.srv.TotalHits(f.upstream("pool")); n != 0 {
		t.Errorf("dry run fetched packages %d times", n)
	}
}

func TestMirrorDryRunRejectsCorruptIndex(t *testing.T) {
	f := newFixture(t)
	f.srv.SetFault(f.upstream(dist+amdIndex+".gz"), testrepo.FaultCorrupt)
	dl := client()
	dl.DryRun = true
	if _, err := Mirror(f.opts, dl); err == nil {
		t.Fatal("dry run accepted a corrupt Packages.gz")
	}
}

func TestMirrorNoSource(t *testing.T) {
	f := newFixture(t)
	f.opts.MirrorURL = ""
	if _, err := f.mirror(); err == nil || !strings.Contains(err.Error(), "resolve sources") {
		t.Fatalf("err = %v", err)
	}
	f.opts.MirrorURL = f.srv.URL + "/" + repoPrefix
	f.srv.Delete(keyFile)
	if _, err := f.mirror(); err == nil || !strings.Contains(err.Error(), "GPG key") {
		t.Fatalf("err = %v, want GPG key error", err)
	}
}

func TestPrepareSuiteErrors(t *testing.T) {
	cases := []struct {
		name  string
		setup func(f *fixture)
		want  string
	}{
		{"no Release files", func(f *fixture) { f.srv.Publish(repoPrefix, nil) }, "fetch Release"},
		{"unsigned InRelease", func(f *fixture) {
			f.srv.Put(f.upstream(dist+"InRelease"), f.files[dist+"Release"])
		}, "not a PGP clearsigned document"},
		{"no checksums", func(f *fixture) {
			f.srv.Publish(repoPrefix, map[string][]byte{dist + "Release": []byte("Suite: stable\n")})
		}, "no checksums"},
		{"component not listed", func(f *fixture) { f.opts.Components = []string{"contrib"} }, "not available from any source"},
		{"index unavailable", func(f *fixture) {
			f.repo.ByHash = false
			f.publish()
			f.srv.SetFault(f.upstream(dist+amdIndex+".gz"), testrepo.FaultMissing)
		}, "not available from any source"},
		{"index unparsable", func(f *fixture) {
			f.repo.ByHash = false
			f.repo.Compressions = []string{testrepo.Gz}
			f.repo.Packages = nil
			bogus := []byte("not gzip data")
			release := "SHA256:\n " + testrepo.SHA256(bogus) + " 13 " + amdIndex + ".gz\n"
			f.srv.Publish(repoPrefix, map[string][]byte{dist + "Release": []byte(release), dist + amdIndex + ".gz": bogus})
		}, "parse " + amdIndex + ".gz"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t)
			tc.setup(f)
			f.opts.VerifySignatures = false
			dl := client()
			ss := downloader.NewSourceSet([]string{f.opts.MirrorURL})
			_, err := prepareSuite(dl, ss, f.opts, nil, suite)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want %q", err, tc.want)
			}
		})
	}
}
