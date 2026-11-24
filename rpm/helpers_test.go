package rpm

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"repomirror/internal/testrepo"
)

func archesOf(pkgs []rpmPkg) []string {
	out := make([]string, 0, len(pkgs))
	for _, p := range pkgs {
		out = append(out, p.Arch)
	}
	return out
}

func TestFilterArches(t *testing.T) {
	all := func() []rpmPkg {
		return []rpmPkg{{Arch: archX86}, {Arch: archARM}, {Arch: archNoarch}, {Arch: " x86_64 "}, {Arch: "i686"}}
	}
	cases := []struct {
		arches []string
		want   []string
	}{
		{nil, []string{archX86, archARM, archNoarch, " x86_64 ", "i686"}},
		{[]string{archX86}, []string{archX86, archNoarch, " x86_64 "}},
		{[]string{" aarch64 ", "i686"}, []string{archARM, archNoarch, "i686"}},
		{[]string{"s390x"}, []string{archNoarch}},
	}
	for _, tc := range cases {
		if got := archesOf(filterArches(all(), tc.arches)); !reflect.DeepEqual(got, tc.want) {
			t.Errorf("filterArches(%v) = %v, want %v", tc.arches, got, tc.want)
		}
	}
}

func TestParsePrimaryBytes(t *testing.T) {
	repo := testrepo.RPMRepo{Packages: []testrepo.RPMPackage{pkgAlpha, pkgGamma}}
	plain := repo.PrimaryXML()
	cases := []struct {
		name, compression, ext string
	}{
		{"gzip by extension", testrepo.Gz, extGz},
		{"xz by extension", testrepo.Xz, extXz},
		{"zstd by extension", testrepo.Zst, extZst},
		{"plain", testrepo.None, ""},
		{"gzip by magic", testrepo.Gz, ""},
		{"xz by magic", testrepo.Xz, ""},
		{"zstd by magic", testrepo.Zst, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			data := testrepo.Compress(t, plain, tc.compression)
			pkgs, err := parsePrimaryBytes(data, tc.ext, "")
			if err != nil {
				t.Fatal(err)
			}
			if len(pkgs) != 2 {
				t.Fatalf("got %d packages, want 2", len(pkgs))
			}
			p := pkgs[0]
			if p.Arch != archX86 || p.Location.Href != pkgAlpha.Href || p.Checksum.Type != "sha256" || p.Checksum.Value != testrepo.SHA256(pkgAlpha.Data) {
				t.Errorf("first package = %+v", p)
			}
		})
	}
	if _, err := parsePrimaryBytes([]byte("not gzip"), extGz, ""); err == nil {
		t.Error("bad gzip accepted")
	}
	if _, err := parsePrimaryBytes([]byte("<metadata><package>"), "", "repo"); err == nil {
		t.Error("truncated XML accepted")
	}
}

func TestParsePrimaryFile(t *testing.T) {
	repo := testrepo.RPMRepo{Packages: []testrepo.RPMPackage{pkgAlpha}}
	dir := t.TempDir()
	for _, ext := range []string{testrepo.Gz, testrepo.Xz, testrepo.Zst, testrepo.None} {
		p := filepath.Join(dir, "primary.xml"+ext)
		if err := os.WriteFile(p, testrepo.Compress(t, repo.PrimaryXML(), ext), 0o600); err != nil {
			t.Fatal(err)
		}
		pkgs, err := parsePrimary(p, fileExt(p), "repo")
		if err != nil || len(pkgs) != 1 {
			t.Errorf("%s: %d packages, err %v", ext, len(pkgs), err)
		}
	}
	if _, err := parsePrimary(filepath.Join(dir, "missing.xml"), "", ""); err == nil {
		t.Error("missing file accepted")
	}
}

func TestResolvePrimaryMetadataMode(t *testing.T) {
	dir := t.TempDir()
	cases := []struct {
		raw        string
		hasDB      bool
		wantMode   string
		wantReason bool
	}{
		{"xml", true, primaryMetaXML, false},
		{" XML ", false, primaryMetaXML, false},
		{"sqlite", true, primaryMetaSQLite, false},
		{"sqlite", false, primaryMetaXML, true},
		{"", false, primaryMetaXML, true},
		{"auto", false, primaryMetaXML, true},
	}
	for _, tc := range cases {
		mode, reason := resolvePrimaryMetadataMode(tc.raw, dir, tc.hasDB)
		if mode != tc.wantMode || (reason != "") != tc.wantReason {
			t.Errorf("resolvePrimaryMetadataMode(%q, %v) = %q, %q", tc.raw, tc.hasDB, mode, reason)
		}
	}
	mode, reason := resolvePrimaryMetadataMode("auto", dir, true)
	if fs, ok := filesystemTypeForPath(dir); ok && strings.Contains(reason, "filesystem=") {
		if mode != primaryMetaXML || reason != "filesystem="+fs {
			t.Errorf("auto on %s = %q, %q", fs, mode, reason)
		}
	} else if mode != primaryMetaSQLite || reason != "" {
		t.Errorf("auto with primary_db = %q, %q, want sqlite", mode, reason)
	}
}

func TestMountHelpers(t *testing.T) {
	cases := []struct {
		path, mount string
		want        bool
	}{
		{"/srv/mirror", "/", true},
		{"/srv/mirror", "/srv", true},
		{"/srv", "/srv", true},
		{"/srvx/mirror", "/srv", false},
		{"/other", "/srv", false},
	}
	for _, tc := range cases {
		if got := pathWithinMount(tc.path, tc.mount); got != tc.want {
			t.Errorf("pathWithinMount(%q, %q) = %v", tc.path, tc.mount, got)
		}
	}
	if got := decodeMountEscapes(`/mnt/my\040drive\134x`); got != `/mnt/my drive\x` {
		t.Errorf("decodeMountEscapes = %q", got)
	}
}

func TestFileExtAndProgressTotal(t *testing.T) {
	for in, want := range map[string]string{
		"repodata/x-primary.xml.GZ": extGz,
		"primary.sqlite.bz2":        extBz2,
		"primary.sqlite.zst":        extZst,
		"primary.sqlite.zstd":       extZstd,
		"primary.xml.xz":            extXz,
		"primary.xml":               "",
	} {
		if got := fileExt(in); got != want {
			t.Errorf("fileExt(%q) = %q, want %q", in, got, want)
		}
	}
	if progressTotalForStream(100, extGz) != -1 || progressTotalForStream(100, "") != 100 {
		t.Error("progressTotalForStream")
	}
	if fileSizeOrUnknown(filepath.Join(t.TempDir(), "none")) != -1 {
		t.Error("fileSizeOrUnknown for a missing file")
	}
}

func TestProgressLines(t *testing.T) {
	line := formatParseProgressLine("repo", "parsing", 500, 1000, 2*time.Second, "|")
	for _, want := range []string{"repo", "parsing...", "(50.0%)", "eta"} {
		if !strings.Contains(line, want) {
			t.Errorf("progress line %q missing %q", line, want)
		}
	}
	if line := formatParseProgressLine("repo", "parsing", 500, -1, time.Second, "|"); !strings.Contains(line, "processed") {
		t.Errorf("unknown total line = %q", line)
	}
	if line := formatParseItemLine("repo", "parsing", 5, 10, time.Second, "/"); !strings.Contains(line, "5/10 (50.0%)") {
		t.Errorf("item line = %q", line)
	}
	if line := formatParseItemLine("repo", "parsing", 5, 0, time.Second, "/"); !strings.Contains(line, "5 processed") {
		t.Errorf("item line without total = %q", line)
	}
}
