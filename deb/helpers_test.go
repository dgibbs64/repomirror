package deb

import (
	"bytes"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"repomirror/internal/testrepo"
)

func TestParseReleaseMetadata(t *testing.T) {
	text := strings.Join([]string{
		"Origin: Test",
		"Acquire-By-Hash: yes",
		"MD5Sum:",
		" 11111111111111111111111111111111 10 main/binary-amd64/Packages",
		" 22222222222222222222222222222222 20 main/binary-amd64/Packages.gz",
		" short line",
		"SHA1: ",
		" aaaa 10 main/binary-amd64/Packages",
		"SHA256:\r",
		" bbbb 10 main/binary-amd64/Packages\r",
		" cccc 20 main/binary-amd64/Packages.gz",
		"Description: resets the section",
		" dddd 30 main/ignored",
		"SHA512:",
		" eeee 10 main/binary-amd64/Packages",
		"",
	}, "\n")
	got := parseReleaseMetadata([]byte(text))
	if !got.acquireByHash {
		t.Error("Acquire-By-Hash not detected")
	}
	want := map[string]map[string]string{
		"main/binary-amd64/Packages": {
			algoMD5: "11111111111111111111111111111111", algoSHA1: "aaaa", algoSHA256: "bbbb", algoSHA512: "eeee",
		},
		"main/binary-amd64/Packages.gz": {algoMD5: "22222222222222222222222222222222", algoSHA256: "cccc"},
	}
	if !reflect.DeepEqual(got.files, want) {
		t.Errorf("files = %v\nwant %v", got.files, want)
	}
	if !got.hasAny("main/binary-amd64/Packages", packagesVariants) || got.hasAny("main/binary-arm64/Packages", packagesVariants) {
		t.Error("hasAny")
	}
	if empty := parseReleaseMetadata(nil); empty.acquireByHash || len(empty.files) != 0 {
		t.Errorf("empty input = %+v", empty)
	}
}

func TestBestSum(t *testing.T) {
	cases := []struct {
		sums             map[string]string
		wantAlgo, wantSu string
	}{
		{map[string]string{algoMD5: "m", algoSHA256: "s2", algoSHA1: "s1"}, algoSHA256, "s2"},
		{map[string]string{algoMD5: "m", algoSHA512: "s5"}, algoSHA512, "s5"},
		{map[string]string{algoMD5: "m"}, algoMD5, "m"},
		{map[string]string{"crc32": "x"}, "", ""},
		{nil, "", ""},
	}
	for _, tc := range cases {
		if a, s := bestSum(tc.sums); a != tc.wantAlgo || s != tc.wantSu {
			t.Errorf("bestSum(%v) = %s %s", tc.sums, a, s)
		}
	}
}

func TestByHashRel(t *testing.T) {
	cases := []struct {
		relBase, name, algo, want string
	}{
		{"dists/stable/", "main/binary-amd64/Packages.gz", algoSHA256, "dists/stable/main/binary-amd64/by-hash/SHA256/abc"},
		{"dists/stable/", "main/i18n/Translation-en", algoSHA512, "dists/stable/main/i18n/by-hash/SHA512/abc"},
		{"dists/stable/", "Contents-amd64", algoSHA1, "dists/stable/by-hash/SHA1/abc"},
		{"", "Packages.xz", algoMD5, "by-hash/MD5Sum/abc"},
		{"dists/stable/", "main/binary-amd64/Packages", "crc32", "dists/stable/main/binary-amd64/Packages"},
	}
	for _, tc := range cases {
		if got := byHashRel(tc.relBase, tc.name, tc.algo, "abc"); got != tc.want {
			t.Errorf("byHashRel(%q, %q, %q) = %q, want %q", tc.relBase, tc.name, tc.algo, got, tc.want)
		}
	}
	if dir, ok := byHashDirName("SHA256"); !ok || dir != "SHA256" {
		t.Errorf("byHashDirName is not case insensitive: %q %v", dir, ok)
	}
}

const packagesText = `Package: zeta
Architecture: amd64
Filename: pool/main/z/zeta/zeta_1_amd64.deb
MD5sum: m1
SHA1: s1
SHA256: s256
SHA512: s512

Package: alpha
Architecture: arm64
Filename: pool/main/a/alpha/alpha_1_arm64.deb
MD5sum: m2

Package: common
Architecture: all
Filename: pool/main/c/common/common_1_all.deb
SHA256: c256
Package-Without-Blank-Line-At-End: yes

Package: nofile
Architecture: amd64
SHA256: n256
`

func TestParsePackages(t *testing.T) {
	plain := []byte(packagesText)
	all := []debPkg{
		{filename: "pool/main/a/alpha/alpha_1_arm64.deb", algo: algoMD5, sum: "m2"},
		{filename: "pool/main/c/common/common_1_all.deb", algo: algoSHA256, sum: "c256"},
		{filename: "pool/main/z/zeta/zeta_1_amd64.deb", algo: algoSHA512, sum: "s512"},
	}
	for _, ext := range []string{testrepo.None, testrepo.Gz, testrepo.Xz} {
		got, err := parsePackages(bytes.NewReader(testrepo.Compress(t, plain, ext)), ext, nil)
		if err != nil {
			t.Fatalf("%q: %v", ext, err)
		}
		if !reflect.DeepEqual(got, all) {
			t.Errorf("%q: got %+v", ext, got)
		}
	}
	got, err := parsePackages(strings.NewReader(packagesText), "", []string{archAMD})
	if err != nil {
		t.Fatal(err)
	}
	if want := []debPkg{all[1], all[2]}; !reflect.DeepEqual(got, want) {
		t.Errorf("amd64 filter = %+v, want %+v", got, want)
	}
	for _, ext := range []string{testrepo.Gz, testrepo.Xz} {
		if _, err := parsePackages(strings.NewReader("garbage"), ext, nil); err == nil {
			t.Errorf("%s: garbage accepted", ext)
		}
	}
	if pkgs, err := parsePackages(strings.NewReader("garbage"), extBz2, nil); err == nil && len(pkgs) != 0 {
		t.Errorf("bz2 garbage produced %v", pkgs)
	}
}

func TestSmallHelpers(t *testing.T) {
	if !contains([]string{"a", "b"}, "b") || contains(nil, "a") {
		t.Error("contains")
	}
	if formatListForLog(nil) != "-" || formatListForLog([]string{"a", "b"}) != "a,b" {
		t.Error("formatListForLog")
	}
	plan := &suitePlan{}
	if got := stagedOrDest(plan, "/x"); got != "/x" {
		t.Errorf("stagedOrDest = %q", got)
	}
}

func TestEnsureCopy(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "Packages.gz")
	data := []byte("index")
	if err := os.WriteFile(src, data, 0o600); err != nil {
		t.Fatal(err)
	}
	dst := filepath.Join(dir, "by-hash", "SHA256", testrepo.SHA256(data))
	bh := byHashCopy{src: src, dst: dst, algo: algoSHA256, sum: testrepo.SHA256(data)}
	for i := 0; i < 2; i++ {
		if err := ensureCopy(bh); err != nil {
			t.Fatal(err)
		}
	}
	if got, err := os.ReadFile(dst); err != nil || !bytes.Equal(got, data) {
		t.Fatalf("by-hash copy = %q, %v", got, err)
	}
	copied := filepath.Join(dir, "copy")
	if err := copyFile(src, copied); err != nil {
		t.Fatal(err)
	}
	if err := copyFile(filepath.Join(dir, "missing"), copied); err == nil {
		t.Error("copyFile from a missing file succeeded")
	}
	bh.src = filepath.Join(dir, "missing")
	bh.dst = filepath.Join(dir, "by-hash", "SHA256", "other")
	if err := ensureCopy(bh); err == nil {
		t.Error("ensureCopy from a missing file succeeded")
	}
}
