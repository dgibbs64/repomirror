package downloader

import (
	"encoding/hex"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"repomirror/internal/testrepo"
)

func TestMain(m *testing.M) { os.Exit(testrepo.RunIsolated(m)) }

func TestNormalizeAlgo(t *testing.T) {
	cases := []struct{ in, want string }{
		{"sha", "sha1"},
		{"SHA", "sha1"},
		{"SHA-256", "sha256"},
		{" sha512 ", "sha512"},
		{"MD5Sum", "md5"},
		{"md5", "md5"},
		{"sha-1", "sha1"},
		{"crc32", "crc32"},
		{"", ""},
	}
	for _, tc := range cases {
		if got := NormalizeAlgo(tc.in); got != tc.want {
			t.Errorf("NormalizeAlgo(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestSafeJoin(t *testing.T) {
	root := filepath.Join(t.TempDir(), "mirror")
	cases := []struct {
		rel     string
		want    string
		wantErr bool
	}{
		{rel: "", want: root},
		{rel: ".", want: root},
		{rel: "a/b.rpm", want: filepath.Join(root, "a", "b.rpm")},
		{rel: "a/../b.rpm", want: filepath.Join(root, "b.rpm")},
		{rel: "./a//b/", want: filepath.Join(root, "a", "b")},
		{rel: "..", wantErr: true},
		{rel: "../escape", wantErr: true},
		{rel: "a/../../escape", wantErr: true},
		{rel: "/etc/passwd", wantErr: true},
		{rel: "..foo/bar", want: filepath.Join(root, "..foo", "bar")},
	}
	for _, tc := range cases {
		got, err := SafeJoin(root, tc.rel)
		if (err != nil) != tc.wantErr || got != tc.want {
			t.Errorf("SafeJoin(%q) = %q, %v; want %q, error %v", tc.rel, got, err, tc.want, tc.wantErr)
		}
	}
}

func writeFile(t *testing.T, p string, data []byte) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(p), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, data, 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestFileMatches(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "pkg.rpm")
	data := []byte("package payload")
	writeFile(t, p, data)
	sum := testrepo.SHA256(data)

	cases := []struct {
		name, path, algo, expected string
		want                       bool
	}{
		{"match", p, "sha256", sum, true},
		{"match cached", p, "SHA-256", sum, true},
		{"upper case checksum", p, "sha256", strings.ToUpper(sum), true},
		{"md5", p, "md5sum", testrepo.MD5(data), true},
		{"mismatch", p, "sha256", testrepo.SHA256([]byte("other")), false},
		{"unknown algorithm", p, "crc32", sum, false},
		{"empty checksum", p, "sha256", "", false},
		{"missing file", filepath.Join(dir, "missing"), "sha256", sum, false},
		{"directory", dir, "sha256", sum, false},
	}
	for _, tc := range cases {
		if got := FileMatches(tc.path, tc.algo, tc.expected); got != tc.want {
			t.Errorf("%s: FileMatches = %v, want %v", tc.name, got, tc.want)
		}
	}

	writeFile(t, p, []byte("changed payload, longer than before"))
	if FileMatches(p, "sha256", sum) {
		t.Error("stale checksum cache entry matched a changed file")
	}
}

func TestBytesMatch(t *testing.T) {
	data := []byte("abc")
	if !BytesMatch(data, "sha256", " "+testrepo.SHA256(data)+" ") {
		t.Error("sha256 should match")
	}
	for _, algo := range []string{"sha", "sha224", "sha384", "sha512", "md5"} {
		h, err := newHash(NormalizeAlgo(algo))
		if err != nil {
			t.Fatal(err)
		}
		h.Write(data)
		if !BytesMatch(data, algo, hex.EncodeToString(h.Sum(nil))) || BytesMatch(data, algo, "00") {
			t.Errorf("%s: BytesMatch wrong", algo)
		}
	}
	if BytesMatch(data, "crc32", "00") {
		t.Error("unknown algorithm matched")
	}
}

func TestFmtBytes(t *testing.T) {
	cases := map[float64]string{
		0:             "0 B",
		999:           "999 B",
		1000:          "1.0 KB",
		1500:          "1.5 KB",
		2_500_000:     "2.5 MB",
		3_500_000_000: "3.5 GB",
	}
	for in, want := range cases {
		if got := FmtBytes(in); got != want {
			t.Errorf("FmtBytes(%v) = %q, want %q", in, got, want)
		}
	}
}

func TestFmtDuration(t *testing.T) {
	cases := map[time.Duration]string{
		0:                       "0s",
		1400 * time.Millisecond: "1s",
		1600 * time.Millisecond: "2s",
		59 * time.Second:        "59s",
		61 * time.Second:        "1m01s",
		time.Hour + 2*time.Minute + 3*time.Second: "1h02m03s",
		26 * time.Hour: "26h00m00s",
	}
	for in, want := range cases {
		if got := FmtDuration(in); got != want {
			t.Errorf("FmtDuration(%v) = %q, want %q", in, got, want)
		}
	}
}

func TestIsTempFile(t *testing.T) {
	for _, p := range []string{"a" + PartSuffix, "a" + TempSuffix, "a" + StageSuffix} {
		if !IsTempFile(p) {
			t.Errorf("IsTempFile(%q) = false", p)
		}
	}
	if IsTempFile("a.rpm") || IsTempFile("a.repomirror.partial") {
		t.Error("IsTempFile matched a normal file")
	}
}

func TestWriteFileAtomicAndMovePath(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "sub", "repomd.xml")
	if err := WriteFileAtomic(p, []byte("one")); err != nil {
		t.Fatal(err)
	}
	if err := WriteFileAtomic(p, []byte("two")); err != nil {
		t.Fatal(err)
	}
	if got, err := os.ReadFile(p); err != nil || string(got) != "two" {
		t.Fatalf("content = %q, %v", got, err)
	}
	if _, err := os.Stat(p + TempSuffix); !os.IsNotExist(err) {
		t.Error("temporary file left behind")
	}
	dst := filepath.Join(dir, "moved", "repomd.xml")
	if err := MovePath(p, dst); err != nil {
		t.Fatal(err)
	}
	if !FileMatches(dst, "sha256", testrepo.SHA256([]byte("two"))) {
		t.Error("moved file does not match")
	}
	if err := MovePath(p, dst); err == nil {
		t.Error("moving a missing file succeeded")
	}
	if err := WriteFileAtomic(filepath.Join(dst, "under-a-file"), nil); err == nil {
		t.Error("writing below a regular file succeeded")
	}
}

func TestHTTPStatusError(t *testing.T) {
	cases := map[int]bool{
		http.StatusNotFound:            true,
		http.StatusForbidden:           true,
		http.StatusTooManyRequests:     false,
		http.StatusRequestTimeout:      false,
		http.StatusInternalServerError: false,
		http.StatusBadGateway:          false,
	}
	for code, permanent := range cases {
		err := httpStatusError(code, "http://x/y")
		if got := errors.As(err, new(permanentError)); got != permanent {
			t.Errorf("HTTP %d permanent = %v, want %v", code, got, permanent)
		}
	}
	if retryDelay(1) != 2*time.Second || retryDelay(10) != 32*time.Second {
		t.Error("retryDelay")
	}
}

func TestPathHelpers(t *testing.T) {
	if shortURLForLog("http://h/a/b.rpm") != "b.rpm" || shortURLForLog("") != "" {
		t.Error("shortURLForLog")
	}
	long := "http://h/" + strings.Repeat("x", 120)
	if got := shortURLForLog(long); len(got) != 83 || !strings.Contains(got, "...") {
		t.Errorf("shortURLForLog(long) = %q", got)
	}
	if useTempWritePath("/mnt/c/mirror/a.rpm") || !useTempWritePath("/srv/mirror/a.rpm") {
		t.Error("useTempWritePath")
	}
	einval := &os.PathError{Op: "open", Path: "/mnt/c/x", Err: syscall.EINVAL}
	if !isTransientMntPathError("/mnt/c/x", einval) {
		t.Error("EINVAL on /mnt should be transient")
	}
	if isTransientMntPathError("/srv/x", einval) || isTransientMntPathError("/mnt/c/x", errors.New("plain")) {
		t.Error("only /mnt path errors are transient")
	}
	if !strings.HasSuffix(stateDBPath(), filepath.Join("repomirror", "state", "state.db")) {
		t.Errorf("stateDBPath = %s", stateDBPath())
	}
}

func TestCounter(t *testing.T) {
	c := NewCounter("repo", "rpm", 2)
	c.SetActiveProgress("a.rpm", 5, 10)
	c.AddBytes(10)
	c.Done()
	c.Log()
	c.SetActiveValidation("a.rpm", 10, 10)
	c.Log()
	c.ClearActiveProgress("other")
	c.ClearActiveProgress("a.rpm")
	c.Done()
	c.Log()
	if c.Bytes() != 10 {
		t.Errorf("Bytes = %d", c.Bytes())
	}
	c.Finish()
	NewCounter("repo", "deb", 1).Finish()
	NewCounter("repo", "deb", 0).Finish()

	stop := make(chan struct{})
	c.StartLogger(time.Millisecond, stop)
	time.Sleep(5 * time.Millisecond)
	close(stop)
}

func TestTermHelpers(t *testing.T) {
	colored := termTheme{color: true, unicode: true}
	plain := termTheme{}
	if plain.typeBadge("rpm") != "[RPM]" || plain.icon("rpm") != "pkg" {
		t.Error("plain theme")
	}
	for _, typ := range []string{"rpm", "deb", "other"} {
		if !strings.Contains(stripANSI(colored.typeBadge(typ)), strings.ToUpper(typ)) {
			t.Errorf("badge for %s", typ)
		}
	}
	if colored.icon("deb") == "pkg" {
		t.Error("unicode icon")
	}
	line := colored.paint("1", "repo") + " | " + strings.Repeat("y", 200)
	if got := fitToWidth(line, 80); visualLen(got) > 76 {
		t.Errorf("fitToWidth kept %d columns", visualLen(got))
	}
	if got := fitToWidth(strings.Repeat("z", 200), 30); visualLen(got) != 26 || !strings.HasSuffix(got, "…") {
		t.Errorf("fitToWidth long = %q", got)
	}
	if fitToWidth("short", 0) != "short" || trimVisual("abc", 1) != "abc" {
		t.Error("fitToWidth edge cases")
	}
	t.Setenv("COLUMNS", "99")
	if termWidth() != 99 {
		t.Error("termWidth ignores COLUMNS")
	}
	t.Setenv("NO_COLOR", "1")
	if detectTermTheme().color {
		t.Error("NO_COLOR ignored")
	}
}
