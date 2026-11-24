package downloader

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"repomirror/internal/testrepo"
)

const pkgPath = "repo/pkg.rpm"

type dlFixture struct {
	srv  *testrepo.Server
	dl   *Client
	data []byte
	sum  string
	url  string
	dest string
}

func newDLFixture(t *testing.T) *dlFixture {
	t.Helper()
	srv := testrepo.NewServer(t)
	data := bytes.Repeat([]byte("0123456789abcdef"), 256)
	srv.Put(pkgPath, data)
	dl := New()
	dl.Retries = 0
	dl.UserAgent = "repomirror-test"
	return &dlFixture{
		srv:  srv,
		dl:   dl,
		data: data,
		sum:  testrepo.SHA256(data),
		url:  srv.URL + "/" + pkgPath,
		dest: filepath.Join(t.TempDir(), "out", "pkg.rpm"),
	}
}

func (f *dlFixture) assertDest(t *testing.T) {
	t.Helper()
	got, err := os.ReadFile(f.dest)
	if err != nil || !bytes.Equal(got, f.data) {
		t.Fatalf("dest has %d bytes, err %v; want %d bytes", len(got), err, len(f.data))
	}
	if _, err := os.Stat(f.dest + PartSuffix); !os.IsNotExist(err) {
		t.Errorf("partial file left behind: %v", err)
	}
}

func TestDownloadFileSkipsMatchingFile(t *testing.T) {
	f := newDLFixture(t)
	if err := f.dl.DownloadFile(f.url, f.dest, "sha256", f.sum); err != nil {
		t.Fatal(err)
	}
	f.assertDest(t)
	if err := f.dl.DownloadFile(f.url, f.dest, "sha256", f.sum); err != nil {
		t.Fatal(err)
	}
	if n := f.srv.Hits(pkgPath); n != 1 {
		t.Errorf("hits = %d, want 1", n)
	}
	if err := f.dl.DownloadFile(f.url, f.dest, "", ""); err != nil {
		t.Fatal(err)
	}
	if n := f.srv.Hits(pkgPath); n != 1 {
		t.Errorf("existing file without checksum was fetched again: %d", n)
	}

	writeFile(t, f.dest, []byte("stale"))
	if err := f.dl.DownloadFile(f.url, f.dest, "sha256", f.sum); err != nil {
		t.Fatal(err)
	}
	f.assertDest(t)
}

func TestDownloadFileErrors(t *testing.T) {
	f := newDLFixture(t)
	f.dl.Retries = 3
	f.srv.SetFault(pkgPath, testrepo.FaultMissing)
	if err := f.dl.DownloadFile(f.url, f.dest, "sha256", f.sum); err == nil || !strings.Contains(err.Error(), "HTTP 404") {
		t.Fatalf("err = %v, want HTTP 404", err)
	}
	if n := f.srv.Hits(pkgPath); n != 1 {
		t.Errorf("404 was retried: %d requests", n)
	}

	f.dl.Retries = 0
	f.srv.SetFault(pkgPath, testrepo.FaultServerError)
	if err := f.dl.DownloadFile(f.url, f.dest, "sha256", f.sum); err == nil || !strings.Contains(err.Error(), "HTTP 500") {
		t.Fatalf("err = %v, want HTTP 500", err)
	}

	f.srv.SetFault(pkgPath, testrepo.FaultCorrupt)
	if err := f.dl.DownloadFile(f.url, f.dest, "sha256", f.sum); err == nil || !strings.Contains(err.Error(), "checksum mismatch") {
		t.Fatalf("err = %v, want checksum mismatch", err)
	}
	for _, p := range []string{f.dest, f.dest + PartSuffix} {
		if _, err := os.Stat(p); !os.IsNotExist(err) {
			t.Errorf("%s left after checksum mismatch", p)
		}
	}

	f.srv.SetFault(pkgPath, testrepo.FaultNone)
	if err := f.dl.DownloadFile(f.url, f.dest, "crc32", "00"); err == nil {
		t.Error("unknown checksum algorithm accepted")
	}
	if err := f.dl.DownloadFile("http://bad host/", f.dest, "", ""); err == nil {
		t.Error("invalid URL accepted")
	}
}

func TestDownloadFileDryRun(t *testing.T) {
	f := newDLFixture(t)
	f.dl.DryRun = true
	if err := f.dl.DownloadFile(f.url, f.dest, "sha256", f.sum); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(f.dest); !os.IsNotExist(err) {
		t.Error("dry run wrote the file")
	}
	if info, err := os.Stat(filepath.Dir(f.dest)); err != nil || !info.IsDir() {
		t.Error("dry run did not create the directory")
	}
	if f.srv.Hits(pkgPath) != 0 {
		t.Error("dry run made a request")
	}
}

func TestDownloadFileResume(t *testing.T) {
	f := newDLFixture(t)
	f.srv.SetFault(pkgPath, testrepo.FaultTruncate)
	prog := NewCounter("repo", "rpm", 1)
	if err := f.dl.DownloadFileP(f.url, f.dest, "sha256", f.sum, prog); err == nil {
		t.Fatal("truncated download succeeded")
	}
	half := int64(len(f.data) / 2)
	if info, err := os.Stat(f.dest + PartSuffix); err != nil || info.Size() != half {
		t.Fatalf("partial file: %v", err)
	}

	f.srv.SetFault(pkgPath, testrepo.FaultNone)
	if err := f.dl.DownloadFileP(f.url, f.dest, "sha256", f.sum, prog); err != nil {
		t.Fatal(err)
	}
	f.assertDest(t)
	if got := f.srv.Ranges(pkgPath); len(got) != 1 || got[0] != "bytes=2048-" {
		t.Errorf("ranges = %v", got)
	}
	if prog.Bytes() != int64(len(f.data)) {
		t.Errorf("counted %d bytes, want %d", prog.Bytes(), len(f.data))
	}
}

func TestDownloadFileIgnoresUntrustedPartial(t *testing.T) {
	f := newDLFixture(t)
	writeFile(t, f.dest+PartSuffix, []byte("leftover from something else"))
	if err := f.dl.DownloadFile(f.url, f.dest, "sha256", f.sum); err != nil {
		t.Fatal(err)
	}
	f.assertDest(t)
	if got := f.srv.Ranges(pkgPath); len(got) != 0 {
		t.Errorf("resumed an untrusted partial file: %v", got)
	}
}

func TestDownloadFileCompletePartial(t *testing.T) {
	f := newDLFixture(t)
	part := f.dest + PartSuffix
	writeFile(t, part, f.data)
	if err := writeResumeMarker(part, f.url); err != nil {
		t.Fatal(err)
	}
	if err := f.dl.DownloadFile(f.url, f.dest, "sha256", f.sum); err != nil {
		t.Fatal(err)
	}
	f.assertDest(t)

	other := f.dest + ".other"
	part = other + PartSuffix
	writeFile(t, part, bytes.Repeat([]byte("x"), len(f.data)))
	if err := writeResumeMarker(part, f.url); err != nil {
		t.Fatal(err)
	}
	if err := f.dl.DownloadFile(f.url, other, "sha256", f.sum); err != nil {
		t.Fatal(err)
	}
	if got, err := os.ReadFile(other); err != nil || !bytes.Equal(got, f.data) {
		t.Fatalf("corrupt complete partial was kept: %v", err)
	}
}

func TestFetchBytes(t *testing.T) {
	f := newDLFixture(t)
	f.dl.SetRateLimit(64 * 1024 * 1024)
	data, err := f.dl.FetchBytes(f.url)
	if err != nil || !bytes.Equal(data, f.data) {
		t.Fatalf("FetchBytes: %v", err)
	}
	f.dl.SetRateLimit(0)
	if f.dl.limiter != nil {
		t.Error("SetRateLimit(0) kept the limiter")
	}
	f.srv.SetFault(pkgPath, testrepo.FaultMissing)
	if _, err := f.dl.FetchBytes(f.url); err == nil || !strings.Contains(err.Error(), "HTTP 404") {
		t.Errorf("err = %v", err)
	}
	f.srv.SetFault(pkgPath, testrepo.FaultServerError)
	if _, err := f.dl.FetchBytes(f.url); err == nil || !strings.Contains(err.Error(), "after 0 retries") {
		t.Errorf("err = %v", err)
	}
	f.srv.SetFault(pkgPath, testrepo.FaultTruncate)
	if _, err := f.dl.FetchBytes(f.url); err == nil {
		t.Error("truncated body accepted")
	}
	if _, err := f.dl.FetchBytes("http://bad host/"); err == nil {
		t.Error("invalid URL accepted")
	}
}
