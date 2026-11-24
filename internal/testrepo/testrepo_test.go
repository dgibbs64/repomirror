package testrepo

import (
	"bytes"
	"io"
	"net/http"
	"strings"
	"testing"
)

func get(t *testing.T, url, rangeHeader string) (int, []byte, error) {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	if rangeHeader != "" {
		req.Header.Set("Range", rangeHeader)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer func() {
		if cerr := resp.Body.Close(); cerr != nil {
			t.Log(cerr)
		}
	}()
	body, err := io.ReadAll(resp.Body)
	return resp.StatusCode, body, err
}

func mustGet(t *testing.T, url, rangeHeader string) (int, []byte) {
	t.Helper()
	code, body, err := get(t, url, rangeHeader)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	return code, body
}

const filePath = "repo/a/file"

func TestServerFaults(t *testing.T) {
	s := NewServer(t)
	data := []byte("0123456789")
	s.Put(filePath, data)
	url := s.URL + "/" + filePath

	if code, body := mustGet(t, url, ""); code != http.StatusOK || !bytes.Equal(body, data) {
		t.Fatalf("plain get: %d %q", code, body)
	}
	if code, body := mustGet(t, url, "bytes=4-"); code != http.StatusPartialContent || string(body) != "456789" {
		t.Fatalf("range get: %d %q", code, body)
	}
	if got := s.Ranges(filePath); len(got) != 1 || got[0] != "bytes=4-" {
		t.Fatalf("ranges = %v", got)
	}
	s.SetFault(filePath, FaultCorrupt)
	if _, body := mustGet(t, url, ""); bytes.Equal(body, data) || len(body) != len(data) {
		t.Fatalf("corrupt body = %q", body)
	}
	s.SetFault(filePath, FaultServerError)
	if code, _ := mustGet(t, url, ""); code != http.StatusInternalServerError {
		t.Fatalf("server error code = %d", code)
	}
	s.SetFault(filePath, FaultMissing)
	if code, _ := mustGet(t, url, ""); code != http.StatusNotFound {
		t.Fatalf("missing code = %d", code)
	}
	s.SetFault(filePath, FaultTruncate)
	if _, body, err := get(t, url, ""); err == nil {
		t.Fatalf("truncated read succeeded with %q", body)
	}
	s.SetFault(filePath, FaultNone)
	if got := s.Hits(filePath); got != 6 {
		t.Fatalf("hits = %d", got)
	}
}

func TestServerTree(t *testing.T) {
	s := NewServer(t)
	s.Publish("repo", map[string][]byte{"a/file": []byte("a"), "b": []byte("x")})
	s.Publish("repo", map[string][]byte{"c": []byte("y")})
	if code, _ := mustGet(t, s.URL+"/repo/b", ""); code != http.StatusNotFound {
		t.Fatalf("republished tree still serves old file: %d", code)
	}
	if code, body := mustGet(t, s.URL+"/repo/c", ""); code != http.StatusOK || string(body) != "y" || string(s.Body("repo/c")) != "y" {
		t.Fatalf("new file: %d %q", code, body)
	}
	s.Put("/other", []byte("z"))
	s.Delete("other")
	if code, _ := mustGet(t, s.URL+"/other", ""); code != http.StatusNotFound {
		t.Fatalf("deleted file served: %d", code)
	}
	if got := s.TotalHits("repo"); got != 2 {
		t.Fatalf("total hits = %d", got)
	}
}

func TestRepoBuilders(t *testing.T) {
	key := NewKey(t, "builder")
	rpm := RPMRepo{
		Packages: []RPMPackage{{Href: "Packages/a.rpm", Arch: "noarch", Data: []byte("a")}},
		SQLite:   true,
		Key:      key,
	}
	files := rpm.Files(t)
	if _, ok := files[RPMRepomd+".asc"]; !ok {
		t.Fatal("repomd.xml.asc missing")
	}
	if !strings.Contains(string(files[RPMRepomd]), "primary_db") {
		t.Fatal("primary_db record missing")
	}

	deb := DEBRepo{
		Suite: "stable", Component: "main", Arches: []string{"amd64"},
		Packages:     []DEBPackage{{Name: "hello", Arch: "amd64", Data: []byte("h")}},
		Compressions: []string{Gz, Xz, None},
		ByHash:       true,
		Key:          key,
	}
	files = deb.Files(t)
	for _, p := range []string{"dists/stable/InRelease", "dists/stable/Release.gpg", "dists/stable/main/binary-amd64/Packages.xz", "pool/main/h/hello/hello_1.0_amd64.deb"} {
		if _, ok := files[p]; !ok {
			t.Errorf("%s missing", p)
		}
	}
	if got := ByHashPath("Packages", "SHA256", "abc"); got != "by-hash/SHA256/abc" {
		t.Errorf("flat by-hash path = %s", got)
	}
	deb.Flat = true
	if _, ok := deb.Files(t)["Packages.gz"]; !ok {
		t.Error("flat Packages.gz missing")
	}
}
