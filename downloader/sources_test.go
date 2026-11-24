package downloader

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"repomirror/internal/testrepo"
)

func TestResolveSourceURLs(t *testing.T) {
	srv := testrepo.NewServer(t)
	srv.Put("mirrorlist", []byte("# comment\n\nhttps://a.example/repo/\nftp://ignored.example/repo\nhttps://b.example/repo\nhttps://a.example/repo\n"))
	srv.Put("metalink", []byte(`<metalink><files><file name="repomd.xml"><resources>
<url protocol="https">https://c.example/repo/repodata/repomd.xml</url>
<url protocol="rsync">rsync://d.example/repo/repodata/repomd.xml</url>
<url protocol="http">http://e.example/deb/InRelease</url>
<url>https://f.example/deb/Release</url>
</resources></file></files></metalink>`))
	srv.Put("badmetalink", []byte("<metalink"))
	dl := New()
	dl.Retries = 0
	ml := srv.URL + "/mirrorlist"
	meta := srv.URL + "/metalink"
	missing := srv.URL + "/missing"

	cases := []struct {
		name                         string
		primary, list, link, prefers string
		want                         []string
		wantErr                      string
	}{
		{name: "primary only", primary: " https://p.example/repo/ ", want: []string{"https://p.example/repo"}},
		{name: "mirrorlist", primary: "https://a.example/repo", list: ml, want: []string{"https://a.example/repo", "https://b.example/repo"}},
		{name: "metalink", link: meta, want: []string{"https://c.example/repo", "http://e.example/deb", "https://f.example/deb"}},
		{name: "preferred", list: ml, prefers: "B.EXAMPLE", want: []string{"https://b.example/repo", "https://a.example/repo"}},
		{name: "mirrorlist fails with primary", primary: "https://p.example", list: missing, want: []string{"https://p.example"}},
		{name: "metalink fails with primary", primary: "https://p.example", link: missing, want: []string{"https://p.example"}},
		{name: "bad metalink with primary", primary: "https://p.example", link: srv.URL + "/badmetalink", want: []string{"https://p.example"}},
		{name: "mirrorlist fails alone", list: missing, wantErr: "fetch mirrorlist"},
		{name: "metalink fails alone", link: missing, wantErr: "fetch metalink"},
		{name: "bad metalink alone", link: srv.URL + "/badmetalink", wantErr: "parse metalink"},
		{name: "nothing", primary: "ftp://x.example", wantErr: "no RPM source URL"},
	}
	for _, tc := range cases {
		got, err := ResolveSourceURLs(tc.primary, tc.list, tc.link, tc.prefers, "RPM", dl)
		if tc.wantErr != "" {
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("%s: err = %v, want %q", tc.name, err, tc.wantErr)
			}
			continue
		}
		if err != nil || !reflect.DeepEqual(got, tc.want) {
			t.Errorf("%s: got %v, %v; want %v", tc.name, got, err, tc.want)
		}
	}
}

func TestSourceSet(t *testing.T) {
	if NewSourceSet(nil).Primary() != "" {
		t.Error("empty set has a primary")
	}
	s := NewSourceSet([]string{"a", "b", "c"})
	s.MarkSuccess("c")
	s.MarkSuccess("unknown")
	s.MarkSuccess("c")
	if s.Primary() != "c" || !reflect.DeepEqual(s.Ordered(), []string{"c", "b", "a"}) {
		t.Errorf("order = %v", s.Ordered())
	}
}

func TestSourcesFailover(t *testing.T) {
	srv := testrepo.NewServer(t)
	data := []byte("payload")
	srv.Put("good/dir/file", data)
	dl := New()
	dl.Retries = 0
	bad, good := srv.URL+"/bad", srv.URL+"/good/"
	ss := NewSourceSet([]string{bad, good})

	got, base, err := FetchBytesFromSources(dl, ss, "/dir/file")
	if err != nil || string(got) != "payload" || base != good {
		t.Fatalf("FetchBytesFromSources = %q, %q, %v", got, base, err)
	}
	if ss.Primary() != good {
		t.Errorf("working source not promoted: %v", ss.Ordered())
	}

	dest := filepath.Join(t.TempDir(), "file")
	ss = NewSourceSet([]string{bad, good})
	if err := DownloadFileFromSources(dl, ss, "dir/file", dest, "sha256", testrepo.SHA256(data), nil); err != nil {
		t.Fatal(err)
	}
	if ss.Primary() != good {
		t.Errorf("working source not promoted: %v", ss.Ordered())
	}

	none := NewSourceSet(nil)
	if _, _, err := FetchBytesFromSources(dl, none, "x"); err == nil {
		t.Error("fetch with no sources succeeded")
	}
	if err := DownloadFileFromSources(dl, none, "x", dest, "", "", nil); err == nil {
		t.Error("download with no sources succeeded")
	}
	if err := DownloadFileFromSources(dl, NewSourceSet([]string{bad}), "dir/file", dest+"2", "", "", nil); err == nil {
		t.Error("download from a failing source succeeded")
	}
}

func TestStageFromSources(t *testing.T) {
	srv := testrepo.NewServer(t)
	data := []byte("metadata v2")
	srv.Put("repo/by-hash/abc", data)
	dl := New()
	dl.Retries = 0
	ss := NewSourceSet([]string{srv.URL + "/repo"})
	dest := filepath.Join(t.TempDir(), "Packages.gz")
	sum := testrepo.SHA256(data)

	p, st, err := StageFromSources(dl, ss, []string{"missing", "by-hash/abc"}, dest, "sha256", sum)
	if err != nil || st == nil || p != dest+StageSuffix {
		t.Fatalf("StageFromSources = %q, %v, %v", p, st, err)
	}
	if _, err = os.Stat(dest); !os.IsNotExist(err) {
		t.Fatal("staging touched the destination")
	}
	if err = st.Publish(); err != nil {
		t.Fatal(err)
	}
	p, st, err = StageFromSources(dl, ss, []string{"by-hash/abc"}, dest, "sha256", sum)
	if err != nil || st != nil || p != dest {
		t.Fatalf("up-to-date file restaged: %q, %v, %v", p, st, err)
	}

	writeFile(t, dest+StageSuffix, []byte("stale staged copy"))
	if _, st, err = StageFromSources(dl, ss, []string{"by-hash/abc"}, dest, "", ""); err != nil || st == nil {
		t.Fatalf("unchecked file not refetched: %v", err)
	}
	got, err := os.ReadFile(st.Tmp)
	if err != nil || string(got) != string(data) {
		t.Errorf("staged copy = %q, %v", got, err)
	}
	if _, _, err = StageFromSources(dl, ss, nil, dest+"x", "sha256", sum); err == nil {
		t.Error("staging with no paths succeeded")
	}
	if _, _, err = StageFromSources(dl, ss, []string{"missing"}, dest+"x", "sha256", sum); err == nil {
		t.Error("staging a missing file succeeded")
	}
}

func TestMetalinkBaseURL(t *testing.T) {
	cases := []struct{ in, want string }{
		{"https://m/repo/repodata/repomd.xml", "https://m/repo"},
		{" https://m/deb/InRelease ", "https://m/deb"},
		{"https://m/deb/Release", "https://m/deb"},
		{"https://m/other", "https://m/other"},
	}
	for _, tc := range cases {
		if got := metalinkBaseURL(tc.in); got != tc.want {
			t.Errorf("metalinkBaseURL(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}
