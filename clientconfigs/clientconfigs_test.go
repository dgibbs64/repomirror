package clientconfigs

import (
	"os"
	"path/filepath"
	"testing"

	"repomirror/config"
)

func boolPtr(b bool) *bool { return &b }

func sampleConfig() *config.Config {
	return &config.Config{
		RPMRepos: []config.RPMRepo{
			{Name: "rocky-9-baseos", Path: "/rocky/9/BaseOS/x86_64/os/", GPGKey: "https://dl.example/pub/RPM-GPG-KEY-Rocky-9"},
			{Name: "nokey"},
			{Name: "disabled", Enable: boolPtr(false)},
			{Path: "no/name"},
		},
		DEBRepos: []config.DEBRepo{
			{Name: "ubuntu", Path: "ubuntu", Suites: []string{"jammy", "jammy-updates"}, Components: []string{"main", "universe"},
				GPGKey: "https://ks.example/pks/lookup?op=get&search=0x1"},
			{Name: "flat", Path: "vendor/flat", Suites: []string{"/"}},
		},
	}
}

func read(t *testing.T, p string) string {
	t.Helper()
	data, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

func TestGenerate(t *testing.T) {
	out := t.TempDir()
	if err := Generate(sampleConfig(), out, "http://mirror.example/", true); err != nil {
		t.Fatal(err)
	}
	yum := filepath.Join(out, "client-configs", "yum.repos.d")
	apt := filepath.Join(out, "client-configs", "apt", "sources.list.d")

	want := map[string]string{
		filepath.Join(yum, "rocky-9-baseos.repo"): "[rocky-9-baseos]\nname=rocky-9-baseos\n" +
			"baseurl=http://mirror.example/rocky/9/BaseOS/x86_64/os/\nenabled=1\ngpgcheck=1\n" +
			"gpgkey=http://mirror.example/rocky/9/BaseOS/x86_64/os/gpg-keys/RPM-GPG-KEY-Rocky-9\n",
		filepath.Join(yum, "nokey.repo"): "[nokey]\nname=nokey\nbaseurl=http://mirror.example/nokey/\nenabled=1\ngpgcheck=0\n",
		filepath.Join(apt, "ubuntu.list"): "# Install the signing key: sudo curl -fsSL http://mirror.example/ubuntu/gpg-keys/lookup-3e4f1319 -o /etc/apt/keyrings/ubuntu.asc\n" +
			"deb [signed-by=/etc/apt/keyrings/ubuntu.asc] http://mirror.example/ubuntu jammy main universe\n" +
			"deb [signed-by=/etc/apt/keyrings/ubuntu.asc] http://mirror.example/ubuntu jammy-updates main universe\n",
		filepath.Join(apt, "ubuntu.sources"): "# Install the signing key: sudo curl -fsSL http://mirror.example/ubuntu/gpg-keys/lookup-3e4f1319 -o /etc/apt/keyrings/ubuntu.asc\n" +
			"Types: deb\nURIs: http://mirror.example/ubuntu\nSuites: jammy jammy-updates\nComponents: main universe\nSigned-By: /etc/apt/keyrings/ubuntu.asc\n",
		filepath.Join(apt, "flat.list"):    "deb http://mirror.example/vendor/flat /\n",
		filepath.Join(apt, "flat.sources"): "Types: deb\nURIs: http://mirror.example/vendor/flat\nSuites: /\n",
	}
	for p, body := range want {
		if got := read(t, p); got != body {
			t.Errorf("%s:\n got %q\nwant %q", filepath.Base(p), got, body)
		}
	}
	for _, dir := range []string{yum, apt} {
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatal(err)
		}
		for _, e := range entries {
			if _, ok := want[filepath.Join(dir, e.Name())]; !ok {
				t.Errorf("unexpected file %s", e.Name())
			}
		}
	}
}

func TestGenerateRemovesStaleConfigs(t *testing.T) {
	out := t.TempDir()
	if err := Generate(sampleConfig(), out, "http://m", true); err != nil {
		t.Fatal(err)
	}
	yum := filepath.Join(out, "client-configs", "yum.repos.d")
	apt := filepath.Join(out, "client-configs", "apt", "sources.list.d")
	note := filepath.Join(yum, "README.txt")
	if err := os.WriteFile(note, []byte("keep"), 0o600); err != nil {
		t.Fatal(err)
	}

	cfg := sampleConfig()
	cfg.RPMRepos[1].Enable = boolPtr(false)
	cfg.DEBRepos = cfg.DEBRepos[:1]
	if err := Generate(cfg, out, "http://m", false); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(yum, "nokey.repo")); err != nil {
		t.Error("stale config removed although cleanStale is false")
	}
	if err := Generate(cfg, out, "http://m", true); err != nil {
		t.Fatal(err)
	}
	for _, p := range []string{filepath.Join(yum, "nokey.repo"), filepath.Join(apt, "flat.list"), filepath.Join(apt, "flat.sources")} {
		if _, err := os.Stat(p); !os.IsNotExist(err) {
			t.Errorf("%s not removed", filepath.Base(p))
		}
	}
	for _, p := range []string{note, filepath.Join(yum, "rocky-9-baseos.repo"), filepath.Join(apt, "ubuntu.list")} {
		if _, err := os.Stat(p); err != nil {
			t.Errorf("%s removed", filepath.Base(p))
		}
	}
}

func TestGenerateWithoutMirrorURL(t *testing.T) {
	out := t.TempDir()
	if err := Generate(sampleConfig(), out, "", true); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(out, "client-configs")); !os.IsNotExist(err) {
		t.Error("files written without a mirror URL")
	}
}

func TestGenerateErrors(t *testing.T) {
	out := t.TempDir()
	if err := os.WriteFile(filepath.Join(out, "client-configs"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := Generate(sampleConfig(), out, "http://m", true); err == nil {
		t.Error("Generate succeeded with client-configs blocked by a file")
	}
	cfg := &config.Config{DEBRepos: sampleConfig().DEBRepos}
	if err := Generate(cfg, out, "http://m", true); err == nil {
		t.Error("Generate succeeded with client-configs blocked by a file")
	}
	if err := removeStale(filepath.Join(out, "missing"), nil, ".repo"); err != nil {
		t.Errorf("removeStale on a missing dir: %v", err)
	}
}
