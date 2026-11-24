package config

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func writeConfig(t *testing.T, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "mirrors.yml")
	if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestLoadExampleConfig(t *testing.T) {
	cfg, err := Load(writeConfig(t, ExampleConfig()))
	if err != nil {
		t.Fatal(err)
	}
	if int(cfg.Workers) != max(runtime.NumCPU(), 1) {
		t.Errorf("workers = %d", cfg.Workers)
	}
	if !cfg.Prune || !cfg.SignaturesEnabled() || cfg.OutputDir != "./mirror" || cfg.RPMPrimaryMetadata != "auto" {
		t.Errorf("unexpected settings: %+v", cfg)
	}
	if len(cfg.RPMRepos) != 5 || len(cfg.DEBRepos) != 2 {
		t.Errorf("repos = %d rpm, %d deb", len(cfg.RPMRepos), len(cfg.DEBRepos))
	}
	if !cfg.RPMRepos[0].Enabled() || !cfg.DEBRepos[0].Enabled() {
		t.Error("example repos should be enabled")
	}
}

func TestLoadDefaults(t *testing.T) {
	cfg, err := Load(writeConfig(t, "rpm_repos: []\n"))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.OutputDir != "." || cfg.Workers != 4 || cfg.RPMPrimaryMetadata != "auto" || !cfg.SignaturesEnabled() || cfg.RateLimit != 0 {
		t.Errorf("defaults = %+v", cfg)
	}
}

func TestLoadOverrides(t *testing.T) {
	cfg, err := Load(writeConfig(t, `
workers: 7
verify_signatures: false
rate_limit: 1.5M
rpm_primary_metadata: XML
rpm_repos:
  - name: r
    enable: false
    verify_signatures: true
    metalink: https://m.example/metalink
deb_repos:
  - name: flat
    mirrorlist: https://m.example/list
    verify_signatures: false
    suites: ["/"]
`))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Workers != 7 || cfg.SignaturesEnabled() || cfg.RateLimit != ByteRate(1.5*1024*1024) {
		t.Errorf("settings = %+v", cfg)
	}
	r, d := cfg.RPMRepos[0], cfg.DEBRepos[0]
	if r.Enabled() || !r.Verify(false) || d.Verify(true) || !d.Enabled() {
		t.Error("per-repo overrides not applied")
	}
	if !(DEBRepo{}).Verify(true) || (RPMRepo{}).Verify(false) {
		t.Error("repos without an override must follow the global setting")
	}
}

func TestLoadErrors(t *testing.T) {
	cases := map[string]string{
		"workers: many\n":                    "workers must be an integer or 'auto'",
		"workers: [1]\n":                     "workers must be an integer or 'auto'",
		"rate_limit: fast\n":                 "rate_limit must be a number",
		"rate_limit: [1]\n":                  "rate_limit must be a number",
		"rpm_primary_metadata: json\n":       "rpm_primary_metadata must be one of",
		"unknown_key: 1\n":                   "field unknown_key not found",
		"rpm_repos:\n  - name: r\n":          "rpm_repos[0] (r): set at least one source URL",
		"rpm_repos:\n  - path: a/b\n":        "rpm_repos[0] (a/b): set at least one source URL",
		"deb_repos:\n  - mirror: http://m\n": "deb_repos[0] (unnamed): suites must not be empty",
		"deb_repos:\n  - name: d\n":          "deb_repos[0] (d): set at least one source URL",
		"deb_repos:\n  - name: d\n    mirror: http://m\n    suites: [s]\n":                         "components must not be empty",
		"deb_repos:\n  - name: d\n    mirror: http://m\n    suites: [s]\n    components: [main]\n": "arches must not be empty",
	}
	for body, want := range cases {
		_, err := Load(writeConfig(t, body))
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%q: err = %v, want %q", body, err, want)
		}
	}
	if _, err := Load(filepath.Join(t.TempDir(), "missing.yml")); err == nil || !strings.Contains(err.Error(), "reading config") {
		t.Errorf("missing file err = %v", err)
	}
}

func TestParseByteRate(t *testing.T) {
	cases := []struct {
		in   string
		want int64
	}{
		{"", 0},
		{"0", 0},
		{"512", 512},
		{"750K", 750 * 1024},
		{"10M", 10 * 1024 * 1024},
		{"10mb/s", 10 * 1024 * 1024},
		{"1.5G", 3 * 512 * 1024 * 1024},
		{"2MiB", 2 * 1024 * 1024},
		{" 100kb ", 100 * 1024},
	}
	for _, tc := range cases {
		got, err := ParseByteRate(tc.in)
		if err != nil || got != tc.want {
			t.Errorf("ParseByteRate(%q) = %d, %v; want %d", tc.in, got, err, tc.want)
		}
	}
	for _, bad := range []string{"-1", "M", "ten", "1T"} {
		if _, err := ParseByteRate(bad); err == nil {
			t.Errorf("ParseByteRate(%q) accepted", bad)
		}
	}
}

func TestRepoDisplayName(t *testing.T) {
	if repoDisplayName("n", "p") != "n" || repoDisplayName(" ", "p") != "p" || repoDisplayName("", " ") != "unnamed" {
		t.Error("repoDisplayName")
	}
}
