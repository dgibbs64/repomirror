package config

import (
	"bytes"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

// Config is the top-level configuration structure read from mirrors.yaml.
type Config struct {
	OutputDir string `yaml:"output_dir"`
	MirrorURL string `yaml:"mirror_url"` // base URL clients use to reach this mirror, e.g. http://mirror.example.com
	// Workers accepts an integer (e.g. 4) or "auto" to use runtime.NumCPU().
	Workers  WorkerCount `yaml:"workers"`
	RPMRepos []RPMRepo   `yaml:"rpm_repos"`
	DEBRepos []DEBRepo   `yaml:"deb_repos"`
}

// WorkerCount supports YAML values like `workers: 4` and `workers: auto`.
type WorkerCount int

func (w *WorkerCount) UnmarshalYAML(value *yaml.Node) error {
	if value.Kind != yaml.ScalarNode {
		return fmt.Errorf("workers must be an integer or 'auto'")
	}
	v := strings.TrimSpace(value.Value)
	if strings.EqualFold(v, "auto") {
		n := runtime.NumCPU()
		if n < 1 {
			n = 1
		}
		*w = WorkerCount(n)
		return nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return fmt.Errorf("workers must be an integer or 'auto', got %q", value.Value)
	}
	*w = WorkerCount(n)
	return nil
}

// RPMRepo describes a YUM/DNF repository to mirror.
type RPMRepo struct {
	Enable          *bool    `yaml:"enable"` // nil or true => enabled, false => skipped
	Name            string   `yaml:"name"`
	Path            string   `yaml:"path"`             // URL path served by the webserver, e.g. rocky/9/BaseOS/x86_64/os
	BaseURL         string   `yaml:"base_url"`         // explicit mirror/base URL; preferred when set
	Mirrorlist      string   `yaml:"mirrorlist"`       // optional URL returning plain-text mirror URLs
	Metalink        string   `yaml:"metalink"`         // optional URL returning metalink XML mirror URLs
	PreferredMirror string   `yaml:"preferred_mirror"` // optional host/substr preferred when using mirrorlist/metalink
	Arches          []string `yaml:"arches"`
	GPGKey          string   `yaml:"gpg_key"` // URL to the GPG public key
}

// DEBRepo describes an APT repository to mirror.
type DEBRepo struct {
	Enable          *bool    `yaml:"enable"` // nil or true => enabled, false => skipped
	Name            string   `yaml:"name"`
	Path            string   `yaml:"path"`             // URL path served by the webserver, e.g. ubuntu
	Mirror          string   `yaml:"mirror"`           // explicit mirror/base URL; preferred when set
	Mirrorlist      string   `yaml:"mirrorlist"`       // optional URL returning plain-text mirror URLs
	Metalink        string   `yaml:"metalink"`         // optional URL returning metalink XML mirror URLs
	PreferredMirror string   `yaml:"preferred_mirror"` // optional host/substr preferred when using mirrorlist/metalink
	Suites          []string `yaml:"suites"`           // e.g. jammy, jammy-updates
	Components      []string `yaml:"components"`       // e.g. main, restricted, universe
	Arches          []string `yaml:"arches"`           // e.g. amd64
	GPGKey          string   `yaml:"gpg_key"`          // URL to the GPG public key
}

// Enabled returns true unless enable is explicitly set to false.
func (r RPMRepo) Enabled() bool {
	return r.Enable == nil || *r.Enable
}

// Enabled returns true unless enable is explicitly set to false.
func (r DEBRepo) Enabled() bool {
	return r.Enable == nil || *r.Enable
}

// Load reads and parses the YAML config file at path.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading config %s: %w", path, err)
	}
	var cfg Config
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	if err := dec.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("parsing config %s: %w", path, err)
	}
	if cfg.OutputDir == "" {
		cfg.OutputDir = "."
	}
	if cfg.Workers <= 0 {
		cfg.Workers = WorkerCount(4)
	}
	if err := validate(&cfg); err != nil {
		return nil, fmt.Errorf("validating config %s: %w", path, err)
	}
	return &cfg, nil
}

func validate(cfg *Config) error {
	for i, r := range cfg.RPMRepos {
		if strings.TrimSpace(r.BaseURL) == "" && strings.TrimSpace(r.Mirrorlist) == "" && strings.TrimSpace(r.Metalink) == "" {
			return fmt.Errorf("rpm_repos[%d] (%s): set at least one source URL (base_url, mirrorlist, or metalink)", i, repoDisplayName(r.Name, r.Path))
		}
	}
	for i, r := range cfg.DEBRepos {
		if strings.TrimSpace(r.Mirror) == "" && strings.TrimSpace(r.Mirrorlist) == "" && strings.TrimSpace(r.Metalink) == "" {
			return fmt.Errorf("deb_repos[%d] (%s): set at least one source URL (mirror, mirrorlist, or metalink)", i, repoDisplayName(r.Name, r.Path))
		}
		if len(r.Suites) == 0 {
			return fmt.Errorf("deb_repos[%d] (%s): suites must not be empty", i, repoDisplayName(r.Name, r.Path))
		}
		if len(r.Components) == 0 {
			return fmt.Errorf("deb_repos[%d] (%s): components must not be empty", i, repoDisplayName(r.Name, r.Path))
		}
		if len(r.Arches) == 0 {
			return fmt.Errorf("deb_repos[%d] (%s): arches must not be empty", i, repoDisplayName(r.Name, r.Path))
		}
	}
	return nil
}

func repoDisplayName(name, path string) string {
	if strings.TrimSpace(name) != "" {
		return name
	}
	if strings.TrimSpace(path) != "" {
		return path
	}
	return "unnamed"
}

// ExampleConfig returns a commented example config YAML string.
func ExampleConfig() string {
	return `# repomirror configuration file
# Place this file (mirrors.yaml) in the same directory as the repomirror binary.
#
# 'output_dir' is the root that your webserver serves.  Each repo's 'path'
# maps directly to the URL path clients will use, e.g.:
#   deb http://example.com/ubuntu jammy main
#   baseurl=http://example.com/rocky/9/BaseOS/x86_64/os/

output_dir: ./mirror              # root served by nginx/apache
mirror_url: http://mirror.example.com  # base URL clients use to reach this server
workers: auto                     # integer (e.g. 4) or auto

rpm_repos:

  # Rocky Linux 9 — two separate repo entries, one per repo path
  - name: rocky-9-baseos
    enable: true
    path: rocky/9/BaseOS/x86_64/os
    base_url: https://dl.rockylinux.org/pub/rocky/9/BaseOS/x86_64/os/
    # Alternative auto-mirror source options (optional):
    # mirrorlist: https://mirrors.example.org/mirrorlist.txt
    # metalink: https://mirrors.example.org/repo.metalink
    # preferred_mirror: dl.rockylinux.org
    gpg_key: https://dl.rockylinux.org/pub/rocky/RPM-GPG-KEY-Rocky-9

  - name: rocky-9-appstream
    path: rocky/9/AppStream/x86_64/os
    base_url: https://dl.rockylinux.org/pub/rocky/9/AppStream/x86_64/os/
    gpg_key: https://dl.rockylinux.org/pub/rocky/RPM-GPG-KEY-Rocky-9

  # CentOS Stream 9
  - name: centos-9stream-baseos
    path: centos/9-stream/BaseOS/x86_64/os
    base_url: https://mirror.stream.centos.org/9-stream/BaseOS/x86_64/os/
    gpg_key: https://www.centos.org/keys/RPM-GPG-KEY-CentOS-Official

  # AlmaLinux 9
  - name: almalinux-9-baseos
    path: almalinux/9/BaseOS/x86_64/os
    base_url: https://repo.almalinux.org/almalinux/9/BaseOS/x86_64/os/
    gpg_key: https://repo.almalinux.org/almalinux/RPM-GPG-KEY-AlmaLinux-9

  # Zabbix 7.0 for Rocky Linux 9
  - name: zabbix-70-rocky9
    path: zabbix/7.0/rhel/9/x86_64
    base_url: https://repo.zabbix.com/zabbix/7.0/rhel/9/x86_64/
    gpg_key: https://repo.zabbix.com/RPM-GPG-KEY-ZABBIX-A14FE591

deb_repos:

  # Ubuntu 22.04 LTS (Jammy)
  # Client config: deb http://example.com/ubuntu jammy main restricted universe
  - name: ubuntu-jammy
    enable: true
    path: ubuntu
    mirror: http://archive.ubuntu.com/ubuntu
    # Optional failover sources:
    # mirrorlist: https://mirrors.example.org/ubuntu.list
    # metalink: https://mirrors.example.org/ubuntu.metalink
    # preferred_mirror: archive.ubuntu.com
    suites:
      - jammy
      - jammy-updates
      - jammy-security
    components:
      - main
      - restricted
      - universe
    arches:
      - amd64
    gpg_key: https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x871920D1991BC93C

  # Zabbix 7.0 for Ubuntu 22.04
  # Client config: deb http://example.com/zabbix/7.0/ubuntu jammy main
  - name: zabbix-70-ubuntu-jammy
    path: zabbix/7.0/ubuntu
    mirror: https://repo.zabbix.com/zabbix/7.0/ubuntu
    suites:
      - jammy
    components:
      - main
    arches:
      - amd64
    gpg_key: https://repo.zabbix.com/zabbix-official-repo.key
`
}
