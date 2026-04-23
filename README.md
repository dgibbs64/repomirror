# repomirror

A single static binary that mirrors RPM (YUM/DNF) and DEB (APT) repositories for use in isolated or air-gapped environments.

## Features

- Mirrors RPM and DEB repositories over HTTP/HTTPS
- Resumable downloads with checksum verification
- GPG key download and signature validation
- Incremental sync — skips files already downloaded and verified
- Dry-run mode to preview what would be downloaded
- URL-path-aligned directory layout (mirrors the upstream URL structure)
- Live progress output with speed and ETA
- Sequential per-repo execution with configurable concurrent connections per repo
- Generates ready-to-use client config files (`.repo`, `.list`, `.sources`)
- Single static binary — no runtime dependencies

## Installation

Download the latest binary from [Releases](https://github.com/dgibbs64/repomirror/releases) or build from source:

```bash
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags="-s -w" -o repomirror .
```

## Usage

### Generate an example config

```bash
./repomirror -init
```

This writes an example config file next to the binary. By default, repomirror
accepts either `mirrors.yaml` or `mirrors.yml`.

### Dry run (preview only)

```bash
./repomirror -dry-run
```

Parses repository metadata and shows package counts without downloading anything. Also generates client config files so you can inspect them before running a full sync.

### Mirror repositories

```bash
./repomirror
```

### Flags

| Flag | Default | Description |
|------|---------|-------------|
| `-config` | `mirrors.yaml` or `mirrors.yml` (next to binary) | Path to config file |
| `-init` | | Write example config and exit |
| `-dry-run` | | Preview downloads without writing files |
| `-version` | | Print version and exit |

## Configuration

```yaml
output_dir: ./mirror              # where to write mirrored files
mirror_url: http://mirror.example.com  # base URL clients use to reach this server
workers: 4                        # concurrent connections per repo

rpm_repos:
  - name: centos-9stream-baseos
    enable: true                  # optional; set false to skip this repo
    path: centos/9-stream/BaseOS/x86_64/os   # output directory under output_dir
    base_url: https://mirror.stream.centos.org/9-stream/BaseOS/x86_64/os/ # explicit mirror (always supported)
    # Optional failover sources:
    # mirrorlist: https://mirrors.example.org/centos-baseos.list
    # metalink: https://mirrors.example.org/centos-baseos.metalink
    # preferred_mirror: mirror.stream.centos.org
    gpg_key: https://www.centos.org/keys/RPM-GPG-KEY-CentOS-Official

deb_repos:
  - name: ubuntu-jammy
    enable: true                  # optional; set false to skip this repo
    path: ubuntu
    mirror: http://archive.ubuntu.com/ubuntu # explicit mirror (always supported)
    # Optional failover sources:
    # mirrorlist: https://mirrors.example.org/ubuntu.list
    # metalink: https://mirrors.example.org/ubuntu.metalink
    # preferred_mirror: archive.ubuntu.com
    suites: [jammy, jammy-updates, jammy-security]
    components: [main, restricted, universe]
    arches: [amd64]
    gpg_key: https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x871920D1991BC93C
```

See [mirrors.yaml](mirrors.yaml) for a full working example covering CentOS Stream 9, Ubuntu 22.04, Zabbix 6.0, PostgreSQL 16, and EPEL 9.

## Repo snippets

The [examples/](examples/) directory contains ready-to-paste snippets for common repositories:

| File | Contents |
|------|----------|
| [examples/rpm-repos.yaml](examples/rpm-repos.yaml) | CentOS Stream 9, Rocky Linux 8/9, AlmaLinux 8/9, Fedora 42, EPEL 8/9, Zabbix 6.0/7.0, PostgreSQL 16/17, MySQL 8.0, Docker CE, Elasticsearch/OpenSearch, Microsoft, HashiCorp, Grafana, InfluxDB, Nginx, MariaDB, MongoDB, Kubernetes |
| [examples/deb-repos.yaml](examples/deb-repos.yaml) | Ubuntu 20.04/22.04/24.04/26.04, Debian 11/12, Zabbix 6.0/7.0, PostgreSQL, Docker CE, Elasticsearch, Microsoft, HashiCorp, Grafana, InfluxDB, Nginx, MariaDB, MongoDB, Kubernetes |

Copy the relevant blocks into the `rpm_repos` or `deb_repos` section of your `mirrors.yaml`.

## Directory layout

Files are written under `output_dir` using the `path` field from each repo entry. This means the on-disk structure mirrors the upstream URL path, making it straightforward to serve with any static HTTP server (nginx, Apache, etc.).

Example:

```
mirror/
  centos/9-stream/BaseOS/x86_64/os/
  ubuntu/
  zabbix/6.0/rhel/9/x86_64/
  postgresql/16/redhat/rhel-9-x86_64/
  epel/9/Everything/x86_64/
  client-configs/
    yum.repos.d/
    apt/sources.list.d/
```

## Serving the mirror

Any static HTTP server pointed at `output_dir` works. Example with nginx:

```nginx
server {
    listen 80;
    server_name mirror.example.com;
    root /path/to/mirror;
    autoindex on;
}
```

Set `mirror_url: http://mirror.example.com` in `mirrors.yaml` to match.

## Client configuration files

When `mirror_url` is set, repomirror generates client config files after each run under `output_dir/client-configs/`:

```
client-configs/
  yum.repos.d/
    centos-9stream-baseos.repo      ← drop in /etc/yum.repos.d/
    rocky-9-baseos.repo
    ...
  apt/sources.list.d/
    ubuntu-jammy.list               ← legacy format, all Debian/Ubuntu versions
    ubuntu-jammy.sources            ← deb822 format, Ubuntu 22.04+ / Debian 12+
    ...
```

### RPM clients (RHEL, Rocky, Alma, CentOS, Fedora)

```bash
cp centos-9stream-baseos.repo /etc/yum.repos.d/
dnf makecache
```

### DEB clients — legacy format (Ubuntu 20.04 / Debian 10+)

Each `.list` file includes a `signed-by=` option and a comment with the GPG import command:

```
# Import GPG key: curl -fsSL https://... | gpg --dearmor -o /etc/apt/keyrings/ubuntu-jammy.gpg
deb [signed-by=/etc/apt/keyrings/ubuntu-jammy.gpg] http://mirror.example.com/ubuntu jammy main restricted universe
```

```bash
# 1. Import the GPG key (run the curl command from the comment in the .list file)
curl -fsSL https://... | gpg --dearmor -o /etc/apt/keyrings/ubuntu-jammy.gpg

# 2. Install the source file
cp ubuntu-jammy.list /etc/apt/sources.list.d/
apt update
```

### DEB clients — deb822 format (Ubuntu 22.04+ / Debian 12+)

```bash
# 1. Import the GPG key (command is in the .sources file as a comment)
curl -fsSL https://... | gpg --dearmor -o /etc/apt/keyrings/ubuntu-jammy.gpg

# 2. Install the source file
cp ubuntu-jammy.sources /etc/apt/sources.list.d/
apt update
```

## Building

Requires Go 1.21+.

```bash
go build -o repomirror .
```

For a fully static binary:

```bash
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags="-s -w" -o repomirror .
```

