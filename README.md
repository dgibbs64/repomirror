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

This writes an example `mirrors.yaml` next to the binary.

### Dry run (preview only)

```bash
./repomirror -dry-run
```

Parses repository metadata and shows package counts without downloading anything.

### Mirror repositories

```bash
./repomirror
```

### Flags

| Flag | Default | Description |
|------|---------|-------------|
| `-config` | `mirrors.yaml` (next to binary) | Path to config file |
| `-init` | | Write example config and exit |
| `-dry-run` | | Preview downloads without writing files |
| `-version` | | Print version and exit |

## Configuration

```yaml
output_dir: ./mirror   # where to write mirrored files
workers: 4             # concurrent connections per repo

rpm_repos:
  - name: centos-9stream-baseos
    path: centos/9-stream/BaseOS/x86_64/os   # output directory path
    base_url: https://mirror.stream.centos.org/9-stream/BaseOS/x86_64/os/
    gpg_key: https://www.centos.org/keys/RPM-GPG-KEY-CentOS-Official

deb_repos:
  - name: ubuntu-jammy
    path: ubuntu
    mirror: http://archive.ubuntu.com/ubuntu
    suites: [jammy, jammy-updates, jammy-security]
    components: [main, restricted, universe]
    arches: [amd64]
    gpg_key: https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x871920D1991BC93C
```

See [mirrors.yaml](mirrors.yaml) for a full example covering CentOS Stream 9, Ubuntu 22.04, Zabbix 6.0, PostgreSQL 16, and EPEL 9.

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

Then configure clients to use `http://mirror.example.com` as the repository base URL.

## Building

Requires Go 1.21+.

```bash
go build -o repomirror .
```

For a fully static binary:

```bash
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags="-s -w" -o repomirror .
```
