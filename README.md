# repomirror

A single static binary that mirrors RPM (YUM/DNF) and DEB (APT) repositories for use in isolated or air-gapped environments.

## Features

- Mirrors RPM and DEB repositories over HTTP/HTTPS
- Checks repository signatures (`InRelease`, `Release.gpg`, `repomd.xml.asc`) against each repo's `gpg_key`, with no `gpg` binary needed
- Resumable downloads with checksum verification
- Safe updates: new metadata is staged and only published once every package is in place, so clients never see a half-synced repo
- Optional pruning of files upstream no longer lists, and automatic cleanup of interrupted downloads
- Mirrors APT `by-hash` files and `Packages.xz`-only repositories
- RPM architecture filtering (for example, drop `i686` multilib packages)
- Incremental sync: skips files already downloaded and verified
- Dry-run mode to preview downloads and pruning
- URL-path-aligned directory layout (mirrors the upstream URL structure)
- Live progress output with speed and ETA
- Optional download rate limit and custom User-Agent
- Generates ready-to-use client config files (`.repo`, `.list`, `.sources`) that fetch signing keys from the mirror itself
- Single static binary with no runtime dependencies

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
looks for `mirrors.yml` (or `mirrors.yaml`).

### Dry run (preview only)

```bash
./repomirror -dry-run
```

Parses and verifies repository metadata and shows package counts without downloading anything. It also lists what pruning would remove, and generates client config files so you can inspect them before running a full sync.

### Mirror repositories

```bash
./repomirror
```

The exit code is non-zero if any repository, suite, metadata file or package failed. A repository that fails keeps serving its previous metadata.

### Flags

| Flag        | Default                                          | Description                                    |
| ----------- | ------------------------------------------------ | ---------------------------------------------- |
| `-config`   | `mirrors.yml` or `mirrors.yaml` (next to binary) | Path to config file                            |
| `-init`     |                                                  | Write example config and exit                  |
| `-validate` |                                                  | Validate config and exit                       |
| `-dry-run`  |                                                  | Preview downloads and pruning without changes  |
| `-version`  |                                                  | Print version and exit                         |

## Configuration

```yaml
output_dir: ./mirror # where to write mirrored files
mirror_url: http://mirror.example.com # base URL clients use to reach this server
rpm_primary_metadata: auto # global: auto, sqlite, or xml
workers: auto # integer (e.g. 4) or auto
verify_signatures: true # check repo signatures against gpg_key (default true)
prune: true # delete files upstream no longer lists (default false)
# rate_limit: 10M # total download speed cap in bytes/s (K, M, G suffixes)
# user_agent: repomirror/custom # override the HTTP User-Agent

rpm_repos:
  - name: centos-9stream-baseos
    enable: true # optional; set false to skip this repo
    path: centos/9-stream/BaseOS/x86_64/os # output directory under output_dir
    base_url: https://mirror.stream.centos.org/9-stream/BaseOS/x86_64/os/ # explicit mirror (always supported)
    # Optional failover sources:
    # mirrorlist: https://mirrors.example.org/centos-baseos.list
    # metalink: https://mirrors.example.org/centos-baseos.metalink
    # preferred_mirror: mirror.stream.centos.org
    # arches: [x86_64] # optional; only mirror these package arches (noarch is always kept)
    gpg_key: https://www.centos.org/keys/RPM-GPG-KEY-CentOS-Official

deb_repos:
  - name: ubuntu-noble
    enable: true # optional; set false to skip this repo
    path: ubuntu
    mirror: http://archive.ubuntu.com/ubuntu # explicit mirror (always supported)
    # Optional failover sources:
    # mirrorlist: https://mirrors.example.org/ubuntu.list
    # preferred_mirror: archive.ubuntu.com
    suites: [noble, noble-updates, noble-security]
    components: [main, restricted, universe]
    arches: [amd64]
    gpg_key: https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x871920D1991BC93C
```

See [mirrors.yml](mirrors.yml) for a full working example covering CentOS Stream 9, Ubuntu 24.04, Zabbix 7.0, PostgreSQL 18, and EPEL 9.

### Signature checks

With `verify_signatures: true` (the default), each repo's `gpg_key` is downloaded on every run and used to check:

- APT: `InRelease` (or `Release` with `Release.gpg`). A suite whose signature does not match is not mirrored.
- RPM: `repomd.xml.asc` when the upstream repo signs its metadata. Many RPM repos only sign packages; those are logged as "not signed upstream" and rely on the package signatures checked by `dnf` on the client.

A repo without a `gpg_key` is mirrored with a warning. `verify_signatures` can also be set on a single repo entry, which overrides the global setting for that repo. Set it to `false` only to work around a broken upstream signature, as the `elasticsearch-9` RPM example does.

### Pruning and cleanup

Downloads that were interrupted leave `.repomirror.part` files behind, and older versions of repomirror could also leave them after a successful sync. After a repo path syncs without errors, repomirror always removes its leftover `.repomirror.part`, `.repomirror.tmp` and `.repomirror.new` files.

With `prune: true` it also removes every other file under that repo's `path` that the current upstream metadata no longer references (old packages, old `repodata/` files, old `by-hash` entries, renamed keys), and any directories left empty. To stay safe, a path is skipped when:

- the sync for that path had any error,
- any repo entry that shares the path has `enable: false`,
- the path is `output_dir` itself.

Repos nested inside another repo's path are never touched by the outer repo's prune. Run `-dry-run` first to see what would be removed.

## Repo snippets

The [examples/](examples/) directory contains ready-to-paste snippets for common repositories:

| File                                               | Contents |
| -------------------------------------------------- | -------- |
| [examples/rpm-repos.yaml](examples/rpm-repos.yaml) | CentOS Stream 9/10, Rocky Linux 8/9/10, AlmaLinux 8/9/10, Fedora 44/45, EPEL 8/9/10, Zabbix 7.0/8.0, PostgreSQL 17/18, MySQL 8.4, Docker CE, Elasticsearch 8/9, OpenSearch 2/3, Microsoft, HashiCorp, Grafana, InfluxData, Nginx, MariaDB 10.11/11.8, MongoDB 8.0, Kubernetes 1.36/1.37, osquery |
| [examples/deb-repos.yaml](examples/deb-repos.yaml) | Ubuntu 22.04/24.04/26.04, Debian 12/13, Zabbix 7.0/8.0, PostgreSQL, Docker CE, Elasticsearch 8/9, OpenSearch 3, Microsoft, HashiCorp, Grafana, InfluxData, Nginx, MariaDB 10.11/11.8, MongoDB 8.0, Kubernetes 1.36/1.37, osquery |

Copy the relevant blocks into the `rpm_repos` or `deb_repos` section of your `mirrors.yml`.

Repo entries for different releases of the same vendor can share a `path` (for example every Ubuntu release uses `ubuntu`), so the shared `pool/` is only stored once. Keep one entry per release so each gets its own client config file.

## Directory layout

Files are written under `output_dir` using the `path` field from each repo entry. This means the on-disk structure mirrors the upstream URL path, making it straightforward to serve with any static HTTP server (nginx, Apache, etc.).

Example:

```text
mirror/
  centos/9-stream/BaseOS/x86_64/os/
  ubuntu/
    gpg-keys/
  zabbix/7.0/rhel/9/x86_64/
  postgresql/18/redhat/rhel-9-x86_64/
  epel/9/Everything/x86_64/
  client-configs/
    yum.repos.d/
    apt/sources.list.d/
```

Each repo path has a `gpg-keys/` directory with an ASCII-armoured copy of its signing key, which the client configs point at.

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

Set `mirror_url: http://mirror.example.com` in `mirrors.yml` to match.

## Client configuration files

When `mirror_url` is set, repomirror generates client config files after each run under `output_dir/client-configs/`. Files for repos that were removed or disabled are deleted.

```text
client-configs/
  yum.repos.d/
    centos-9stream-baseos.repo      ← drop in /etc/yum.repos.d/
    rocky-9-baseos.repo
    ...
  apt/sources.list.d/
    ubuntu-noble.list               ← one-line format, all Debian/Ubuntu versions
    ubuntu-noble.sources            ← deb822 format, Ubuntu 22.04+ / Debian 12+
    ...
```

All signing keys are fetched from the mirror, so clients do not need internet access.

### RPM clients (RHEL, Rocky, Alma, CentOS, Fedora)

The `.repo` file's `gpgkey=` already points at the mirrored key.

```bash
cp centos-9stream-baseos.repo /etc/yum.repos.d/
dnf makecache
```

### DEB clients

Each `.list` and `.sources` file starts with a comment holding the command that installs its signing key from the mirror:

```text
# Install the signing key: sudo curl -fsSL http://mirror.example.com/ubuntu/gpg-keys/lookup-1a2b3c4d -o /etc/apt/keyrings/ubuntu-noble.asc
Types: deb
URIs: http://mirror.example.com/ubuntu
Suites: noble noble-updates noble-security
Components: main restricted universe
Signed-By: /etc/apt/keyrings/ubuntu-noble.asc
```

```bash
# 1. Install the signing key (run the curl command from the comment)
sudo mkdir -p /etc/apt/keyrings
sudo curl -fsSL http://mirror.example.com/ubuntu/gpg-keys/... -o /etc/apt/keyrings/ubuntu-noble.asc

# 2. Install the source file (.sources on Ubuntu 22.04+ / Debian 12+, .list on older releases)
sudo cp ubuntu-noble.sources /etc/apt/sources.list.d/
sudo apt update
```

## Building

Requires Go 1.26+.

```bash
go build -o repomirror .
```

For a fully static binary:

```bash
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags="-s -w" -o repomirror .
```
