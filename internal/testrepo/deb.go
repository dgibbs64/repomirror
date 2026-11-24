package testrepo

import (
	"fmt"
	"strings"
	"testing"
)

// ArchAll is the Debian architecture of architecture independent packages.
const ArchAll = "all"

// DEBPackage is one package file in an APT repository.
type DEBPackage struct {
	Name string
	Arch string
	Data []byte
}

// Filename is the pool path of the package relative to the repository root.
func (p DEBPackage) Filename(component string) string {
	return fmt.Sprintf("pool/%s/%s/%s/%s_1.0_%s.deb", component, p.Name[:1], p.Name, p.Name, p.Arch)
}

// DEBRepo describes an APT repository with one suite and one component.
type DEBRepo struct {
	Suite     string
	Component string
	Arches    []string
	Packages  []DEBPackage
	// Compressions lists the Packages variants published, Gz by default.
	Compressions []string
	// ByHash publishes by-hash copies and sets Acquire-By-Hash.
	ByHash bool
	// Flat publishes a flat repository with Release and Packages at the root.
	Flat bool
	// Key signs Release (Release.gpg) and InRelease when set.
	Key *Key
	// NoInRelease leaves out InRelease.
	NoInRelease bool
}

// DistDir is the directory holding the Release files, relative to the
// repository root and with a trailing slash unless the repository is flat.
func (r DEBRepo) DistDir() string {
	if r.Flat {
		return ""
	}
	return "dists/" + r.Suite + "/"
}

// PackagesPath is the path of the uncompressed Packages index for arch,
// relative to DistDir.
func (r DEBRepo) PackagesPath(arch string) string {
	if r.Flat {
		return "Packages"
	}
	return r.Component + "/binary-" + arch + "/Packages"
}

// PackagesText returns the Packages index for arch. A flat repository lists
// every package in one index.
func (r DEBRepo) PackagesText(arch string) []byte {
	var b strings.Builder
	for _, p := range r.Packages {
		if !r.Flat && p.Arch != arch && p.Arch != ArchAll {
			continue
		}
		fmt.Fprintf(&b, "Package: %s\nVersion: 1.0\nArchitecture: %s\n", p.Name, p.Arch)
		fmt.Fprintf(&b, "Filename: %s\nSize: %d\n", p.Filename(r.Component), len(p.Data))
		fmt.Fprintf(&b, "MD5sum: %s\nSHA256: %s\n\n", MD5(p.Data), SHA256(p.Data))
	}
	return []byte(b.String())
}

func (r DEBRepo) indexArches() []string {
	if r.Flat {
		return []string{""}
	}
	return r.Arches
}

// Release returns the unsigned Release text and the index files it lists,
// keyed by path relative to DistDir.
func (r DEBRepo) Release(t testing.TB) ([]byte, map[string][]byte) {
	t.Helper()
	exts := r.Compressions
	if len(exts) == 0 {
		exts = []string{Gz}
	}
	indexes := map[string][]byte{}
	var names []string
	for _, arch := range r.indexArches() {
		plain := r.PackagesText(arch)
		for _, ext := range exts {
			name := r.PackagesPath(arch) + ext
			indexes[name] = Compress(t, plain, ext)
			names = append(names, name)
		}
	}
	var b strings.Builder
	fmt.Fprintf(&b, "Origin: repomirror-test\nLabel: repomirror-test\nSuite: %s\nCodename: %s\n", r.Suite, r.Suite)
	fmt.Fprintf(&b, "Date: Thu, 01 Jan 2026 00:00:00 UTC\nArchitectures: %s\nComponents: %s\n", strings.Join(r.Arches, " "), r.Component)
	if r.ByHash {
		b.WriteString("Acquire-By-Hash: yes\n")
	}
	b.WriteString("MD5Sum:\n")
	for _, n := range names {
		fmt.Fprintf(&b, " %s %d %s\n", MD5(indexes[n]), len(indexes[n]), n)
	}
	b.WriteString("SHA256:\n")
	for _, n := range names {
		fmt.Fprintf(&b, " %s %d %s\n", SHA256(indexes[n]), len(indexes[n]), n)
	}
	return []byte(b.String()), indexes
}

// Files returns every file of the repository keyed by its path relative to
// the repository root.
func (r DEBRepo) Files(t testing.TB) map[string][]byte {
	t.Helper()
	release, indexes := r.Release(t)
	dist := r.DistDir()
	files := map[string][]byte{dist + "Release": release}
	if r.Key != nil {
		files[dist+"Release.gpg"] = r.Key.DetachSign(t, release)
		if !r.NoInRelease {
			files[dist+"InRelease"] = r.Key.Clearsign(t, release)
		}
	}
	for name, data := range indexes {
		files[dist+name] = data
		if r.ByHash {
			files[dist+ByHashPath(name, "SHA256", SHA256(data))] = data
			files[dist+ByHashPath(name, "MD5Sum", MD5(data))] = data
		}
	}
	for _, p := range r.Packages {
		files[p.Filename(r.Component)] = p.Data
	}
	return files
}

// ByHashPath returns the by-hash location of the index name.
func ByHashPath(name, dir, sum string) string {
	i := strings.LastIndex(name, "/")
	if i < 0 {
		return "by-hash/" + dir + "/" + sum
	}
	return name[:i] + "/by-hash/" + dir + "/" + sum
}
