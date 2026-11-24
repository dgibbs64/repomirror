package testrepo

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	_ "modernc.org/sqlite" // register sqlite driver
)

// RPMPackage is one package file in an RPM repository.
type RPMPackage struct {
	Href string
	Arch string
	Data []byte
}

// RPMRepo describes a YUM/DNF repository.
type RPMRepo struct {
	Packages []RPMPackage
	// Compression of primary.xml: Gz (the default), Xz, Zst or None.
	Compression string
	// SQLite adds a zstd-compressed primary_db record.
	SQLite bool
	// Key signs repomd.xml with repomd.xml.asc when set.
	Key *Key
}

// RPMRepomd is the path of repomd.xml inside the repository.
const RPMRepomd = "repodata/repomd.xml"

// PrimaryXML returns the uncompressed primary.xml document.
func (r RPMRepo) PrimaryXML() []byte {
	var b strings.Builder
	fmt.Fprintf(&b, "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n")
	fmt.Fprintf(&b, "<metadata xmlns=\"http://linux.duke.edu/metadata/common\" xmlns:rpm=\"http://linux.duke.edu/metadata/rpm\" packages=\"%d\">\n", len(r.Packages))
	for _, p := range r.Packages {
		name := strings.TrimSuffix(filepath.Base(p.Href), ".rpm")
		fmt.Fprintf(&b, "<package type=\"rpm\">\n  <name>%s</name>\n  <arch>%s</arch>\n", name, p.Arch)
		fmt.Fprintf(&b, "  <checksum type=\"sha256\" pkgid=\"YES\">%s</checksum>\n", SHA256(p.Data))
		fmt.Fprintf(&b, "  <size package=\"%d\"/>\n  <location href=\"%s\"/>\n</package>\n", len(p.Data), p.Href)
	}
	b.WriteString("</metadata>\n")
	return []byte(b.String())
}

// Files returns every file of the repository keyed by its path relative to
// the repository root.
func (r RPMRepo) Files(t testing.TB) map[string][]byte {
	t.Helper()
	ext := r.Compression
	if ext == "" {
		ext = Gz
	}
	if ext == None {
		ext = ""
	}
	files := map[string][]byte{}
	var records strings.Builder

	plain := r.PrimaryXML()
	primary := Compress(t, plain, ext)
	primaryHref := "repodata/" + SHA256(primary) + "-primary.xml" + ext
	files[primaryHref] = primary
	writeRecord(&records, "primary", primaryHref, primary, plain)

	if r.SQLite {
		dbPlain := r.primaryDB(t)
		db := Compress(t, dbPlain, Zst)
		dbHref := "repodata/" + SHA256(db) + "-primary.sqlite" + Zst
		files[dbHref] = db
		writeRecord(&records, "primary_db", dbHref, db, dbPlain)
	}

	repomd := []byte("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
		"<repomd xmlns=\"http://linux.duke.edu/metadata/repo\">\n  <revision>1</revision>\n" +
		records.String() + "</repomd>\n")
	files[RPMRepomd] = repomd
	if r.Key != nil {
		files[RPMRepomd+".asc"] = r.Key.DetachSign(t, repomd)
	}
	for _, p := range r.Packages {
		files[p.Href] = p.Data
	}
	return files
}

func writeRecord(b *strings.Builder, kind, href string, data, open []byte) {
	fmt.Fprintf(b, "  <data type=\"%s\">\n", kind)
	fmt.Fprintf(b, "    <checksum type=\"sha256\">%s</checksum>\n", SHA256(data))
	fmt.Fprintf(b, "    <open-checksum type=\"sha256\">%s</open-checksum>\n", SHA256(open))
	fmt.Fprintf(b, "    <location href=\"%s\"/>\n  </data>\n", href)
}

func (r RPMRepo) primaryDB(t testing.TB) []byte {
	t.Helper()
	p := filepath.Join(t.TempDir(), "primary.sqlite")
	db, err := sql.Open("sqlite", p)
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	if _, err = db.Exec(`CREATE TABLE packages (pkgKey INTEGER PRIMARY KEY, pkgId TEXT, name TEXT, arch TEXT, checksum_type TEXT, location_href TEXT)`); err != nil {
		t.Fatalf("create table: %v", err)
	}
	for _, pkg := range r.Packages {
		name := strings.TrimSuffix(filepath.Base(pkg.Href), ".rpm")
		if _, err = db.Exec(`INSERT INTO packages (pkgId, name, arch, checksum_type, location_href) VALUES (?, ?, ?, 'sha256', ?)`,
			SHA256(pkg.Data), name, pkg.Arch, pkg.Href); err != nil {
			t.Fatalf("insert: %v", err)
		}
	}
	if err = db.Close(); err != nil {
		t.Fatalf("close sqlite: %v", err)
	}
	data, err := os.ReadFile(p) // #nosec G304
	if err != nil {
		t.Fatalf("read sqlite: %v", err)
	}
	return data
}
