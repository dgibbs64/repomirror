package gpg

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"repomirror/downloader"
	"repomirror/internal/testrepo"
)

func TestMain(m *testing.M) { os.Exit(testrepo.RunIsolated(m)) }

func TestKeyFileName(t *testing.T) {
	cases := map[string]string{
		"https://example.com/keys/RPM-GPG-KEY-Rocky-9":                   "RPM-GPG-KEY-Rocky-9",
		"https://example.com/repo.key ":                                  "repo.key",
		"https://example.com/":                                           "gpg.key",
		"https://example.com":                                            "gpg.key",
		"https://ks.example/pks/lookup?op=get&search=0x871920D1991BC93C": "lookup-90d666b7",
		"https://example.com/keys/odd name(1).asc":                       "odd_name_1_.asc",
		"::not a url/key.asc":                                            "key.asc",
	}
	for in, want := range cases {
		if got := KeyFileName(in); got != want {
			t.Errorf("KeyFileName(%q) = %q, want %q", in, got, want)
		}
	}
	a := KeyFileName("https://ks.example/pks/lookup?search=0x1")
	b := KeyFileName("https://ks.example/pks/lookup?search=0x2")
	if a == b {
		t.Error("different key queries share a file name")
	}
}

type keyFixture struct {
	srv  *testrepo.Server
	key  *testrepo.Key
	dl   *downloader.Client
	dir  string
	url  string
	path string
}

func newKeyFixture(t *testing.T, body func(k *testrepo.Key) []byte) *keyFixture {
	t.Helper()
	key := testrepo.NewKey(t, "gpg-test")
	srv := testrepo.NewServer(t)
	srv.Put("keys/test.key", body(key))
	dl := downloader.New()
	dl.Retries = 0
	dir := filepath.Join(t.TempDir(), KeysDirName)
	return &keyFixture{srv: srv, key: key, dl: dl, dir: dir, url: srv.URL + "/keys/test.key", path: filepath.Join(dir, "test.key")}
}

func TestFetchArmoredAndBinaryKeys(t *testing.T) {
	bodies := map[string]func(k *testrepo.Key) []byte{
		"armored":             func(k *testrepo.Key) []byte { return k.Armored },
		"binary":              func(k *testrepo.Key) []byte { return k.Binary },
		"leading blank lines": func(k *testrepo.Key) []byte { return append([]byte("\n\n"), k.Armored...) },
	}
	for name, body := range bodies {
		t.Run(name, func(t *testing.T) {
			f := newKeyFixture(t, body)
			kr, p, err := Fetch(f.url, f.dir, f.dl)
			if err != nil || kr == nil || p != f.path {
				t.Fatalf("Fetch = %v, %q, %v", kr, p, err)
			}
			stored, err := os.ReadFile(p)
			if err != nil || !bytes.HasPrefix(stored, []byte("-----BEGIN PGP PUBLIC KEY BLOCK-----")) {
				t.Fatalf("stored key is not armoured: %v", err)
			}
			data := []byte("signed data")
			if err := kr.VerifyDetached(data, f.key.DetachSign(t, data)); err != nil {
				t.Errorf("loaded key cannot verify: %v", err)
			}
		})
	}
}

func TestFetchFallsBackToStoredKey(t *testing.T) {
	f := newKeyFixture(t, func(k *testrepo.Key) []byte { return k.Armored })
	if _, _, err := Fetch(f.url, f.dir, f.dl); err != nil {
		t.Fatal(err)
	}
	f.srv.SetFault("keys/test.key", testrepo.FaultMissing)
	kr, _, err := Fetch(f.url, f.dir, f.dl)
	if err != nil || kr == nil {
		t.Fatalf("fallback to stored key failed: %v", err)
	}
	if err := os.Remove(f.path); err != nil {
		t.Fatal(err)
	}
	if _, _, err := Fetch(f.url, f.dir, f.dl); err == nil || !strings.Contains(err.Error(), "fetching GPG key") {
		t.Fatalf("err = %v, want fetch error", err)
	}
}

func TestFetchEdgeCases(t *testing.T) {
	f := newKeyFixture(t, func(*testrepo.Key) []byte { return []byte("this is not a key") })
	if kr, p, err := Fetch(" ", f.dir, f.dl); kr != nil || p != "" || err != nil {
		t.Errorf("empty URL = %v, %q, %v", kr, p, err)
	}
	if _, _, err := Fetch(f.url, f.dir, f.dl); err == nil || !strings.Contains(err.Error(), "not a usable PGP public key") {
		t.Errorf("garbage key err = %v", err)
	}
	f.srv.Put("keys/test.key", []byte("-----BEGIN PGP PUBLIC KEY BLOCK-----\n\nbroken\n"))
	if _, _, err := Fetch(f.url, f.dir, f.dl); err == nil {
		t.Error("broken armoured key accepted")
	}

	f.srv.Put("keys/test.key", f.key.Armored)
	f.dl.DryRun = true
	if _, _, err := Fetch(f.url, f.dir, f.dl); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(f.dir); !os.IsNotExist(err) {
		t.Error("dry run wrote the key")
	}
}

func TestVerify(t *testing.T) {
	key := testrepo.NewKey(t, "signer")
	other := testrepo.NewKey(t, "other")
	kr, _, err := parseKey(key.Armored)
	if err != nil {
		t.Fatal(err)
	}
	text := []byte("Origin: test\nSuite: stable\n")

	signed, err := kr.VerifyClearsigned(key.Clearsign(t, text))
	if err != nil || !bytes.Equal(bytes.TrimSpace(signed), bytes.TrimSpace(text)) {
		t.Fatalf("VerifyClearsigned = %q, %v", signed, err)
	}
	if _, err := kr.VerifyClearsigned(other.Clearsign(t, text)); err == nil {
		t.Error("clearsigned by another key accepted")
	}
	if _, err := kr.VerifyClearsigned(text); !errors.Is(err, ErrNotSigned) {
		t.Errorf("unsigned err = %v", err)
	}

	for name, sig := range map[string][]byte{"armored": key.DetachSign(t, text), "binary": key.DetachSignBinary(t, text)} {
		if err := kr.VerifyDetached(text, sig); err != nil {
			t.Errorf("%s detached: %v", name, err)
		}
		if err := kr.VerifyDetached(append(text, 'x'), sig); err == nil {
			t.Errorf("%s detached accepted modified data", name)
		}
	}
	if err := kr.VerifyDetached(text, other.DetachSign(t, text)); err == nil {
		t.Error("detached signature by another key accepted")
	}
	if err := kr.VerifyDetached(text, []byte("-----BEGIN PGP SIGNATURE-----\nbroken")); err == nil {
		t.Error("broken armoured signature accepted")
	}
}

func TestClearsignedText(t *testing.T) {
	key := testrepo.NewKey(t, "signer")
	text := []byte("Suite: stable\n")
	if got := ClearsignedText(key.Clearsign(t, text)); !bytes.Equal(bytes.TrimSpace(got), bytes.TrimSpace(text)) {
		t.Errorf("ClearsignedText = %q", got)
	}
	if got := ClearsignedText(text); !bytes.Equal(got, text) {
		t.Errorf("unsigned text changed: %q", got)
	}
}

func TestReadArmoredBlocks(t *testing.T) {
	a := testrepo.NewKey(t, "a")
	b := testrepo.NewKey(t, "b")
	both := append(append([]byte("intro\n"), a.Armored...), b.Armored...)
	entities, err := readArmoredBlocks(both)
	if err != nil || len(entities) != 2 {
		t.Fatalf("got %d entities, %v", len(entities), err)
	}
	if _, err := readArmoredBlocks([]byte("nothing here")); err == nil {
		t.Error("no blocks accepted")
	}
}
