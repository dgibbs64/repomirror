// Package gpg handles downloading and importing GPG public keys used to verify
// repository metadata signatures.
package gpg

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"

	"repomirror/downloader"
)

// FetchAndImport downloads the GPG key at keyURL, saves it to keysDir, and
// imports it into the system GPG keyring so that signature verification tools
// can use it. Skips the network fetch if the key file already exists.
func FetchAndImport(keyURL, keysDir string, dl *downloader.Client) error {
	if keyURL == "" {
		return nil
	}

	if err := os.MkdirAll(keysDir, 0o755); err != nil {
		return fmt.Errorf("mkdir %s: %w", keysDir, err)
	}

	base := filepath.Base(keyURL)
	if base == "" || base == "." {
		base = "gpg.key"
	}
	keyPath := filepath.Join(keysDir, base)

	if _, err := os.Stat(keyPath); err == nil {
		return importKey(keyPath)
	}

	data, err := dl.FetchBytes(keyURL)
	if err != nil {
		return fmt.Errorf("fetching GPG key %s: %w", keyURL, err)
	}
	if err := os.WriteFile(keyPath, data, 0o644); err != nil { //nolint:gosec
		return fmt.Errorf("writing GPG key %s: %w", keyPath, err)
	}

	return importKey(keyPath)
}

// importKey imports a GPG public key file into the current user's keyring.
func importKey(keyPath string) error {
	cmd := exec.Command("gpg", "--import", keyPath) // #nosec G204 – path constructed internally
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("gpg --import %s: %w\n%s", keyPath, err, out)
	}
	return nil
}
