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
// can use it. It is a no-op if the key file already exists.
func FetchAndImport(keyURL, keysDir string, dl *downloader.Client) error {
	if keyURL == "" {
		return nil
	}

	if err := os.MkdirAll(keysDir, 0o755); err != nil {
		return fmt.Errorf("mkdir %s: %w", keysDir, err)
	}

	// Derive a safe filename from the URL.
	base := filepath.Base(keyURL)
	if base == "" || base == "." {
		base = "gpg.key"
	}
	keyPath := filepath.Join(keysDir, base)

	// Download the key (skips if already present).
	data, err := dl.FetchBytes(keyURL)
	if err != nil {
		return fmt.Errorf("fetching GPG key %s: %w", keyURL, err)
	}
	if err := os.WriteFile(keyPath, data, 0o644); err != nil {
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

// VerifyDetached verifies a file against a detached GPG signature file.
// sigPath is the .gpg or .asc detached signature.
func VerifyDetached(dataPath, sigPath string) error {
	cmd := exec.Command("gpg", "--verify", sigPath, dataPath) // #nosec G204
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("gpg --verify %s %s: %w\n%s", sigPath, dataPath, err, out)
	}
	return nil
}
