// Package clientconfigs generates repository client configuration files
// (yum .repo files, apt sources.list files, and apt deb822 .sources files)
// that point at the local mirror.
package clientconfigs

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"repomirror/config"
)

// Generate writes client config files under outputDir/client-configs/.
//
//	outputDir/client-configs/yum.repos.d/<name>.repo              — one per RPM repo
//	outputDir/client-configs/apt/sources.list.d/<name>.list       — one per DEB repo (legacy format)
//	outputDir/client-configs/apt/sources.list.d/<name>.sources    — one per DEB repo (deb822 format, Ubuntu 22.04+ / Debian 12+)
//
// mirrorURL is the base URL clients use to reach the mirror server,
// e.g. "http://mirror.example.com". If empty the files are not written.
func Generate(cfg *config.Config, outputDir, mirrorURL string) error {
	if mirrorURL == "" {
		return nil
	}
	mirrorURL = strings.TrimRight(mirrorURL, "/")

	if err := generateRPM(cfg, outputDir, mirrorURL); err != nil {
		return err
	}
	return generateDEB(cfg, outputDir, mirrorURL)
}

func generateRPM(cfg *config.Config, outputDir, mirrorURL string) error {
	if len(cfg.RPMRepos) == 0 {
		return nil
	}
	dir := filepath.Join(outputDir, "client-configs", "yum.repos.d")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("mkdir %s: %w", dir, err)
	}

	for _, repo := range cfg.RPMRepos {
		path := repo.Path
		if path == "" {
			path = repo.Name
		}
		baseURL := mirrorURL + "/" + strings.TrimLeft(filepath.ToSlash(path), "/") + "/"

		// GPG key URL: point at the locally mirrored copy.
		gpgKeyURL := ""
		if repo.GPGKey != "" {
			keyFile := filepath.Base(repo.GPGKey)
			gpgKeyURL = baseURL + "gpg-keys/" + keyFile
		}

		var sb strings.Builder
		fmt.Fprintf(&sb, "[%s]\n", repo.Name)
		fmt.Fprintf(&sb, "name=%s\n", repo.Name)
		fmt.Fprintf(&sb, "baseurl=%s\n", baseURL)
		fmt.Fprintf(&sb, "enabled=1\n")
		if gpgKeyURL != "" {
			fmt.Fprintf(&sb, "gpgcheck=1\n")
			fmt.Fprintf(&sb, "gpgkey=%s\n", gpgKeyURL)
		} else {
			fmt.Fprintf(&sb, "gpgcheck=0\n")
		}

		dest := filepath.Join(dir, repo.Name+".repo")
		if err := os.WriteFile(dest, []byte(sb.String()), 0o644); err != nil {
			return fmt.Errorf("write %s: %w", dest, err)
		}
	}
	return nil
}

func generateDEB(cfg *config.Config, outputDir, mirrorURL string) error {
	if len(cfg.DEBRepos) == 0 {
		return nil
	}
	dir := filepath.Join(outputDir, "client-configs", "apt", "sources.list.d")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("mkdir %s: %w", dir, err)
	}

	for _, repo := range cfg.DEBRepos {
		path := repo.Path
		if path == "" {
			path = repo.Name
		}
		repoURL := mirrorURL + "/" + strings.TrimLeft(filepath.ToSlash(path), "/")

		keyringPath := fmt.Sprintf("/etc/apt/keyrings/%s.gpg", repo.Name)

		// Legacy one-line format — works on all Debian/Ubuntu versions.
		// [signed-by=] is supported on Ubuntu 20.04+ / Debian 10+ and is
		// strongly recommended over the deprecated apt-key approach.
		var legacy strings.Builder
		if repo.GPGKey != "" {
			fmt.Fprintf(&legacy, "# Import GPG key: curl -fsSL %s | gpg --dearmor -o %s\n", repo.GPGKey, keyringPath)
		}
		for _, suite := range repo.Suites {
			if repo.GPGKey != "" {
				fmt.Fprintf(&legacy, "deb [signed-by=%s] %s %s %s\n", keyringPath, repoURL, suite, strings.Join(repo.Components, " "))
			} else {
				fmt.Fprintf(&legacy, "deb %s %s %s\n", repoURL, suite, strings.Join(repo.Components, " "))
			}
		}
		dest := filepath.Join(dir, repo.Name+".list")
		if err := os.WriteFile(dest, []byte(legacy.String()), 0o644); err != nil {
			return fmt.Errorf("write %s: %w", dest, err)
		}

		// deb822 format — preferred on Ubuntu 22.04+ and Debian 12+.
		// Multiple suites are combined into a single stanza.
		var deb822 strings.Builder
		fmt.Fprintf(&deb822, "Types: deb\n")
		fmt.Fprintf(&deb822, "URIs: %s\n", repoURL)
		fmt.Fprintf(&deb822, "Suites: %s\n", strings.Join(repo.Suites, " "))
		fmt.Fprintf(&deb822, "Components: %s\n", strings.Join(repo.Components, " "))
		if repo.GPGKey != "" {
			fmt.Fprintf(&deb822, "Signed-By: %s\n", keyringPath)
			fmt.Fprintf(&deb822, "# Import GPG key: curl -fsSL %s | gpg --dearmor -o %s\n", repo.GPGKey, keyringPath)
		}

		dest822 := filepath.Join(dir, repo.Name+".sources")
		if err := os.WriteFile(dest822, []byte(deb822.String()), 0o644); err != nil {
			return fmt.Errorf("write %s: %w", dest822, err)
		}
	}
	return nil
}
