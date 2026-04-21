// Package clientconfigs generates repository client configuration files
// (yum .repo files and apt sources.list files) that point at the local mirror.
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
//	outputDir/client-configs/yum.repos.d/<name>.repo   — one per RPM repo
//	outputDir/client-configs/apt/sources.list.d/<name>.list — one per DEB repo
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

		var sb strings.Builder
		for _, suite := range repo.Suites {
			fmt.Fprintf(&sb, "deb %s %s %s\n", repoURL, suite, strings.Join(repo.Components, " "))
		}

		dest := filepath.Join(dir, repo.Name+".list")
		if err := os.WriteFile(dest, []byte(sb.String()), 0o644); err != nil {
			return fmt.Errorf("write %s: %w", dest, err)
		}
	}
	return nil
}
