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
	"repomirror/gpg"
)

// Generate writes client config files under outputDir/client-configs/ for
// every enabled repo. With cleanStale it also removes config files left over
// from repos that are no longer enabled.
//
//	outputDir/client-configs/yum.repos.d/<name>.repo              one per RPM repo
//	outputDir/client-configs/apt/sources.list.d/<name>.list       one per DEB repo (legacy format)
//	outputDir/client-configs/apt/sources.list.d/<name>.sources    one per DEB repo (deb822 format, Ubuntu 22.04+ / Debian 12+)
//
// Signing keys are referenced from the mirror itself. mirrorURL is the base URL clients use to reach the mirror
// server, e.g. "http://mirror.example.com". If empty no files are written.
func Generate(cfg *config.Config, outputDir, mirrorURL string, cleanStale bool) error {
	if mirrorURL == "" {
		return nil
	}
	mirrorURL = strings.TrimRight(mirrorURL, "/")

	if err := generateRPM(cfg, outputDir, mirrorURL, cleanStale); err != nil {
		return err
	}
	return generateDEB(cfg, outputDir, mirrorURL, cleanStale)
}

func repoURL(mirrorURL, path, name string) string {
	if path == "" {
		path = name
	}
	return mirrorURL + "/" + strings.Trim(filepath.ToSlash(path), "/")
}

func keyURL(repoURL, gpgKey string) string {
	return repoURL + "/" + gpg.KeysDirName + "/" + gpg.KeyFileName(gpgKey)
}

func generateRPM(cfg *config.Config, outputDir, mirrorURL string, cleanStale bool) error {
	dir := filepath.Join(outputDir, "client-configs", "yum.repos.d")
	written := map[string]bool{}

	for _, repo := range cfg.RPMRepos {
		if !repo.Enabled() || repo.Name == "" {
			continue
		}
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return fmt.Errorf("mkdir %s: %w", dir, err)
		}
		base := repoURL(mirrorURL, repo.Path, repo.Name)

		var sb strings.Builder
		fmt.Fprintf(&sb, "[%s]\n", repo.Name)
		fmt.Fprintf(&sb, "name=%s\n", repo.Name)
		fmt.Fprintf(&sb, "baseurl=%s/\n", base)
		fmt.Fprintf(&sb, "enabled=1\n")
		if repo.GPGKey != "" {
			fmt.Fprintf(&sb, "gpgcheck=1\n")
			fmt.Fprintf(&sb, "gpgkey=%s\n", keyURL(base, repo.GPGKey))
		} else {
			fmt.Fprintf(&sb, "gpgcheck=0\n")
		}

		name := repo.Name + ".repo"
		written[name] = true
		dest := filepath.Join(dir, name)
		if err := os.WriteFile(dest, []byte(sb.String()), 0o644); err != nil { //nolint:gosec
			return fmt.Errorf("write %s: %w", dest, err)
		}
	}
	if !cleanStale {
		return nil
	}
	return removeStale(dir, written, ".repo")
}

func generateDEB(cfg *config.Config, outputDir, mirrorURL string, cleanStale bool) error {
	dir := filepath.Join(outputDir, "client-configs", "apt", "sources.list.d")
	written := map[string]bool{}

	for _, repo := range cfg.DEBRepos {
		if !repo.Enabled() || repo.Name == "" {
			continue
		}
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return fmt.Errorf("mkdir %s: %w", dir, err)
		}
		url := repoURL(mirrorURL, repo.Path, repo.Name)
		keyringPath := fmt.Sprintf("/etc/apt/keyrings/%s.asc", repo.Name)
		installKey := ""
		if repo.GPGKey != "" {
			installKey = fmt.Sprintf("# Install the signing key: sudo curl -fsSL %s -o %s\n", keyURL(url, repo.GPGKey), keyringPath)
		}
		components := strings.Join(repo.Components, " ")

		var legacy strings.Builder
		legacy.WriteString(installKey)
		for _, suite := range repo.Suites {
			line := "deb "
			if repo.GPGKey != "" {
				line += fmt.Sprintf("[signed-by=%s] ", keyringPath)
			}
			line += url + " " + suite
			if components != "" {
				line += " " + components
			}
			legacy.WriteString(line + "\n")
		}
		listName := repo.Name + ".list"
		written[listName] = true
		dest := filepath.Join(dir, listName)
		if err := os.WriteFile(dest, []byte(legacy.String()), 0o644); err != nil { //nolint:gosec
			return fmt.Errorf("write %s: %w", dest, err)
		}

		var deb822 strings.Builder
		deb822.WriteString(installKey)
		fmt.Fprintf(&deb822, "Types: deb\n")
		fmt.Fprintf(&deb822, "URIs: %s\n", url)
		fmt.Fprintf(&deb822, "Suites: %s\n", strings.Join(repo.Suites, " "))
		if components != "" {
			fmt.Fprintf(&deb822, "Components: %s\n", components)
		}
		if repo.GPGKey != "" {
			fmt.Fprintf(&deb822, "Signed-By: %s\n", keyringPath)
		}

		sourcesName := repo.Name + ".sources"
		written[sourcesName] = true
		dest822 := filepath.Join(dir, sourcesName)
		if err := os.WriteFile(dest822, []byte(deb822.String()), 0o644); err != nil { //nolint:gosec
			return fmt.Errorf("write %s: %w", dest822, err)
		}
	}
	if !cleanStale {
		return nil
	}
	return removeStale(dir, written, ".list", ".sources")
}

func removeStale(dir string, written map[string]bool, exts ...string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	for _, e := range entries {
		if e.IsDir() || written[e.Name()] {
			continue
		}
		for _, ext := range exts {
			if strings.HasSuffix(e.Name(), ext) {
				if err := os.Remove(filepath.Join(dir, e.Name())); err != nil {
					return err
				}
				break
			}
		}
	}
	return nil
}
