// Package deb mirrors APT (Debian/Ubuntu) repositories by parsing InRelease
// and Packages metadata files.
package deb

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"repomirror/downloader"
	"repomirror/gpg"
)

// Mirror downloads one APT repository (one mirror URL) for all requested
// suites and components into destDir, preserving the upstream directory layout.
func Mirror(mirrorURL, destDir, repoName, gpgKeyURL string, suites, components, arches []string, workers int, dl *downloader.Client) error {
	mirrorURL = strings.TrimRight(mirrorURL, "/")

	log.Printf("[deb] %s  →  %s", mirrorURL, destDir)
	log.Printf("[deb] %s: suites=%s components=%s arches=%s workers=%d",
		repoName, formatListForLog(suites), formatListForLog(components), formatListForLog(arches), workers)

	// Fetch and import the GPG key.
	keysDir := filepath.Join(destDir, "gpg-keys")
	if err := gpg.FetchAndImport(gpgKeyURL, keysDir, dl); err != nil {
		log.Printf("[deb] %s: GPG key warning: %v", repoName, err)
	}

	var allPkgURLs []pkgEntry // collected across all suites/components/arches

	for _, suite := range suites {
		log.Printf("[deb] %s suite %s: fetching Release metadata", repoName, suite)
		pkgs, err := mirrorSuite(dl, mirrorURL, destDir, repoName, suite, components, arches)
		if err != nil {
			log.Printf("[deb] %s suite %s: %v", repoName, suite, err)
			continue
		}
		log.Printf("[deb] %s suite %s: discovered %d package entries", repoName, suite, len(pkgs))
		allPkgURLs = append(allPkgURLs, pkgs...)
	}

	log.Printf("[deb] %s: %d packages to mirror", repoName, len(allPkgURLs))

	// Progress tracking (skipped in dry-run since no bytes are transferred).
	var prog *downloader.Counter
	var stopProg chan struct{}
	if !dl.DryRun {
		prog = downloader.NewCounter(repoName, "deb", len(allPkgURLs))
		stopProg = make(chan struct{})
		prog.StartLogger(1*time.Second, stopProg)
	}

	// Download packages concurrently.
	jobs := make(chan pkgEntry, len(allPkgURLs))
	for _, p := range allPkgURLs {
		jobs <- p
	}
	close(jobs)

	errs := make(chan error, len(allPkgURLs))
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range jobs {
				if err := dl.DownloadFileP(j.url, j.dest, j.algo, j.checksum, prog); err != nil {
					errs <- fmt.Errorf("%s: %w", j.url, err)
				}
				if prog != nil {
					prog.Done()
				}
			}
		}()
	}
	wg.Wait()
	close(errs)

	if stopProg != nil {
		close(stopProg)
		prog.Finish()
	}

	var errCount int
	for err := range errs {
		log.Printf("[deb] %s: package error: %v", repoName, err)
		errCount++
	}
	if errCount > 0 {
		return fmt.Errorf("[deb] %s: %d package(s) failed", repoName, errCount)
	}

	log.Printf("[deb] %s: done", repoName)
	return nil
}

type pkgEntry struct {
	url, dest, algo, checksum string
}

// metaEntry tracks a metadata file URL alongside its local destination path.
type metaEntry struct {
	url      string
	dest     string
	isPkgs   bool // true for Packages / Packages.gz — needs parsing
}

// mirrorSuite fetches the InRelease file for one suite, downloads all metadata
// files listed in it, then collects all package URLs.
func mirrorSuite(dl *downloader.Client, mirrorURL, destDir, repoName, suite string, components, arches []string) ([]pkgEntry, error) {
	distBase := mirrorURL + "/dists/" + suite
	distDest := filepath.Join(destDir, "dists", suite)

	// Try InRelease first, fall back to Release + Release.gpg.
	inReleaseURL := distBase + "/InRelease"
	inReleaseDest := filepath.Join(distDest, "InRelease")

	releaseData, err := dl.FetchBytes(inReleaseURL)
	if err != nil {
		// Try plain Release.
		releaseURL := distBase + "/Release"
		releaseData, err = dl.FetchBytes(releaseURL)
		if err != nil {
			return nil, fmt.Errorf("fetch Release for suite %s: %w", suite, err)
		}
		if !dl.DryRun {
			if err := writeFile(filepath.Join(distDest, "Release"), releaseData); err != nil {
				return nil, err
			}
			_ = dl.DownloadFile(releaseURL+".gpg", filepath.Join(distDest, "Release.gpg"), "", "")
		}
	} else {
		if !dl.DryRun {
			if err := writeFile(inReleaseDest, releaseData); err != nil {
				return nil, err
			}
		}
	}

	// Parse the Release/InRelease file to get checksums for metadata files.
	metaChecksums := parseReleaseChecksums(releaseData)

	// Collect and download all metadata files for our components/arches.
	var pkgMeta []metaEntry
	for _, comp := range components {
		for _, arch := range arches {
			// Build the list of metadata files to fetch.  For Packages we
			// prefer the .gz variant; only include the uncompressed one when
			// the .gz is not listed in the Release checksums (old/small repos).
			_, hasGz := metaChecksums[fmt.Sprintf("%s/binary-%s/Packages.gz", comp, arch)]
			for _, fileName := range []string{
				fmt.Sprintf("%s/binary-%s/Packages", comp, arch),
				fmt.Sprintf("%s/binary-%s/Packages.gz", comp, arch),
				fmt.Sprintf("%s/binary-%s/Release", comp, arch),
				fmt.Sprintf("%s/i18n/Translation-en", comp),
				fmt.Sprintf("%s/i18n/Translation-en.gz", comp),
			} {
				// Skip uncompressed Packages when .gz is available to avoid
				// 404 noise (most modern mirrors only serve the gz variant).
				if fileName == fmt.Sprintf("%s/binary-%s/Packages", comp, arch) && hasGz {
					continue
				}
				fileURL := distBase + "/" + fileName
				fileDest, err := downloader.SafeJoin(distDest, filepath.FromSlash(fileName))
				if err != nil {
					continue
				}
				entry, ok := metaChecksums[fileName]
				var algo, sum string
				if ok {
					algo, sum = entry.algo, entry.sum
				}
				isPkgs := strings.HasSuffix(fileName, "Packages") || strings.HasSuffix(fileName, "Packages.gz")
				if err := dl.DownloadFile(fileURL, fileDest, algo, sum); err != nil {
					// Some files may not exist on all mirrors; skip silently.
					continue
				}
				if isPkgs {
					pkgMeta = append(pkgMeta, metaEntry{url: fileURL, dest: fileDest, isPkgs: true})
				}
			}
		}
	}

	// Parse Packages files and collect .deb download entries.
	// In dry-run mode the files were not written to disk, so we fetch them
	// into memory for parsing instead.
	seen := map[string]bool{}
	var entries []pkgEntry
	for _, m := range pkgMeta {
		var pkgs []debPkg
		var parseErr error
		if dl.DryRun {
			data, err := dl.FetchBytes(m.url)
			if err != nil {
				log.Printf("[deb] suite %s: fetch for parse %s: %v", suite, m.url, err)
				continue
			}
			pkgs, parseErr = parsePackagesBytes(data, strings.HasSuffix(m.url, ".gz"))
		} else {
			pkgs, parseErr = parsePackagesFile(m.dest)
		}
		if parseErr != nil {
			log.Printf("[deb] suite %s: parse %s: %v", suite, m.dest, parseErr)
			continue
		}
		for _, p := range pkgs {
			if seen[p.filename] {
				continue
			}
			seen[p.filename] = true
			pkgURL := mirrorURL + "/" + p.filename
			pkgDest, err := downloader.SafeJoin(destDir, filepath.FromSlash(p.filename))
			if err != nil {
				continue
			}
			entries = append(entries, pkgEntry{
				url:      pkgURL,
				dest:     pkgDest,
				algo:     p.algo,
				checksum: p.sum,
			})
		}
	}

	return entries, nil
}

// ---------- Release checksum parsing ----------

type sumEntry struct {
	algo string
	sum  string
}

// parseReleaseChecksums extracts filename → checksum from a Release/InRelease
// file. We prefer SHA256 over SHA1 over MD5.
func parseReleaseChecksums(data []byte) map[string]sumEntry {
	result := map[string]sumEntry{}

	// InRelease files are PGP clearsigned; strip the armour.
	text := stripPGPArmour(data)

	var currentAlgo string
	scanner := bufio.NewScanner(bytes.NewReader(text))
	for scanner.Scan() {
		line := scanner.Text()
		switch strings.TrimRight(line, " ") {
		case "MD5Sum:":
			currentAlgo = "md5"
			continue
		case "SHA1:":
			currentAlgo = "sha1"
			continue
		case "SHA256:":
			currentAlgo = "sha256"
			continue
		case "SHA512:":
			currentAlgo = "sha512"
			continue
		}
		// Lines under a hash section start with a space.
		if currentAlgo != "" && strings.HasPrefix(line, " ") {
			fields := strings.Fields(line)
			if len(fields) < 3 {
				continue
			}
			filename := fields[2]
			sum := fields[0]
			existing, ok := result[filename]
			if !ok || algoPriority(currentAlgo) > algoPriority(existing.algo) {
				result[filename] = sumEntry{algo: currentAlgo, sum: sum}
			}
		} else if !strings.HasPrefix(line, " ") && line != "" {
			// New top-level stanza; reset section.
			currentAlgo = ""
		}
	}
	return result
}

func algoPriority(algo string) int {
	switch algo {
	case "sha512":
		return 4
	case "sha256":
		return 3
	case "sha1":
		return 2
	case "md5":
		return 1
	}
	return 0
}

// stripPGPArmour removes the PGP clearsign header/footer from an InRelease
// file, leaving just the signed content.
func stripPGPArmour(data []byte) []byte {
	lines := bytes.Split(data, []byte("\n"))
	var out [][]byte
	inHeader := true
	inSignature := false
	for _, line := range lines {
		trimmed := bytes.TrimSpace(line)
		if inHeader {
			if bytes.Equal(trimmed, []byte("")) {
				inHeader = false
			}
			continue
		}
		if bytes.HasPrefix(trimmed, []byte("-----BEGIN PGP SIGNATURE-----")) {
			inSignature = true
		}
		if !inSignature {
			out = append(out, line)
		}
	}
	return bytes.Join(out, []byte("\n"))
}

// ---------- Packages file parsing ----------

type debPkg struct {
	filename string
	algo     string
	sum      string
}

// parsePackagesFile reads a Packages or Packages.gz file and returns package entries.
func parsePackagesFile(path string) ([]debPkg, error) {
	f, err := os.Open(path) // #nosec G304 – path derived from config
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var r io.Reader = f
	if strings.HasSuffix(strings.ToLower(path), ".gz") {
		gz, err := gzip.NewReader(f)
		if err != nil {
			return nil, err
		}
		defer gz.Close()
		r = gz
	}

	return scanPackages(r), nil
}

// parsePackagesBytes parses a Packages file from an in-memory byte slice.
// isGz indicates whether the bytes are gzip-compressed.
func parsePackagesBytes(data []byte, isGz bool) ([]debPkg, error) {
	var r io.Reader = bytes.NewReader(data)
	if isGz {
		gz, err := gzip.NewReader(r)
		if err != nil {
			return nil, err
		}
		defer gz.Close()
		r = gz
	}
	return scanPackages(r), nil
}

func scanPackages(r io.Reader) []debPkg {
	var pkgs []debPkg
	var current debPkg

	scanner := bufio.NewScanner(r)
	// Packages files can have very long lines.
	scanner.Buffer(make([]byte, 1024*1024), 1024*1024)

	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			if current.filename != "" {
				pkgs = append(pkgs, current)
			}
			current = debPkg{}
			continue
		}
		if val, ok := fieldValue(line, "Filename"); ok {
			current.filename = val
		}
		// Prefer SHA256 > SHA1 > MD5.
		if current.algo != "sha256" {
			if val, ok := fieldValue(line, "SHA256"); ok {
				current.algo = "sha256"
				current.sum = val
			} else if current.algo != "sha1" {
				if val, ok := fieldValue(line, "SHA1"); ok {
					current.algo = "sha1"
					current.sum = val
				} else if val, ok := fieldValue(line, "MD5sum"); ok {
					current.algo = "md5"
					current.sum = val
				}
			}
		}
	}
	// Handle last stanza without trailing blank line.
	if current.filename != "" {
		pkgs = append(pkgs, current)
	}
	return pkgs
}

func fieldValue(line, field string) (string, bool) {
	prefix := field + ": "
	if strings.HasPrefix(line, prefix) {
		return strings.TrimSpace(line[len(prefix):]), true
	}
	return "", false
}

// writeFile writes data to path, creating parent directories as needed.
func writeFile(path string, data []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o644)
}

func formatListForLog(values []string) string {
	if len(values) == 0 {
		return "-"
	}
	return strings.Join(values, ",")
}
