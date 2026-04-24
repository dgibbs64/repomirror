// Package deb mirrors APT (Debian/Ubuntu) repositories by parsing InRelease
// and Packages metadata files.
package deb

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/xml"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	"repomirror/downloader"
	"repomirror/gpg"
)

// Mirror downloads one APT repository (one mirror URL) for all requested
// suites and components into destDir, preserving the upstream directory layout.
func Mirror(mirrorURL, mirrorlistURL, metalinkURL, preferredMirror, destDir, repoName, gpgKeyURL string, suites, components, arches []string, workers int, dl *downloader.Client) error {
	sources, err := resolveDEBSourceURLs(mirrorURL, mirrorlistURL, metalinkURL, preferredMirror, dl)
	if err != nil {
		return fmt.Errorf("[deb] %s: resolve sources: %w", repoName, err)
	}
	ss := newSourceSet(sources)
	mirrorURL = strings.TrimRight(ss.primary(), "/")

	log.Printf("[deb] %s  →  %s", mirrorURL, destDir)
	if len(sources) > 1 {
		log.Printf("[deb] %s: source failover enabled (%d sources)", repoName, len(sources))
	}
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
		pkgs, err := mirrorSuite(dl, ss, destDir, repoName, suite, components, arches)
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
				if err := downloadFileFromSources(dl, ss, j.relPath, j.dest, j.algo, j.checksum, prog); err != nil {
					errs <- fmt.Errorf("%s: %w", j.relPath, err)
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
	return nil
}

type pkgEntry struct {
	relPath, dest, algo, checksum string
}

// metaEntry tracks a metadata file URL alongside its local destination path.
type metaEntry struct {
	relPath string
	dest    string
	isGz    bool
}

// mirrorSuite fetches the InRelease file for one suite, downloads all metadata
// files listed in it, then collects all package URLs.
func mirrorSuite(dl *downloader.Client, ss *sourceSet, destDir, repoName, suite string, components, arches []string) ([]pkgEntry, error) {
	distDest := filepath.Join(destDir, "dists", suite)

	// Try InRelease first, fall back to Release + Release.gpg.
	inReleaseRel := "dists/" + suite + "/InRelease"
	inReleaseDest := filepath.Join(distDest, "InRelease")

	releaseData, _, err := fetchBytesFromSources(dl, ss, inReleaseRel)
	if err != nil {
		// Try plain Release.
		releaseRel := "dists/" + suite + "/Release"
		releaseData, _, err = fetchBytesFromSources(dl, ss, releaseRel)
		if err != nil {
			return nil, fmt.Errorf("fetch Release for suite %s: %w", suite, err)
		}
		if !dl.DryRun {
			if err := writeFile(filepath.Join(distDest, "Release"), releaseData); err != nil {
				return nil, err
			}
			_ = downloadFileFromSources(dl, ss, "dists/"+suite+"/Release.gpg", filepath.Join(distDest, "Release.gpg"), "", "", nil)
		}
	} else {
		if !dl.DryRun {
			if err := writeFile(inReleaseDest, releaseData); err != nil {
				return nil, err
			}
		}
	}

	// Parse the Release/InRelease file to get checksums and by-hash policy.
	rmeta := parseReleaseMetadata(releaseData)
	metaChecksums := rmeta.checksums

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
				canonicalRel := "dists/" + suite + "/" + fileName
				fileDest, err := downloader.SafeJoin(distDest, filepath.FromSlash(fileName))
				if err != nil {
					continue
				}
				entry, ok := metaChecksums[fileName]
				var algo, sum string
				if ok {
					algo, sum = entry.algo, entry.sum
				}

				downloadRel := canonicalRel
				if rmeta.acquireByHash && ok {
					if byHashRel, byHashOK := buildByHashRelativePath(suite, fileName, entry.algo, entry.sum); byHashOK {
						downloadRel = byHashRel
					}
				}
				if err := downloadFileFromSources(dl, ss, downloadRel, fileDest, algo, sum, nil); err != nil {
					if downloadRel != canonicalRel {
						if err2 := downloadFileFromSources(dl, ss, canonicalRel, fileDest, algo, sum, nil); err2 != nil {
							// Some files may not exist on all mirrors; skip silently.
							continue
						}
					} else {
						// Some files may not exist on all mirrors; skip silently.
						continue
					}
				}
				if strings.HasSuffix(fileName, "Packages") || strings.HasSuffix(fileName, "Packages.gz") {
					pkgMeta = append(pkgMeta, metaEntry{relPath: canonicalRel, dest: fileDest, isGz: strings.HasSuffix(fileName, ".gz")})
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
			data, _, err := fetchBytesFromSources(dl, ss, m.relPath)
			if err != nil {
				log.Printf("[deb] suite %s: fetch for parse %s: %v", suite, m.relPath, err)
				continue
			}
			pkgs, parseErr = parsePackagesBytes(data, m.isGz)
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
			pkgDest, err := downloader.SafeJoin(destDir, filepath.FromSlash(p.filename))
			if err != nil {
				continue
			}
			entries = append(entries, pkgEntry{
				relPath:  strings.TrimLeft(p.filename, "/"),
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

type releaseMetadata struct {
	checksums     map[string]sumEntry
	acquireByHash bool
}

// parseReleaseMetadata extracts filename checksums and Acquire-By-Hash policy
// from a Release/InRelease file. We prefer SHA256 over SHA1 over MD5.
func parseReleaseMetadata(data []byte) releaseMetadata {
	result := releaseMetadata{checksums: map[string]sumEntry{}}

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
		if strings.EqualFold(strings.TrimSpace(line), "Acquire-By-Hash: yes") {
			result.acquireByHash = true
		}
		// Lines under a hash section start with a space.
		if currentAlgo != "" && strings.HasPrefix(line, " ") {
			fields := strings.Fields(line)
			if len(fields) < 3 {
				continue
			}
			filename := fields[2]
			sum := fields[0]
			existing, ok := result.checksums[filename]
			if !ok || algoPriority(currentAlgo) > algoPriority(existing.algo) {
				result.checksums[filename] = sumEntry{algo: currentAlgo, sum: sum}
			}
		} else if !strings.HasPrefix(line, " ") && line != "" {
			// New top-level stanza; reset section.
			currentAlgo = ""
		}
	}
	return result
}

func buildByHashRelativePath(suite, relativePath, algo, sum string) (string, bool) {
	hashDir, ok := byHashDirName(algo)
	if !ok || sum == "" {
		return "", false
	}
	relativePath = strings.TrimLeft(relativePath, "/")
	idx := strings.LastIndex(relativePath, "/")
	if idx < 0 {
		return "", false
	}
	parent := relativePath[:idx]
	return "dists/" + suite + "/" + parent + "/by-hash/" + hashDir + "/" + sum, true
}

func byHashDirName(algo string) (string, bool) {
	switch strings.ToLower(algo) {
	case "sha512":
		return "SHA512", true
	case "sha256":
		return "SHA256", true
	case "sha1":
		return "SHA1", true
	case "md5":
		return "MD5Sum", true
	default:
		return "", false
	}
}

type sourceSet struct {
	mu   sync.Mutex
	urls []string
}

func newSourceSet(urls []string) *sourceSet {
	return &sourceSet{urls: append([]string(nil), urls...)}
}

func (s *sourceSet) primary() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.urls) == 0 {
		return ""
	}
	return s.urls[0]
}

func (s *sourceSet) ordered() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.urls...)
}

func (s *sourceSet) markSuccess(baseURL string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	idx := -1
	for i, u := range s.urls {
		if u == baseURL {
			idx = i
			break
		}
	}
	if idx <= 0 {
		return
	}
	s.urls[0], s.urls[idx] = s.urls[idx], s.urls[0]
}

func downloadFileFromSources(dl *downloader.Client, ss *sourceSet, relPath, dest, algo, expected string, prog *downloader.Counter) error {
	rel := strings.TrimLeft(relPath, "/")
	ordered := ss.ordered()
	var lastErr error
	for _, base := range ordered {
		url := strings.TrimRight(base, "/") + "/" + rel
		if err := dl.DownloadFileP(url, dest, algo, expected, prog); err != nil {
			lastErr = err
			continue
		}
		ss.markSuccess(base)
		return nil
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("no source URLs configured")
	}
	return lastErr
}

func fetchBytesFromSources(dl *downloader.Client, ss *sourceSet, relPath string) ([]byte, string, error) {
	rel := strings.TrimLeft(relPath, "/")
	ordered := ss.ordered()
	var lastErr error
	for _, base := range ordered {
		url := strings.TrimRight(base, "/") + "/" + rel
		data, err := dl.FetchBytes(url)
		if err != nil {
			lastErr = err
			continue
		}
		ss.markSuccess(base)
		return data, base, nil
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("no source URLs configured")
	}
	return nil, "", lastErr
}

func resolveDEBSourceURLs(mirrorURL, mirrorlistURL, metalinkURL, preferred string, dl *downloader.Client) ([]string, error) {
	var sources []string
	add := func(raw string) {
		raw = strings.TrimSpace(raw)
		if raw == "" {
			return
		}
		raw = strings.TrimRight(raw, "/")
		if !strings.HasPrefix(raw, "http://") && !strings.HasPrefix(raw, "https://") {
			return
		}
		for _, existing := range sources {
			if existing == raw {
				return
			}
		}
		sources = append(sources, raw)
	}

	add(mirrorURL)

	if mirrorlistURL != "" {
		data, err := dl.FetchBytes(mirrorlistURL)
		if err != nil {
			return nil, fmt.Errorf("fetch mirrorlist: %w", err)
		}
		for _, line := range strings.Split(string(data), "\n") {
			line = strings.TrimSpace(line)
			if line == "" || strings.HasPrefix(line, "#") {
				continue
			}
			add(line)
		}
	}

	if metalinkURL != "" {
		data, err := dl.FetchBytes(metalinkURL)
		if err != nil {
			return nil, fmt.Errorf("fetch metalink: %w", err)
		}
		var ml struct {
			URLs []struct {
				Protocol string `xml:"protocol,attr"`
				Value    string `xml:",chardata"`
			} `xml:"files>file>resources>url"`
		}
		if err := xml.Unmarshal(data, &ml); err != nil {
			return nil, fmt.Errorf("parse metalink: %w", err)
		}
		for _, u := range ml.URLs {
			p := strings.ToLower(strings.TrimSpace(u.Protocol))
			if p != "" && p != "http" && p != "https" {
				continue
			}
			add(u.Value)
		}
	}

	if len(sources) == 0 {
		return nil, fmt.Errorf("no DEB source URL configured (set mirror, mirrorlist, or metalink)")
	}

	if preferred != "" {
		preferred = strings.ToLower(strings.TrimSpace(preferred))
		slices.SortStableFunc(sources, func(a, b string) int {
			aa := strings.Contains(strings.ToLower(a), preferred)
			bb := strings.Contains(strings.ToLower(b), preferred)
			switch {
			case aa && !bb:
				return -1
			case !aa && bb:
				return 1
			default:
				return 0
			}
		})
	}

	return sources, nil
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
