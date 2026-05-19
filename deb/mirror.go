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

const (
	algoMD5    = "md5"
	algoSHA1   = "sha1"
	algoSHA256 = "sha256"
	algoSHA512 = "sha512"
)

// Mirror downloads one APT repository (one mirror URL) for all requested
// suites and components into destDir, preserving the upstream directory layout.
func Mirror(mirrorURL, mirrorlistURL, metalinkURL, preferredMirror, destDir, repoName, gpgKeyURL string, suites, components, arches []string, workers int, dl *downloader.Client) error {
	sources, err := downloader.ResolveSourceURLs(mirrorURL, mirrorlistURL, metalinkURL, preferredMirror, "DEB", dl)
	if err != nil {
		return fmt.Errorf("[deb] %s: resolve sources: %w", repoName, err)
	}
	ss := downloader.NewSourceSet(sources)
	mirrorURL = strings.TrimRight(ss.Primary(), "/")

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
				if err := downloader.DownloadFileFromSources(dl, ss, j.relPath, j.dest, j.algo, j.checksum, prog); err != nil {
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
// files listed in it, then collects all package URLs. When suite is "/" the
// repo uses a flat (trivial) layout and mirrorFlatSuite is called instead.
func mirrorSuite(dl *downloader.Client, ss *downloader.SourceSet, destDir, repoName, suite string, components, arches []string) ([]pkgEntry, error) { //nolint:gocyclo
	if suite == "/" {
		return mirrorFlatSuite(dl, ss, destDir, repoName, arches)
	}
	distDest := filepath.Join(destDir, "dists", suite)

	// Try InRelease first, fall back to Release + Release.gpg.
	inReleaseRel := "dists/" + suite + "/InRelease"
	inReleaseDest := filepath.Join(distDest, "InRelease")

	releaseData, _, err := downloader.FetchBytesFromSources(dl, ss, inReleaseRel)
	if err != nil {
		// Try plain Release.
		releaseRel := "dists/" + suite + "/Release"
		releaseData, _, err = downloader.FetchBytesFromSources(dl, ss, releaseRel)
		if err != nil {
			return nil, fmt.Errorf("fetch Release for suite %s: %w", suite, err)
		}
		if !dl.DryRun {
			if err := writeFile(filepath.Join(distDest, "Release"), releaseData); err != nil {
				return nil, err
			}
			_ = downloader.DownloadFileFromSources(dl, ss, "dists/"+suite+"/Release.gpg", filepath.Join(distDest, "Release.gpg"), "", "", nil) //nolint:errcheck
		}
	} else if !dl.DryRun {
		if err := writeFile(inReleaseDest, releaseData); err != nil {
			return nil, err
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
				if err := downloader.DownloadFileFromSources(dl, ss, downloadRel, fileDest, algo, sum, nil); err != nil {
					if downloadRel != canonicalRel {
						if err2 := downloader.DownloadFileFromSources(dl, ss, canonicalRel, fileDest, algo, sum, nil); err2 != nil {
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
			data, _, err := downloader.FetchBytesFromSources(dl, ss, m.relPath)
			if err != nil {
				log.Printf("[deb] suite %s: fetch for parse %s: %v", suite, m.relPath, err)
				continue
			}
			pkgs, parseErr = parsePackagesBytes(data, m.isGz, nil)
		} else {
			pkgs, parseErr = parsePackagesFile(m.dest, nil)
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

// mirrorFlatSuite mirrors a flat (trivial) APT repository where InRelease and
// Packages sit at the mirror root rather than under dists/. Used by repos such
// as pkgs.k8s.io that use suite "/" in their sources.list line.
func mirrorFlatSuite(dl *downloader.Client, ss *downloader.SourceSet, destDir, repoName string, arches []string) ([]pkgEntry, error) { //nolint:gocyclo
	releaseData, _, err := downloader.FetchBytesFromSources(dl, ss, "InRelease")
	if err != nil {
		releaseData, _, err = downloader.FetchBytesFromSources(dl, ss, "Release")
		if err != nil {
			return nil, fmt.Errorf("fetch Release for flat repo: %w", err)
		}
		if !dl.DryRun {
			if err := writeFile(filepath.Join(destDir, "Release"), releaseData); err != nil {
				return nil, err
			}
			_ = downloader.DownloadFileFromSources(dl, ss, "Release.gpg", filepath.Join(destDir, "Release.gpg"), "", "", nil) //nolint:errcheck
		}
	} else if !dl.DryRun {
		if err := writeFile(filepath.Join(destDir, "InRelease"), releaseData); err != nil {
			return nil, err
		}
	}

	rmeta := parseReleaseMetadata(releaseData)

	// Prefer Packages.gz; fall back to uncompressed Packages.
	var pkgMeta []metaEntry
	_, hasGz := rmeta.checksums["Packages.gz"]
	for _, filename := range []string{"Packages", "Packages.gz"} {
		if filename == "Packages" && hasGz {
			continue
		}
		fileDest, err := downloader.SafeJoin(destDir, filename)
		if err != nil {
			continue
		}
		entry, ok := rmeta.checksums[filename]
		var algo, sum string
		if ok {
			algo, sum = entry.algo, entry.sum
		}
		if err := downloader.DownloadFileFromSources(dl, ss, filename, fileDest, algo, sum, nil); err != nil {
			continue
		}
		pkgMeta = append(pkgMeta, metaEntry{
			relPath: filename,
			dest:    fileDest,
			isGz:    strings.HasSuffix(filename, ".gz"),
		})
	}

	seen := map[string]bool{}
	var entries []pkgEntry
	for _, m := range pkgMeta {
		var pkgs []debPkg
		var parseErr error
		if dl.DryRun {
			data, _, err := downloader.FetchBytesFromSources(dl, ss, m.relPath)
			if err != nil {
				log.Printf("[deb] %s flat repo: fetch for parse %s: %v", repoName, m.relPath, err)
				continue
			}
			pkgs, parseErr = parsePackagesBytes(data, m.isGz, arches)
		} else {
			pkgs, parseErr = parsePackagesFile(m.dest, arches)
		}
		if parseErr != nil {
			log.Printf("[deb] %s flat repo: parse %s: %v", repoName, m.dest, parseErr)
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
			currentAlgo = algoMD5
			continue
		case "SHA1:":
			currentAlgo = algoSHA1
			continue
		case "SHA256:":
			currentAlgo = algoSHA256
			continue
		case "SHA512:":
			currentAlgo = algoSHA512
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
	case algoSHA512:
		return "SHA512", true
	case algoSHA256:
		return "SHA256", true
	case algoSHA1:
		return "SHA1", true
	case algoMD5:
		return "MD5Sum", true
	default:
		return "", false
	}
}

func algoPriority(algo string) int {
	switch algo {
	case algoSHA512:
		return 4
	case algoSHA256:
		return 3
	case algoSHA1:
		return 2
	case algoMD5:
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

// parsePackagesFile reads a Packages or Packages.gz file and returns package
// entries. If arches is non-empty only packages matching those architectures
// are included (used for flat repos where one Packages file covers all arches).
func parsePackagesFile(path string, arches []string) ([]debPkg, error) {
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

	return scanPackages(r, arches), nil
}

// parsePackagesBytes parses a Packages file from an in-memory byte slice.
// isGz indicates whether the bytes are gzip-compressed. arches behaves as in
// parsePackagesFile.
func parsePackagesBytes(data []byte, isGz bool, arches []string) ([]debPkg, error) {
	var r io.Reader = bytes.NewReader(data)
	if isGz {
		gz, err := gzip.NewReader(r)
		if err != nil {
			return nil, err
		}
		defer gz.Close()
		r = gz
	}
	return scanPackages(r, arches), nil
}

// scanPackages parses a Debian Packages control file. If arches is non-empty,
// only entries whose Architecture field matches one of the listed values are
// returned.
func scanPackages(r io.Reader, arches []string) []debPkg { //nolint:gocyclo
	archSet := make(map[string]bool, len(arches))
	for _, a := range arches {
		archSet[a] = true
	}
	filterArches := len(archSet) > 0

	var pkgs []debPkg
	var current debPkg
	var currentArch string

	scanner := bufio.NewScanner(r)
	// Packages files can have very long lines.
	scanner.Buffer(make([]byte, 1024*1024), 1024*1024)

	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			if current.filename != "" && (!filterArches || archSet[currentArch]) {
				pkgs = append(pkgs, current)
			}
			current = debPkg{}
			currentArch = ""
			continue
		}
		if val, ok := fieldValue(line, "Filename"); ok {
			current.filename = val
		}
		if val, ok := fieldValue(line, "Architecture"); ok {
			currentArch = val
		}
		// Prefer SHA256 > SHA1 > MD5.
		if current.algo != algoSHA256 {
			if val, ok := fieldValue(line, "SHA256"); ok {
				current.algo = algoSHA256
				current.sum = val
			} else if current.algo != algoSHA1 {
				if val, ok := fieldValue(line, "SHA1"); ok {
					current.algo = algoSHA1
					current.sum = val
				} else if val, ok := fieldValue(line, "MD5sum"); ok {
					current.algo = algoMD5
					current.sum = val
				}
			}
		}
	}
	// Handle last stanza without trailing blank line.
	if current.filename != "" && (!filterArches || archSet[currentArch]) {
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
	return os.WriteFile(path, data, 0o644) //nolint:gosec
}

func formatListForLog(values []string) string {
	if len(values) == 0 {
		return "-"
	}
	return strings.Join(values, ",")
}
