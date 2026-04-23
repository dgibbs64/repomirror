// Package rpm mirrors YUM/DNF (RPM) repositories by parsing repomd.xml metadata.
package rpm

import (
	"encoding/xml"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"repomirror/downloader"
	"repomirror/gpg"
)

// ---------- XML structs for repomd.xml ----------

type repoMD struct {
	XMLName  xml.Name    `xml:"repomd"`
	Data     []repoData  `xml:"data"`
}

type repoData struct {
	Type     string      `xml:"type,attr"`
	Location location    `xml:"location"`
	Checksum checksum    `xml:"checksum"`
	OpenChecksum checksum `xml:"open-checksum"`
}

type location struct {
	Href string `xml:"href,attr"`
}

type checksum struct {
	Type  string `xml:"type,attr"`
	Value string `xml:",chardata"`
}

// ---------- XML structs for primary.xml ----------

type primaryMD struct {
	XMLName  xml.Name   `xml:"metadata"`
	Packages []rpmPkg   `xml:"package"`
}

type rpmPkg struct {
	Location pkgLocation `xml:"location"`
	Checksum checksum    `xml:"checksum"`
}

type pkgLocation struct {
	Href string `xml:"href,attr"`
}

// ---------- Mirror ----------

// Mirror downloads a YUM/DNF repository rooted at baseURL into destDir.
// It mirrors the repodata directory exactly and all referenced RPM packages,
// preserving the upstream directory layout.
func Mirror(baseURL, destDir, repoName, gpgKeyURL string, workers int, dl *downloader.Client) error {
	baseURL = strings.TrimRight(baseURL, "/")

	log.Printf("[rpm] %s  →  %s", baseURL, destDir)
	log.Printf("[rpm] %s: preparing metadata (workers=%d)", repoName, workers)

	// Fetch and import the GPG key.
	keysDir := filepath.Join(destDir, "gpg-keys")
	if err := gpg.FetchAndImport(gpgKeyURL, keysDir, dl); err != nil {
		log.Printf("[rpm] %s: GPG key warning: %v", repoName, err)
	}

	// Fetch repomd.xml (always into memory so dry-run can parse it).
	repomdURL := baseURL + "/repodata/repomd.xml"
	repomdDest := filepath.Join(destDir, "repodata", "repomd.xml")
	log.Printf("[rpm] %s: fetching repomd.xml", repoName)
	repomdData, err := dl.FetchBytes(repomdURL)
	if err != nil {
		return fmt.Errorf("[rpm] %s: fetch repomd.xml: %w", repoName, err)
	}
	if !dl.DryRun {
		if err := downloadForce(dl, repomdURL, repomdDest); err != nil {
			return fmt.Errorf("[rpm] %s: save repomd.xml: %w", repoName, err)
		}
	} else {
		log.Printf("[dry-run] would download: %s", repomdURL)
	}

	// Download repomd.xml.asc / repomd.xml.key (signature) if present.
	for _, sigSuffix := range []string{".asc", ".key", ".sig"} {
		sigURL := repomdURL + sigSuffix
		sigDest := repomdDest + sigSuffix
		_ = dl.DownloadFile(sigURL, sigDest, "", "")
	}
	var rmd repoMD
	if err := xml.Unmarshal(repomdData, &rmd); err != nil {
		return fmt.Errorf("[rpm] %s: parse repomd.xml: %w", repoName, err)
	}
	log.Printf("[rpm] %s: repomd.xml loaded (%d metadata records)", repoName, len(rmd.Data))

	// Download all metadata files listed in repomd.xml.
	// Track the primary metadata URL/dest for later parsing.
	var primaryURL, primaryDest string
	for _, d := range rmd.Data {
		href := strings.TrimLeft(d.Location.Href, "/")
		fileURL := baseURL + "/" + href
		fileDest := filepath.Join(destDir, filepath.FromSlash(href))
		if err := dl.DownloadFile(fileURL, fileDest, d.Checksum.Type, strings.TrimSpace(d.Checksum.Value)); err != nil {
			log.Printf("[rpm] %s: metadata %s: %v", repoName, href, err)
			continue
		}
		if d.Type == "primary" {
			primaryURL = fileURL
			primaryDest = fileDest
		}
	}

	if primaryURL == "" {
		return fmt.Errorf("[rpm] %s: no primary metadata found in repomd.xml", repoName)
	}
	log.Printf("[rpm] %s: parsing primary metadata", repoName)

	// Parse primary.xml — in dry-run mode fetch into memory since the file
	// was never written to disk.
	var packages []rpmPkg
	if dl.DryRun {
		data, err := dl.FetchBytes(primaryURL)
		if err != nil {
			return fmt.Errorf("[rpm] %s: fetch primary for parse: %w", repoName, err)
		}
		packages, err = parsePrimaryBytes(data, fileExt(primaryURL))
		if err != nil {
			return fmt.Errorf("[rpm] %s: parse primary metadata: %w", repoName, err)
		}
	} else {
		var parseErr error
		packages, parseErr = parsePrimary(primaryDest)
		if parseErr != nil {
			return fmt.Errorf("[rpm] %s: parse primary metadata: %w", repoName, parseErr)
		}
	}

	log.Printf("[rpm] %s: %d packages to mirror", repoName, len(packages))

	// Progress tracking (skipped in dry-run since no bytes are transferred).
	var prog *downloader.Counter
	var stopProg chan struct{}
	if !dl.DryRun {
		prog = downloader.NewCounter(repoName, "rpm", len(packages))
		stopProg = make(chan struct{})
		prog.StartLogger(1*time.Second, stopProg)
	}

	// Download packages concurrently.
	type job struct {
		pkg rpmPkg
	}
	jobs := make(chan job, len(packages))
	for _, p := range packages {
		jobs <- job{p}
	}
	close(jobs)

	var wg sync.WaitGroup
	errs := make(chan error, len(packages))

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range jobs {
				href := strings.TrimLeft(j.pkg.Location.Href, "/")
				pkgURL := baseURL + "/" + href
				pkgDest := filepath.Join(destDir, filepath.FromSlash(href))
				if err := dl.DownloadFileP(pkgURL, pkgDest, j.pkg.Checksum.Type, strings.TrimSpace(j.pkg.Checksum.Value), prog); err != nil {
					errs <- fmt.Errorf("%s: %w", href, err)
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
		log.Printf("[rpm] %s: package error: %v", repoName, err)
		errCount++
	}
	if errCount > 0 {
		return fmt.Errorf("[rpm] %s: %d package(s) failed to download", repoName, errCount)
	}

	log.Printf("[rpm] %s: done", repoName)
	return nil
}

// downloadForce downloads a file, always overwriting it (for metadata files).
func downloadForce(dl *downloader.Client, url, dest string) error {
	if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
		return err
	}
	data, err := dl.FetchBytes(url)
	if err != nil {
		return err
	}
	return os.WriteFile(dest, data, 0o644)
}

// parsePrimary reads and parses a possibly gzip-compressed primary.xml file.
func parsePrimary(path string) ([]rpmPkg, error) {
	r, cleanup, err := openPossiblyCompressed(path)
	if err != nil {
		return nil, err
	}
	defer cleanup()

	var pm primaryMD
	if err := xml.NewDecoder(r).Decode(&pm); err != nil {
		return nil, err
	}
	return pm.Packages, nil
}

// fileExt returns the lowercase extension of a URL or file path.
func fileExt(path string) string {
	lower := strings.ToLower(path)
	for _, ext := range []string{".gz", ".xz", ".bz2"} {
		if strings.HasSuffix(lower, ext) {
			return ext
		}
	}
	return ""
}

// parsePrimaryBytes parses a primary.xml from an in-memory byte slice.
// ext is the file extension (".gz", ".xz", or "") indicating compression.
func parsePrimaryBytes(data []byte, ext string) ([]rpmPkg, error) {
	r, cleanup, err := openPossiblyCompressedBytes(data, ext)
	if err != nil {
		return nil, err
	}
	defer cleanup()

	var pm primaryMD
	if err := xml.NewDecoder(r).Decode(&pm); err != nil {
		return nil, err
	}
	return pm.Packages, nil
}
