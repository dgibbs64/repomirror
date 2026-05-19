// Package rpm mirrors YUM/DNF (RPM) repositories by parsing repomd.xml metadata.
package rpm

import (
	"bufio"
	"database/sql"
	"encoding/xml"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"repomirror/downloader"
	"repomirror/gpg"

	_ "modernc.org/sqlite"
)

// ---------- XML structs for repomd.xml ----------

type repoMD struct {
	XMLName xml.Name   `xml:"repomd"`
	Data    []repoData `xml:"data"`
}

type repoData struct {
	Type         string   `xml:"type,attr"`
	Location     location `xml:"location"`
	Checksum     checksum `xml:"checksum"`
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
	XMLName  xml.Name `xml:"metadata"`
	Packages []rpmPkg `xml:"package"`
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
func Mirror(baseURL, mirrorlistURL, metalinkURL, preferredMirror, primaryMetadata, destDir, repoName, gpgKeyURL string, workers int, dl *downloader.Client) error {
	sources, err := downloader.ResolveSourceURLs(baseURL, mirrorlistURL, metalinkURL, preferredMirror, "RPM", dl)
	if err != nil {
		return fmt.Errorf("[rpm] %s: resolve sources: %w", repoName, err)
	}
	ss := downloader.NewSourceSet(sources)

	baseURL = strings.TrimRight(ss.Primary(), "/")

	log.Printf("[rpm] %s  →  %s", baseURL, destDir)
	log.Printf("[rpm] %s: preparing metadata (workers=%d)", repoName, workers)
	if len(sources) > 1 {
		log.Printf("[rpm] %s: source failover enabled (%d sources)", repoName, len(sources))
	}

	// Fetch and import the GPG key.
	keysDir := filepath.Join(destDir, "gpg-keys")
	if err := gpg.FetchAndImport(gpgKeyURL, keysDir, dl); err != nil {
		log.Printf("[rpm] %s: GPG key warning: %v", repoName, err)
	}

	// Fetch repomd.xml (always into memory so dry-run can parse it).
	repomdRel := "repodata/repomd.xml"
	repomdURL := baseURL + "/" + repomdRel
	repomdDest := filepath.Join(destDir, "repodata", "repomd.xml")
	log.Printf("[rpm] %s: fetching repomd.xml", repoName)
	repomdData, usedBase, err := downloader.FetchBytesFromSources(dl, ss, repomdRel)
	if err != nil {
		return fmt.Errorf("[rpm] %s: fetch repomd.xml: %w", repoName, err)
	}
	repomdURL = strings.TrimRight(usedBase, "/") + "/" + repomdRel
	if !dl.DryRun {
		// Write the fresh repomd.xml atomically — we already have the bytes in
		// memory. Using a direct write (rather than downloadFileFromSources)
		// ensures the file is updated on every run, not just the first.
		if err := os.MkdirAll(filepath.Dir(repomdDest), 0o755); err != nil {
			return fmt.Errorf("[rpm] %s: save repomd.xml: %w", repoName, err)
		}
		tmpRepomd := repomdDest + ".repomirror.tmp"
		if err := os.WriteFile(tmpRepomd, repomdData, 0o644); err != nil {
			_ = os.Remove(tmpRepomd) //nolint:errcheck
			return fmt.Errorf("[rpm] %s: save repomd.xml: %w", repoName, err)
		}
		if err := os.Rename(tmpRepomd, repomdDest); err != nil {
			_ = os.Remove(tmpRepomd) //nolint:errcheck
			return fmt.Errorf("[rpm] %s: save repomd.xml: %w", repoName, err)
		}
	} else {
		log.Printf("[dry-run] would download: %s", repomdURL)
	}

	// Download repomd.xml.asc / repomd.xml.key (signature) if present.
	// Fetch fresh each run so the sig always matches the current repomd.xml.
	for _, sigSuffix := range []string{".asc", ".key", ".sig"} {
		sigRel := repomdRel + sigSuffix
		sigURL := repomdURL + sigSuffix
		sigDest := repomdDest + sigSuffix
		if dl.DryRun {
			log.Printf("[dry-run] would download: %s", sigURL)
			continue
		}
		sigData, _, sigErr := downloader.FetchBytesFromSources(dl, ss, sigRel)
		if sigErr != nil {
			_ = os.Remove(sigDest) //nolint:errcheck
			continue
		}
		_ = os.WriteFile(sigDest, sigData, 0o644) //nolint:errcheck,gosec
	}
	var rmd repoMD
	if err := xml.Unmarshal(repomdData, &rmd); err != nil {
		return fmt.Errorf("[rpm] %s: parse repomd.xml: %w", repoName, err)
	}
	log.Printf("[rpm] %s: repomd.xml loaded (%d metadata records)", repoName, len(rmd.Data))

	// Download all metadata files listed in repomd.xml.
	// Track primary metadata URL/dest for later parsing.
	var primaryRel, primaryDest, primaryDBRel, primaryDBDest string
	for _, d := range rmd.Data {
		href := strings.TrimLeft(d.Location.Href, "/")
		fileDest, err := downloader.SafeJoin(destDir, filepath.FromSlash(href))
		if err != nil {
			log.Printf("[rpm] %s: metadata %s: %v", repoName, href, err)
			continue
		}
		if err := downloader.DownloadFileFromSources(dl, ss, href, fileDest, d.Checksum.Type, strings.TrimSpace(d.Checksum.Value), nil); err != nil {
			log.Printf("[rpm] %s: metadata %s: %v", repoName, href, err)
			continue
		}
		if d.Type == "primary" {
			primaryRel = href
			primaryDest = fileDest
		}
		if d.Type == "primary_db" {
			primaryDBRel = href
			primaryDBDest = fileDest
		}
	}

	if primaryRel == "" {
		return fmt.Errorf("[rpm] %s: no primary metadata found in repomd.xml", repoName)
	}
	mode, reason := resolvePrimaryMetadataMode(primaryMetadata, destDir, primaryDBRel != "")
	if reason != "" {
		log.Printf("[rpm] %s: primary metadata mode auto -> %s (%s)", repoName, mode, reason)
	}
	useSQLite := primaryDBRel != "" && mode != "xml"
	parseSource := "primary.xml"
	if useSQLite {
		parseSource = "primary.sqlite"
	}
	log.Printf("[rpm] %s: parsing primary metadata (%s)", repoName, parseSource)

	// Parse primary.xml — in dry-run mode fetch into memory since the file
	// was never written to disk.
	var packages []rpmPkg
	if dl.DryRun {
		if useSQLite {
			data, _, err := downloader.FetchBytesFromSources(dl, ss, primaryDBRel)
			if err == nil {
				packages, err = parsePrimaryDBBytes(data, fileExt(primaryDBRel), repoName)
			}
			if err != nil {
				log.Printf("[rpm] %s: primary sqlite unavailable, falling back to primary.xml: %v", repoName, err)
				data, _, err = downloader.FetchBytesFromSources(dl, ss, primaryRel)
				if err != nil {
					return fmt.Errorf("[rpm] %s: fetch primary for parse: %w", repoName, err)
				}
				packages, err = parsePrimaryBytes(data, fileExt(primaryRel), repoName)
				if err != nil {
					return fmt.Errorf("[rpm] %s: parse primary metadata: %w", repoName, err)
				}
			}
		} else {
			data, _, err := downloader.FetchBytesFromSources(dl, ss, primaryRel)
			if err != nil {
				return fmt.Errorf("[rpm] %s: fetch primary for parse: %w", repoName, err)
			}
			packages, err = parsePrimaryBytes(data, fileExt(primaryRel), repoName)
			if err != nil {
				return fmt.Errorf("[rpm] %s: parse primary metadata: %w", repoName, err)
			}
		}
	} else {
		var parseErr error
		if useSQLite {
			packages, parseErr = parsePrimaryDB(primaryDBDest, repoName)
			if parseErr != nil {
				log.Printf("[rpm] %s: primary sqlite unavailable, falling back to primary.xml: %v", repoName, parseErr)
				packages, parseErr = parsePrimary(primaryDest, repoName)
				if parseErr != nil {
					return fmt.Errorf("[rpm] %s: parse primary metadata: %w", repoName, parseErr)
				}
			}
		} else {
			packages, parseErr = parsePrimary(primaryDest, repoName)
			if parseErr != nil {
				return fmt.Errorf("[rpm] %s: parse primary metadata: %w", repoName, parseErr)
			}
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
	jobs := make(chan rpmPkg, len(packages))
	for _, p := range packages {
		jobs <- p
	}
	close(jobs)

	var wg sync.WaitGroup
	errs := make(chan error, len(packages))

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range jobs {
				href := strings.TrimLeft(j.Location.Href, "/")
				pkgDest, err := downloader.SafeJoin(destDir, filepath.FromSlash(href))
				if err != nil {
					errs <- fmt.Errorf("%s: %w", href, err)
					if prog != nil {
						prog.Done()
					}
					continue
				}
				if err := downloader.DownloadFileFromSources(dl, ss, href, pkgDest, j.Checksum.Type, strings.TrimSpace(j.Checksum.Value), prog); err != nil {
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
	return nil
}

// parsePrimary reads and parses a possibly gzip-compressed primary.xml file.
func parsePrimary(path, repoName string) ([]rpmPkg, error) {
	r, cleanup, err := openPossiblyCompressed(path)
	if err != nil {
		return nil, err
	}
	defer cleanup()
	total := progressTotalForStream(fileSizeOrUnknown(path), fileExt(path))
	r, stopProgress := withPrimaryParseProgress(r, repoName, "parsing primary metadata", total)
	defer stopProgress()

	var pm primaryMD
	if err := xml.NewDecoder(r).Decode(&pm); err != nil {
		return nil, err
	}
	return pm.Packages, nil
}

// fileExt returns the lowercase extension of a URL or file path.
func fileExt(path string) string {
	lower := strings.ToLower(path)
	for _, ext := range []string{".gz", ".xz", ".bz2", ".zst", ".zstd"} {
		if strings.HasSuffix(lower, ext) {
			return ext
		}
	}
	return ""
}

// parsePrimaryBytes parses a primary.xml from an in-memory byte slice.
// ext is the file extension (".gz", ".xz", or "") indicating compression.
func parsePrimaryBytes(data []byte, ext, repoName string) ([]rpmPkg, error) {
	r, cleanup, err := openPossiblyCompressedBytes(data, ext)
	if err != nil {
		return nil, err
	}
	defer cleanup()
	total := progressTotalForStream(int64(len(data)), ext)
	r, stopProgress := withPrimaryParseProgress(r, repoName, "parsing primary metadata", total)
	defer stopProgress()

	var pm primaryMD
	if err := xml.NewDecoder(r).Decode(&pm); err != nil {
		return nil, err
	}
	return pm.Packages, nil
}

func parsePrimaryDB(path, repoName string) ([]rpmPkg, error) {
	r, cleanup, err := openPossiblyCompressed(path)
	if err != nil {
		return nil, err
	}
	defer cleanup()
	total := progressTotalForStream(fileSizeOrUnknown(path), fileExt(path))
	r, stopProgress := withPrimaryParseProgress(r, repoName, "loading primary sqlite", total)

	tmp, err := os.CreateTemp("", "repomirror-primary-*.sqlite")
	if err != nil {
		stopProgress()
		return nil, err
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)
	defer tmp.Close()

	if _, err := io.Copy(tmp, r); err != nil {
		stopProgress()
		return nil, err
	}
	if err := tmp.Close(); err != nil {
		stopProgress()
		return nil, err
	}
	stopProgress() // end byte-progress before starting the SQL item-progress

	return queryPrimarySQLite(tmpPath, repoName)
}

func parsePrimaryDBBytes(data []byte, ext, repoName string) ([]rpmPkg, error) {
	r, cleanup, err := openPossiblyCompressedBytes(data, ext)
	if err != nil {
		return nil, err
	}
	defer cleanup()
	total := progressTotalForStream(int64(len(data)), ext)
	r, stopProgress := withPrimaryParseProgress(r, repoName, "loading primary sqlite", total)

	tmp, err := os.CreateTemp("", "repomirror-primary-*.sqlite")
	if err != nil {
		stopProgress()
		return nil, err
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)
	defer tmp.Close()

	if _, err := io.Copy(tmp, r); err != nil {
		stopProgress()
		return nil, err
	}
	if err := tmp.Close(); err != nil {
		stopProgress()
		return nil, err
	}
	stopProgress() // end byte-progress before starting the SQL item-progress

	return queryPrimarySQLite(tmpPath, repoName)
}

func queryPrimarySQLite(path, repoName string) ([]rpmPkg, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	defer db.Close()

	var total int64
	if err := db.QueryRow("SELECT COUNT(*) FROM packages").Scan(&total); err != nil {
		return nil, err
	}

	progress := newParseItemProgress(repoName, "parsing primary metadata", total)
	defer progress.finish()

	rows, err := db.Query("SELECT location_href, pkgId, checksum_type FROM packages")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	packages := make([]rpmPkg, 0, total)
	for rows.Next() {
		var href, pkgID, checksumType string
		if err := rows.Scan(&href, &pkgID, &checksumType); err != nil {
			return nil, err
		}
		packages = append(packages, rpmPkg{
			Location: pkgLocation{Href: href},
			Checksum: checksum{Type: checksumType, Value: pkgID},
		})
		progress.inc()
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return packages, nil
}

type parseProgressReader struct {
	r io.Reader
	n *atomic.Int64
}

func (p *parseProgressReader) Read(b []byte) (int, error) {
	n, err := p.r.Read(b)
	if n > 0 {
		p.n.Add(int64(n))
	}
	return n, err
}

func withPrimaryParseProgress(r io.Reader, repoName, stage string, total int64) (io.Reader, func()) {
	if repoName == "" {
		return r, func() {}
	}
	var bytesRead atomic.Int64
	wrapped := &parseProgressReader{r: r, n: &bytesRead}
	stop := make(chan struct{})
	interactive := downloader.IsInteractiveStderr()
	start := time.Now()
	go func() {
		t := time.NewTicker(3 * time.Second)
		defer t.Stop()
		spinner := []string{"|", "/", "-", "\\"}
		spin := 0
		lastLogged := int64(-1)
		for {
			select {
			case <-t.C:
				current := bytesRead.Load()
				line := formatParseProgressLine(repoName, stage, current, total, time.Since(start), spinner[spin%len(spinner)])
				if interactive {
					fmt.Fprintf(os.Stderr, "\r\033[K%s", line)
					spin++
					continue
				}
				if current != lastLogged {
					log.Printf("%s", line)
					lastLogged = current
				}
			case <-stop:
				if interactive {
					fmt.Fprint(os.Stderr, "\r\033[K")
				}
				return
			}
		}
	}()
	var once sync.Once
	return wrapped, func() { once.Do(func() { close(stop) }) }
}

type parseItemProgress struct {
	repoName    string
	stage       string
	total       int64
	done        atomic.Int64
	start       time.Time
	stop        chan struct{}
	interactive bool
}

func newParseItemProgress(repoName, stage string, total int64) *parseItemProgress {
	p := &parseItemProgress{
		repoName:    repoName,
		stage:       stage,
		total:       total,
		start:       time.Now(),
		stop:        make(chan struct{}),
		interactive: downloader.IsInteractiveStderr(),
	}
	go p.loop()
	return p
}

func (p *parseItemProgress) loop() {
	t := time.NewTicker(1 * time.Second)
	defer t.Stop()
	spinner := []string{"|", "/", "-", "\\"}
	spin := 0
	last := int64(-1)
	for {
		select {
		case <-t.C:
			done := p.done.Load()
			line := formatParseItemLine(p.repoName, p.stage, done, p.total, time.Since(p.start), spinner[spin%len(spinner)])
			if p.interactive {
				fmt.Fprintf(os.Stderr, "\r\033[K%s", line)
				spin++
				continue
			}
			if done != last {
				log.Printf("%s", line)
				last = done
			}
		case <-p.stop:
			if p.interactive {
				fmt.Fprint(os.Stderr, "\r\033[K")
			}
			return
		}
	}
}

func (p *parseItemProgress) inc() {
	p.done.Add(1)
}

func (p *parseItemProgress) finish() {
	close(p.stop)
}

func formatParseProgressLine(repoName, stage string, processed, total int64, elapsed time.Duration, spinner string) string {
	speed := float64(processed) / max(elapsed.Seconds(), 0.001)
	prefix := formatParseRepoPrefix(repoName)
	if total > 0 {
		pct := float64(processed) / float64(total) * 100
		eta := ""
		if processed > 0 && processed < total {
			remaining := float64(total-processed) / speed
			eta = " eta " + downloader.FmtDuration(time.Duration(remaining)*time.Second)
		}
		return fmt.Sprintf("%s %s... %s %s/%s (%.1f%%) %s/s%s", prefix, stage, spinner, downloader.FmtBytes(float64(processed)), downloader.FmtBytes(float64(total)), pct, downloader.FmtBytes(speed), eta)
	}
	return fmt.Sprintf("%s %s... %s %s processed %s/s elapsed %s", prefix, stage, spinner, downloader.FmtBytes(float64(processed)), downloader.FmtBytes(speed), downloader.FmtDuration(elapsed))
}

func formatParseItemLine(repoName, stage string, done, total int64, elapsed time.Duration, spinner string) string {
	prefix := formatParseRepoPrefix(repoName)
	rate := float64(done) / max(elapsed.Seconds(), 0.001)
	if total > 0 {
		pct := float64(done) / float64(total) * 100
		eta := ""
		if done > 0 && done < total {
			remaining := float64(total-done) / rate
			eta = " eta " + downloader.FmtDuration(time.Duration(remaining)*time.Second)
		}
		return fmt.Sprintf("%s %s... %s %d/%d (%.1f%%) %.0f/s%s", prefix, stage, spinner, done, total, pct, rate, eta)
	}
	return fmt.Sprintf("%s %s... %s %d processed %.0f/s elapsed %s", prefix, stage, spinner, done, rate, downloader.FmtDuration(elapsed))
}

func formatParseRepoPrefix(repoName string) string {
	icon := "pkg"
	if downloader.SupportsUnicode() {
		icon = "📥"
	}
	return fmt.Sprintf("[RPM] %s %s:", icon, repoName)
}

func fileSizeOrUnknown(path string) int64 {
	info, err := os.Stat(path)
	if err != nil {
		return -1
	}
	return info.Size()
}

func progressTotalForStream(total int64, ext string) int64 {
	switch strings.ToLower(ext) {
	case ".xz", ".gz", ".bz2":
		// The progress reader measures decompressed bytes, so compressed size is
		// not a valid denominator for percent/eta.
		return -1
	default:
		return total
	}
}

func resolvePrimaryMetadataMode(rawMode, destDir string, hasPrimaryDB bool) (string, string) {
	mode := strings.ToLower(strings.TrimSpace(rawMode))
	if mode == "" {
		mode = "auto"
	}

	if mode == "sqlite" && !hasPrimaryDB {
		return "xml", "primary_db unavailable"
	}
	if mode == "xml" || mode == "sqlite" {
		return mode, ""
	}

	if !hasPrimaryDB {
		return "xml", "primary_db unavailable"
	}

	fsType, ok := filesystemTypeForPath(destDir)
	if ok {
		fs := strings.ToLower(fsType)
		switch fs {
		case "drvfs", "fuseblk", "ntfs", "ntfs3", "exfat", "vfat", "msdos", "fuse", "9p", "v9fs":
			return "xml", "filesystem=" + fsType
		}
	}

	return "sqlite", ""
}

func filesystemTypeForPath(target string) (string, bool) {
	f, err := os.Open("/proc/self/mountinfo")
	if err != nil {
		return "", false
	}
	defer f.Close()

	target = filepath.Clean(target)
	bestMount := ""
	bestFS := ""

	s := bufio.NewScanner(f)
	for s.Scan() {
		line := s.Text()
		parts := strings.SplitN(line, " - ", 2)
		if len(parts) != 2 {
			continue
		}
		left := strings.Fields(parts[0])
		right := strings.Fields(parts[1])
		if len(left) < 5 || len(right) < 1 {
			continue
		}
		mountPoint := decodeMountEscapes(left[4])
		if !pathWithinMount(target, mountPoint) {
			continue
		}
		if len(mountPoint) >= len(bestMount) {
			bestMount = mountPoint
			bestFS = right[0]
		}
	}

	if bestFS == "" {
		return "", false
	}
	return bestFS, true
}

func pathWithinMount(path, mountPoint string) bool {
	if path == mountPoint {
		return true
	}
	if mountPoint == "/" {
		return strings.HasPrefix(path, "/")
	}
	return strings.HasPrefix(path, mountPoint+"/")
}

func decodeMountEscapes(s string) string {
	replacer := strings.NewReplacer(
		`\040`, " ",
		`\011`, "\t",
		`\012`, "\n",
		`\134`, "\\",
	)
	return replacer.Replace(s)
}
