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
	"slices"
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
	sources, err := resolveRPMSourceURLs(baseURL, mirrorlistURL, metalinkURL, preferredMirror, dl)
	if err != nil {
		return fmt.Errorf("[rpm] %s: resolve sources: %w", repoName, err)
	}
	ss := newSourceSet(sources)

	baseURL = strings.TrimRight(ss.primary(), "/")

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
	repomdData, usedBase, err := fetchBytesFromSources(dl, ss, repomdRel)
	if err != nil {
		return fmt.Errorf("[rpm] %s: fetch repomd.xml: %w", repoName, err)
	}
	repomdURL = strings.TrimRight(usedBase, "/") + "/" + repomdRel
	if !dl.DryRun {
		if err := downloadFileFromSources(dl, ss, repomdRel, repomdDest, "", "", nil); err != nil {
			return fmt.Errorf("[rpm] %s: save repomd.xml: %w", repoName, err)
		}
	} else {
		log.Printf("[dry-run] would download: %s", repomdURL)
	}

	// Download repomd.xml.asc / repomd.xml.key (signature) if present.
	for _, sigSuffix := range []string{".asc", ".key", ".sig"} {
		sigRel := repomdRel + sigSuffix
		sigURL := repomdURL + sigSuffix
		sigDest := repomdDest + sigSuffix
		if err := downloadFileFromSources(dl, ss, sigRel, sigDest, "", "", nil); err != nil && dl.DryRun {
			log.Printf("[dry-run] would download: %s", sigURL)
		}
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
		if err := downloadFileFromSources(dl, ss, href, fileDest, d.Checksum.Type, strings.TrimSpace(d.Checksum.Value), nil); err != nil {
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
			data, _, err := fetchBytesFromSources(dl, ss, primaryDBRel)
			if err == nil {
				packages, err = parsePrimaryDBBytes(data, fileExt(primaryDBRel), repoName)
			}
			if err != nil {
				log.Printf("[rpm] %s: primary sqlite unavailable, falling back to primary.xml: %v", repoName, err)
				data, _, err = fetchBytesFromSources(dl, ss, primaryRel)
				if err != nil {
					return fmt.Errorf("[rpm] %s: fetch primary for parse: %w", repoName, err)
				}
				packages, err = parsePrimaryBytes(data, fileExt(primaryRel), repoName)
				if err != nil {
					return fmt.Errorf("[rpm] %s: parse primary metadata: %w", repoName, err)
				}
			}
		} else {
			data, _, err := fetchBytesFromSources(dl, ss, primaryRel)
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
				pkgDest, err := downloader.SafeJoin(destDir, filepath.FromSlash(href))
				if err != nil {
					errs <- fmt.Errorf("%s: %w", href, err)
					if prog != nil {
						prog.Done()
					}
					continue
				}
				if err := downloadFileFromSources(dl, ss, href, pkgDest, j.pkg.Checksum.Type, strings.TrimSpace(j.pkg.Checksum.Value), prog); err != nil {
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

func resolveRPMSourceURLs(baseURL, mirrorlistURL, metalinkURL, preferred string, dl *downloader.Client) ([]string, error) {
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

	add(baseURL)

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
		return nil, fmt.Errorf("no RPM source URL configured (set base_url, mirrorlist, or metalink)")
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
	for _, ext := range []string{".gz", ".xz", ".bz2"} {
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
	defer stopProgress()

	tmp, err := os.CreateTemp("", "repomirror-primary-*.sqlite")
	if err != nil {
		return nil, err
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)
	defer tmp.Close()

	if _, err := io.Copy(tmp, r); err != nil {
		return nil, err
	}
	if err := tmp.Close(); err != nil {
		return nil, err
	}

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
	defer stopProgress()

	tmp, err := os.CreateTemp("", "repomirror-primary-*.sqlite")
	if err != nil {
		return nil, err
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)
	defer tmp.Close()

	if _, err := io.Copy(tmp, r); err != nil {
		return nil, err
	}
	if err := tmp.Close(); err != nil {
		return nil, err
	}

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
	interactive := isInteractiveStderr()
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
	stopped := false
	return wrapped, func() {
		if stopped {
			return
		}
		close(stop)
		stopped = true
	}
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
		interactive: isInteractiveStderr(),
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
			eta = " eta " + formatDuration(time.Duration(remaining)*time.Second)
		}
		return fmt.Sprintf("%s %s... %s %s/%s (%.1f%%) %s/s%s", prefix, stage, spinner, formatBytes(float64(processed)), formatBytes(float64(total)), pct, formatBytes(speed), eta)
	}
	return fmt.Sprintf("%s %s... %s %s processed %s/s elapsed %s", prefix, stage, spinner, formatBytes(float64(processed)), formatBytes(speed), formatDuration(elapsed))
}

func formatParseItemLine(repoName, stage string, done, total int64, elapsed time.Duration, spinner string) string {
	prefix := formatParseRepoPrefix(repoName)
	rate := float64(done) / max(elapsed.Seconds(), 0.001)
	if total > 0 {
		pct := float64(done) / float64(total) * 100
		eta := ""
		if done > 0 && done < total {
			remaining := float64(total-done) / rate
			eta = " eta " + formatDuration(time.Duration(remaining)*time.Second)
		}
		return fmt.Sprintf("%s %s... %s %d/%d (%.1f%%) %.0f/s%s", prefix, stage, spinner, done, total, pct, rate, eta)
	}
	return fmt.Sprintf("%s %s... %s %d processed %.0f/s elapsed %s", prefix, stage, spinner, done, rate, formatDuration(elapsed))
}

func formatParseRepoPrefix(repoName string) string {
	icon := "pkg"
	if parseProgressSupportsUnicode() {
		icon = "📥"
	}
	return fmt.Sprintf("[RPM] %s %s:", icon, repoName)
}

func parseProgressSupportsUnicode() bool {
	check := strings.ToUpper(os.Getenv("LC_ALL") + " " + os.Getenv("LC_CTYPE") + " " + os.Getenv("LANG"))
	return strings.Contains(check, "UTF-8") || strings.Contains(check, "UTF8")
}

func formatDuration(d time.Duration) string {
	d = d.Round(time.Second)
	h := int(d.Hours())
	m := int(d.Minutes()) % 60
	s := int(d.Seconds()) % 60
	if h > 0 {
		return fmt.Sprintf("%dh%02dm%02ds", h, m, s)
	}
	if m > 0 {
		return fmt.Sprintf("%dm%02ds", m, s)
	}
	return fmt.Sprintf("%ds", s)
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

func isInteractiveStderr() bool {
	if os.Getenv("TERM") == "dumb" {
		return false
	}
	fi, err := os.Stderr.Stat()
	if err != nil {
		return false
	}
	return (fi.Mode() & os.ModeCharDevice) != 0
}

func formatBytes(n float64) string {
	switch {
	case n >= 1e9:
		return fmt.Sprintf("%.1f GB", n/1e9)
	case n >= 1e6:
		return fmt.Sprintf("%.1f MB", n/1e6)
	case n >= 1e3:
		return fmt.Sprintf("%.1f KB", n/1e3)
	default:
		return fmt.Sprintf("%.0f B", n)
	}
}
