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

	_ "modernc.org/sqlite" // register sqlite driver
)

const (
	primaryMetaXML    = "xml"
	primaryMetaSQLite = "sqlite"
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
	Arch     string      `xml:"arch"`
	Location pkgLocation `xml:"location"`
	Checksum checksum    `xml:"checksum"`
}

type pkgLocation struct {
	Href string `xml:"href,attr"`
}

// ---------- Mirror ----------

// Options describes one YUM/DNF repository entry from the config.
type Options struct {
	Name             string
	BaseURL          string
	Mirrorlist       string
	Metalink         string
	PreferredMirror  string
	PrimaryMetadata  string
	GPGKey           string
	DestDir          string
	Arches           []string
	Workers          int
	VerifySignatures bool
}

// Result lists the files a run wants to keep under DestDir. Complete is true
// only when all metadata and packages were mirrored without errors.
type Result struct {
	Referenced map[string]bool
	Complete   bool
}

var repomdSigSuffixes = []string{".asc", ".key", ".sig"}

// Mirror downloads a YUM/DNF repository into o.DestDir, preserving the
// upstream layout. repomd.xml is written after all metadata and packages are
// in place.
func Mirror(o Options, dl *downloader.Client) (*Result, error) { //nolint:gocyclo
	res := &Result{Referenced: map[string]bool{}}
	repoName := o.Name
	sources, err := downloader.ResolveSourceURLs(o.BaseURL, o.Mirrorlist, o.Metalink, o.PreferredMirror, "RPM", dl)
	if err != nil {
		return res, fmt.Errorf("[rpm] %s: resolve sources: %w", repoName, err)
	}
	ss := downloader.NewSourceSet(sources)
	destDir := o.DestDir

	log.Printf("[rpm] %s  →  %s", strings.TrimRight(ss.Primary(), "/"), destDir)
	log.Printf("[rpm] %s: preparing metadata (workers=%d)", repoName, o.Workers)
	if len(sources) > 1 {
		log.Printf("[rpm] %s: source failover enabled (%d sources)", repoName, len(sources))
	}

	kr, keyPath, err := gpg.Fetch(o.GPGKey, filepath.Join(destDir, gpg.KeysDirName), dl)
	if err != nil {
		if o.VerifySignatures {
			return res, fmt.Errorf("[rpm] %s: %w", repoName, err)
		}
		log.Printf("[rpm] %s: GPG key warning: %v", repoName, err)
	} else if keyPath != "" {
		res.Referenced[keyPath] = true
	}

	repomdRel := "repodata/repomd.xml"
	repomdDest := filepath.Join(destDir, "repodata", "repomd.xml")
	log.Printf("[rpm] %s: fetching repomd.xml", repoName)
	repomdData, usedBase, err := downloader.FetchBytesFromSources(dl, ss, repomdRel)
	if err != nil {
		return res, fmt.Errorf("[rpm] %s: fetch repomd.xml: %w", repoName, err)
	}
	var rmd repoMD
	if err := xml.Unmarshal(repomdData, &rmd); err != nil {
		return res, fmt.Errorf("[rpm] %s: repomd.xml is not valid repository metadata (is base_url correct?): %w", repoName, err)
	}
	if len(rmd.Data) == 0 {
		return res, fmt.Errorf("[rpm] %s: repomd.xml lists no metadata", repoName)
	}
	log.Printf("[rpm] %s: repomd.xml loaded (%d metadata records)", repoName, len(rmd.Data))

	sigFiles := map[string][]byte{}
	for _, suffix := range repomdSigSuffixes {
		data, err := dl.FetchBytes(strings.TrimRight(usedBase, "/") + "/" + repomdRel + suffix)
		if err == nil {
			sigFiles[suffix] = data
		}
	}
	if o.VerifySignatures && kr != nil {
		if sig, ok := sigFiles[".asc"]; ok {
			if err := kr.VerifyDetached(repomdData, sig); err != nil {
				return res, fmt.Errorf("[rpm] %s: repomd.xml: %w", repoName, err)
			}
			log.Printf("[rpm] %s: repomd.xml signature verified", repoName)
		} else {
			log.Printf("[rpm] %s: repomd.xml is not signed upstream; relying on package signatures", repoName)
		}
	} else if o.VerifySignatures {
		log.Printf("[rpm] %s: warning: no gpg_key set, repomd.xml signature is not checked", repoName)
	}

	var staged []downloader.Staged
	var referenced []string
	var primaryHref, primaryPath, primaryDBHref, primaryDBPath string
	var metaErrors int
	for _, d := range rmd.Data {
		href := strings.TrimLeft(d.Location.Href, "/")
		fileDest, err := downloader.SafeJoin(destDir, filepath.FromSlash(href))
		if err != nil {
			log.Printf("[rpm] %s: metadata %s: %v", repoName, href, err)
			metaErrors++
			continue
		}
		referenced = append(referenced, fileDest)
		readPath := fileDest
		if !dl.DryRun {
			p, st, err := downloader.StageFromSources(dl, ss, []string{href}, fileDest, d.Checksum.Type, strings.TrimSpace(d.Checksum.Value))
			if err != nil {
				log.Printf("[rpm] %s: metadata %s: %v", repoName, href, err)
				metaErrors++
				continue
			}
			if st != nil {
				staged = append(staged, *st)
			}
			readPath = p
		}
		switch d.Type {
		case "primary":
			primaryHref, primaryPath = href, readPath
		case "primary_db":
			primaryDBHref, primaryDBPath = href, readPath
		}
	}
	if metaErrors > 0 {
		return res, fmt.Errorf("[rpm] %s: %d metadata file(s) failed to download", repoName, metaErrors)
	}
	if primaryHref == "" {
		return res, fmt.Errorf("[rpm] %s: no primary metadata found in repomd.xml", repoName)
	}

	mode, reason := resolvePrimaryMetadataMode(o.PrimaryMetadata, destDir, primaryDBHref != "")
	if reason != "" {
		log.Printf("[rpm] %s: primary metadata mode auto -> %s (%s)", repoName, mode, reason)
	}
	useSQLite := primaryDBHref != "" && mode != primaryMetaXML
	parseSource := "primary.xml"
	if useSQLite {
		parseSource = "primary.sqlite"
	}
	log.Printf("[rpm] %s: parsing primary metadata (%s)", repoName, parseSource)

	var packages []rpmPkg
	if dl.DryRun {
		if useSQLite {
			data, _, err := downloader.FetchBytesFromSources(dl, ss, primaryDBHref)
			if err == nil {
				packages, err = parsePrimaryDBBytes(data, fileExt(primaryDBHref), repoName)
			}
			if err != nil {
				log.Printf("[rpm] %s: primary sqlite unavailable, falling back to primary.xml: %v", repoName, err)
				useSQLite = false
			}
		}
		if !useSQLite {
			data, _, err := downloader.FetchBytesFromSources(dl, ss, primaryHref)
			if err != nil {
				return res, fmt.Errorf("[rpm] %s: fetch primary for parse: %w", repoName, err)
			}
			packages, err = parsePrimaryBytes(data, fileExt(primaryHref), repoName)
			if err != nil {
				return res, fmt.Errorf("[rpm] %s: parse primary metadata: %w", repoName, err)
			}
		}
	} else {
		var parseErr error
		if useSQLite {
			packages, parseErr = parsePrimaryDB(primaryDBPath, fileExt(primaryDBHref), repoName)
			if parseErr != nil {
				log.Printf("[rpm] %s: primary sqlite unavailable, falling back to primary.xml: %v", repoName, parseErr)
				useSQLite = false
			}
		}
		if !useSQLite {
			packages, parseErr = parsePrimary(primaryPath, fileExt(primaryHref), repoName)
			if parseErr != nil {
				return res, fmt.Errorf("[rpm] %s: parse primary metadata: %w", repoName, parseErr)
			}
		}
	}

	total := len(packages)
	packages = filterArches(packages, o.Arches)
	if len(o.Arches) > 0 {
		log.Printf("[rpm] %s: arches=%s keeps %d of %d packages", repoName, strings.Join(o.Arches, ","), len(packages), total)
	}

	type job struct{ href, dest, algo, sum string }
	seen := map[string]bool{}
	var jobsList []job
	var pathErrors int
	for _, p := range packages {
		href := strings.TrimLeft(p.Location.Href, "/")
		dest, err := downloader.SafeJoin(destDir, filepath.FromSlash(href))
		if err != nil {
			log.Printf("[rpm] %s: package %s: %v", repoName, href, err)
			pathErrors++
			continue
		}
		if seen[dest] {
			continue
		}
		seen[dest] = true
		jobsList = append(jobsList, job{href, dest, p.Checksum.Type, strings.TrimSpace(p.Checksum.Value)})
	}
	log.Printf("[rpm] %s: %d packages to mirror", repoName, len(jobsList))

	var prog *downloader.Counter
	var stopProg chan struct{}
	if !dl.DryRun {
		prog = downloader.NewCounter(repoName, "rpm", len(jobsList))
		stopProg = make(chan struct{})
		prog.StartLogger(1*time.Second, stopProg)
	}

	jobs := make(chan job, len(jobsList))
	for _, j := range jobsList {
		jobs <- j
	}
	close(jobs)

	var wg sync.WaitGroup
	errs := make(chan error, len(jobsList))
	for i := 0; i < max(o.Workers, 1); i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range jobs {
				if err := downloader.DownloadFileFromSources(dl, ss, j.href, j.dest, j.algo, j.sum, prog); err != nil {
					errs <- fmt.Errorf("%s: %w", j.href, err)
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

	errCount := pathErrors
	for err := range errs {
		log.Printf("[rpm] %s: package error: %v", repoName, err)
		errCount++
	}
	if errCount > 0 {
		return res, fmt.Errorf("[rpm] %s: %d package(s) failed to download; repomd.xml not updated", repoName, errCount)
	}

	if !dl.DryRun {
		for _, st := range staged {
			if err := st.Publish(); err != nil {
				return res, fmt.Errorf("[rpm] %s: publish metadata: %w", repoName, err)
			}
		}
		for _, suffix := range repomdSigSuffixes {
			dest := repomdDest + suffix
			if data, ok := sigFiles[suffix]; ok {
				if err := downloader.WriteFileAtomic(dest, data); err != nil {
					return res, fmt.Errorf("[rpm] %s: save repomd.xml%s: %w", repoName, suffix, err)
				}
			} else {
				_ = os.Remove(dest) //nolint:errcheck
			}
		}
		if err := downloader.WriteFileAtomic(repomdDest, repomdData); err != nil {
			return res, fmt.Errorf("[rpm] %s: save repomd.xml: %w", repoName, err)
		}
	}

	res.Referenced[repomdDest] = true
	for suffix := range sigFiles {
		res.Referenced[repomdDest+suffix] = true
	}
	for _, p := range referenced {
		res.Referenced[p] = true
	}
	for _, j := range jobsList {
		res.Referenced[j.dest] = true
	}
	res.Complete = true
	return res, nil
}

// filterArches keeps packages whose architecture is listed in arches. noarch
// packages are always kept. An empty list keeps everything.
func filterArches(pkgs []rpmPkg, arches []string) []rpmPkg {
	if len(arches) == 0 {
		return pkgs
	}
	keep := map[string]bool{"noarch": true}
	for _, a := range arches {
		keep[strings.TrimSpace(a)] = true
	}
	out := pkgs[:0]
	for _, p := range pkgs {
		if keep[strings.TrimSpace(p.Arch)] {
			out = append(out, p)
		}
	}
	return out
}

// parsePrimary reads and parses a possibly gzip-compressed primary.xml file.
func parsePrimary(path, ext, repoName string) ([]rpmPkg, error) {
	r, cleanup, err := openPossiblyCompressed(path, ext)
	if err != nil {
		return nil, err
	}
	defer cleanup()
	total := progressTotalForStream(fileSizeOrUnknown(path), ext)
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

func parsePrimaryDB(path, ext, repoName string) ([]rpmPkg, error) {
	r, cleanup, err := openPossiblyCompressed(path, ext)
	if err != nil {
		return nil, err
	}
	defer cleanup()
	total := progressTotalForStream(fileSizeOrUnknown(path), ext)
	r, stopProgress := withPrimaryParseProgress(r, repoName, "loading primary sqlite", total)

	tmp, err := os.CreateTemp("", "repomirror-primary-*.sqlite")
	if err != nil {
		stopProgress()
		return nil, err
	}
	tmpPath := tmp.Name()
	defer func() { _ = os.Remove(tmpPath) }()
	defer func() { _ = tmp.Close() }()

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
	defer func() { _ = os.Remove(tmpPath) }()
	defer func() { _ = tmp.Close() }()

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
	defer func() { _ = db.Close() }()

	var total int64
	if countErr := db.QueryRow("SELECT COUNT(*) FROM packages").Scan(&total); countErr != nil {
		return nil, countErr
	}

	progress := newParseItemProgress(repoName, "parsing primary metadata", total)
	defer progress.finish()

	rows, err := db.Query("SELECT location_href, pkgId, checksum_type, COALESCE(arch, '') FROM packages")
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	packages := make([]rpmPkg, 0, total)
	for rows.Next() {
		var href, pkgID, checksumType, arch string
		if err := rows.Scan(&href, &pkgID, &checksumType, &arch); err != nil {
			return nil, err
		}
		packages = append(packages, rpmPkg{
			Arch:     arch,
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

	if mode == primaryMetaSQLite && !hasPrimaryDB {
		return primaryMetaXML, "primary_db unavailable"
	}
	if mode == "xml" || mode == primaryMetaSQLite {
		return mode, ""
	}

	if !hasPrimaryDB {
		return primaryMetaXML, "primary_db unavailable"
	}

	fsType, ok := filesystemTypeForPath(destDir)
	if ok {
		fs := strings.ToLower(fsType)
		switch fs {
		case "drvfs", "fuseblk", "ntfs", "ntfs3", "exfat", "vfat", "msdos", "fuse", "9p", "v9fs":
			return primaryMetaXML, "filesystem=" + fsType
		}
	}

	return primaryMetaSQLite, ""
}

func filesystemTypeForPath(target string) (string, bool) {
	f, err := os.Open("/proc/self/mountinfo")
	if err != nil {
		return "", false
	}
	defer func() { _ = f.Close() }()

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
