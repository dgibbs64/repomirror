// Package deb mirrors APT (Debian/Ubuntu) repositories by parsing InRelease
// and Packages metadata files.
package deb

import (
	"bufio"
	"bytes"
	"compress/bzip2"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/ulikunitz/xz"

	"repomirror/downloader"
	"repomirror/gpg"
)

const (
	algoMD5    = "md5"
	algoSHA1   = "sha1"
	algoSHA256 = "sha256"
	algoSHA512 = "sha512"
)

// Options describes one APT repository entry from the config.
type Options struct {
	Name             string
	MirrorURL        string
	Mirrorlist       string
	Metalink         string
	PreferredMirror  string
	GPGKey           string
	DestDir          string
	Suites           []string
	Components       []string
	Arches           []string
	Workers          int
	VerifySignatures bool
}

// Result lists the files a run wants to keep under DestDir. Complete is true
// only when every suite and package was mirrored without errors. Packages,
// Failed and Bytes count the package files handled, failed and downloaded.
type Result struct {
	Referenced map[string]bool
	Complete   bool
	Packages   int
	Failed     int
	Bytes      int64
}

type pkgEntry struct {
	relPath, dest, algo, checksum string
}

type memFile struct {
	path string
	data []byte
}

type byHashCopy struct {
	src, dst, algo, sum string
}

type suitePlan struct {
	suite      string
	pkgs       []pkgEntry
	staged     []downloader.Staged
	byHash     []byHashCopy
	release    []memFile
	referenced []string
}

const (
	extGz  = ".gz"
	extXz  = ".xz"
	extBz2 = ".bz2"
)

// compression variants in the order they are preferred for parsing.
var packagesVariants = []string{extGz, extXz, extBz2, ""}
var translationVariants = []string{"", extGz, extBz2, extXz}

// Mirror downloads one APT repository for all requested suites and
// components into o.DestDir, preserving the upstream directory layout.
// Release files are written after all metadata and packages are in place.
func Mirror(o Options, dl *downloader.Client) (*Result, error) { //nolint:gocyclo
	res := &Result{Referenced: map[string]bool{}}
	sources, err := downloader.ResolveSourceURLs(o.MirrorURL, o.Mirrorlist, o.Metalink, o.PreferredMirror, "DEB", dl)
	if err != nil {
		return res, fmt.Errorf("[deb] %s: resolve sources: %w", o.Name, err)
	}
	ss := downloader.NewSourceSet(sources)

	log.Printf("[deb] %s  →  %s", strings.TrimRight(ss.Primary(), "/"), o.DestDir)
	if len(sources) > 1 {
		log.Printf("[deb] %s: source failover enabled (%d sources)", o.Name, len(sources))
	}
	log.Printf("[deb] %s: suites=%s components=%s arches=%s workers=%d",
		o.Name, formatListForLog(o.Suites), formatListForLog(o.Components), formatListForLog(o.Arches), o.Workers)

	kr, keyPath, err := gpg.Fetch(o.GPGKey, filepath.Join(o.DestDir, gpg.KeysDirName), dl)
	if err != nil {
		if o.VerifySignatures {
			return res, fmt.Errorf("[deb] %s: %w", o.Name, err)
		}
		log.Printf("[deb] %s: GPG key warning: %v", o.Name, err)
	} else if keyPath != "" {
		res.Referenced[keyPath] = true
	}
	if o.VerifySignatures && kr == nil {
		log.Printf("[deb] %s: warning: no gpg_key set, Release signatures are not checked", o.Name)
	}

	var plans []*suitePlan
	suiteFailures := 0
	for _, suite := range o.Suites {
		log.Printf("[deb] %s suite %s: fetching Release metadata", o.Name, suite)
		plan, err := prepareSuite(dl, ss, o, kr, suite)
		if err != nil {
			log.Printf("[deb] %s suite %s: ERROR: %v", o.Name, suite, err)
			suiteFailures++
			continue
		}
		log.Printf("[deb] %s suite %s: discovered %d package entries", o.Name, suite, len(plan.pkgs))
		plans = append(plans, plan)
	}

	seen := map[string]bool{}
	var jobsList []pkgEntry
	for _, plan := range plans {
		for _, p := range plan.pkgs {
			if seen[p.dest] {
				continue
			}
			seen[p.dest] = true
			jobsList = append(jobsList, p)
		}
	}
	log.Printf("[deb] %s: %d packages to mirror", o.Name, len(jobsList))

	errCount, bytes := downloadPackages(dl, ss, o, jobsList)
	res.Packages = len(jobsList)
	res.Failed = errCount
	res.Bytes = bytes

	if suiteFailures > 0 || errCount > 0 {
		if errCount > 0 {
			log.Printf("[deb] %s: not publishing new Release files because %d package(s) failed", o.Name, errCount)
		}
		if errCount == 0 && !dl.DryRun {
			for _, plan := range plans {
				if err := plan.publish(); err != nil {
					return res, fmt.Errorf("[deb] %s suite %s: publish: %w", o.Name, plan.suite, err)
				}
			}
		}
		return res, fmt.Errorf("[deb] %s: %d suite(s) and %d package(s) failed", o.Name, suiteFailures, errCount)
	}

	if !dl.DryRun {
		for _, plan := range plans {
			if err := plan.publish(); err != nil {
				return res, fmt.Errorf("[deb] %s suite %s: publish: %w", o.Name, plan.suite, err)
			}
		}
	}
	for _, plan := range plans {
		for _, p := range plan.referenced {
			res.Referenced[p] = true
		}
	}
	for _, p := range jobsList {
		res.Referenced[p.dest] = true
	}
	res.Complete = true
	return res, nil
}

func downloadPackages(dl *downloader.Client, ss *downloader.SourceSet, o Options, list []pkgEntry) (int, int64) {
	var prog *downloader.Counter
	var stopProg chan struct{}
	if !dl.DryRun {
		prog = downloader.NewCounter(o.Name, "deb", len(list))
		stopProg = make(chan struct{})
		prog.StartLogger(1*time.Second, stopProg)
	}

	jobs := make(chan pkgEntry, len(list))
	for _, p := range list {
		jobs <- p
	}
	close(jobs)

	errs := make(chan error, len(list))
	var wg sync.WaitGroup
	for i := 0; i < max(o.Workers, 1); i++ {
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

	count := 0
	for err := range errs {
		log.Printf("[deb] %s: package error: %v", o.Name, err)
		count++
	}
	var bytes int64
	if prog != nil {
		bytes = prog.Bytes()
	}
	return count, bytes
}

// publish moves staged metadata into place, creates by-hash copies and then
// writes the Release files.
func (p *suitePlan) publish() error {
	for _, st := range p.staged {
		if err := st.Publish(); err != nil {
			return err
		}
	}
	for _, bh := range p.byHash {
		if err := ensureCopy(bh); err != nil {
			return err
		}
	}
	for _, f := range p.release {
		if err := downloader.WriteFileAtomic(f.path, f.data); err != nil {
			return err
		}
	}
	return nil
}

func ensureCopy(bh byHashCopy) error {
	if downloader.FileMatches(bh.dst, bh.algo, bh.sum) {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(bh.dst), 0o755); err != nil {
		return err
	}
	tmp := bh.dst + downloader.TempSuffix
	_ = os.Remove(tmp) //nolint:errcheck
	if err := os.Link(bh.src, tmp); err != nil {
		if err := copyFile(bh.src, tmp); err != nil {
			_ = os.Remove(tmp) //nolint:errcheck
			return err
		}
	}
	if err := os.Rename(tmp, bh.dst); err != nil {
		_ = os.Remove(tmp) //nolint:errcheck
		return err
	}
	downloader.ForgetPath(bh.dst)
	return nil
}

func copyFile(src, dst string) error {
	in, err := os.Open(src) // #nosec G304 – path derived from config
	if err != nil {
		return err
	}
	defer func() { _ = in.Close() }()
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644) // #nosec G304
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		_ = out.Close() //nolint:errcheck
		return err
	}
	return out.Close()
}

// prepareSuite fetches and checks the Release files for one suite, stages
// the metadata files it needs and collects the package list. Nothing visible
// to clients is changed. A suite of "/" is a flat repository.
func prepareSuite(dl *downloader.Client, ss *downloader.SourceSet, o Options, kr *gpg.Keyring, suite string) (*suitePlan, error) { //nolint:gocyclo
	flat := suite == "/"
	relBase := "dists/" + suite + "/"
	distDest := filepath.Join(o.DestDir, "dists", filepath.FromSlash(suite))
	if flat {
		relBase = ""
		distDest = o.DestDir
	}
	plan := &suitePlan{suite: suite}
	verify := o.VerifySignatures && kr != nil

	inRelease, _, inErr := downloader.FetchBytesFromSources(dl, ss, relBase+"InRelease")
	release, _, relErr := downloader.FetchBytesFromSources(dl, ss, relBase+"Release")
	var releaseSig []byte
	var sigErr error
	if relErr == nil {
		releaseSig, _, sigErr = downloader.FetchBytesFromSources(dl, ss, relBase+"Release.gpg")
	}
	if inErr != nil && relErr != nil {
		return nil, fmt.Errorf("fetch Release: %w", inErr)
	}

	var text []byte
	if inErr == nil {
		if verify {
			t, err := kr.VerifyClearsigned(inRelease)
			if err != nil {
				return nil, fmt.Errorf("InRelease: %w", err)
			}
			text = t
		} else {
			t := gpg.ClearsignedText(inRelease)
			if bytes.Equal(t, inRelease) {
				return nil, fmt.Errorf("InRelease: %w", gpg.ErrNotSigned)
			}
			text = t
		}
	}

	var releaseFiles []memFile
	if relErr == nil {
		ok := true
		if verify {
			switch {
			case sigErr != nil:
				ok = false
				if inErr != nil {
					return nil, fmt.Errorf("fetch Release.gpg: %w", sigErr)
				}
			default:
				if err := kr.VerifyDetached(release, releaseSig); err != nil {
					ok = false
					if inErr != nil {
						return nil, fmt.Errorf("verify Release: %w", err)
					}
					log.Printf("[deb] %s suite %s: skipping Release/Release.gpg: %v", o.Name, suite, err)
				}
			}
		}
		if ok {
			if text == nil {
				text = release
			}
			releaseFiles = append(releaseFiles, memFile{filepath.Join(distDest, "Release"), release})
			if sigErr == nil {
				releaseFiles = append(releaseFiles, memFile{filepath.Join(distDest, "Release.gpg"), releaseSig})
			}
		}
	}
	if inErr == nil {
		releaseFiles = append(releaseFiles, memFile{filepath.Join(distDest, "InRelease"), inRelease})
	}
	plan.release = releaseFiles
	for _, f := range releaseFiles {
		plan.referenced = append(plan.referenced, f.path)
	}

	rmeta := parseReleaseMetadata(text)
	if len(rmeta.files) == 0 {
		return nil, errors.New("no checksums in the Release file (is the mirror URL correct?)")
	}

	type group struct {
		base     string
		variants []string
		required bool
		packages bool
	}
	var groups []group
	if flat {
		groups = append(groups, group{base: "Packages", variants: packagesVariants, required: true, packages: true})
	} else {
		for _, comp := range o.Components {
			arches := append([]string{}, o.Arches...)
			if !contains(arches, "all") && rmeta.hasAny(comp+"/binary-all/Packages", packagesVariants) {
				arches = append(arches, "all")
			}
			for _, arch := range arches {
				base := fmt.Sprintf("%s/binary-%s/Packages", comp, arch)
				groups = append(groups,
					group{base: base, variants: packagesVariants, required: arch != "all", packages: true},
					group{base: fmt.Sprintf("%s/binary-%s/Release", comp, arch), variants: []string{""}},
				)
			}
			groups = append(groups,
				group{base: comp + "/i18n/Translation-en", variants: translationVariants},
				group{base: comp + "/i18n/Index", variants: []string{""}},
			)
		}
	}

	seen := map[string]bool{}
	for _, g := range groups {
		var have []string
		for _, v := range g.variants {
			name := g.base + v
			sums, listed := rmeta.files[name]
			if !listed {
				continue
			}
			dest, err := downloader.SafeJoin(distDest, filepath.FromSlash(name))
			if err != nil {
				continue
			}
			algo, sum := bestSum(sums)
			canonical := relBase + name
			rels := []string{canonical}
			if rmeta.acquireByHash {
				rels = []string{byHashRel(relBase, name, algo, sum), canonical}
			}

			if dl.DryRun {
				have = append(have, name)
				plan.referenced = append(plan.referenced, dest)
				continue
			}
			readPath, st, err := downloader.StageFromSources(dl, ss, rels, dest, algo, sum)
			if err != nil {
				continue
			}
			if st != nil {
				plan.staged = append(plan.staged, *st)
			}
			have = append(have, name)
			plan.referenced = append(plan.referenced, dest)
			if rmeta.acquireByHash {
				for a, s := range sums {
					dir, ok := byHashDirName(a)
					if !ok {
						continue
					}
					dst := filepath.Join(filepath.Dir(dest), "by-hash", dir, s)
					plan.byHash = append(plan.byHash, byHashCopy{src: dest, dst: dst, algo: a, sum: s})
					plan.referenced = append(plan.referenced, dst)
				}
			}
			_ = readPath
		}
		if len(have) == 0 {
			if g.required {
				return nil, fmt.Errorf("%s: not available from any source (check suites/components/arches)", g.base)
			}
			continue
		}
		if !g.packages {
			continue
		}

		pkgs, err := loadPackages(dl, ss, plan, relBase, distDest, g.base, have, rmeta, flat, o.Arches)
		if err != nil {
			return nil, err
		}
		for _, p := range pkgs {
			dest, err := downloader.SafeJoin(o.DestDir, filepath.FromSlash(p.filename))
			if err != nil || seen[dest] {
				continue
			}
			seen[dest] = true
			plan.pkgs = append(plan.pkgs, pkgEntry{
				relPath:  strings.TrimLeft(path.Clean("/"+p.filename), "/"),
				dest:     dest,
				algo:     p.algo,
				checksum: p.sum,
			})
		}
	}
	return plan, nil
}

// loadPackages parses the preferred available variant of one Packages file.
func loadPackages(dl *downloader.Client, ss *downloader.SourceSet, plan *suitePlan, relBase, distDest, base string, have []string, rmeta releaseMetadata, flat bool, arches []string) ([]debPkg, error) {
	var filter []string
	if flat {
		filter = arches
	}
	var lastErr error
	for _, v := range packagesVariants {
		name := base + v
		if !contains(have, name) {
			continue
		}
		algo, sum := bestSum(rmeta.files[name])
		var r io.Reader
		if dl.DryRun {
			data, _, err := downloader.FetchBytesFromSources(dl, ss, relBase+name)
			if err != nil {
				lastErr = err
				continue
			}
			if !downloader.BytesMatch(data, algo, sum) {
				lastErr = fmt.Errorf("%s: checksum mismatch", name)
				continue
			}
			r = bytes.NewReader(data)
		} else {
			p := stagedOrDest(plan, filepath.Join(distDest, filepath.FromSlash(name)))
			f, err := os.Open(p) // #nosec G304 – path derived from config
			if err != nil {
				lastErr = err
				continue
			}
			defer func() { _ = f.Close() }()
			r = f
		}
		pkgs, err := parsePackages(r, v, filter)
		if err != nil {
			lastErr = fmt.Errorf("parse %s: %w", name, err)
			continue
		}
		return pkgs, nil
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("%s: no usable Packages file", base)
	}
	return nil, lastErr
}

func stagedOrDest(plan *suitePlan, dest string) string {
	for _, st := range plan.staged {
		if st.Dest == dest {
			return st.Tmp
		}
	}
	return dest
}

func contains(list []string, v string) bool {
	for _, s := range list {
		if s == v {
			return true
		}
	}
	return false
}

// ---------- Release checksum parsing ----------

type releaseMetadata struct {
	files         map[string]map[string]string // file name -> algo -> checksum
	acquireByHash bool
}

func (r releaseMetadata) hasAny(base string, variants []string) bool {
	for _, v := range variants {
		if _, ok := r.files[base+v]; ok {
			return true
		}
	}
	return false
}

// parseReleaseMetadata extracts file checksums and the Acquire-By-Hash policy
// from the signed text of a Release/InRelease file.
func parseReleaseMetadata(text []byte) releaseMetadata {
	result := releaseMetadata{files: map[string]map[string]string{}}

	var currentAlgo string
	scanner := bufio.NewScanner(bytes.NewReader(text))
	scanner.Buffer(make([]byte, 64*1024), 16*1024*1024)
	for scanner.Scan() {
		line := strings.TrimRight(scanner.Text(), "\r")
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
		if currentAlgo != "" && strings.HasPrefix(line, " ") {
			fields := strings.Fields(line)
			if len(fields) < 3 {
				continue
			}
			name := fields[2]
			if result.files[name] == nil {
				result.files[name] = map[string]string{}
			}
			result.files[name][currentAlgo] = fields[0]
		} else if !strings.HasPrefix(line, " ") && line != "" {
			currentAlgo = ""
		}
	}
	return result
}

func bestSum(sums map[string]string) (string, string) {
	best, bestSum := "", ""
	for a, s := range sums {
		if algoPriority(a) > algoPriority(best) {
			best, bestSum = a, s
		}
	}
	return best, bestSum
}

func byHashRel(relBase, name, algo, sum string) string {
	dir, ok := byHashDirName(algo)
	if !ok {
		return relBase + name
	}
	parent := path.Dir(name)
	if parent == "." {
		return relBase + "by-hash/" + dir + "/" + sum
	}
	return relBase + parent + "/by-hash/" + dir + "/" + sum
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

// ---------- Packages file parsing ----------

type debPkg struct {
	filename string
	algo     string
	sum      string
}

// parsePackages reads a Packages file compressed as indicated by ext (".gz",
// ".xz", ".bz2" or ""). If arches is non-empty only packages for those
// architectures (plus "all") are returned.
func parsePackages(r io.Reader, ext string, arches []string) ([]debPkg, error) {
	switch ext {
	case extGz:
		gz, err := gzip.NewReader(r)
		if err != nil {
			return nil, err
		}
		defer func() { _ = gz.Close() }()
		r = gz
	case extXz:
		xzr, err := xz.NewReader(r)
		if err != nil {
			return nil, err
		}
		r = xzr
	case extBz2:
		r = bzip2.NewReader(r)
	}
	return scanPackages(r, arches)
}

// scanPackages parses a Debian Packages control file.
func scanPackages(r io.Reader, arches []string) ([]debPkg, error) { //nolint:gocyclo
	archSet := make(map[string]bool, len(arches))
	for _, a := range arches {
		archSet[a] = true
	}
	filterArches := len(archSet) > 0
	if filterArches {
		archSet["all"] = true
	}

	var pkgs []debPkg
	var current debPkg
	var currentArch string
	flush := func() {
		if current.filename != "" && (!filterArches || archSet[currentArch]) {
			pkgs = append(pkgs, current)
		}
		current = debPkg{}
		currentArch = ""
	}

	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 1024*1024), 16*1024*1024)

	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			flush()
			continue
		}
		if val, ok := fieldValue(line, "Filename"); ok {
			current.filename = val
		}
		if val, ok := fieldValue(line, "Architecture"); ok {
			currentArch = val
		}
		for _, f := range []struct{ field, algo string }{{"SHA512", algoSHA512}, {"SHA256", algoSHA256}, {"SHA1", algoSHA1}, {"MD5sum", algoMD5}} {
			if val, ok := fieldValue(line, f.field); ok && algoPriority(f.algo) > algoPriority(current.algo) {
				current.algo = f.algo
				current.sum = val
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	flush()
	sort.SliceStable(pkgs, func(i, j int) bool { return pkgs[i].filename < pkgs[j].filename })
	return pkgs, nil
}

func fieldValue(line, field string) (string, bool) {
	prefix := field + ": "
	if strings.HasPrefix(line, prefix) {
		return strings.TrimSpace(line[len(prefix):]), true
	}
	return "", false
}

func formatListForLog(values []string) string {
	if len(values) == 0 {
		return "-"
	}
	return strings.Join(values, ",")
}
