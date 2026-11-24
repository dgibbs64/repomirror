package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"repomirror/clientconfigs"
	"repomirror/config"
	"repomirror/deb"
	"repomirror/downloader"
	prunepkg "repomirror/prune"
	"repomirror/rpm"
)

var version = "2026.05.20"

func main() { //nolint:gocyclo
	// Determine the directory the binary lives in (USB drive root).
	execPath, err := os.Executable()
	if err != nil {
		log.Fatalf("cannot determine executable path: %v", err)
	}
	binDir := filepath.Dir(execPath)
	defaultCfgPath := defaultConfigPath(binDir)

	// Flags
	cfgPath := flag.String("config", defaultCfgPath, "path to mirrors.yml or mirrors.yaml config file")
	genConfig := flag.Bool("init", false, "write an example config file to the config path and exit")
	validateConfig := flag.Bool("validate", false, "validate config file and exit")
	showVersion := flag.Bool("version", false, "print version and exit")
	dryRun := flag.Bool("dry-run", false, "parse metadata and show what would be downloaded; create directory structure but skip actual downloads")
	verbose := flag.Bool("verbose", false, "with -dry-run, list every file that would be downloaded")
	flag.Parse()

	if *showVersion {
		fmt.Println("repomirror", version)
		return
	}

	if *genConfig {
		if err := os.WriteFile(*cfgPath, []byte(config.ExampleConfig()), 0o644); err != nil { //nolint:gosec,govet
			log.Fatalf("write example config: %v", err)
		}
		fmt.Printf("Example config written to %s\n", *cfgPath)
		fmt.Println("Edit it, then run repomirror again to start mirroring.")
		return
	}

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		log.Fatalf("load config: %v\n\nRun with -init to generate an example config.", err)
	}
	log.Printf("Using %d workers per repo", cfg.Workers)
	if *validateConfig {
		fmt.Printf("Config is valid: %s\n", *cfgPath)
		return
	}

	// Resolve output dir relative to the config file's location so that
	// relative paths work regardless of the working directory.
	outputDir := cfg.OutputDir
	if !filepath.IsAbs(outputDir) {
		outputDir = filepath.Join(filepath.Dir(*cfgPath), outputDir)
	}

	if mkdirErr := os.MkdirAll(outputDir, 0o755); mkdirErr != nil {
		log.Fatalf("create output dir: %v", mkdirErr)
	}

	unlock, err := lockOutputTree(outputDir)
	if err != nil {
		log.Fatalf("lock output dir: %v", err)
	}

	dl := downloader.New()
	dl.UserAgent = cfg.UserAgent
	if dl.UserAgent == "" {
		dl.UserAgent = "repomirror/" + version
	}
	if cfg.RateLimit > 0 {
		dl.SetRateLimit(int64(cfg.RateLimit))
		log.Printf("Download rate limited to %s/s", downloader.FmtBytes(float64(cfg.RateLimit)))
	}
	if *dryRun {
		dl.DryRun = true
		dl.Verbose = *verbose
		log.Println("[dry-run] mode enabled, no files will be downloaded or removed")
	}
	verify := cfg.SignaturesEnabled()
	if !verify {
		log.Println("WARNING: verify_signatures is false, repository signatures are only checked for repos that enable it")
	}

	var exitCode int
	paths := newPathTracker()
	runStart := time.Now()
	report := &runReport{DryRun: *dryRun, Prune: cfg.Prune, Style: detectReportStyle()}

	// Mirror DEB repos first, sequentially. Each repo uses cfg.Workers
	// concurrent connections internally.
	for _, repo := range cfg.DEBRepos {
		destDir := repoDestDir(outputDir, repo.Path, repo.Name)
		if !repo.Enabled() {
			log.Printf("[deb] %s: skipped (enable=false)", displayName(repo.Name, repo.Path))
			paths.disabled(destDir)
			report.add(repoReport{Type: "deb", Name: displayName(repo.Name, repo.Path), Status: statusSkipped})
			continue
		}
		start := time.Now()
		res, err := deb.Mirror(deb.Options{
			Name:             repo.Name,
			MirrorURL:        repo.Mirror,
			Mirrorlist:       repo.Mirrorlist,
			Metalink:         repo.Metalink,
			PreferredMirror:  repo.PreferredMirror,
			GPGKey:           repo.GPGKey,
			DestDir:          destDir,
			Suites:           repo.Suites,
			Components:       repo.Components,
			Arches:           repo.Arches,
			Workers:          int(cfg.Workers),
			VerifySignatures: repo.Verify(verify),
		}, dl)
		if err != nil {
			log.Printf("ERROR: %s: %v", displayName(repo.Name, repo.Path), err)
			exitCode = 1
		}
		paths.record(destDir, res.Referenced, err == nil && res.Complete)
		report.add(newRepoReport("deb", displayName(repo.Name, repo.Path), res.Packages, res.Failed, res.Bytes, time.Since(start), err))
	}

	// Then mirror RPM repos, sequentially.
	for _, repo := range cfg.RPMRepos {
		destDir := repoDestDir(outputDir, repo.Path, repo.Name)
		if !repo.Enabled() {
			log.Printf("[rpm] %s: skipped (enable=false)", displayName(repo.Name, repo.Path))
			paths.disabled(destDir)
			report.add(repoReport{Type: "rpm", Name: displayName(repo.Name, repo.Path), Status: statusSkipped})
			continue
		}
		start := time.Now()
		res, err := rpm.Mirror(rpm.Options{
			Name:             repo.Name,
			BaseURL:          repo.BaseURL,
			Mirrorlist:       repo.Mirrorlist,
			Metalink:         repo.Metalink,
			PreferredMirror:  repo.PreferredMirror,
			PrimaryMetadata:  cfg.RPMPrimaryMetadata,
			GPGKey:           repo.GPGKey,
			DestDir:          destDir,
			Arches:           repo.Arches,
			Workers:          int(cfg.Workers),
			VerifySignatures: repo.Verify(verify),
		}, dl)
		if err != nil {
			log.Printf("ERROR: %s: %v", displayName(repo.Name, repo.Path), err)
			exitCode = 1
		}
		paths.record(destDir, res.Referenced, err == nil && res.Complete)
		report.add(newRepoReport("rpm", displayName(repo.Name, repo.Path), res.Packages, res.Failed, res.Bytes, time.Since(start), err))
	}

	cleanMirrorPaths(paths, outputDir, cfg.Prune, *dryRun, report)

	// Generate client config files (yum .repo / apt .list) pointing at the mirror.
	if cfg.MirrorURL != "" {
		if err := clientconfigs.Generate(cfg, outputDir, cfg.MirrorURL, !*dryRun); err != nil {
			log.Printf("ERROR: generating client configs: %v", err)
			report.addError(fmt.Sprintf("generating client configs: %v", err))
			exitCode = 1
		} else {
			log.Printf("Client configs written to %s", filepath.Join(outputDir, "client-configs"))
		}
	}

	report.write(log.Writer(), time.Since(runStart))

	if exitCode == 0 {
		if *dryRun {
			log.Println("Dry-run complete. No files were downloaded.")
		} else {
			log.Println("All repositories mirrored successfully.")
		}
	} else {
		log.Println("Mirror completed with errors (see the report above).")
	}
	unlock()
	os.Exit(exitCode)
}

func newRepoReport(repoType, name string, packages, failed int, bytes int64, d time.Duration, err error) repoReport {
	rep := repoReport{
		Type:     repoType,
		Name:     name,
		Status:   statusOK,
		Packages: packages,
		Failed:   failed,
		Bytes:    bytes,
		Duration: d,
		Err:      err,
	}
	if err != nil {
		rep.Status = statusFailed
	}
	return rep
}

func repoDestDir(outputDir, path, name string) string {
	if path == "" {
		path = name
	}
	return filepath.Join(outputDir, filepath.FromSlash(path))
}

func displayName(name, path string) string {
	if name != "" {
		return name
	}
	return path
}

type pathState struct {
	keep        map[string]bool
	complete    bool
	hasDisabled bool
}

type pathTracker struct {
	order  []string
	states map[string]*pathState
}

func newPathTracker() *pathTracker {
	return &pathTracker{states: map[string]*pathState{}}
}

func (t *pathTracker) get(dir string) *pathState {
	dir = filepath.Clean(dir)
	s, ok := t.states[dir]
	if !ok {
		s = &pathState{keep: map[string]bool{}, complete: true}
		t.states[dir] = s
		t.order = append(t.order, dir)
	}
	return s
}

func (t *pathTracker) disabled(dir string) { t.get(dir).hasDisabled = true }

func (t *pathTracker) record(dir string, keep map[string]bool, ok bool) {
	s := t.get(dir)
	for p := range keep {
		s.keep[filepath.Clean(p)] = true
	}
	if !ok {
		s.complete = false
	}
}

// cleanMirrorPaths removes leftover temporary files and, when prune is set,
// files upstream no longer lists. A path is only cleaned when every repo
// entry that writes to it is enabled and finished without errors.
func cleanMirrorPaths(t *pathTracker, outputDir string, prune bool, dryRun bool, report *runReport) {
	outputDir = filepath.Clean(outputDir)
	for _, dir := range t.order {
		s := t.states[dir]
		label := "[prune] " + dir + ":"
		entry := cleanupReport{Path: relPath(outputDir, dir), Skipped: cleanSkipReason(s, dir, outputDir, prune)}
		if entry.Skipped != "" {
			report.addCleanup(entry)
			continue
		}
		var nested []string
		for _, other := range t.order {
			if other != dir && strings.HasPrefix(other, dir+string(filepath.Separator)) {
				nested = append(nested, other)
			}
		}
		st, err := prunepkg.Run(dir, prunepkg.Options{
			Keep:     s.keep,
			Skip:     nested,
			TempOnly: !prune,
			DryRun:   dryRun,
			Label:    label,
		})
		if err != nil {
			log.Printf("%s ERROR: %v", label, err)
			entry.Err = err
			report.addCleanup(entry)
			report.addError(fmt.Sprintf("%s %v", label, err))
			continue
		}
		entry.Files = st.Files
		entry.Bytes = st.Bytes
		report.addCleanup(entry)
		if st.Files == 0 {
			continue
		}
		what := "stale temporary file(s)"
		if prune {
			what = "unreferenced file(s)"
		}
		log.Printf("%s %s %d %s (%s)", label, removeVerb(dryRun), st.Files, what, downloader.FmtBytes(float64(st.Bytes)))
	}
}

// cleanSkipReason logs and returns why a path must not be cleaned, or "" when
// it can be.
func cleanSkipReason(s *pathState, dir, outputDir string, prune bool) string {
	label := "[prune] " + dir + ":"
	switch {
	case s.hasDisabled:
		log.Printf("%s skipped, a repo using this path has enable=false", label)
		return "a repo is disabled"
	case !s.complete:
		log.Printf("%s skipped, the sync for this path had errors", label)
		return "sync had errors"
	case dir == outputDir || dir == string(filepath.Separator):
		log.Printf("%s skipped, a repo path must be a subdirectory of output_dir", label)
		return "not a subdirectory of output_dir"
	case prune && len(s.keep) == 0:
		log.Printf("%s skipped, nothing was referenced", label)
		return "nothing referenced"
	}
	return ""
}

func relPath(base, p string) string {
	if rel, err := filepath.Rel(base, p); err == nil && !strings.HasPrefix(rel, "..") {
		return filepath.ToSlash(rel)
	}
	return p
}

func defaultConfigPath(binDir string) string {
	ymlPath := filepath.Join(binDir, "mirrors.yml")
	yamlPath := filepath.Join(binDir, "mirrors.yaml")
	if _, err := os.Stat(ymlPath); err == nil {
		return ymlPath
	}
	if _, err := os.Stat(yamlPath); err == nil {
		return yamlPath
	}
	return ymlPath
}

func lockOutputTree(lockDir string) (func(), error) {
	lockPath := filepath.Join(lockDir, ".repomirror.lock")
	f, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open lock file %s: %w", lockPath, err)
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil { //nolint:gosec
		_ = f.Close()
		return nil, fmt.Errorf("another repomirror process is already running")
	}
	if err := f.Truncate(0); err != nil {
		_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN) //nolint:errcheck,gosec
		_ = f.Close()
		return nil, fmt.Errorf("truncate lock file: %w", err)
	}
	if _, err := fmt.Fprintf(f, "pid=%d\n", os.Getpid()); err != nil {
		_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN) //nolint:errcheck,gosec
		_ = f.Close()
		return nil, fmt.Errorf("write lock file: %w", err)
	}
	return func() {
		_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN) //nolint:errcheck,gosec
		_ = f.Close()
		_ = os.Remove(lockPath)
	}, nil
}
