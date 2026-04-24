package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"syscall"

	"repomirror/clientconfigs"
	"repomirror/config"
	"repomirror/deb"
	"repomirror/downloader"
	"repomirror/rpm"
)

var version = "2026.04.22"

func main() {
	// Determine the directory the binary lives in (USB drive root).
	execPath, err := os.Executable()
	if err != nil {
		log.Fatalf("cannot determine executable path: %v", err)
	}
	binDir := filepath.Dir(execPath)
	defaultCfgPath := defaultConfigPath(binDir)

	// Flags
	cfgPath := flag.String("config", defaultCfgPath, "path to mirrors.yaml or mirrors.yml config file")
	genConfig := flag.Bool("init", false, "write an example config file to the config path and exit")
	validateConfig := flag.Bool("validate", false, "validate config file and exit")
	showVersion := flag.Bool("version", false, "print version and exit")
	dryRun := flag.Bool("dry-run", false, "parse metadata and show what would be downloaded; create directory structure but skip actual downloads")
	flag.Parse()

	if *showVersion {
		fmt.Println("repomirror", version)
		return
	}

	if *genConfig {
		if err := os.WriteFile(*cfgPath, []byte(config.ExampleConfig()), 0o644); err != nil {
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

	if err := os.MkdirAll(outputDir, 0o755); err != nil {
		log.Fatalf("create output dir: %v", err)
	}

	unlock, err := lockOutputTree(outputDir)
	if err != nil {
		log.Fatalf("lock output dir: %v", err)
	}
	defer unlock()

	dl := downloader.New()
	if *dryRun {
		dl.DryRun = true
		log.Println("[dry-run] mode enabled — directories will be created, no files will be downloaded")
	}

	var exitCode int

	// Mirror DEB repos first, sequentially. Each repo uses cfg.Workers
	// concurrent connections internally.
	for _, repo := range cfg.DEBRepos {
		if !repo.Enabled() {
			name := repo.Name
			if name == "" {
				name = repo.Path
			}
			log.Printf("[deb] %s: skipped (enable=false)", name)
			continue
		}
		path := repo.Path
		if path == "" {
			path = repo.Name
		}
		destDir := filepath.Join(outputDir, filepath.FromSlash(path))
		if err := deb.Mirror(repo.Mirror, repo.Mirrorlist, repo.Metalink, repo.PreferredMirror, destDir, repo.Name, repo.GPGKey, repo.Suites, repo.Components, repo.Arches, cfg.Workers, dl); err != nil {
			log.Printf("ERROR: %s: %v", path, err)
			exitCode = 1
		}
	}

	// Then mirror RPM repos, sequentially.
	for _, repo := range cfg.RPMRepos {
		if !repo.Enabled() {
			name := repo.Name
			if name == "" {
				name = repo.Path
			}
			log.Printf("[rpm] %s: skipped (enable=false)", name)
			continue
		}
		path := repo.Path
		if path == "" {
			path = repo.Name
		}
		destDir := filepath.Join(outputDir, filepath.FromSlash(path))
		if err := rpm.Mirror(repo.BaseURL, repo.Mirrorlist, repo.Metalink, repo.PreferredMirror, destDir, repo.Name, repo.GPGKey, cfg.Workers, dl); err != nil {
			log.Printf("ERROR: %s: %v", path, err)
			exitCode = 1
		}
	}

	// Generate client config files (yum .repo / apt .list) pointing at the mirror.
	if cfg.MirrorURL != "" {
		if err := clientconfigs.Generate(cfg, outputDir, cfg.MirrorURL); err != nil {
			log.Printf("ERROR: generating client configs: %v", err)
			exitCode = 1
		} else {
			log.Printf("Client configs written to %s", filepath.Join(outputDir, "client-configs"))
		}
	}

	if exitCode == 0 {
		if *dryRun {
			log.Println("Dry-run complete. No files were downloaded.")
		} else {
			log.Println("All repositories mirrored successfully.")
		}
	} else {
		log.Println("Mirror completed with errors (see above).")
	}
	os.Exit(exitCode)
}

func defaultConfigPath(binDir string) string {
	yamlPath := filepath.Join(binDir, "mirrors.yaml")
	ymlPath := filepath.Join(binDir, "mirrors.yml")
	if _, err := os.Stat(yamlPath); err == nil {
		return yamlPath
	}
	if _, err := os.Stat(ymlPath); err == nil {
		return ymlPath
	}
	return yamlPath
}

func lockOutputTree(outputDir string) (func(), error) {
	lockPath := filepath.Join(outputDir, ".repomirror.lock")
	f, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open lock file %s: %w", lockPath, err)
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("another repomirror process is already using %s", outputDir)
	}
	if err := f.Truncate(0); err != nil {
		_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
		_ = f.Close()
		return nil, fmt.Errorf("truncate lock file: %w", err)
	}
	if _, err := f.WriteString(fmt.Sprintf("pid=%d\n", os.Getpid())); err != nil {
		_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
		_ = f.Close()
		return nil, fmt.Errorf("write lock file: %w", err)
	}
	return func() {
		_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
		_ = f.Close()
	}, nil
}
