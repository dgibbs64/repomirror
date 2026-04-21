package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"repomirror/config"
	"repomirror/deb"
	"repomirror/downloader"
	"repomirror/rpm"
)

const version = "1.0.0"

func main() {
	// Determine the directory the binary lives in (USB drive root).
	execPath, err := os.Executable()
	if err != nil {
		log.Fatalf("cannot determine executable path: %v", err)
	}
	binDir := filepath.Dir(execPath)

	// Flags
	cfgPath := flag.String("config", filepath.Join(binDir, "mirrors.yaml"), "path to mirrors.yaml config file")
	genConfig := flag.Bool("init", false, "write an example mirrors.yaml to the config path and exit")
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

	// Resolve output dir relative to the config file's location so that
	// relative paths work regardless of the working directory.
	outputDir := cfg.OutputDir
	if !filepath.IsAbs(outputDir) {
		outputDir = filepath.Join(filepath.Dir(*cfgPath), outputDir)
	}

	if err := os.MkdirAll(outputDir, 0o755); err != nil {
		log.Fatalf("create output dir: %v", err)
	}

	dl := downloader.New()
	if *dryRun {
		dl.DryRun = true
		log.Println("[dry-run] mode enabled — directories will be created, no files will be downloaded")
	}

	var exitCode int

	// Mirror DEB repos first, sequentially. Each repo uses cfg.Workers
	// concurrent connections internally.
	for _, repo := range cfg.DEBRepos {
		path := repo.Path
		if path == "" {
			path = repo.Name
		}
		destDir := filepath.Join(outputDir, filepath.FromSlash(path))
		if err := deb.Mirror(repo.Mirror, destDir, repo.Name, repo.GPGKey, repo.Suites, repo.Components, repo.Arches, cfg.Workers, dl); err != nil {
			log.Printf("ERROR: %s: %v", path, err)
			exitCode = 1
		}
	}

	// Then mirror RPM repos, sequentially.
	for _, repo := range cfg.RPMRepos {
		path := repo.Path
		if path == "" {
			path = repo.Name
		}
		destDir := filepath.Join(outputDir, filepath.FromSlash(path))
		if err := rpm.Mirror(repo.BaseURL, destDir, repo.Name, repo.GPGKey, cfg.Workers, dl); err != nil {
			log.Printf("ERROR: %s: %v", path, err)
			exitCode = 1
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
