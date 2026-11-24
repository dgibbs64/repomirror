// Package prune removes files from a mirrored repository directory that the
// latest sync no longer references, plus leftover temporary files.
package prune

import (
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"repomirror/downloader"
)

// Stats summarises what a prune run removed (or would remove).
type Stats struct {
	Files int
	Bytes int64
	Dirs  int
}

// Options controls one prune run.
type Options struct {
	// Keep lists absolute file paths that must not be removed.
	Keep map[string]bool
	// Skip lists directories that are never entered, such as other repos
	// nested inside this one.
	Skip []string
	// TempOnly removes only repomirror temporary files (.repomirror.part,
	// .repomirror.tmp, .repomirror.new) and leaves everything else.
	TempOnly bool
	DryRun   bool
	// Label prefixes log lines.
	Label string
}

const maxListed = 20

// Run walks root and removes files that are not in o.Keep. Symlinks are left
// alone. Directories left empty are removed, except root itself.
func Run(root string, o Options) (Stats, error) {
	var st Stats
	root = filepath.Clean(root)
	skip := map[string]bool{}
	for _, s := range o.Skip {
		skip[filepath.Clean(s)] = true
	}
	var dirs []string
	listed := 0

	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		if d.IsDir() {
			if p != root && skip[p] {
				return filepath.SkipDir
			}
			if p != root {
				dirs = append(dirs, p)
			}
			return nil
		}
		if !d.Type().IsRegular() {
			return nil
		}
		if o.Keep[p] {
			return nil
		}
		if o.TempOnly && !downloader.IsTempFile(p) {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return nil
		}
		st.Files++
		st.Bytes += info.Size()
		if o.DryRun {
			if listed < maxListed {
				log.Printf("%s would remove %s", o.Label, p)
				listed++
			}
			return nil
		}
		if err := os.Remove(p); err != nil && !os.IsNotExist(err) {
			log.Printf("%s could not remove %s: %v", o.Label, p, err)
			st.Files--
			st.Bytes -= info.Size()
			return nil
		}
		downloader.ForgetPath(p)
		return nil
	})
	if err != nil {
		return st, err
	}

	if !o.DryRun && !o.TempOnly {
		sort.Slice(dirs, func(i, j int) bool { return len(dirs[i]) > len(dirs[j]) })
		for _, d := range dirs {
			if isUnder(d, o.Skip) {
				continue
			}
			if err := os.Remove(d); err == nil {
				st.Dirs++
			}
		}
	}
	return st, nil
}

func isUnder(p string, roots []string) bool {
	for _, r := range roots {
		r = filepath.Clean(r)
		if p == r || strings.HasPrefix(p, r+string(filepath.Separator)) {
			return true
		}
	}
	return false
}
