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

type walker struct {
	o      Options
	base   string
	root   *os.Root
	skip   map[string]bool
	dirs   []string
	listed int
	st     Stats
}

// Run walks root and removes files that are not in o.Keep. Symlinks are left
// alone. Directories left empty are removed, except root itself. All
// removals go through an os.Root so nothing outside root can be touched.
func Run(root string, o Options) (Stats, error) {
	w := &walker{o: o, base: filepath.Clean(root), skip: map[string]bool{}}
	for _, s := range o.Skip {
		w.skip[filepath.Clean(s)] = true
	}
	r, err := os.OpenRoot(w.base)
	if err != nil {
		if os.IsNotExist(err) {
			return w.st, nil
		}
		return w.st, err
	}
	defer func() { _ = r.Close() }()
	w.root = r

	if err := fs.WalkDir(r.FS(), ".", w.visit); err != nil {
		return w.st, err
	}
	if !o.DryRun && !o.TempOnly {
		w.removeEmptyDirs()
	}
	return w.st, nil
}

func (w *walker) visit(rel string, d fs.DirEntry, err error) error {
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	if rel == "." {
		return nil
	}
	abs := filepath.Join(w.base, filepath.FromSlash(rel))
	if d.IsDir() {
		if w.skip[abs] {
			return filepath.SkipDir
		}
		w.dirs = append(w.dirs, rel)
		return nil
	}
	if !d.Type().IsRegular() || !w.shouldRemove(abs) {
		return nil
	}
	info, err := d.Info()
	if err != nil {
		return nil
	}
	w.remove(rel, abs, info.Size())
	return nil
}

func (w *walker) shouldRemove(abs string) bool {
	if w.o.Keep[abs] {
		return false
	}
	return !w.o.TempOnly || downloader.IsTempFile(abs)
}

func (w *walker) remove(rel, abs string, size int64) {
	if w.o.DryRun {
		if w.listed < maxListed {
			log.Printf("%s would remove %s", w.o.Label, abs)
			w.listed++
		}
		w.st.Files++
		w.st.Bytes += size
		return
	}
	if err := w.root.Remove(filepath.FromSlash(rel)); err != nil && !os.IsNotExist(err) {
		log.Printf("%s could not remove %s: %v", w.o.Label, abs, err)
		return
	}
	w.st.Files++
	w.st.Bytes += size
	downloader.ForgetPath(abs)
}

func (w *walker) removeEmptyDirs() {
	sort.Slice(w.dirs, func(i, j int) bool { return len(w.dirs[i]) > len(w.dirs[j]) })
	for _, rel := range w.dirs {
		abs := filepath.Join(w.base, filepath.FromSlash(rel))
		if isUnder(abs, w.o.Skip) {
			continue
		}
		if err := w.root.Remove(filepath.FromSlash(rel)); err == nil {
			w.st.Dirs++
		}
	}
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
