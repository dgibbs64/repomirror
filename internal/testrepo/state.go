package testrepo

import (
	"fmt"
	"os"
	"testing"
)

// RunIsolated runs the tests in m with the downloader state database under a
// temporary cache directory and returns the exit code.
func RunIsolated(m *testing.M) int {
	dir, err := os.MkdirTemp("", "repomirror-test-")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	defer func() {
		if err := os.RemoveAll(dir); err != nil {
			fmt.Fprintln(os.Stderr, err)
		}
	}()
	if err := os.Setenv("XDG_CACHE_HOME", dir); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	return m.Run()
}
