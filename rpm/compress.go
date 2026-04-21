package rpm

import (
	"bytes"
	"compress/gzip"
	"io"
	"os"
	"strings"

	"github.com/ulikunitz/xz"
)

// openPossiblyCompressed opens a file for reading, transparently decompressing
// gzip (.gz) or xz (.xz) files. Returns a reader, a cleanup function, and any error.
func openPossiblyCompressed(path string) (io.Reader, func(), error) {
	f, err := os.Open(path) // #nosec G304 – path derived from config
	if err != nil {
		return nil, nil, err
	}

	lower := strings.ToLower(path)
	switch {
	case strings.HasSuffix(lower, ".gz"):
		gz, err := gzip.NewReader(f)
		if err != nil {
			f.Close()
			return nil, nil, err
		}
		return gz, func() { gz.Close(); f.Close() }, nil
	case strings.HasSuffix(lower, ".xz"):
		xzr, err := xz.NewReader(f)
		if err != nil {
			f.Close()
			return nil, nil, err
		}
		return xzr, func() { f.Close() }, nil
	}

	return f, func() { f.Close() }, nil
}

// openPossiblyCompressedBytes opens an in-memory byte slice for reading,
// transparently decompressing based on the file extension (.gz or .xz).
func openPossiblyCompressedBytes(data []byte, ext string) (io.Reader, func(), error) {
	r := bytes.NewReader(data)
	switch strings.ToLower(ext) {
	case ".gz":
		gz, err := gzip.NewReader(r)
		if err != nil {
			return nil, nil, err
		}
		return gz, func() { gz.Close() }, nil
	case ".xz":
		xzr, err := xz.NewReader(r)
		if err != nil {
			return nil, nil, err
		}
		return xzr, func() {}, nil
	}
	return r, func() {}, nil
}
