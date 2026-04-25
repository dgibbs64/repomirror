package rpm

import (
	"bufio"
	"bytes"
	"compress/bzip2"
	"compress/gzip"
	"io"
	"os"
	"strings"

	"github.com/klauspost/compress/zstd"
	"github.com/ulikunitz/xz"
)

var (
	gzipMagic = []byte{0x1f, 0x8b}
	xzMagic   = []byte{0xfd, 0x37, 0x7a, 0x58, 0x5a, 0x00}
	bz2Magic  = []byte{0x42, 0x5a, 0x68}
	zstdMagic = []byte{0x28, 0xb5, 0x2f, 0xfd}
)

// openPossiblyCompressed opens a file for reading, transparently decompressing
// gzip (.gz) or xz (.xz) files. Returns a reader, a cleanup function, and any error.
func openPossiblyCompressed(path string) (io.Reader, func(), error) {
	f, err := os.Open(path) // #nosec G304 – path derived from config
	if err != nil {
		return nil, nil, err
	}

	br := bufio.NewReader(f)
	peek, _ := br.Peek(8)
	lower := strings.ToLower(path)
	switch {
	case hasPrefix(peek, gzipMagic) || strings.HasSuffix(lower, ".gz"):
		gz, err := gzip.NewReader(br)
		if err != nil {
			f.Close()
			return nil, nil, err
		}
		return gz, func() { gz.Close(); f.Close() }, nil
	case hasPrefix(peek, xzMagic) || strings.HasSuffix(lower, ".xz"):
		xzr, err := xz.NewReader(br)
		if err != nil {
			f.Close()
			return nil, nil, err
		}
		return xzr, func() { f.Close() }, nil
	case hasPrefix(peek, bz2Magic) || strings.HasSuffix(lower, ".bz2"):
		return bzip2.NewReader(br), func() { f.Close() }, nil
	case hasPrefix(peek, zstdMagic) || strings.HasSuffix(lower, ".zst") || strings.HasSuffix(lower, ".zstd"):
		zr, err := zstd.NewReader(br)
		if err != nil {
			f.Close()
			return nil, nil, err
		}
		return zr, func() { zr.Close(); f.Close() }, nil
	}

	return br, func() { f.Close() }, nil
}

// openPossiblyCompressedBytes opens an in-memory byte slice for reading,
// transparently decompressing based on the file extension (.gz or .xz).
func openPossiblyCompressedBytes(data []byte, ext string) (io.Reader, func(), error) {
	r := bytes.NewReader(data)
	peekLen := min(8, len(data))
	peek := data[:peekLen]
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
	case ".bz2":
		return bzip2.NewReader(r), func() {}, nil
	case ".zst", ".zstd":
		zr, err := zstd.NewReader(r)
		if err != nil {
			return nil, nil, err
		}
		return zr, func() { zr.Close() }, nil
	}

	// Extension is unreliable in some repos, so detect by magic bytes.
	switch {
	case hasPrefix(peek, gzipMagic):
		gz, err := gzip.NewReader(bytes.NewReader(data))
		if err != nil {
			return nil, nil, err
		}
		return gz, func() { gz.Close() }, nil
	case hasPrefix(peek, xzMagic):
		xzr, err := xz.NewReader(bytes.NewReader(data))
		if err != nil {
			return nil, nil, err
		}
		return xzr, func() {}, nil
	case hasPrefix(peek, bz2Magic):
		return bzip2.NewReader(bytes.NewReader(data)), func() {}, nil
	case hasPrefix(peek, zstdMagic):
		zr, err := zstd.NewReader(bytes.NewReader(data))
		if err != nil {
			return nil, nil, err
		}
		return zr, func() { zr.Close() }, nil
	}
	return r, func() {}, nil
}

func hasPrefix(buf, sig []byte) bool {
	if len(buf) < len(sig) {
		return false
	}
	for i := range sig {
		if buf[i] != sig[i] {
			return false
		}
	}
	return true
}
