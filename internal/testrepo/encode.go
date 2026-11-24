package testrepo

import (
	"bytes"
	"compress/gzip"
	"crypto/md5" //nolint:gosec
	"crypto/sha256"
	"encoding/hex"
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/ulikunitz/xz"
)

// Compression extensions understood by Compress.
const (
	Gz   = ".gz"
	Xz   = ".xz"
	Zst  = ".zst"
	None = ""
)

// SHA256 returns the hex SHA-256 of data.
func SHA256(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// MD5 returns the hex MD5 of data.
func MD5(data []byte) string {
	sum := md5.Sum(data) //nolint:gosec
	return hex.EncodeToString(sum[:])
}

// Compress encodes data with the compression named by ext.
func Compress(t testing.TB, data []byte, ext string) []byte {
	t.Helper()
	var buf bytes.Buffer
	switch ext {
	case Gz:
		w := gzip.NewWriter(&buf)
		mustWrite(t, w.Write, data)
		mustClose(t, w.Close)
	case Xz:
		w, err := xz.NewWriter(&buf)
		if err != nil {
			t.Fatalf("xz: %v", err)
		}
		mustWrite(t, w.Write, data)
		mustClose(t, w.Close)
	case Zst:
		w, err := zstd.NewWriter(&buf)
		if err != nil {
			t.Fatalf("zstd: %v", err)
		}
		mustWrite(t, w.Write, data)
		mustClose(t, w.Close)
	case None:
		return append([]byte(nil), data...)
	default:
		t.Fatalf("unsupported compression %q", ext)
	}
	return buf.Bytes()
}

func mustWrite(t testing.TB, write func([]byte) (int, error), data []byte) {
	t.Helper()
	if _, err := write(data); err != nil {
		t.Fatalf("compress: %v", err)
	}
}

func mustClose(t testing.TB, closeFn func() error) {
	t.Helper()
	if err := closeFn(); err != nil {
		t.Fatalf("compress: %v", err)
	}
}
