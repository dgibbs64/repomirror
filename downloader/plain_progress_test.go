package downloader

import (
	"testing"
	"time"
)

func TestPlainProgressDue(t *testing.T) {
	start := time.Now()
	p := PlainProgress{last: start}

	if p.Due(start.Add(PlainProgressInterval - time.Second)) {
		t.Fatal("due before the first interval elapsed")
	}
	if !p.Due(start.Add(PlainProgressInterval)) {
		t.Fatal("not due once the first interval elapsed")
	}
	if p.Due(start.Add(PlainProgressInterval + time.Second)) {
		t.Fatal("due again straight after logging")
	}
	if !p.Due(start.Add(2 * PlainProgressInterval)) {
		t.Fatal("not due after the second interval")
	}
}
