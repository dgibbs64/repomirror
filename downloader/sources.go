package downloader

import (
	"encoding/xml"
	"fmt"
	"slices"
	"strings"
	"sync"
)

// SourceSet is a mutex-protected list of mirror base URLs that promotes the
// fastest-responding source to the front on success.
type SourceSet struct {
	mu   sync.Mutex
	urls []string
}

// NewSourceSet creates a SourceSet from the given base URLs.
func NewSourceSet(urls []string) *SourceSet {
	return &SourceSet{urls: append([]string(nil), urls...)}
}

// Primary returns the current preferred base URL.
func (s *SourceSet) Primary() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.urls) == 0 {
		return ""
	}
	return s.urls[0]
}

// Ordered returns a snapshot of the current URL order.
func (s *SourceSet) Ordered() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.urls...)
}

// MarkSuccess promotes baseURL to the front of the list.
func (s *SourceSet) MarkSuccess(baseURL string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	idx := -1
	for i, u := range s.urls {
		if u == baseURL {
			idx = i
			break
		}
	}
	if idx <= 0 {
		return
	}
	s.urls[0], s.urls[idx] = s.urls[idx], s.urls[0]
}

// DownloadFileFromSources tries each source in ss until one succeeds.
func DownloadFileFromSources(dl *Client, ss *SourceSet, relPath, dest, algo, expected string, prog *Counter) error {
	rel := strings.TrimLeft(relPath, "/")
	ordered := ss.Ordered()
	var lastErr error
	for _, base := range ordered {
		url := strings.TrimRight(base, "/") + "/" + rel
		if err := dl.DownloadFileP(url, dest, algo, expected, prog); err != nil {
			lastErr = err
			continue
		}
		ss.MarkSuccess(base)
		return nil
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("no source URLs configured")
	}
	return lastErr
}

// FetchBytesFromSources tries each source in ss until one succeeds, returning
// the response body and the base URL that served it.
func FetchBytesFromSources(dl *Client, ss *SourceSet, relPath string) ([]byte, string, error) {
	rel := strings.TrimLeft(relPath, "/")
	ordered := ss.Ordered()
	var lastErr error
	for _, base := range ordered {
		url := strings.TrimRight(base, "/") + "/" + rel
		data, err := dl.FetchBytes(url)
		if err != nil {
			lastErr = err
			continue
		}
		ss.MarkSuccess(base)
		return data, base, nil
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("no source URLs configured")
	}
	return nil, "", lastErr
}

// ResolveSourceURLs builds an ordered list of base URLs from an explicit URL,
// mirrorlist, and metalink. repoType is used only in the error message
// (e.g. "RPM" or "DEB").
func ResolveSourceURLs(primaryURL, mirrorlistURL, metalinkURL, preferred, repoType string, dl *Client) ([]string, error) { //nolint:gocyclo
	var sources []string
	add := func(raw string) {
		raw = strings.TrimSpace(raw)
		if raw == "" {
			return
		}
		raw = strings.TrimRight(raw, "/")
		if !strings.HasPrefix(raw, "http://") && !strings.HasPrefix(raw, "https://") {
			return
		}
		for _, existing := range sources {
			if existing == raw {
				return
			}
		}
		sources = append(sources, raw)
	}

	add(primaryURL)

	if mirrorlistURL != "" {
		data, err := dl.FetchBytes(mirrorlistURL)
		if err != nil {
			return nil, fmt.Errorf("fetch mirrorlist: %w", err)
		}
		for _, line := range strings.Split(string(data), "\n") {
			line = strings.TrimSpace(line)
			if line == "" || strings.HasPrefix(line, "#") {
				continue
			}
			add(line)
		}
	}

	if metalinkURL != "" {
		data, err := dl.FetchBytes(metalinkURL)
		if err != nil {
			return nil, fmt.Errorf("fetch metalink: %w", err)
		}
		var ml struct {
			URLs []struct {
				Protocol string `xml:"protocol,attr"`
				Value    string `xml:",chardata"`
			} `xml:"files>file>resources>url"`
		}
		if err := xml.Unmarshal(data, &ml); err != nil {
			return nil, fmt.Errorf("parse metalink: %w", err)
		}
		for _, u := range ml.URLs {
			p := strings.ToLower(strings.TrimSpace(u.Protocol))
			if p != "" && p != "http" && p != "https" {
				continue
			}
			add(u.Value)
		}
	}

	if len(sources) == 0 {
		return nil, fmt.Errorf("no %s source URL configured (set base_url/mirror, mirrorlist, or metalink)", repoType)
	}

	if preferred != "" {
		preferred = strings.ToLower(strings.TrimSpace(preferred))
		slices.SortStableFunc(sources, func(a, b string) int {
			aa := strings.Contains(strings.ToLower(a), preferred)
			bb := strings.Contains(strings.ToLower(b), preferred)
			switch {
			case aa && !bb:
				return -1
			case !aa && bb:
				return 1
			default:
				return 0
			}
		})
	}

	return sources, nil
}
