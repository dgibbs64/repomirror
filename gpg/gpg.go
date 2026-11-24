// Package gpg downloads repository signing keys and verifies repository
// metadata signatures with them. It does not need a gpg binary.
package gpg

import (
	"bytes"
	"crypto"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/ProtonMail/go-crypto/openpgp"
	"github.com/ProtonMail/go-crypto/openpgp/armor"
	"github.com/ProtonMail/go-crypto/openpgp/clearsign"
	"github.com/ProtonMail/go-crypto/openpgp/packet"

	"repomirror/downloader"
)

// KeysDirName is the directory, inside each repository path, that holds the
// mirrored signing keys.
const KeysDirName = "gpg-keys"

const publicKeyBlockType = "PGP PUBLIC KEY BLOCK"

var unsafeFileChars = regexp.MustCompile(`[^A-Za-z0-9._-]+`)

// Keyring holds the public keys loaded from one gpg_key URL.
type Keyring struct {
	entities openpgp.EntityList
}

// KeyFileName returns the file name used to store the key fetched from
// keyURL. Query strings (as used by keyservers) are replaced by a short hash.
func KeyFileName(keyURL string) string {
	base := ""
	query := ""
	if u, err := url.Parse(strings.TrimSpace(keyURL)); err == nil {
		base = path.Base(u.Path)
		query = u.RawQuery
	} else {
		base = path.Base(keyURL)
	}
	if base == "" || base == "." || base == "/" {
		base = "gpg.key"
	}
	if query != "" {
		sum := sha256.Sum256([]byte(query))
		base += "-" + hex.EncodeToString(sum[:4])
	}
	base = unsafeFileChars.ReplaceAllString(base, "_")
	return base
}

// Fetch downloads the key at keyURL, stores an ASCII-armoured copy in
// keysDir and returns the parsed keyring and the stored file path. The key is
// refreshed on every run; if the download fails a previously stored copy is
// used instead. In dry-run mode nothing is written.
func Fetch(keyURL, keysDir string, dl *downloader.Client) (*Keyring, string, error) {
	if strings.TrimSpace(keyURL) == "" {
		return nil, "", nil
	}
	keyPath := filepath.Join(keysDir, KeyFileName(keyURL))

	data, fetchErr := dl.FetchBytes(keyURL)
	if fetchErr != nil {
		existing, err := os.ReadFile(keyPath) // #nosec G304 – path derived from config
		if err != nil {
			return nil, keyPath, fmt.Errorf("fetching GPG key %s: %w", keyURL, fetchErr)
		}
		data = existing
	}

	kr, armored, err := parseKey(data)
	if err != nil {
		return nil, keyPath, fmt.Errorf("GPG key %s: %w", keyURL, err)
	}
	if !dl.DryRun {
		current, readErr := os.ReadFile(keyPath) // #nosec G304 – path derived from config
		if readErr != nil || !bytes.Equal(current, armored) {
			if err := downloader.WriteFileAtomic(keyPath, armored); err != nil {
				return kr, keyPath, fmt.Errorf("writing GPG key %s: %w", keyPath, err)
			}
		}
	}
	return kr, keyPath, nil
}

func parseKey(data []byte) (*Keyring, []byte, error) {
	trimmed := bytes.TrimSpace(data)
	if bytes.HasPrefix(trimmed, []byte("-----BEGIN PGP")) {
		entities, err := readArmoredBlocks(trimmed)
		if err != nil {
			return nil, nil, fmt.Errorf("not a usable PGP public key: %w", err)
		}
		return &Keyring{entities: entities}, append(trimmed, '\n'), nil
	}
	entities, err := openpgp.ReadKeyRing(bytes.NewReader(data))
	if err != nil {
		return nil, nil, fmt.Errorf("not a usable PGP public key: %w", err)
	}
	var buf bytes.Buffer
	w, err := armor.Encode(&buf, publicKeyBlockType, nil)
	if err != nil {
		return nil, nil, err
	}
	if _, err := w.Write(data); err != nil {
		return nil, nil, err
	}
	if err := w.Close(); err != nil {
		return nil, nil, err
	}
	buf.WriteByte('\n')
	return &Keyring{entities: entities}, buf.Bytes(), nil
}

// readArmoredBlocks reads every armoured public key block in data.
func readArmoredBlocks(data []byte) (openpgp.EntityList, error) {
	marker := []byte("-----BEGIN PGP PUBLIC KEY BLOCK-----")
	var all openpgp.EntityList
	var lastErr error
	rest := data
	for {
		i := bytes.Index(rest, marker)
		if i < 0 {
			break
		}
		rest = rest[i:]
		block, err := armor.Decode(bytes.NewReader(rest))
		rest = rest[len(marker):]
		if err != nil {
			lastErr = err
			continue
		}
		entities, err := openpgp.ReadKeyRing(block.Body)
		if err != nil {
			lastErr = err
			continue
		}
		all = append(all, entities...)
	}
	if len(all) == 0 {
		if lastErr == nil {
			lastErr = errors.New("no public key block found")
		}
		return nil, lastErr
	}
	return all, nil
}

// verifyConfig allows DSA keys and SHA-1 signatures.
var verifyConfig = &packet.Config{
	RejectPublicKeyAlgorithms: map[packet.PublicKeyAlgorithm]bool{
		packet.PubKeyAlgoElGamal: true,
	},
	RejectHashAlgorithms: map[crypto.Hash]bool{
		crypto.MD5:       true,
		crypto.RIPEMD160: true,
	},
	RejectMessageHashAlgorithms: map[crypto.Hash]bool{
		crypto.MD5:       true,
		crypto.RIPEMD160: true,
	},
}

// ErrNotSigned is returned when data that should be clearsigned is not.
var ErrNotSigned = errors.New("not a PGP clearsigned document")

// VerifyClearsigned checks an InRelease style clearsigned document and
// returns the signed text.
func (k *Keyring) VerifyClearsigned(data []byte) ([]byte, error) {
	block, _ := clearsign.Decode(data)
	if block == nil {
		return nil, ErrNotSigned
	}
	if _, err := openpgp.CheckDetachedSignature(k.entities, bytes.NewReader(block.Bytes), block.ArmoredSignature.Body, verifyConfig); err != nil {
		return nil, fmt.Errorf("signature check failed: %w", err)
	}
	return block.Plaintext, nil
}

// VerifyDetached checks a detached signature (armoured or binary) over
// signed, as used by Release.gpg and repomd.xml.asc.
func (k *Keyring) VerifyDetached(signed, signature []byte) error {
	var sig io.Reader = bytes.NewReader(signature)
	if bytes.HasPrefix(bytes.TrimSpace(signature), []byte("-----BEGIN PGP")) {
		block, err := armor.Decode(bytes.NewReader(bytes.TrimSpace(signature)))
		if err != nil {
			return fmt.Errorf("reading signature: %w", err)
		}
		sig = block.Body
	}
	if _, err := openpgp.CheckDetachedSignature(k.entities, bytes.NewReader(signed), sig, verifyConfig); err != nil {
		return fmt.Errorf("signature check failed: %w", err)
	}
	return nil
}

// ClearsignedText returns the signed text of a clearsigned document without
// checking the signature. Data that is not clearsigned is returned as is.
func ClearsignedText(data []byte) []byte {
	block, _ := clearsign.Decode(data)
	if block == nil {
		return data
	}
	return block.Plaintext
}
