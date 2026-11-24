// Package testrepo builds small signed RPM and DEB repositories in memory
// and serves them over HTTP with injectable faults, for use in tests.
package testrepo

import (
	"bytes"
	"crypto"
	"testing"

	"github.com/ProtonMail/go-crypto/openpgp"
	"github.com/ProtonMail/go-crypto/openpgp/armor"
	"github.com/ProtonMail/go-crypto/openpgp/clearsign"
	"github.com/ProtonMail/go-crypto/openpgp/packet"
)

var keyConfig = &packet.Config{
	Algorithm:   packet.PubKeyAlgoEdDSA,
	DefaultHash: crypto.SHA256,
}

// Key is an OpenPGP signing key generated for a test.
type Key struct {
	entity *openpgp.Entity
	// Armored is the ASCII-armoured public key.
	Armored []byte
	// Binary is the public key in binary packet form.
	Binary []byte
}

// NewKey generates a fresh signing key.
func NewKey(t testing.TB, name string) *Key {
	t.Helper()
	e, err := openpgp.NewEntity(name, "", name+"@example.invalid", keyConfig)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	var bin bytes.Buffer
	if err = e.Serialize(&bin); err != nil {
		t.Fatalf("serialize key: %v", err)
	}
	var arm bytes.Buffer
	w, err := armor.Encode(&arm, openpgp.PublicKeyType, nil)
	if err != nil {
		t.Fatalf("armor key: %v", err)
	}
	if _, err = w.Write(bin.Bytes()); err != nil {
		t.Fatalf("armor key: %v", err)
	}
	if err = w.Close(); err != nil {
		t.Fatalf("armor key: %v", err)
	}
	arm.WriteByte('\n')
	return &Key{entity: e, Armored: arm.Bytes(), Binary: bin.Bytes()}
}

// DetachSign returns an armoured detached signature over data.
func (k *Key) DetachSign(t testing.TB, data []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	if err := openpgp.ArmoredDetachSign(&buf, k.entity, bytes.NewReader(data), keyConfig); err != nil {
		t.Fatalf("detach sign: %v", err)
	}
	return buf.Bytes()
}

// DetachSignBinary returns a binary detached signature over data.
func (k *Key) DetachSignBinary(t testing.TB, data []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	if err := openpgp.DetachSign(&buf, k.entity, bytes.NewReader(data), keyConfig); err != nil {
		t.Fatalf("detach sign: %v", err)
	}
	return buf.Bytes()
}

// Clearsign returns data wrapped in a clearsigned document.
func (k *Key) Clearsign(t testing.TB, data []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	w, err := clearsign.Encode(&buf, k.entity.PrivateKey, keyConfig)
	if err != nil {
		t.Fatalf("clearsign: %v", err)
	}
	if _, err = w.Write(data); err != nil {
		t.Fatalf("clearsign: %v", err)
	}
	if err = w.Close(); err != nil {
		t.Fatalf("clearsign: %v", err)
	}
	return buf.Bytes()
}
