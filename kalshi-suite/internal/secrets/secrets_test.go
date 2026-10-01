package secrets

import (
	"bytes"
	"testing"
)

func TestSealOpenRoundTrip(t *testing.T) {
	pass := "correct horse battery staple"
	plain := []byte("-----BEGIN RSA PRIVATE KEY-----\nMOCKKEYMATERIAL\n-----END RSA PRIVATE KEY-----\n")

	sealed, err := Seal(pass, plain)
	if err != nil {
		t.Fatalf("seal: %v", err)
	}
	if bytes.Equal(sealed.Ciphertext, plain) {
		t.Fatal("ciphertext must not equal plaintext")
	}
	got, err := Open(pass, sealed)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if !bytes.Equal(got, plain) {
		t.Fatalf("round-trip mismatch: got %q want %q", got, plain)
	}
}

func TestOpenWrongPassphraseFails(t *testing.T) {
	sealed, err := Seal("right-passphrase", []byte("top secret"))
	if err != nil {
		t.Fatalf("seal: %v", err)
	}
	if _, err := Open("wrong-passphrase", sealed); err == nil {
		t.Fatal("expected decryption to fail with the wrong passphrase")
	}
}

func TestEmptyPassphraseRejected(t *testing.T) {
	if _, err := Seal("", []byte("x")); err == nil {
		t.Fatal("expected empty passphrase to be rejected")
	}
}
