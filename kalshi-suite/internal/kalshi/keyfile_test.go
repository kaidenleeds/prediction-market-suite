package kalshi

// R87 tests — ParseKeyFile, the operator key-file shape parser. Every key here is a THROWAWAY
// RSA key generated in-process; the tests never touch (or even name) the operator's real key
// files, per the R87 handling rule.

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"strings"
	"sync"
	"testing"
)

var (
	testPEMOnce sync.Once
	testPEMStr  string
)

// testPEM returns one shared throwaway PKCS#1 RSA private key (the Kalshi portal's format),
// generated once per test run.
func testPEM(t *testing.T) string {
	t.Helper()
	testPEMOnce.Do(func() {
		k, err := rsa.GenerateKey(rand.Reader, 2048)
		if err != nil {
			t.Fatalf("generate throwaway RSA key: %v", err)
		}
		testPEMStr = string(pem.EncodeToMemory(&pem.Block{
			Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(k),
		}))
	})
	if testPEMStr == "" {
		t.Fatal("throwaway key generation failed in an earlier test")
	}
	return testPEMStr
}

func TestParseKeyFilePEMOnly(t *testing.T) {
	p := testPEM(t)
	keyID, pemBytes, err := ParseKeyFile([]byte("\n  " + p + "\n\n"))
	if err != nil {
		t.Fatalf("PEM-only parse: %v", err)
	}
	if keyID != "" {
		t.Fatalf("PEM-only file must yield an empty embedded key id, got %q", keyID)
	}
	if _, err := NewSigner("cfg-key-id", pemBytes); err != nil {
		t.Fatalf("parsed PEM must build a signer with a config key id: %v", err)
	}
}

func TestParseKeyFileEmbeddedKeyID(t *testing.T) {
	p := testPEM(t)
	cases := map[string]string{
		"key_id: abc-123":  "abc-123",
		"keyid=xyz-9":      "xyz-9",
		"KEY_ID = Q-1":     "Q-1",
		"Key-Id:\tuuid-77": "uuid-77",
	}
	for line, want := range cases {
		// BOM + CRLF: the exact bytes a Windows editor writes.
		in := "\ufeff" + line + "\r\n" + strings.ReplaceAll(p, "\n", "\r\n")
		keyID, pemBytes, err := ParseKeyFile([]byte(in))
		if err != nil {
			t.Fatalf("parse with first line %q: %v", line, err)
		}
		if keyID != want {
			t.Fatalf("first line %q: embedded key id = %q, want %q", line, keyID, want)
		}
		if _, err := NewSigner(keyID, pemBytes); err != nil {
			t.Fatalf("first line %q: signer from parsed parts: %v", line, err)
		}
	}
}

func TestParseKeyFileRejectsNonPEM(t *testing.T) {
	if _, _, err := ParseKeyFile([]byte("this is not a key")); err == nil {
		t.Fatal("garbage without a PEM BEGIN marker must error")
	}
	if _, _, err := ParseKeyFile([]byte("   \r\n \n")); err == nil {
		t.Fatal("whitespace-only file must error")
	}
	if _, _, err := ParseKeyFile(nil); err == nil {
		t.Fatal("empty file must error")
	}
}

func TestParseKeyFileKeyIDLineWithoutPEM(t *testing.T) {
	if _, _, err := ParseKeyFile([]byte("key_id: abc-123\n")); err == nil {
		t.Fatal("a key_id line with no PEM body must error")
	}
}
