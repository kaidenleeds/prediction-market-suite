package main

// R87 tests — file-based credential loading: loadSigner's source order (key file WINS over the
// encrypted store, store over nothing), the missing-kalshi_key_id red state, and polyUSLoad's
// file-over-env order. Every key/secret is a THROWAWAY generated in-process in a temp dir; the
// operator's real key files are never read, named, or depended on (cfg paths always point into
// t.TempDir()).

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/kalshi-suite/kalshi-suite/internal/config"
	"github.com/kalshi-suite/kalshi-suite/internal/secrets"
	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

// genPEM generates a throwaway PKCS#1 RSA private key for this test only.
func genPEM(t *testing.T) []byte {
	t.Helper()
	k, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate throwaway RSA key: %v", err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(k)})
}

// openStore opens a fresh temp-dir store, optionally seeding an encrypted demo credential.
func openStore(t *testing.T, seedKeyID, pass string, pemBytes []byte) *storage.Store {
	t.Helper()
	st, err := storage.Open(t.TempDir())
	if err != nil {
		t.Fatalf("storage.Open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	if seedKeyID != "" {
		sealed, err := secrets.Seal(pass, pemBytes)
		if err != nil {
			t.Fatalf("secrets.Seal: %v", err)
		}
		if err := st.SaveCredential(context.Background(), storage.Credential{
			Environment: string(config.EnvDemo), KeyID: seedKeyID,
			Salt: sealed.Salt, Nonce: sealed.Nonce, Ciphertext: sealed.Ciphertext,
		}); err != nil {
			t.Fatalf("SaveCredential: %v", err)
		}
	}
	return st
}

func writeKeyFile(t *testing.T, content []byte) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "throwaway-key.txt")
	if err := os.WriteFile(p, content, 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

// baseCfg is a minimal config whose key-file paths NEVER touch the operator's real files.
func baseCfg() config.Config {
	return config.Config{Environment: config.EnvDemo}
}

func TestLoadSignerFileWinsOverStore(t *testing.T) {
	t.Setenv("KALSHI_SUITE_PASSPHRASE", "pw") // store IS unlockable — the file must still win
	pemBytes := genPEM(t)
	st := openStore(t, "store-key-id", "pw", pemBytes)
	cfg := baseCfg()
	cfg.KalshiKeyFile = writeKeyFile(t, append([]byte("key_id: file-key-id\n"), pemBytes...))
	cfg.KalshiKeyID = "cfg-key-id" // embedded key_id line must beat this too

	s, src, err := loadSigner(context.Background(), cfg, st)
	if err != nil || s == nil {
		t.Fatalf("loadSigner(file present) = %v, %v", s, err)
	}
	if src != "file" || s.KeyID != "file-key-id" {
		t.Fatalf("got source=%q key_id=%q, want file/file-key-id (file wins; embedded id wins)", src, s.KeyID)
	}
}

func TestLoadSignerFilePEMOnlyUsesConfigKeyID(t *testing.T) {
	t.Setenv("KALSHI_SUITE_PASSPHRASE", "")
	st := openStore(t, "", "", nil)
	cfg := baseCfg()
	cfg.KalshiKeyFile = writeKeyFile(t, genPEM(t))
	cfg.KalshiKeyID = " cfg-key-id \n" // whitespace-tolerant

	s, src, err := loadSigner(context.Background(), cfg, st)
	if err != nil || s == nil {
		t.Fatalf("loadSigner(PEM-only + kalshi_key_id) = %v, %v", s, err)
	}
	if src != "file" || s.KeyID != "cfg-key-id" {
		t.Fatalf("got source=%q key_id=%q, want file/cfg-key-id", src, s.KeyID)
	}
}

func TestLoadSignerFileMissingKeyIDIsRed(t *testing.T) {
	t.Setenv("KALSHI_SUITE_PASSPHRASE", "pw")
	pemBytes := genPEM(t)
	st := openStore(t, "store-key-id", "pw", pemBytes) // an unlockable store must NOT rescue it
	cfg := baseCfg()
	cfg.KalshiKeyFile = writeKeyFile(t, pemBytes) // PEM-only, no kalshi_key_id anywhere

	s, src, err := loadSigner(context.Background(), cfg, st)
	if !errors.Is(err, errKeyFileNoKeyID) {
		t.Fatalf("want errKeyFileNoKeyID (no silent store fallback), got signer=%v src=%q err=%v", s, src, err)
	}
	if src != "file" {
		t.Fatalf("source = %q, want file", src)
	}
}

func TestLoadSignerFallsBackToStore(t *testing.T) {
	t.Setenv("KALSHI_SUITE_PASSPHRASE", "pw")
	st := openStore(t, "store-key-id", "pw", genPEM(t))
	cfg := baseCfg()
	cfg.KalshiKeyFile = filepath.Join(t.TempDir(), "does-not-exist.txt")
	cfg.KalshiKeyID = "cfg-key-id" // irrelevant on the store path

	s, src, err := loadSigner(context.Background(), cfg, st)
	if err != nil || s == nil {
		t.Fatalf("loadSigner(store) = %v, %v", s, err)
	}
	if src != "store" || s.KeyID != "store-key-id" {
		t.Fatalf("got source=%q key_id=%q, want store/store-key-id", src, s.KeyID)
	}
}

func TestLoadSignerStoreLockedWithoutPassphrase(t *testing.T) {
	t.Setenv("KALSHI_SUITE_PASSPHRASE", "")
	st := openStore(t, "store-key-id", "pw", genPEM(t))
	cfg := baseCfg() // no key file configured at all

	_, src, err := loadSigner(context.Background(), cfg, st)
	if !errors.Is(err, errCredsLocked) {
		t.Fatalf("want errCredsLocked, got src=%q err=%v", src, err)
	}
	if src != "store" {
		t.Fatalf("source = %q, want store", src)
	}
}

func TestLoadSignerNothingConfigured(t *testing.T) {
	t.Setenv("KALSHI_SUITE_PASSPHRASE", "")
	st := openStore(t, "", "", nil)
	cfg := baseCfg()

	s, src, err := loadSigner(context.Background(), cfg, st)
	if s != nil || src != "" || err != nil {
		t.Fatalf("public-only boot must be (nil, \"\", nil), got %v %q %v", s, src, err)
	}
}

// throwawaySecret is a valid base64 one-liner of the PolyUS shape (>=32 bytes, ends in "=").
func throwawaySecret(t *testing.T) string {
	t.Helper()
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	return base64.StdEncoding.EncodeToString(b)
}

func TestPolyUSLoadFileWinsOverEnv(t *testing.T) {
	t.Setenv("POLY_US_KEY_ID", "pus-key-id")
	t.Setenv("POLY_US_SECRET", throwawaySecret(t)) // env also valid — the file must still win
	t.Setenv("POLY_US_SECRET_FILE", "")
	cfg := baseCfg()
	cfg.PolyUSKeyFile = writeKeyFile(t, []byte(" \n"+throwawaySecret(t)+" \r\n")) // whitespace-trimmed

	c, src, err := polyUSLoad(cfg)
	if err != nil || c == nil {
		t.Fatalf("polyUSLoad(file) = %v, %v", c, err)
	}
	if src != "file" || c.KeyID() != "pus-key-id" {
		t.Fatalf("got source=%q key_id=%q, want file/pus-key-id", src, c.KeyID())
	}
}

func TestPolyUSLoadFileWithoutKeyIDIsRed(t *testing.T) {
	t.Setenv("POLY_US_KEY_ID", "")
	t.Setenv("POLY_US_SECRET", "")
	t.Setenv("POLY_US_SECRET_FILE", "")
	cfg := baseCfg()
	cfg.PolyUSKeyFile = writeKeyFile(t, []byte(throwawaySecret(t)))

	c, src, err := polyUSLoad(cfg)
	if !errors.Is(err, errPolyUSNoKeyID) {
		t.Fatalf("want errPolyUSNoKeyID, got client=%v src=%q err=%v", c, src, err)
	}
	if src != "file" {
		t.Fatalf("source = %q, want file", src)
	}
}

func TestPolyUSLoadEnvFallbackAndNone(t *testing.T) {
	t.Setenv("POLY_US_KEY_ID", "pus-key-id")
	t.Setenv("POLY_US_SECRET", throwawaySecret(t))
	t.Setenv("POLY_US_SECRET_FILE", "")
	cfg := baseCfg()
	cfg.PolyUSKeyFile = filepath.Join(t.TempDir(), "does-not-exist.txt")

	c, src, err := polyUSLoad(cfg)
	if err != nil || c == nil || src != "env" {
		t.Fatalf("env fallback: got client=%v src=%q err=%v, want client/env/nil", c, src, err)
	}

	t.Setenv("POLY_US_KEY_ID", "")
	t.Setenv("POLY_US_SECRET", "")
	c, src, err = polyUSLoad(cfg)
	if c != nil || src != "" || err != nil {
		t.Fatalf("nothing configured must be (nil, \"\", nil), got %v %q %v", c, src, err)
	}
}
