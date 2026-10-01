// Package secrets encrypts sensitive material (the Kalshi RSA private key) at
// rest, using Argon2id key derivation from a user passphrase and AES-256-GCM.
//
// The passphrase and the derived key live only in memory. The plaintext private
// key is never written to disk; only the sealed (salt, nonce, ciphertext) tuple
// is persisted.
package secrets

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"errors"
	"fmt"
	"io"

	"golang.org/x/crypto/argon2"
)

const (
	saltLen      = 16
	keyLen       = 32 // AES-256
	argonTime    = 1
	argonMem     = 64 * 1024 // 64 MiB
	argonThreads = 4
)

// Sealed holds everything required to decrypt later (all safe to persist).
type Sealed struct {
	Salt       []byte
	Nonce      []byte
	Ciphertext []byte
}

func deriveKey(passphrase string, salt []byte) []byte {
	return argon2.IDKey([]byte(passphrase), salt, argonTime, argonMem, argonThreads, keyLen)
}

// Seal encrypts plaintext under a passphrase, generating a fresh salt and nonce.
func Seal(passphrase string, plaintext []byte) (Sealed, error) {
	if passphrase == "" {
		return Sealed{}, errors.New("empty passphrase")
	}
	salt := make([]byte, saltLen)
	if _, err := io.ReadFull(rand.Reader, salt); err != nil {
		return Sealed{}, err
	}
	gcm, err := newGCM(deriveKey(passphrase, salt))
	if err != nil {
		return Sealed{}, err
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return Sealed{}, err
	}
	ct := gcm.Seal(nil, nonce, plaintext, nil)
	return Sealed{Salt: salt, Nonce: nonce, Ciphertext: ct}, nil
}

// Open decrypts a Sealed value under the passphrase. A wrong passphrase (or any
// tampering) yields an error rather than garbage, thanks to GCM authentication.
func Open(passphrase string, s Sealed) ([]byte, error) {
	if passphrase == "" {
		return nil, errors.New("empty passphrase")
	}
	gcm, err := newGCM(deriveKey(passphrase, s.Salt))
	if err != nil {
		return nil, err
	}
	if len(s.Nonce) != gcm.NonceSize() {
		return nil, fmt.Errorf("bad nonce length %d", len(s.Nonce))
	}
	pt, err := gcm.Open(nil, s.Nonce, s.Ciphertext, nil)
	if err != nil {
		return nil, fmt.Errorf("decrypt failed (wrong passphrase or corrupted data): %w", err)
	}
	return pt, nil
}

func newGCM(key []byte) (cipher.AEAD, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}
