package kalshi

import (
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Signer produces the KALSHI-ACCESS-* headers for authenticated requests.
//
// Kalshi signs the string  <timestamp_ms> + <HTTP METHOD> + <request path>
// using RSA-PSS over SHA-256, then base64-encodes the signature. The request
// path is the URL path only (no scheme, host, or query string), e.g.
// "/trade-api/v2/portfolio/balance".
type Signer struct {
	KeyID string
	priv  *rsa.PrivateKey
}

// NewSigner parses a PEM-encoded RSA private key (PKCS#1 or PKCS#8) and returns
// a Signer. It is the single place that validates key material.
func NewSigner(keyID string, pemBytes []byte) (*Signer, error) {
	if keyID == "" {
		return nil, errors.New("empty key id")
	}
	priv, err := parseRSAPrivateKey(pemBytes)
	if err != nil {
		return nil, err
	}
	return &Signer{KeyID: keyID, priv: priv}, nil
}

func parseRSAPrivateKey(pemBytes []byte) (*rsa.PrivateKey, error) {
	block, _ := pem.Decode(pemBytes)
	if block == nil {
		return nil, errors.New("no PEM block found in private key")
	}
	if k, err := x509.ParsePKCS1PrivateKey(block.Bytes); err == nil {
		return k, nil
	}
	keyAny, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("parse private key (tried PKCS#1 and PKCS#8): %w", err)
	}
	k, ok := keyAny.(*rsa.PrivateKey)
	if !ok {
		return nil, errors.New("private key is not an RSA key")
	}
	return k, nil
}

// Headers returns the signed headers for a method + path pair.
//
// SIGN PATH ONLY — NEVER THE QUERY (R21, probed live 2026-07-02): Kalshi verifies the signature
// against the bare path; including "?rfq_id=..." yields 401 INCORRECT_API_KEY_SIGNATURE. This one
// character was why the suite NEVER saw a maker quote: every authenticated GET-with-query
// (GetQuotes above all) failed, while unauthenticated query GETs (bulk markets) worked — so the
// bug hid. The app "always fills" because makers DO quote; we just couldn't read the replies.
func (s *Signer) Headers(method, path string) (map[string]string, error) {
	if i := strings.IndexByte(path, '?'); i >= 0 {
		path = path[:i]
	}
	ts := strconv.FormatInt(time.Now().UnixMilli(), 10)
	digest := sha256.Sum256([]byte(ts + method + path))
	sig, err := rsa.SignPSS(rand.Reader, s.priv, crypto.SHA256, digest[:], &rsa.PSSOptions{
		SaltLength: rsa.PSSSaltLengthEqualsHash,
		Hash:       crypto.SHA256,
	})
	if err != nil {
		return nil, fmt.Errorf("sign request: %w", err)
	}
	return map[string]string{
		"KALSHI-ACCESS-KEY":       s.KeyID,
		"KALSHI-ACCESS-TIMESTAMP": ts,
		"KALSHI-ACCESS-SIGNATURE": base64.StdEncoding.EncodeToString(sig),
	}, nil
}
