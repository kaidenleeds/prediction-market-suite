package kalshi

import (
	"errors"
	"strings"
)

// ParseKeyFile splits the operator's plain Kalshi key file (R87 file-based credentials) into an
// optional embedded key id and the PEM bytes. Accepted shapes:
//
//	PEM header followed by key data             (the key id comes from config kalshi_key_id)
//	key_id: <uuid>                             (or keyid=, KEY-ID = …; case-insensitive, ':' or '=')
//	PEM header followed by key data
//
// A UTF-8 BOM, CRLF line endings and surrounding whitespace are tolerated (Windows editors). Only
// the SHAPE is validated here (a "-----BEGIN" PEM marker must exist) — NewSigner still owns real
// key parsing. SECURITY: callers must NEVER log the input bytes or the returned PEM; error text
// deliberately contains no file content.
func ParseKeyFile(b []byte) (keyID string, pemBytes []byte, err error) {
	s := strings.TrimPrefix(string(b), "\ufeff") // Notepad's "UTF-8 with BOM" et al.
	s = strings.TrimSpace(s)
	if s == "" {
		return "", nil, errors.New("key file is empty")
	}
	if !strings.HasPrefix(s, "-----BEGIN") {
		line, _, _ := strings.Cut(s, "\n")
		line = strings.TrimSpace(strings.TrimSuffix(line, "\r"))
		lower := strings.ToLower(line)
		for _, prefix := range []string{"key_id", "key-id", "keyid", "key id"} {
			if strings.HasPrefix(lower, prefix) {
				v := strings.TrimSpace(line[len(prefix):])
				if v != "" && (v[0] == ':' || v[0] == '=') {
					v = v[1:]
				}
				keyID = strings.TrimSpace(v)
				break
			}
		}
	}
	i := strings.Index(s, "-----BEGIN")
	if i < 0 {
		return "", nil, errors.New("no PEM block in key file (expected the RSA private key with -----BEGIN/-----END markers)")
	}
	return keyID, []byte(s[i:]), nil
}
