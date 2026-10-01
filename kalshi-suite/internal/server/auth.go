// auth.go — local-only authentication for the dashboard API (audit #3).
//
// Before this layer, every endpoint — including the real-money ones (/api/live/arm,
// /api/live/place, /api/live/combo/place, /api/killswitch reset, /api/mode, /api/settings) —
// was an open POST on 127.0.0.1: any web page the operator visited could fire a
// "simple" (no-preflight) cross-site request at it, and DNS rebinding defeated the
// localhost assumption because Host was never validated. The defenses, layered:
//
//  1. Host allow-list  — kills DNS rebinding (a rebound request carries the attacker's Host).
//  2. Origin allow-list — kills browser cross-site requests (browsers always attach Origin
//     to cross-origin POSTs; same-origin dashboard requests carry the local origin).
//  3. Bearer token on every mutating request — defense-in-depth for non-browser callers.
//     The token lives in DataDir/api-token.txt (0600) and is injected into the served
//     pages, whose fetch() wrapper attaches it automatically. curl users: read the file.
//  4. application/json Content-Type on bodies — a cross-site HTML form cannot produce it.
//
// Exception: POST /api/killswitch {"action":"trip"} is allowed WITHOUT a token. Tripping
// only reduces risk (halt + cancel), and in an emergency the operator must never be locked
// out of the stop button. Reset — which re-enables trading — requires the token.
package server

import (
	"bytes"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"runtime/debug"
	"strconv"
	"strings"
)

// loadOrCreateAPIToken returns the persistent local API token, generating one on first run.
func loadOrCreateAPIToken(dataDir string, log *slog.Logger) string {
	path := filepath.Join(dataDir, "api-token.txt")
	if b, err := os.ReadFile(path); err == nil {
		if t := strings.TrimSpace(string(b)); len(t) >= 16 {
			return t
		}
	}
	buf := make([]byte, 32)
	if _, err := rand.Read(buf); err != nil {
		// No randomness ⇒ no token ⇒ all mutating requests are refused (fail CLOSED, never open).
		log.Error("api token generation failed — all POST endpoints will refuse", "err", err)
		return ""
	}
	tok := hex.EncodeToString(buf)
	_ = os.MkdirAll(dataDir, 0o755)
	if err := os.WriteFile(path, []byte(tok+"\n"), 0o600); err != nil {
		log.Error("could not persist api token — dashboards will re-key every restart", "path", path, "err", err)
	} else {
		log.Info("API auth token created", "path", path)
	}
	return tok
}

func hostOnly(hostport string) string {
	if h, _, err := net.SplitHostPort(hostport); err == nil {
		return h
	}
	return hostport
}

func isLocalHostname(h string) bool {
	h = strings.Trim(strings.ToLower(strings.TrimSpace(h)), "[]")
	return h == "127.0.0.1" || h == "localhost" || h == "::1"
}

// killswitchTrip reports whether this request is the token-free emergency stop:
// POST /api/killswitch with {"action":"trip"}. It restores the body for the handler.
func killswitchTrip(r *http.Request) bool {
	if r.URL.Path != "/api/killswitch" {
		return false
	}
	b, err := io.ReadAll(io.LimitReader(r.Body, 4096))
	_ = r.Body.Close()
	r.Body = io.NopCloser(bytes.NewReader(b))
	if err != nil {
		return false
	}
	var req struct {
		Action string `json:"action"`
	}
	return json.Unmarshal(b, &req) == nil && req.Action == "trip"
}

// secureMux wraps the API mux with the local-only auth layer. See the file header for the model.
func (s *Server) secureMux(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// R67a: net/http's default panic recovery CLOSES the connection with no response body — the
		// dashboard's fetch() then rejects and the panel sticks on "Could not load …" with zero trace.
		// Convert handler panics into a logged stack + a JSON 500 (best-effort if headers already sent).
		defer func() {
			if p := recover(); p != nil {
				if s.log != nil {
					s.log.Error("HTTP handler panic", "path", r.URL.Path, "panic", fmt.Sprint(p), "stack", string(debug.Stack()))
				}
				func() {
					defer func() { _ = recover() }() // headers may already be sent — never double-panic
					writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal: " + fmt.Sprint(p)})
				}()
			}
		}()
		// (1) DNS-rebinding defense: the browser sends the attacker's hostname in Host.
		if !isLocalHostname(hostOnly(r.Host)) {
			http.Error(w, "forbidden: non-local Host", http.StatusForbidden)
			return
		}
		// (2) Cross-site defense: browsers attach Origin to every cross-origin request
		// (and to same-origin POSTs, where it will be our local origin). "null" (sandboxed
		// iframe / file://) is treated as hostile.
		if o := r.Header.Get("Origin"); o != "" {
			ou, err := url.Parse(o)
			if err != nil || !isLocalHostname(ou.Hostname()) {
				http.Error(w, "forbidden: cross-origin request", http.StatusForbidden)
				return
			}
		}
		// (3+4) Mutating requests: token + JSON body.
		if r.Method != http.MethodGet && r.Method != http.MethodHead && r.Method != http.MethodOptions {
			if killswitchTrip(r) {
				next.ServeHTTP(w, r) // emergency stop is never locked out
				return
			}
			tok := r.Header.Get("X-Api-Token")
			if tok == "" {
				if ah := r.Header.Get("Authorization"); strings.HasPrefix(ah, "Bearer ") {
					tok = strings.TrimSpace(strings.TrimPrefix(ah, "Bearer "))
				}
			}
			if s.apiToken == "" || subtle.ConstantTimeCompare([]byte(tok), []byte(s.apiToken)) != 1 {
				writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "missing or invalid X-Api-Token (token file: data/api-token.txt)"})
				return
			}
			if r.ContentLength != 0 {
				if ct := strings.ToLower(r.Header.Get("Content-Type")); !strings.HasPrefix(ct, "application/json") {
					writeJSON(w, http.StatusUnsupportedMediaType, map[string]string{"error": "Content-Type must be application/json"})
					return
				}
			}
		}
		next.ServeHTTP(w, r)
	})
}

// servePage writes one of the static dashboard pages with the API token + a fetch()
// wrapper injected, so every mutating request the page makes carries X-Api-Token
// without touching the ~60 fetch call sites. no-store keeps the token off disk caches.
func (s *Server) servePage(w http.ResponseWriter, html string) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	inj := "<script>window.API_TOKEN=" + strconv.Quote(s.apiToken) +
		";(function(){var f=window.fetch;window.fetch=function(u,o){if(o&&o.method&&String(o.method).toUpperCase()!=='GET'){o.headers=o.headers||{};if(typeof o.headers.set==='function'){o.headers.set('X-Api-Token',window.API_TOKEN);}else{o.headers['X-Api-Token']=window.API_TOKEN;}}return f.call(this,u,o);};})();</script>"
	if i := strings.Index(html, "<head>"); i >= 0 {
		html = html[:i+len("<head>")] + inj + html[i+len("<head>"):]
	} else {
		html = inj + html
	}
	_, _ = w.Write([]byte(html))
}
