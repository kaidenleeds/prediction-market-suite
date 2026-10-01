package server

// R89 regression tests — auditor items actioned this pass:
//   bug 72 (P1): `defer s.runGuarded(name, func(){})` was a NO-OP panic guard — recover() in a
//                nested deferred closure never sees the caller's in-flight panic. recoverGuard is
//                the working DEFER form; both behaviors are pinned here so neither regresses.
//   bug 70 (P2): telegram transport errors embed the bot token (full request URL) — redactSecret
//                masks it before the error reaches log/audit/UI.

import (
	"io"
	"log/slog"
	"testing"
)

func testSrv() *Server {
	return &Server{log: slog.New(slog.NewTextHandler(io.Discard, nil))}
}

// The fixed idiom: `defer s.recoverGuard(name)` must actually recover the caller's panic.
func TestRecoverGuardRecoversPanic(t *testing.T) {
	s := testSrv()
	done := false
	func() {
		defer s.recoverGuard("test-loop")
		done = true
		panic("boom")
	}()
	if !done {
		t.Fatal("body never ran")
	}
	// reaching here at all means the panic was recovered
}

// The OLD idiom (auditor bug 72): the panic must escape — this pins WHY every call site moved to
// recoverGuard. If Go's recover semantics ever change and this fails, the call sites can revisit.
func TestLegacyRunGuardedDeferIdiomDoesNotRecover(t *testing.T) {
	s := testSrv()
	escaped := false
	func() {
		defer func() {
			if recover() != nil {
				escaped = true
			}
		}()
		func() {
			defer s.runGuarded("legacy-noop-guard", func() {})
			panic("boom")
		}()
	}()
	if !escaped {
		t.Fatal("legacy defer-runGuarded idiom unexpectedly recovered the panic — revisit R89 bug-72 call sites")
	}
}

// runGuarded's CALL form (the correct, widely-used one) must keep recovering panics in f.
func TestRunGuardedCallFormRecovers(t *testing.T) {
	s := testSrv()
	s.runGuarded("call-form", func() { panic("boom") })
	// reaching here means the panic was recovered
}

// bug 70: the bot token must never survive into a loggable error string.
func TestRedactSecret(t *testing.T) {
	const token = "8123456789:AAE-secret_token_value"
	in := `Post "https://api.telegram.org/bot` + token + `/sendMessage": context deadline exceeded`
	out := redactSecret(in, token)
	if out == in {
		t.Fatal("redactSecret changed nothing")
	}
	if containsToken := (len(out) > 0 && stringsContains(out, token)); containsToken {
		t.Fatalf("token survived redaction: %q", out)
	}
	if !stringsContains(out, "***REDACTED***") {
		t.Fatalf("redaction marker missing: %q", out)
	}
	if got := redactSecret(in, ""); got != in {
		t.Fatal("empty secret must be a no-op")
	}
}

// tiny local alias so the test reads clean without importing strings twice in diffs.
func stringsContains(haystack, needle string) bool {
	return len(needle) > 0 && len(haystack) >= len(needle) && (func() bool {
		for i := 0; i+len(needle) <= len(haystack); i++ {
			if haystack[i:i+len(needle)] == needle {
				return true
			}
		}
		return false
	})()
}
