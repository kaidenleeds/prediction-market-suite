// mllock.go — cross-process advisory lock for ml_paper.json (audit #6).
//
// The ML paper book has TWO writers: the Python sidecar (manage_portfolio / settle_book in
// live_ml.py) and the Go settler (settleMLBook). Each write was individually atomic
// (temp+rename), but the two read-modify-write cycles interleaved: last-writer-wins silently
// dropped the other side's settlements, and that is the "Extra data" corruption observed on
// disk. Both sides now wrap their ENTIRE read-modify-write in this lock (the Python twin is
// _book_lock in live_ml.py — keep the protocol identical):
//
//   - lock file:  <dataDir>/ml_paper.lock, created with O_CREATE|O_EXCL (atomic everywhere)
//   - holder crash: a lock older than bookLockStale is broken by the next acquirer
//   - contention: wait up to bookLockWait, then SKIP the write (retry next cycle) — a trading
//     loop must never block on the book file
package server

import (
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// R92: vars (not consts) so the lock-contention regression test can shrink the wait and pin the
// stale-steal window — production values unchanged.
var (
	bookLockWait  = 3 * time.Second
	bookLockStale = 15 * time.Second
)

// acquireBookLock takes the cross-process ml_paper.json lock. ok=false means contention
// outlasted bookLockWait — the caller must skip its write and try again next cycle.
// release is always safe to call (no-op when ok=false).
func acquireBookLock(dataDir string) (release func(), ok bool) {
	lock := filepath.Join(dataDir, "ml_paper.lock")
	deadline := time.Now().Add(bookLockWait)
	for {
		f, err := os.OpenFile(lock, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
		if err == nil {
			_, _ = fmt.Fprintf(f, "%d %s", os.Getpid(), time.Now().UTC().Format(time.RFC3339))
			_ = f.Close()
			return func() { _ = os.Remove(lock) }, true
		}
		if fi, e := os.Stat(lock); e == nil && time.Since(fi.ModTime()) > bookLockStale {
			_ = os.Remove(lock) // holder crashed mid-write — break the stale lock and retry
			continue
		}
		if time.Now().After(deadline) {
			return func() {}, false
		}
		time.Sleep(50 * time.Millisecond)
	}
}
