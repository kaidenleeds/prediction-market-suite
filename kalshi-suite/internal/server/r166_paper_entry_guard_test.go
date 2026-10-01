package server

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

func TestR166FreshInvUsesOneGenericDelayedHandoffAndNoLegacyLot(t *testing.T) {
	for _, tc := range []struct {
		name, platform, ticker, topology string
	}{
		{name: "kalshi", platform: "kalshi", ticker: "KXR166-FRESHINV", topology: "K"},
		{name: "polyus", platform: "polyus", ticker: "r166-freshinv-pus", topology: "PUS"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := testServer(t)
			paperCh := r163InstallPaperCapture(s, 4, false)
			t.Cleanup(func() { r163RemovePaperCapture(s) })

			sig := storage.Signal{Platform: tc.platform, Ticker: tc.ticker,
				Title: "R166 fresh listing", Side: "YES", SignalType: "freshlist",
				EntryPrice: .45, ResolveHours: 1,
				ExecExpr: r147InputReceiptExpr(tc.topology, time.Now().UTC())}
			if err := s.insertSignal(context.Background(), sig); err != nil {
				t.Fatalf("insert exact freshlist signal: %v", err)
			}

			// Production calls freshInvPlace after insertSignal. It must neither open the old JSON
			// portfolio nor enqueue the same opportunity a second time.
			r128FreshFeeds(s, tc.ticker)
			bookReads := 0
			s.completeBookSignalFn = func(in storage.Signal) (storage.Signal, string, bool) {
				bookReads++
				return storage.Signal{}, "", false
			}
			s.freshInvPlace(context.Background(), tc.platform, tc.ticker, sig.Title, .45, 2, 1)
			if bookReads != 0 {
				t.Fatalf("retired FreshInv direct path performed %d book read(s)", bookReads)
			}

			var intent genfollowPaperIntent
			select {
			case intent = <-paperCh:
			case <-time.After(250 * time.Millisecond):
				t.Fatal("insertSignal did not preserve the generic delayed Paper handoff")
			}
			matches := 0
			for _, queued := range intent.Signals {
				got := queued.Signal
				if got.Platform == tc.platform && got.Ticker == tc.ticker &&
					got.SignalType == "freshlist" && strings.EqualFold(got.Side, "YES") &&
					r147SignalInputTopology(got) == tc.topology {
					matches++
				}
			}
			if matches != 1 {
				t.Fatalf("exact freshlist handoff count=%d, want 1: %+v", matches, intent.Signals)
			}
			if len(paperCh) != 0 {
				t.Fatalf("FreshInv direct seam duplicated the generic Paper handoff: %d extra batch(es)", len(paperCh))
			}

			s.fiBookMu.Lock()
			open := len(s.fiLoadLocked().Open)
			s.fiBookMu.Unlock()
			if open != 0 {
				t.Fatalf("retired FreshInv JSON book opened %d modeled lot(s)", open)
			}
		})
	}
}

func TestR166MLPostIsTrackedButCannotCreatePaperEntry(t *testing.T) {
	for _, platform := range []string{"kalshi", "polyus"} {
		t.Run(platform, func(t *testing.T) {
			s := testServer(t)
			body, _ := json.Marshal(map[string]any{
				"row": map[string]any{"ticker": "R166-MLPOST-" + platform,
					"side": "YES", "platform": platform, "contracts": 2.0, "title": "R166"},
				"ref_price": .40,
			})
			req := httptest.NewRequest("POST", "/api/mlpost", strings.NewReader(string(body)))
			w := httptest.NewRecorder()
			s.handleMLPost(w, req)

			var out map[string]any
			if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil {
				t.Fatalf("decode response: %v (%s)", err, w.Body.String())
			}
			if out["status"] != "reject" || out["reason"] != "book-native-ml-paper-log-only" {
				t.Fatalf("ML post was not fail-closed: %v", out)
			}
			s.pendMu.Lock()
			pending := len(s.pendingMakers)
			s.pendMu.Unlock()
			if pending != 0 {
				t.Fatalf("retired ML endpoint created %d pending maker entry/entries", pending)
			}
			var attempts int
			if err := s.store.DBForTest().QueryRow(`SELECT COUNT(*) FROM maker_fill_stats`).Scan(&attempts); err != nil {
				t.Fatal(err)
			}
			if attempts != 0 {
				t.Fatalf("retired ML endpoint created %d maker attempt(s)", attempts)
			}

			now := time.Now().UTC()
			snapshot := s.r147SystemVariantRuntimeSnapshot(context.Background(), now)
			for _, route := range []string{"maker", "taker"} {
				found := false
				for _, row := range snapshot.Rows {
					if row.SystemID != "ml-book" || row.ExecutionVenue != platform ||
						row.Side != "YES" || row.Route != route || row.InputTopology != r147VenueCode(platform) {
						continue
					}
					found = true
					if !row.FreshFed || row.State != "excluded" ||
						row.PaperExecution != "log_only_book_native_ml" ||
						row.Blocker != "book-native-ml-paper-log-only" {
						t.Fatalf("%s runtime row did not report exact log-only exclusion: %+v", route, row)
					}
				}
				if !found {
					t.Fatalf("missing exact %s ML runtime row", route)
				}
			}
		})
	}
}
