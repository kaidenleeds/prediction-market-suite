package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/kalshi"
	"github.com/kalshi-suite/kalshi-suite/internal/paper"
	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

const (
	addedGhibaudoEvent = "KXATPCHALLENGERMATCH-26JUL16ADDGHI"
	addedTicker        = addedGhibaudoEvent + "-ADD"
	ghibaudoTicker     = addedGhibaudoEvent + "-GHI"
)

func TestKalshiEventExposureConflictAddedGhibaudo(t *testing.T) {
	tests := []struct {
		kind, want string
	}{
		{"position", "existing-kalshi-event-position"},
		{"order", "existing-kalshi-event-resting-order"},
		{"reservation", "existing-kalshi-event-pending-order"},
	}
	for _, tc := range tests {
		t.Run(tc.kind, func(t *testing.T) {
			got := kalshiEventExposureConflict(ghibaudoTicker, addedGhibaudoEvent, []kalshiEventExposure{{
				Ticker: addedTicker, EventTicker: addedGhibaudoEvent, Kind: tc.kind,
			}})
			if got != tc.want {
				t.Fatalf("Added held + Ghibaudo candidate via %s = %q, want %q", tc.kind, got, tc.want)
			}
		})
	}
}

func TestKalshiEventExposureConflictAllowsDifferentSubbetHeader(t *testing.T) {
	held := []kalshiEventExposure{{
		Ticker: addedTicker, EventTicker: addedGhibaudoEvent, Kind: "position",
	}}
	// A spread/total/prop for the same underlying match has its own venue event_ticker. It is
	// related for cluster exposure, but it is not an outcome sibling under this exact header.
	if got := kalshiEventExposureConflict("KXATPCHALLENGERSETTOTAL-26JUL16ADDGHI-3",
		"KXATPCHALLENGERSETTOTAL-26JUL16ADDGHI", held); got != "" {
		t.Fatalf("different Kalshi event_ticker was hard-blocked: %q", got)
	}
}

func TestPaperKalshiEventHeaderParityAddedGhibaudo(t *testing.T) {
	s := testServer(t)
	s.kmkts = map[string]kalshi.Market{
		addedTicker:    {Ticker: addedTicker, EventTicker: addedGhibaudoEvent},
		ghibaudoTicker: {Ticker: ghibaudoTicker, EventTicker: addedGhibaudoEvent},
	}
	s.kmktsAt = map[string]time.Time{}
	held := []paper.Position{{Platform: "kalshi", Ticker: addedTicker, Side: "YES", Contracts: 13}}
	if got := s.paperKalshiEventHeaderConflict(context.Background(), ghibaudoTicker, "YES", held); got != "existing-kalshi-event-position" {
		t.Fatalf("Paper sibling outcome = %q, want exact event-header conflict", got)
	}
}

func TestKalshiEventExposureConflictFailsClosedWithoutCandidateIdentity(t *testing.T) {
	if got := kalshiEventExposureConflict(ghibaudoTicker, "", nil); got != "kalshi-event-identity-unavailable" {
		t.Fatalf("missing event identity = %q", got)
	}
}

func TestKalshiPackageRejectsDuplicateNativeEventHeader(t *testing.T) {
	s := testServer(t)
	s.kmkts = map[string]kalshi.Market{
		addedTicker:    {Ticker: addedTicker, EventTicker: addedGhibaudoEvent},
		ghibaudoTicker: {Ticker: ghibaudoTicker, EventTicker: addedGhibaudoEvent},
	}
	if _, why := s.kalshiPackageEventTickersStrict(context.Background(),
		[]kalshiPackageLeg{{Ticker: addedTicker, Side: "YES"}, {Ticker: ghibaudoTicker, Side: "YES"}}); why != "duplicate-kalshi-event-header-in-package" {
		t.Fatalf("sibling outcomes in one package = %q", why)
	}
	other := "KXATPCHALLENGERTOTAL-26JUL16ADDGHI-25"
	s.kmkts[other] = kalshi.Market{Ticker: other, EventTicker: "KXATPCHALLENGERTOTAL-26JUL16ADDGHI"}
	if _, why := s.kalshiPackageEventTickersStrict(context.Background(),
		[]kalshiPackageLeg{{Ticker: addedTicker, Side: "YES"}, {Ticker: other, Side: "YES"}}); why != "" {
		t.Fatalf("different native headers were refused: %q", why)
	}
}

func TestR152StagedAdmissionWiresNativeEventHeaderGuard(t *testing.T) {
	s := testServer(t)
	cfg := *s.cfg()
	cfg.Risk.LiveStagedBundles = true
	s.cfgP.Store(&cfg)
	s.kmkts = map[string]kalshi.Market{
		addedTicker:    {Ticker: addedTicker, EventTicker: addedGhibaudoEvent},
		ghibaudoTicker: {Ticker: ghibaudoTicker, EventTicker: addedGhibaudoEvent},
	}
	bundle := storage.ResearchRouteBundle{BundleID: "r152-sibling-package", SystemID: "event-basket-lock",
		Legs: []storage.ResearchRouteBundleLeg{
			{Index: 0, Venue: "kalshi", Ticker: addedTicker, Side: "YES", Quantity: 1},
			{Index: 1, Venue: "kalshi", Ticker: ghibaudoTicker, Side: "NO", Quantity: 1},
		}}
	_, err := newR148ServerStagedBroker(s).Admission(context.Background(), bundle,
		storage.StagedBundleExecutionProof{})
	if err == nil || !strings.Contains(err.Error(), "duplicate-kalshi-event-header-in-package") {
		t.Fatalf("staged admission did not enforce package header ownership: %v", err)
	}
}

func TestKalshiPackageGuardChecksAccountAndCanIgnoreOnlyItsOwnReservation(t *testing.T) {
	positionTicker := "KX-PACKAGE-HELD"
	positionEvent := "KX-PACKAGE-EVENT"
	candidateTicker := "KX-PACKAGE-CANDIDATE"
	distinctTicker := "KX-PACKAGE-DISTINCT"
	var exposePosition atomic.Bool
	exposePosition.Store(true)
	venue := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/portfolio/positions":
			if exposePosition.Load() {
				_, _ = w.Write([]byte(`{"market_positions":[{"ticker":"` + positionTicker +
					`","position_fp":"1.00"}],"cursor":""}`))
			} else {
				_, _ = w.Write([]byte(`{"market_positions":[],"cursor":""}`))
			}
		case "/portfolio/orders":
			_, _ = w.Write([]byte(`{"orders":[],"cursor":""}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer venue.Close()
	s := testServer(t)
	s.kal = kalshi.NewClient(venue.URL, queueTestSigner(t), 10_000, time.Second)
	s.kmkts = map[string]kalshi.Market{
		positionTicker:  {Ticker: positionTicker, EventTicker: positionEvent},
		candidateTicker: {Ticker: candidateTicker, EventTicker: positionEvent},
		distinctTicker:  {Ticker: distinctTicker, EventTicker: "KX-PACKAGE-OTHER-EVENT"},
	}
	if why := s.liveKalshiPackageEventHeaderConflict(context.Background(),
		[]kalshiPackageLeg{{Ticker: candidateTicker, Side: "YES"}}, ""); why != "existing-kalshi-event-position" {
		t.Fatalf("held sibling position conflict = %q", why)
	}
	if why := s.liveKalshiPackageEventHeaderConflict(context.Background(),
		[]kalshiPackageLeg{{Ticker: distinctTicker, Side: "YES"}}, ""); why != "" {
		t.Fatalf("different event package leg refused: %q", why)
	}

	// Remove venue exposure and prove that only the exact package reservation may be ignored.
	exposePosition.Store(false)
	risk := r148RiskTestReservation(t, s.store, "risk-package-event-guard")
	s.kmkts["KXTEST"] = kalshi.Market{Ticker: "KXTEST", EventTicker: positionEvent}
	if why := s.liveKalshiPackageEventHeaderConflict(context.Background(),
		[]kalshiPackageLeg{{Ticker: candidateTicker, Side: "YES"}}, ""); why != "existing-kalshi-event-pending-order" {
		t.Fatalf("other package reservation conflict = %q", why)
	}
	if why := s.liveKalshiPackageEventHeaderConflict(context.Background(),
		[]kalshiPackageLeg{{Ticker: candidateTicker, Side: "YES"}}, risk.ReservationID); why != "" {
		t.Fatalf("package could not advance past its own exact reservation: %q", why)
	}
}
