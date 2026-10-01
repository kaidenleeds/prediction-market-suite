package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/polymarketus"
)

func TestR142PolyUSHealthyTransportZeroQuotesIsOnlyDisarmedWarming(t *testing.T) {
	in := healthyPolyUSBooksInput()
	in.Fresh, in.Executable = 0, 0
	warming := polyUSBooksClassify(in, true)
	if warming.OK || warming.State != "warming" || !warming.TransportCoverageOK || warming.ExecutableOK ||
		!strings.Contains(warming.Detail, "awaiting") {
		t.Fatalf("disarmed zero-quote classification=%+v", warming)
	}
	strict := polyUSBooksClassify(in, false)
	if strict.OK || strict.State != "blocked" || !strict.TransportCoverageOK || strict.ExecutableOK ||
		!strings.Contains(strict.Detail, "LIVE/pre-arm") {
		t.Fatalf("strict zero-quote classification=%+v", strict)
	}
	if ok, _ := polyUSBooksReady(in); ok {
		t.Fatal("strict compatibility readiness treated zero executable quotes as ready")
	}
}

func TestR182PolyUSCurrentGenerationWaitingForFirstPayloadIsWarmingWhenDisarmed(t *testing.T) {
	in := healthyPolyUSBooksInput()
	in.HaveData, in.Fresh, in.Executable, in.DataAge = false, 0, 0, -1
	warming := polyUSBooksClassify(in, true)
	if warming.OK || warming.State != "warming" || warming.TransportCoverageOK || warming.ExecutableOK ||
		!strings.Contains(warming.Detail, "awaiting its first market payload") {
		t.Fatalf("connected first-payload wait classification=%+v", warming)
	}
	strict := polyUSBooksClassify(in, false)
	if strict.OK || strict.State != "blocked" || strict.TransportCoverageOK || strict.ExecutableOK {
		t.Fatalf("strict first-payload wait classification=%+v", strict)
	}
}

func TestR142PolyIntEOFIsWarmingButNeverExecutable(t *testing.T) {
	warming := polyIntCLOBClassify(false, 0, 2, "read: EOF", 10*time.Second)
	if warming.OK || warming.ExecutableOK || warming.State != "warming" ||
		!strings.Contains(strings.ToLower(warming.Label), "eof") {
		t.Fatalf("transient EOF classification=%+v", warming)
	}
	stale := polyIntCLOBClassify(false, 0, 90, "read: EOF", 3*time.Minute)
	if stale.OK || stale.State != "blocked" {
		t.Fatalf("old repeated EOF remained warming: %+v", stale)
	}
	ready := polyIntCLOBClassify(true, 4, 3, "read: EOF", time.Second)
	if !ready.OK || !ready.ExecutableOK || ready.State != "ready" {
		t.Fatalf("fresh replacement generation did not recover: %+v", ready)
	}
}

func TestR142PolyUSWarmingNeverMasksTransportOrCoverageFailure(t *testing.T) {
	for name, mutate := range map[string]func(*polyUSBooksReadyInput){
		"stale proof":     func(in *polyUSBooksReadyInput) { in.ProofAge = in.MaxProofAge },
		"stale transport": func(in *polyUSBooksReadyInput) { in.TransportAge = in.MaxTransportAge },
		"short lite":      func(in *polyUSBooksReadyInput) { in.Lite = in.Proofs - 1 },
		"no primary": func(in *polyUSBooksReadyInput) {
			in.HavePrimary, in.HaveData, in.HaveTransport = false, false, false
		},
	} {
		t.Run(name, func(t *testing.T) {
			in := healthyPolyUSBooksInput()
			in.Fresh, in.Executable = 0, 0
			mutate(&in)
			got := polyUSBooksClassify(in, true)
			if got.OK || got.State != "blocked" || got.TransportCoverageOK {
				t.Fatalf("broken transport was softened to warming: %+v", got)
			}
		})
	}
}

func TestR142PolyUSWarmingCannotFabricatePaperCandidateQuote(t *testing.T) {
	s := testServer(t)
	s.polyUSWS = (&polymarketus.Client{}).NewMarketsWS()
	if ask, depth, source, ok := s.executableAsk(context.Background(), "polyus", "quiet-market", "YES", false); ok || ask != 0 || depth != 0 || source != "polyus-ws" {
		t.Fatalf("empty warming transport fabricated Paper quote ask=%v depth=%v source=%q ok=%v",
			ask, depth, source, ok)
	}
	// A readiness allowance is not a fill allowance: zero depth remains non-executable.
	if got := executableContracts(1, 0); got != 0 {
		t.Fatalf("zero-depth candidate filled %v contracts", got)
	}
}

func TestR142PolyUSWarmingCannotPassLivePrearm(t *testing.T) {
	s := testServer(t)
	r149EnableLiveDestinations(s, false, true)
	s.pusAuthState = "ok"
	s.polyUSWS = (&polymarketus.Client{}).NewMarketsWS()
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/api/live/arm", strings.NewReader(`{"confirm":true}`))
	s.handleLiveArm(w, r)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("zero-quote PolyUS prearm status=%d body=%s", w.Code, w.Body.String())
	}
	s.liveMu.Lock()
	armed := s.liveArmed
	s.liveMu.Unlock()
	kalshiArmed := s.kal != nil && s.kal.Armed()
	if armed || kalshiArmed {
		t.Fatalf("warming prearm enabled write gates server=%v kalshi=%v", armed, kalshiArmed)
	}
}
