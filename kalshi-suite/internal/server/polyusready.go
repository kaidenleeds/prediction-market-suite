package server

import (
	"fmt"
	"strings"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/polymarketus"
)

// polyUSBooksReadyInput is a pure snapshot of the existing PolyUS WS/proof telemetry. Keeping the
// verdict pure makes cold-start, stale-feed and genuinely-small-universe behavior testable without
// a socket or clock sleeps.
type polyUSBooksReadyInput struct {
	Proofs, Requested, Frames                   int
	Full, Lite, Trade                           int
	FullCap                                     int
	Fresh, Executable                           int
	PriorityShortfall                           int
	ProofAge, DataAge, TransportAge, PrimaryAge time.Duration
	HavePrimary, HaveData, HaveTransport        bool
	MaxProofAge, MaxTransportAge                time.Duration
}

type polyUSBooksReadiness struct {
	OK, TransportCoverageOK, ExecutableOK bool
	State                                 string
	Detail                                string
}

func ageReadyText(age time.Duration, have bool) string {
	if !have || age < 0 {
		return "none"
	}
	return age.Truncate(time.Second).String()
}

// polyUSBooksClassify separates transport/coverage health from the current existence of an
// executable quote. A quiet complete board may be labelled WARMING while disarmed, but it is not
// ready: OK stays false until an executable quote exists. Strict LIVE/pre-arm callers also label
// the same condition BLOCKED so no diagnostic state can become execution authority.
func polyUSBooksClassify(in polyUSBooksReadyInput, allowWarming bool) polyUSBooksReadiness {
	if in.MaxProofAge <= 0 {
		in.MaxProofAge = polymarketus.MarketsRESTLifecycleMaxAge
	}
	if in.MaxTransportAge <= 0 {
		in.MaxTransportAge = polymarketus.MarketsWSPrimaryTransportMaxAge
	}
	fullCap := polymarketus.ClampMarketsFullBookCap(in.FullCap)
	wantFull := in.Proofs
	if wantFull > fullCap {
		wantFull = fullCap
	}
	wantWide := in.Proofs
	if wantWide > polymarketus.DefaultMarketsWideBookCap {
		wantWide = polymarketus.DefaultMarketsWideBookCap
	}
	proofOK := in.Proofs > 0 && in.ProofAge >= 0 && in.ProofAge < in.MaxProofAge
	coverageOK := in.Requested >= wantWide && in.Full >= wantFull &&
		in.Lite >= wantWide && in.Trade >= wantWide
	// Official docs define MARKET_DATA as a full order book, and runtime shows unchanged books may
	// be quiet. One valid current-generation payload is required before readiness can turn green;
	// after that, transport continuity (not time since the last book change) proves authority.
	primaryTransportOK := in.HavePrimary && in.HaveTransport &&
		in.TransportAge >= 0 && in.TransportAge < in.MaxTransportAge
	dataOK := primaryTransportOK && in.HaveData
	quoteOK := in.Fresh > 0 && in.Executable > 0
	transportOK := proofOK && coverageOK && dataOK
	detail := fmt.Sprintf("proofs=%d age=%s · requested=%d frames=%d · full=%d/%d cap=%d lite=%d/%d trade=%d/%d · fresh=%d executable=%d · primary=%v age=%s transport_age=%s data_age=%s · priority_shortfall=%d",
		in.Proofs, ageReadyText(in.ProofAge, in.ProofAge >= 0), in.Requested, in.Frames,
		in.Full, wantFull, fullCap, in.Lite, wantWide, in.Trade, wantWide, in.Fresh, in.Executable,
		in.HavePrimary, ageReadyText(in.PrimaryAge, in.HavePrimary),
		ageReadyText(in.TransportAge, in.HaveTransport), ageReadyText(in.DataAge, in.HaveData),
		in.PriorityShortfall)
	out := polyUSBooksReadiness{TransportCoverageOK: transportOK, ExecutableOK: quoteOK, Detail: detail}
	switch {
	case transportOK && quoteOK:
		out.OK, out.State = true, "ready"
	case allowWarming && proofOK && coverageOK && primaryTransportOK && !in.HaveData:
		out.State = "warming"
		out.Detail += " | current socket and complete subscription plan installed; awaiting its first market payload"
	case transportOK && allowWarming:
		out.State = "warming"
		out.Detail += " | transport/coverage healthy; awaiting a fresh explicitly-open two-sided executable quote"
	default:
		out.State = "blocked"
		if transportOK {
			out.Detail += " | exact executable quote required for LIVE/pre-arm"
		}
	}
	return out
}

// Compatibility helper remains strict: its callers ask whether the venue is execution-ready.
func polyUSBooksReady(in polyUSBooksReadyInput) (bool, string) {
	out := polyUSBooksClassify(in, false)
	return out.OK, out.Detail
}

type polyIntCLOBReadiness struct {
	OK, ExecutableOK bool
	State, Label     string
}

// polyIntCLOBClassify labels short EOF reconnects as transient warming, not a broken collector.
// It never makes a quote executable: OK remains false until the current socket has a fresh BBO.
func polyIntCLOBClassify(connected bool, fresh int, frameAgeSec float64,
	lastErr string, lastErrAge time.Duration) polyIntCLOBReadiness {
	if connected && fresh > 0 && frameAgeSec >= 0 && frameAgeSec <= 45 {
		return polyIntCLOBReadiness{OK: true, ExecutableOK: true, State: "ready", Label: "fresh current-generation CLOB BBO"}
	}
	errText := strings.ToLower(strings.TrimSpace(lastErr))
	transientEOF := strings.Contains(errText, "eof") && lastErrAge >= 0 && lastErrAge <= 2*time.Minute
	if transientEOF || (connected && fresh == 0 && frameAgeSec >= 0 && frameAgeSec <= 45) {
		label := "current socket connected; initial executable snapshots are warming"
		if transientEOF {
			label = "transient EOF; reconnect loop is backing off and obtaining a new generation"
		}
		return polyIntCLOBReadiness{State: "warming", Label: label}
	}
	return polyIntCLOBReadiness{State: "blocked", Label: "no fresh current-generation executable CLOB quote"}
}
