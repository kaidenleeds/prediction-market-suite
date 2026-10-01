package server

import (
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/polymarketus"
	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

func r147ContractForTest(t *testing.T, family, venue, side, topology string) r147SystemVariantSignalContract {
	t.Helper()
	for _, row := range r147ExactSignalContracts(family, venue, side, "taker") {
		if row.InputTopology == topology {
			return row
		}
	}
	t.Fatalf("missing exact contract %s %s %s %s", family, venue, side, topology)
	return r147SystemVariantSignalContract{}
}

func TestR147ProspectiveSignalContractBindsExactTopology(t *testing.T) {
	xmatch := liveMirrorCandidate{Platform: "kalshi", Ticker: "KXTEST", Side: "YES", Source: "auto-cons-xmatch"}
	bound, row, why := r147BindSignalContract(xmatch, "taker")
	if why != "" || bound.InputTopology != "K-PINT" || bound.SignalContractID == "" ||
		bound.SignalContractID != r147SignalContractIdentity(row) {
		t.Fatalf("unambiguous bind=%+v row=%+v why=%q", bound, row, why)
	}

	confluence := xmatch
	confluence.Source = "auto-cons-confluence"
	if _, _, why := r147BindSignalContract(confluence, "taker"); why != "prospective-allocation-signal-topology-ambiguous" {
		t.Fatalf("untyped confluence did not fail closed: %q", why)
	}
	confluence.InputTopology = "K-PUS"
	bound, row, why = r147BindSignalContract(confluence, "taker")
	if why != "" || bound.InputTopology != "K-PUS" || bound.SignalContractID != r147SignalContractIdentity(row) {
		t.Fatalf("explicit confluence bind=%+v row=%+v why=%q", bound, row, why)
	}

	mismatch := confluence
	mismatch.SignalContractID = "r147sc-v1|wrong"
	if _, _, why := r147BindSignalContract(mismatch, "taker"); why != "prospective-allocation-signal-contract-mismatch" {
		t.Fatalf("mismatched identity did not fail closed: %q", why)
	}
	inputOnly := xmatch
	inputOnly.Platform = "polymarket"
	if _, _, why := r147BindSignalContract(inputOnly, "taker"); why != "prospective-allocation-signal-contract-absent" {
		t.Fatalf("PINT became an execution venue: %q", why)
	}
}

func TestR167ProperMomentumContractRequiresSELLAction(t *testing.T) {
	base := liveMirrorCandidate{Platform: "kalshi", Ticker: "KXSELL", Side: "YES",
		Family: "proper-score-momentum", Source: "r139ms:fixture",
		InputTopology: "K-PROPER-BRIER", Action: "SELL"}
	bound, row, why := r147BindSignalContract(base, "taker")
	if why != "" || !row.ExecutionDerivative || bound.SignalContractID != r147SignalContractIdentity(row) {
		t.Fatalf("SELL derivative bind=%+v row=%+v why=%q", bound, row, why)
	}
	buy := base
	buy.Action = "BUY"
	if _, _, why := r147BindSignalContract(buy, "taker"); why != "prospective-allocation-signal-contract-absent" {
		t.Fatalf("BUY borrowed SELL/FOK contract: %q", why)
	}
}

func TestR148SignalContractKeysSeparateCurrentCrossVenueReceiptsWithoutResettingEconomics(t *testing.T) {
	base := liveMirrorCandidate{Platform: "kalshi", Ticker: "KXTEST", Side: "YES", Source: "auto-cons-confluence"}
	bind := func(topology string) liveMirrorCandidate {
		c := base
		c.InputTopology = topology
		bound, _, why := r147BindSignalContract(c, "taker")
		if why != "" {
			t.Fatalf("bind %s: %s", topology, why)
		}
		return bound
	}
	pint, pus, triple := bind("K-PINT"), bind("K-PUS"), bind("K-PUS-PINT")
	seenSignal := map[string]bool{}
	for _, c := range []liveMirrorCandidate{pint, pus, triple} {
		if seenSignal[liveAllocationSignalKey(c)] {
			t.Fatalf("current topology receipts pooled: %+v", c)
		}
		seenSignal[liveAllocationSignalKey(c)] = true
	}
	// R148 operator override: current input topology remains exact, while valid executable
	// UnitTrial economics stay in the existing family+venue+origin+side lane instead of resetting
	// history into a newly introduced SignalContractID namespace.
	if pint.key() == pus.key() || pus.key() == triple.key() {
		t.Fatal("dispatch/dedup keys pooled different input topologies")
	}
}

func TestR167LiveIntentDedupCannotCoalesceDistinctInputTopologies(t *testing.T) {
	at := time.Now().UTC()
	makeIntent := func(topology string) liveSignalIntent {
		return liveSignalIntent{At: at, Signal: storage.Signal{
			Platform: "kalshi", Ticker: "KXCONFLUENCE", Side: "YES",
			SignalType: "confluence", EntryPrice: .40,
			ExecExpr: r147InputReceiptExpr(topology, at),
		}}
	}
	pint, pus := makeIntent("K-PINT"), makeIntent("K-PUS")
	if liveSignalIntentExactKey(pint) == liveSignalIntentExactKey(pus) {
		t.Fatal("LIVE burst/dedup key pooled K-PINT and K-PUS opportunities")
	}
	for _, intent := range []liveSignalIntent{pint, pus} {
		candidate := liveCandidateFromSignalIntent(intent)
		if candidate.SignalContractID == "" || candidate.InputTopology == "" {
			t.Fatalf("exact signal contract was not bound before dedup: %+v", candidate)
		}
	}
}

func TestR147AmbiguousHandlerRecoveryRequiresOneExactFreshReceipt(t *testing.T) {
	s := testServer(t)
	base := liveMirrorCandidate{Platform: "kalshi", Ticker: "KXTEST", Side: "YES", Source: "auto-cons-confluence"}
	pint := base
	pint.InputTopology = "K-PINT"
	pint, _, why := r147BindSignalContract(pint, "taker")
	if why != "" {
		t.Fatal(why)
	}
	s.liveAllocationSignal = map[string]time.Time{liveAllocationSignalKey(pint): time.Now()}
	bound, _, why := s.liveAllocationBindSignalContract(base, "taker", true)
	if why != "" || bound.SignalContractID != pint.SignalContractID {
		t.Fatalf("single receipt recovery=%+v why=%q", bound, why)
	}

	pus := base
	pus.InputTopology = "K-PUS"
	pus, _, why = r147BindSignalContract(pus, "taker")
	if why != "" {
		t.Fatal(why)
	}
	s.liveAllocationSignal[liveAllocationSignalKey(pus)] = time.Now()
	if _, _, why := s.liveAllocationBindSignalContract(base, "taker", true); why != "prospective-allocation-multiple-fresh-signal-topologies" {
		t.Fatalf("multiple receipts did not fail closed: %q", why)
	}
}

func TestR147RequiredInputClocksFailClosedByExactTopology(t *testing.T) {
	fresh := r147LiveInputClockSnapshot{PINTClient: true, PINTConnected: true, PINTFresh: 10,
		PINTAgeSeconds: 1, PUSClient: true, PUSFrameOK: true, PUSExecutable: 10,
		PUSFrameAge: time.Second}
	pint := r147ContractForTest(t, "confluence", "kalshi", "YES", "K-PINT")
	pus := r147ContractForTest(t, "confluence", "kalshi", "YES", "K-PUS")
	triple := r147ContractForTest(t, "confluence", "kalshi", "YES", "K-PUS-PINT")
	for _, contract := range []r147SystemVariantSignalContract{pint, pus, triple} {
		if why := liveAllocationInputClockReason(contract, fresh); why != "" {
			t.Fatalf("fresh %s rejected: %s", contract.InputTopology, why)
		}
	}

	missingPINT := fresh
	missingPINT.PINTClient = false
	if why := liveAllocationInputClockReason(pint, missingPINT); why != "prospective-allocation-polyint-input-client-unavailable" {
		t.Fatalf("missing PINT clock=%q", why)
	}
	if why := liveAllocationInputClockReason(pus, missingPINT); why != "" {
		t.Fatalf("K-PUS incorrectly required PINT: %q", why)
	}
	stalePUS := fresh
	stalePUS.PUSFrameAge = polymarketus.MarketsWSPrimaryTransportMaxAge + time.Second
	if why := liveAllocationInputClockReason(pus, stalePUS); why != "prospective-allocation-polyus-input-books-not-fresh" {
		t.Fatalf("stale PUS clock=%q", why)
	}
	if why := liveAllocationInputClockReason(triple, stalePUS); why != "prospective-allocation-polyus-input-books-not-fresh" {
		t.Fatalf("triple did not require PUS: %q", why)
	}

	noaa := pint
	noaa.InputTopology = "K-NOAA"
	noaa.RequiredInputs = []string{"K", "NOAA"}
	if why := liveAllocationInputClockReason(noaa, fresh); why != "prospective-allocation-noaa-input-receipt-not-fresh" {
		t.Fatalf("missing NOAA receipt did not fail closed: %q", why)
	}
	externalFresh := fresh
	externalFresh.Now = time.Now()
	externalFresh.ExternalObservedAt = externalFresh.Now.Add(-time.Minute)
	if why := liveAllocationInputClockReason(noaa, externalFresh); why != "" {
		t.Fatalf("fresh NOAA receipt rejected: %q", why)
	}
	externalFresh.ExternalObservedAt = externalFresh.Now.Add(-91 * time.Minute)
	if why := liveAllocationInputClockReason(noaa, externalFresh); why != "prospective-allocation-noaa-input-receipt-not-fresh" {
		t.Fatalf("stale NOAA receipt=%q", why)
	}
	pintExecution := pint
	pintExecution.ExecutionVenue = "polymarket"
	if why := liveAllocationInputClockReason(pintExecution, fresh); why != "prospective-allocation-pint-is-input-only" {
		t.Fatalf("PINT execution was not rejected: %q", why)
	}
}

func TestR147ArbSignalContractsRemainPairwise(t *testing.T) {
	kalshi := r147ExactSignalContracts("arb", "kalshi", "YES", "taker")
	if len(kalshi) != 2 || kalshi[0].InputTopology != "K-PINT" || kalshi[1].InputTopology != "K-PUS" {
		t.Fatalf("Kalshi arb contracts=%v", kalshi)
	}
	for _, row := range kalshi {
		if row.InputTopology == "K-PUS-PINT" {
			t.Fatal("pairwise arb was mislabeled as a three-venue observation")
		}
	}
	polyUS := r147ExactSignalContracts("arb", "polyus", "YES", "taker")
	if len(polyUS) != 1 || polyUS[0].InputTopology != "K-PUS" {
		t.Fatalf("PolyUS arb contracts=%v", polyUS)
	}
}

func TestR147KCryptoSeparatesSoloAndPINTConfirmationFromSpotFeature(t *testing.T) {
	rows := r147ExactSignalContracts("kcrypto", "kalshi", "YES", "taker")
	if len(rows) != 2 || rows[0].InputTopology != "K" || rows[1].InputTopology != "K-PINT" {
		t.Fatalf("kcrypto decision contracts=%v", rows)
	}
	for _, row := range rows {
		if row.InputTopology == "K-SPOT" {
			t.Fatal("Coinbase feature was mislabeled as the kcrypto decision trigger")
		}
	}
	c := liveMirrorCandidate{Platform: "kalshi", Ticker: "KXBTC15M-TEST", Side: "YES", Source: "auto-cons-kcrypto"}
	if _, _, why := r147BindSignalContract(c, "taker"); why != "prospective-allocation-signal-topology-ambiguous" {
		t.Fatalf("unstamped kcrypto branch did not fail closed: %q", why)
	}
}
