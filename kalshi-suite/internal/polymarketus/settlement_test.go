package polymarketus

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestBookDataSettledYesRequiresVenuePriceAndAcceptsTerminalTokens(t *testing.T) {
	for _, state := range []string{"RESOLVED", "SETTLED", "EXPIRED", "TERMINATED", "GRADED", "CLOSED"} {
		b := BookData{State: state, SettlementPx: 0, HasSettlement: true,
			HasSettlementPreliminary: true, SettlementPreliminary: false,
			SettlementMethod: "SETTLEMENT_PRICE_CALCULATION_METHOD_EVENT_TIER_1"}
		if yes, ok := b.SettledYes(); !ok || yes != 0 {
			t.Fatalf("%s terminal NO result rejected: yes=%g ok=%v", state, yes, ok)
		}
	}
	if _, ok := (BookData{State: "CLOSED", SettlementPx: 0}).SettledYes(); ok {
		t.Fatal("CLOSED book without venue-sent settlementPx became a false NO win")
	}
	final := BookData{SettlementPx: 1, HasSettlement: true, HasSettlementPreliminary: true,
		SettlementMethod: "SETTLEMENT_PRICE_CALCULATION_METHOD_EVENT_TIER_1"}
	if _, ok := (BookData{State: "OPEN", SettlementPx: final.SettlementPx, HasSettlement: true,
		HasSettlementPreliminary: true, SettlementMethod: final.SettlementMethod}).SettledYes(); ok {
		t.Fatal("nonterminal book settlement field was treated as final")
	}
	for _, state := range []string{"UNRESOLVED", "UNSETTLED", "MARKET_STATE_UNRESOLVED"} {
		if _, ok := (BookData{State: state, SettlementPx: 0, HasSettlement: true,
			HasSettlementPreliminary: true, SettlementMethod: final.SettlementMethod}).SettledYes(); ok {
			t.Fatalf("nonterminal substring state %q was treated as final", state)
		}
	}
	if yes, ok := (BookData{State: "MARKET_STATE_RESOLVED", SettlementPx: 1, HasSettlement: true,
		HasSettlementPreliminary: true, SettlementMethod: final.SettlementMethod}).SettledYes(); !ok || yes != 1 {
		t.Fatalf("prefixed terminal state rejected: yes=%g ok=%v", yes, ok)
	}
}

func TestBookDataSettledYesRejectsPreliminaryMarksAndFractionalEventPrices(t *testing.T) {
	base := BookData{State: "CLOSED", SettlementPx: 1, HasSettlement: true,
		HasSettlementPreliminary: true, SettlementMethod: "SETTLEMENT_PRICE_CALCULATION_METHOD_EVENT_TIER_1"}
	for name, mutate := range map[string]func(*BookData){
		"missing preliminary authority": func(b *BookData) { b.HasSettlementPreliminary = false },
		"preliminary":                   func(b *BookData) { b.SettlementPreliminary = true },
		"missing method":                func(b *BookData) { b.SettlementMethod = "" },
		"non event tier one":            func(b *BookData) { b.SettlementMethod = "DAILY_MARK" },
		"fractional mark":               func(b *BookData) { b.SettlementPx = .53 },
	} {
		b := base
		mutate(&b)
		if yes, ok := b.SettledYes(); ok {
			t.Fatalf("%s became final yes=%g book=%+v", name, yes, b)
		}
	}
}

func TestParseBookSettlementRequiresNumericVenueValue(t *testing.T) {
	for _, value := range []string{`"bad"`, `{}`, `{"value":"nope"}`, `null`} {
		raw := []byte(fmt.Sprintf(`{"marketData":{"state":"RESOLVED","stats":{"settlementPx":%s,"settlementPreliminary":false,"settlementPriceCalculationMethod":"EVENT_TIER_1"}}}`, value))
		book := ParseBook(raw, "fixture")
		if book.HasSettlement {
			t.Fatalf("malformed settlement %s became value %g", value, book.SettlementPx)
		}
		if _, ok := book.SettledYes(); ok {
			t.Fatalf("malformed settlement %s became a final result", value)
		}
	}
	for _, value := range []string{`0`, `"0"`, `{"value":"0"}`} {
		raw := []byte(fmt.Sprintf(`{"marketData":{"state":"RESOLVED","stats":{"settlementPx":%s,"settlementPreliminary":false,"settlementPriceCalculationMethod":"EVENT_TIER_1"}}}`, value))
		book := ParseBook(raw, "fixture")
		if yes, ok := book.SettledYes(); !book.HasSettlement || !ok || yes != 0 {
			t.Fatalf("valid zero settlement %s rejected: book=%+v yes=%g ok=%v", value, book, yes, ok)
		}
	}
}

func TestParseBookFinalSettlementMetadataIsRequired(t *testing.T) {
	raw := []byte(`{"marketData":{"state":"CLOSED","stats":{"settlementPx":{"value":"1"},"settlementPreliminary":false,"settlementPriceCalculationMethod":"SETTLEMENT_PRICE_CALCULATION_METHOD_EVENT_TIER_1","settlementPriceCalculationText":"official event result","settlementSetTime":"2026-07-15T18:00:00Z"}}}`)
	b := ParseBook(raw, "final")
	if yes, ok := b.SettledYes(); !ok || yes != 1 || !b.HasSettlementPreliminary || b.SettlementPreliminary ||
		b.SettlementMethod == "" || b.SettlementText == "" || b.SettlementSetTime == "" {
		t.Fatalf("final metadata lost: yes=%g ok=%v book=%+v", yes, ok, b)
	}
	for _, stats := range []string{
		`"settlementPx":1`,
		`"settlementPx":1,"settlementPreliminary":true,"settlementPriceCalculationMethod":"EVENT_TIER_1"`,
		`"settlementPx":1,"settlementPreliminary":false`,
		`"settlementPx":0.61,"settlementPreliminary":false,"settlementPriceCalculationMethod":"EVENT_TIER_1"`,
	} {
		b = ParseBook([]byte(`{"marketData":{"state":"CLOSED","stats":{`+stats+`}}}`), "not-final")
		if _, ok := b.SettledYes(); ok {
			t.Fatalf("unauthorized settlement passed: %s -> %+v", stats, b)
		}
	}
}

func TestBookFullFallsBackToDedicatedFinalSettlementEndpoint(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/markets/final/book", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"marketData":{"marketSlug":"final","state":"MARKET_STATE_CLOSED","stats":{"settlementPx":{"value":"0.53"},"settlementPreliminaryFlag":false}}}`))
	})
	mux.HandleFunc("/v1/markets/final/settlement", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"slug":"final","settlement":1}`))
	})
	mux.HandleFunc("/v1/markets/still-mark/book", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"marketData":{"marketSlug":"still-mark","state":"CLOSED","stats":{"settlementPx":0.61}}}`))
	})
	mux.HandleFunc("/v1/markets/still-mark/settlement", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"slug":"still-mark","settlement":0.61}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()
	p := NewPublicClient(time.Second)
	p.baseURL = srv.URL
	b, _, code, err := p.BookFull(context.Background(), "final")
	if err != nil || code != http.StatusOK {
		t.Fatalf("BookFull final code=%d err=%v", code, err)
	}
	if yes, ok := b.SettledYes(); !ok || yes != 1 || b.SettlementAuthority != "GET /v1/markets/{slug}/settlement" {
		t.Fatalf("dedicated final settlement not authoritative: yes=%g ok=%v book=%+v", yes, ok, b)
	}
	b, _, code, err = p.BookFull(context.Background(), "still-mark")
	if err != nil || code != http.StatusOK {
		t.Fatalf("BookFull fractional code=%d err=%v", code, err)
	}
	if _, ok := b.SettledYes(); ok {
		t.Fatalf("fractional dedicated response became final: %+v", b)
	}
}

func TestDedicatedSettlementRequiresExactIdentityAndBinaryPayout(t *testing.T) {
	for name, response := range map[string]string{
		"wrong slug": `{"slug":"other","settlement":1}`,
		"fractional": `{"slug":"fixture","settlement":0.53}`,
		"missing":    `{"slug":"fixture"}`,
	} {
		t.Run(name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				_, _ = w.Write([]byte(response))
			}))
			defer srv.Close()
			p := NewPublicClient(time.Second)
			p.baseURL = srv.URL
			if _, _, _, err := p.Settlement(context.Background(), "fixture"); err == nil {
				t.Fatalf("unauthorized dedicated settlement accepted: %s", response)
			}
		})
	}
}

func TestDedicatedSettlementRejectsDecodableNon200Body(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"slug":"fixture","settlement":1}`))
	}))
	defer srv.Close()
	p := NewPublicClient(time.Second)
	p.baseURL = srv.URL
	if _, _, code, err := p.Settlement(context.Background(), "fixture"); err == nil || code != http.StatusServiceUnavailable {
		t.Fatalf("decodable non-200 settlement became authority: code=%d err=%v", code, err)
	}
}
