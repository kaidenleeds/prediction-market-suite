package kalshi

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestResearchRFQIsExactlyOneContractAndCannotAcceptWhileDisarmed(t *testing.T) {
	requests := 0
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		if r.Method != http.MethodPost || r.URL.Path != "/communications/rfqs" {
			t.Fatalf("request=%s %s", r.Method, r.URL.Path)
		}
		for _, header := range []string{"KALSHI-ACCESS-KEY", "KALSHI-ACCESS-SIGNATURE", "KALSHI-ACCESS-TIMESTAMP"} {
			if r.Header.Get(header) == "" {
				t.Fatalf("authenticated research RFQ missing %s", header)
			}
		}
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		if body["contracts_fp"] != "1.00" || body["market_ticker"] != "MVE" || body["rest_remainder"] != false {
			t.Fatalf("one-contract payload=%v", body)
		}
		if _, exists := body["target_cost_dollars"]; exists {
			t.Fatalf("research RFQ used target-cost sizing: %v", body)
		}
		_, _ = w.Write([]byte(`{"id":"rfq-1"}`))
	}))
	defer ts.Close()
	signer, err := NewSigner("rfq-research", []byte(testPEM(t)))
	if err != nil {
		t.Fatal(err)
	}
	c := NewClient(ts.URL, signer, 1000, time.Second) // deliberately not armed
	id, err := c.CreateResearchRFQContracts(context.Background(), "MVE", "1.00", false)
	if err != nil || id != "rfq-1" || requests != 1 {
		t.Fatalf("id=%q requests=%d err=%v", id, requests, err)
	}
	if err := c.AcceptQuoteRFQ(context.Background(), "rfq-1", "quote-1", "yes"); err == nil {
		t.Fatal("disarmed research client accepted a quote")
	}
	if requests != 1 {
		t.Fatal("disarmed AcceptQuote reached HTTP")
	}
}
