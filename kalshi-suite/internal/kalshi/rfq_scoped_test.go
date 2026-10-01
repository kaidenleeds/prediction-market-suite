package kalshi

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"
)

type rfqRewriteTransport struct {
	target *url.URL
	base   http.RoundTripper
}

func (t rfqRewriteTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	c := r.Clone(r.Context())
	u := *r.URL
	u.Scheme, u.Host = t.target.Scheme, t.target.Host
	c.URL = &u
	return t.base.RoundTrip(c)
}

func TestR140GetRFQQuoteUsesCurrentScopedResource(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.EscapedPath() != "/communications/rfqs/rfq%2Fone/quotes/quote%2Ftwo" {
			t.Fatalf("scoped quote request=%s %s", r.Method, r.URL.EscapedPath())
		}
		_, _ = w.Write([]byte(`{"quote":{"id":"quote/two","rfq_id":"rfq/one","market_ticker":"MVE","status":"executed","yes_bid_dollars":"0.4200","contracts_fp":"1.00","rfq_creator_order_id":"order-7"}}`))
	}))
	defer ts.Close()
	signer, err := NewSigner("rfq-scoped", []byte(testPEM(t)))
	if err != nil {
		t.Fatal(err)
	}
	c := NewClient(ts.URL, signer, 1000, time.Second)
	q, err := c.GetRFQQuote(context.Background(), "rfq/one", "quote/two")
	if err != nil || q.ID != "quote/two" || q.RFQID != "rfq/one" || q.Status != "executed" ||
		q.YesBidDollars != "0.4200" || q.ContractsFp != "1.00" || q.RFQCreatorOrderID != "order-7" {
		t.Fatalf("scoped quote=%+v err=%v", q, err)
	}
}

func TestR140AcceptQuoteScopedIdentityRefusalNeverFallsBack(t *testing.T) {
	hits := 0
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits++
		if r.URL.Path != "/trade-api/v2/communications/rfqs/rfq-1/quotes/quote-1/accept" {
			t.Fatalf("unsafe accept fallback path reached: %s", r.URL.Path)
		}
		http.Error(w, "identity not found", http.StatusNotFound)
	}))
	defer ts.Close()
	signer, err := NewSigner("rfq-accept-scoped", []byte(testPEM(t)))
	if err != nil {
		t.Fatal(err)
	}
	c := NewClient(ts.URL, signer, 1000, time.Second)
	target, _ := url.Parse(ts.URL)
	c.baseURL = BaseDemo // demo writes are allowed; transport keeps the test off the real demo host
	c.http.Transport = rfqRewriteTransport{target: target, base: http.DefaultTransport}
	if err := c.AcceptQuoteRFQ(context.Background(), "rfq-1", "quote-1", "yes"); err == nil {
		t.Fatal("scoped identity refusal was treated as accepted")
	}
	if hits != 1 {
		t.Fatalf("scoped refusal triggered %d writes, want exactly one", hits)
	}
}

func TestR140CreateComboProbeUsesCurrentCreateMarketEndpoint(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/multivariate_event_collections/COL" {
			t.Fatalf("combo probe request=%s %s", r.Method, r.URL.Path)
		}
		_, _ = w.Write([]byte(`{"event_ticker":"MVE-E","market_ticker":"MVE-M"}`))
	}))
	defer ts.Close()
	signer, err := NewSigner("combo-create-current", []byte(testPEM(t)))
	if err != nil {
		t.Fatal(err)
	}
	c := NewClient(ts.URL, signer, 1000, time.Second)
	ok, reason, err := c.CreateComboProbe(context.Background(), "COL", []MVELeg{
		{MarketTicker: "M1", EventTicker: "E1", Side: "yes"},
		{MarketTicker: "M2", EventTicker: "E2", Side: "no"},
	})
	if err != nil || !ok || reason != "" {
		t.Fatalf("create combo probe ok=%v reason=%q err=%v", ok, reason, err)
	}
}
