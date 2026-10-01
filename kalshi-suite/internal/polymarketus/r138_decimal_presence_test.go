package polymarketus

import (
	"context"
	"crypto/ed25519"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestExplicitZeroDecimalPositionWinsOverLegacyField(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/portfolio/positions" {
			http.NotFound(w, r)
			return
		}
		_, _ = w.Write([]byte(`{"positions":{
			"explicit-zero":{"netPositionDecimal":"0","netPosition":"9","marketMetadata":{"slug":"explicit-zero"}},
			"legacy-only":{"netPosition":"4","marketMetadata":{"slug":"legacy-only"}}
		},"eof":true}`))
	}))
	defer ts.Close()
	c := &Client{baseURL: ts.URL, keyID: "test", priv: make(ed25519.PrivateKey, ed25519.PrivateKeySize), http: ts.Client()}

	rows, err := c.LivePositions(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	got := make(map[string]float64, len(rows))
	for _, row := range rows {
		got[row.Slug] = row.Net
	}
	if got["explicit-zero"] != 0 {
		t.Fatalf("explicit decimal zero was replaced by legacy value: %v", got["explicit-zero"])
	}
	if got["legacy-only"] != 4 {
		t.Fatalf("absent decimal field did not fall back to legacy value: %v", got["legacy-only"])
	}
}

func TestExplicitZeroDecimalActivityQuantitiesWinOverLegacyFields(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/portfolio/activities" {
			http.NotFound(w, r)
			return
		}
		_, _ = w.Write([]byte(`{"activities":[
			{"type":"ACTIVITY_TYPE_TRADE","trade":{"marketSlug":"trade-zero","qtyDecimal":0,"qty":"7","price":{"value":"0.40"}}},
			{"type":"ACTIVITY_TYPE_TRADE","trade":{"marketSlug":"trade-legacy","qty":"3","price":{"value":"0.40"}}},
			{"type":"ACTIVITY_TYPE_POSITION_RESOLUTION","positionResolution":{"marketSlug":"resolution-zero","beforePosition":{"netPositionDecimal":"0","netPosition":"8","avgPx":{"value":"0.40"}},"afterPosition":{"realized":{"value":"1"}}}},
			{"type":"ACTIVITY_TYPE_POSITION_RESOLUTION","positionResolution":{"marketSlug":"resolution-legacy","beforePosition":{"netPosition":"2","avgPx":{"value":"0.40"}},"afterPosition":{"realized":{"value":"0.20"}}}}
		],"eof":true}`))
	}))
	defer ts.Close()
	c := &Client{baseURL: ts.URL, keyID: "test", priv: make(ed25519.PrivateKey, ed25519.PrivateKeySize), http: ts.Client()}

	rows, err := c.Activities(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	got := make(map[string]float64, len(rows))
	for _, row := range rows {
		got[row.Slug] = row.Qty
	}
	if got["trade-zero"] != 0 || got["resolution-zero"] != 0 {
		t.Fatalf("explicit decimal zeros were replaced by legacy quantities: %+v", got)
	}
	if got["trade-legacy"] != 3 || got["resolution-legacy"] != 2 {
		t.Fatalf("absent decimal fields did not fall back to legacy quantities: %+v", got)
	}
}
