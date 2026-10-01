package polymarketus

import (
	"context"
	"crypto/ed25519"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestR153LivePositionsPreserveExplicitZeroCashValue(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/portfolio/positions" {
			http.NotFound(w, r)
			return
		}
		_, _ = w.Write([]byte(`{"positions":{
			"known-zero":{"netPositionDecimal":"2","cost":{"value":"0.80"},"cashValue":{"value":"0"},"marketMetadata":{"slug":"known-zero"}},
			"missing":{"netPositionDecimal":"2","cost":{"value":"0.80"},"marketMetadata":{"slug":"missing"}}
		},"eof":true}`))
	}))
	defer ts.Close()
	c := &Client{baseURL: ts.URL, keyID: "test", priv: make(ed25519.PrivateKey, ed25519.PrivateKeySize), http: ts.Client()}

	rows, err := c.LivePositions(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	got := make(map[string]PUSPosition, len(rows))
	for _, row := range rows {
		got[row.Slug] = row
	}
	if row := got["known-zero"]; !row.CashValKnown || row.CashVal != 0 {
		t.Fatalf("explicit $0 cashValue lost presence: %+v", row)
	}
	if row := got["missing"]; row.CashValKnown {
		t.Fatalf("omitted cashValue was marked known: %+v", row)
	}
}
