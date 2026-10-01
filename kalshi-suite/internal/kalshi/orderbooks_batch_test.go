package kalshi

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"testing"
	"time"
)

func TestMultipleOrderbooksPathUsesRepeatedTickerKeys(t *testing.T) {
	path, err := multipleOrderbooksPath([]string{"KXGOLF-A", "KXGOLF-B", "KXGOLF-A"})
	if err != nil {
		t.Fatal(err)
	}
	u, err := url.Parse(path)
	if err != nil {
		t.Fatal(err)
	}
	if u.Path != "/markets/orderbooks" || !reflect.DeepEqual(u.Query()["tickers"], []string{"KXGOLF-A", "KXGOLF-B"}) {
		t.Fatalf("path=%s query=%v", u.Path, u.Query())
	}
	if _, err := multipleOrderbooksPath(nil); err == nil {
		t.Fatal("empty batch accepted")
	}
	if _, err := multipleOrderbooksPath(make([]string, 101)); err == nil {
		t.Fatal("oversized batch accepted")
	}
}

func TestGetOrderbooksAuthenticatesAndNormalizesAllBooks(t *testing.T) {
	var gotQuery []string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotQuery = append([]string(nil), r.URL.Query()["tickers"]...)
		for _, h := range []string{"KALSHI-ACCESS-KEY", "KALSHI-ACCESS-SIGNATURE", "KALSHI-ACCESS-TIMESTAMP"} {
			if r.Header.Get(h) == "" {
				t.Errorf("missing auth header %s", h)
			}
		}
		_, _ = w.Write([]byte(`{"orderbooks":[
{"ticker":"KXGOLF-A","orderbook_fp":{"yes_dollars":[["0.0020","3.00"],["0.0010","4.00"]],"no_dollars":[["0.9990","8.00"],["0.9980","7.00"]]}},
{"ticker":"KXGOLF-B","orderbook_fp":{"yes_dollars":[],"no_dollars":[["0.9990","2.00"]]}}
]}`))
	}))
	defer ts.Close()
	signer, err := NewSigner("batch-test", []byte(testPEM(t)))
	if err != nil {
		t.Fatal(err)
	}
	c := NewClient(ts.URL, signer, 1000, 2*time.Second)
	books, err := c.GetOrderbooks(context.Background(), []string{"KXGOLF-A", "KXGOLF-B"})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(gotQuery, []string{"KXGOLF-A", "KXGOLF-B"}) {
		t.Fatalf("tickers=%v", gotQuery)
	}
	a := books["KXGOLF-A"]
	if a == nil || len(a.YesBids) != 2 || a.YesBids[0].Price != .002 || len(a.YesAsks) != 2 || a.YesAsks[0].Price != .001 || a.YesAsks[0].Size != 8 {
		t.Fatalf("normalized A=%+v", a)
	}
	b := books["KXGOLF-B"]
	if b == nil || len(b.YesBids) != 0 || len(b.YesAsks) != 1 || b.YesAsks[0].Price != .001 {
		t.Fatalf("normalized B=%+v", b)
	}
}
