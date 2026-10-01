package server

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/kalshi"
	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

func queueTestSigner(t *testing.T) *kalshi.Signer {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	b := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
	signer, err := kalshi.NewSigner("queue-liveness-test", b)
	if err != nil {
		t.Fatal(err)
	}
	return signer
}

func TestQueuePriorityHealthyEmptyMeansNoNaturalRestingOrders(t *testing.T) {
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/portfolio/orders" || r.URL.Query().Get("status") != "resting" {
			t.Fatalf("unexpected queue liveness request %s?%s", r.URL.Path, r.URL.RawQuery)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"orders":[],"cursor":""}`)
	}))
	defer api.Close()
	st, err := storage.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	s := &Server{kal: kalshi.NewClient(api.URL, queueTestSigner(t), 1000, time.Second), store: st,
		log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	s.sweepQueuePriority(context.Background())
	report, err := st.CollectorLivenessReport(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	rows := report["collectors"].([]storage.CollectorLivenessView)
	for _, row := range rows {
		if row.CollectorID != "queue-priority" {
			continue
		}
		if row.Status != "healthy_empty" || !row.ExpectedZero || row.Eligible != 0 ||
			row.ZeroReason != "no naturally resting real Kalshi orders" || row.NeverRan {
			t.Fatalf("queue receipt=%+v", row)
		}
		return
	}
	t.Fatal("queue-priority collector missing from liveness report")
}
