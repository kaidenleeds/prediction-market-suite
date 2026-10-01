package server

import (
	"strings"
	"testing"
	"time"
)

func healthyPolyUSBooksInput() polyUSBooksReadyInput {
	return polyUSBooksReadyInput{
		Proofs: 9000, Requested: 9008, Frames: 186,
		Full: 600, Lite: 9008, Trade: 9008,
		Fresh: 321, Executable: 210, PriorityShortfall: 57,
		ProofAge: 20 * time.Second, DataAge: 3 * time.Hour,
		TransportAge: 3 * time.Second, PrimaryAge: 2 * time.Minute,
		HavePrimary: true, HaveData: true, HaveTransport: true,
		MaxProofAge: 5 * time.Minute, MaxTransportAge: 45 * time.Second,
	}
}

func TestR133PolyUSBooksReadyColdEightWithoutProofsIsFalse(t *testing.T) {
	in := healthyPolyUSBooksInput()
	in.Proofs, in.Requested, in.Full, in.Lite, in.Trade = 0, 8, 8, 8, 8
	if ok, detail := polyUSBooksReady(in); ok {
		t.Fatalf("cold 8-slug dependency plan with zero strict-open proofs returned ready: %s", detail)
	}
}

func TestR133PolyUSBooksReadyFullUniverse(t *testing.T) {
	in := healthyPolyUSBooksInput()
	ok, detail := polyUSBooksReady(in)
	if !ok {
		t.Fatalf("complete 9k tiered plan should be ready: %s", detail)
	}
	for _, want := range []string{"proofs=9000", "requested=9008", "full=600/600", "lite=9008", "trade=9008", "fresh=321", "executable=210", "transport_age=3s", "data_age=3h0m0s", "priority_shortfall=57"} {
		if !strings.Contains(detail, want) {
			t.Fatalf("detail %q missing %q", detail, want)
		}
	}
}

func TestPolyUSBooksReadyUsesBoundedLivePlanForHugeCatalog(t *testing.T) {
	in := healthyPolyUSBooksInput()
	in.Proofs = 72000
	in.Requested = 10000
	in.Full = 1000
	in.FullCap = 1000
	in.Lite = 10000
	in.Trade = 10000
	if ok, detail := polyUSBooksReady(in); !ok {
		t.Fatalf("bounded live plan over complete REST catalog should be ready: %s", detail)
	}
}

func TestR133PolyUSBooksReadyStaleProofOrTransportIsFalse(t *testing.T) {
	t.Run("proof", func(t *testing.T) {
		in := healthyPolyUSBooksInput()
		in.ProofAge = 5 * time.Minute
		if ok, detail := polyUSBooksReady(in); ok {
			t.Fatalf("proof at stale boundary returned ready: %s", detail)
		}
	})
	t.Run("transport", func(t *testing.T) {
		in := healthyPolyUSBooksInput()
		in.TransportAge = in.MaxTransportAge
		if ok, detail := polyUSBooksReady(in); ok {
			t.Fatalf("transport at stale boundary returned ready: %s", detail)
		}
	})
}

func TestR133PolyUSBooksReadyQuietCurrentGenerationIsReady(t *testing.T) {
	in := healthyPolyUSBooksInput()
	in.DataAge = 6 * time.Hour
	if ok, detail := polyUSBooksReady(in); !ok {
		t.Fatalf("unchanged current-generation complete snapshot was expired: %s", detail)
	}
}

func TestR133PolyUSBooksReadyGenuinelySmallUniverse(t *testing.T) {
	in := healthyPolyUSBooksInput()
	in.Proofs, in.Requested = 37, 42 // five extra dependency/league slugs are allowed
	in.Full, in.Lite, in.Trade = 37, 42, 42
	if ok, detail := polyUSBooksReady(in); !ok {
		t.Fatalf("complete small universe should require 37, not an impossible fixed 600: %s", detail)
	}
}
