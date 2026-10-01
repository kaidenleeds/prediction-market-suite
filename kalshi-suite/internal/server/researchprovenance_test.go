package server

import (
	"context"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/researchreplay"
)

func TestResearchReplayVerificationIsRecentBoundedAndCached(t *testing.T) {
	dir := t.TempDir()
	w, err := researchreplay.Open(researchreplay.Config{Dir: dir, MaxSegments: 200})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	frame, err := researchreplay.NewFrame("test", "v1", "book", "KX",
		"bounded test selection", map[string]any{"yes_bid": .4, "yes_ask": .5})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.AppendBatch(context.Background(), []researchreplay.Frame{frame}); err != nil {
		t.Fatal(err)
	}
	s := &Server{}
	now := time.Now()
	first := s.boundedReplayVerification(dir, now)
	second := s.boundedReplayVerification(dir, now.Add(time.Second))
	if !first.Healthy || first.Cached || !second.Cached || first.VerificationScope != "manifest+recent" {
		t.Fatalf("first=%+v second=%+v", first, second)
	}
	if _, err := w.AppendBatch(context.Background(), []researchreplay.Frame{frame}); err != nil {
		t.Fatal(err)
	}
	refreshed := s.boundedReplayVerification(dir, now.Add(31*time.Second))
	if refreshed.Cached || refreshed.Frames != 2 {
		t.Fatalf("refresh=%+v", refreshed)
	}
}
