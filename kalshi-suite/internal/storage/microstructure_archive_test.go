package storage

import (
	"context"
	"testing"
	"time"
)

func archiveMicroFrame(at time.Time, ticker string, seq int64) ResearchMicrostructureFrame {
	return ResearchMicrostructureFrame{Observed: at, Received: at.Add(10 * time.Millisecond),
		SourceChannel: "orderbook_delta", SourceGeneration: 1, SourceSubscriptionID: 7,
		SourceSequence: seq, Ticker: ticker, BookSource: "kalshi_ws_full",
		FeeSource: "kalshi:test-authority", QuoteAge: .01, TickSize: .01,
		YesBid: .40, YesAsk: .42, YesBidDepth: 10, YesAskDepth: 10,
		BidLevels:   []ResearchBookLevel{{Price: .40, Size: 10}},
		AskLevels:   []ResearchBookLevel{{Price: .42, Size: 10}},
		YesTakerFee: .01, NoTakerFee: .01, YesMakerFee: 0, NoMakerFee: 0}
}

func TestArchiveResearchMicrostructureIsTwoPhaseIdempotentAndKeepsAppendOnlyGuard(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	old := time.Now().UTC().Add(-8 * 24 * time.Hour)
	id1, _, inserted, err := st.InsertResearchMicrostructureFrame(ctx, archiveMicroFrame(old, "KX-ARC-1", 1))
	if err != nil || !inserted {
		t.Fatalf("old frame 1 inserted=%v err=%v", inserted, err)
	}
	id2, _, inserted, err := st.InsertResearchMicrostructureFrame(ctx, archiveMicroFrame(old.Add(time.Second), "KX-ARC-2", 2))
	if err != nil || !inserted {
		t.Fatalf("old frame 2 inserted=%v err=%v", inserted, err)
	}
	if inserted, err := st.InsertResearchMicrostructureHorizon(ctx, ResearchMicrostructureHorizon{
		FrameID: id1, Horizon: 5, Captured: old.Add(5 * time.Second), Status: "missed", Reason: "fixture missed"}); err != nil || !inserted {
		t.Fatalf("horizon inserted=%v err=%v", inserted, err)
	}
	frames, horizons, err := st.ArchiveResearchMicrostructure(ctx, 7, 100)
	if err != nil || frames != 2 || horizons != 1 {
		t.Fatalf("archive frames=%d horizons=%d err=%v", frames, horizons, err)
	}
	var mainFrames, mainHorizons int
	if err := st.db.QueryRow(`SELECT COUNT(*) FROM research_microstructure_frames`).Scan(&mainFrames); err != nil {
		t.Fatal(err)
	}
	if err := st.db.QueryRow(`SELECT COUNT(*) FROM research_microstructure_horizons`).Scan(&mainHorizons); err != nil {
		t.Fatal(err)
	}
	archiveFrames, archiveHorizons, err := st.ResearchMicrostructureArchiveCounts(ctx)
	if err != nil || mainFrames != 0 || mainHorizons != 0 || archiveFrames != 2 || archiveHorizons != 1 {
		t.Fatalf("main=%d/%d archive=%d/%d err=%v", mainFrames, mainHorizons, archiveFrames, archiveHorizons, err)
	}
	if frames, horizons, err = st.ArchiveResearchMicrostructure(ctx, 7, 100); err != nil || frames != 0 || horizons != 0 {
		t.Fatalf("idempotent archive frames=%d horizons=%d err=%v", frames, horizons, err)
	}

	newID, _, inserted, err := st.InsertResearchMicrostructureFrame(ctx,
		archiveMicroFrame(time.Now().UTC(), "KX-APPEND-ONLY", 3))
	if err != nil || !inserted || newID == id2 {
		t.Fatalf("new frame id=%d inserted=%v err=%v", newID, inserted, err)
	}
	if _, err := st.db.Exec(`DELETE FROM research_microstructure_frames WHERE id=?`, newID); err == nil {
		t.Fatal("ordinary delete bypassed append-only archive guard")
	}
}
