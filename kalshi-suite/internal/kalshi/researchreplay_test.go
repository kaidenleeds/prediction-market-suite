package kalshi

import (
	"testing"
	"time"
)

func TestBookReplayCarriesActualGenerationSIDSequenceGapAndSourceClock(t *testing.T) {
	c, session := newBookTestClient(t)
	var events []ResearchReplayEvent
	c.SetResearchReplayHandler(func(event ResearchReplayEvent) { events = append(events, event) })
	sourceMS := int64(1700000000123)
	c.ingestBook([]byte(`{"type":"orderbook_snapshot","sid":7,"seq":10,"msg":{"market_ticker":"KX-R","yes_dollars_fp":[["0.4000","5"]],"no_dollars_fp":[["0.5000","6"]],"ts_ms":1700000000123}}`), session)
	book, _, atomicProvenance, atomicOK := c.LiveBookWithProvenance("KX-R", 5*time.Second)
	if !atomicOK || book == nil || len(book.YesBids) != 1 || len(book.YesAsks) != 1 ||
		book.YesBids[0].Price != .4 || book.YesAsks[0].Price != .5 || atomicProvenance.Sequence != 10 ||
		atomicProvenance.SubscriptionID != 7 || atomicProvenance.Generation != 1 {
		t.Fatalf("atomic book/provenance mismatch ok=%v book=%+v provenance=%+v", atomicOK, book, atomicProvenance)
	}
	provenance, provenanceOK := c.LiveBookProvenance("KX-R", 5*time.Second)
	if !provenanceOK || provenance.Generation != 1 || provenance.SubscriptionID != 7 || provenance.Sequence != 10 ||
		!provenance.SourceAt.Equal(time.UnixMilli(sourceMS).UTC()) {
		t.Fatalf("snapshot provenance ok=%v value=%+v", provenanceOK, provenance)
	}
	c.ingestBook([]byte(`{"type":"orderbook_delta","sid":7,"seq":12,"msg":{"market_ticker":"KX-R","price_dollars":"0.4100","delta_fp":"1","side":"yes","ts_ms":1700000000223}}`), session)
	if len(events) != 2 {
		t.Fatalf("events=%d", len(events))
	}
	first, gap := events[0], events[1]
	if first.SourceGeneration != 1 || first.SubscriptionID != 7 || first.Channel != "orderbook_delta" ||
		first.SourceSequence == nil || *first.SourceSequence != 10 || first.PriorSourceSequence != nil ||
		!first.SourceAt.Equal(time.UnixMilli(sourceMS).UTC()) || first.Book == nil || !first.Book.Valid {
		t.Fatalf("first=%+v book=%+v", first, first.Book)
	}
	if provenance, ok := c.LiveBookProvenance("KX-R", 5*time.Second); ok || provenance.Sequence != 0 {
		// The following gap invalidated this book, so no caller may keep using its pre-gap provenance.
		t.Fatalf("invalidated provenance remained executable: ok=%v provenance=%+v", ok, provenance)
	}
	if gap.SourceSequence == nil || gap.PriorSourceSequence == nil || *gap.SourceSequence != 12 ||
		*gap.PriorSourceSequence != 10 || gap.SequenceGap != 1 || gap.Book == nil || gap.Book.Valid {
		t.Fatalf("gap=%+v book=%+v", gap, gap.Book)
	}
	truth := c.BookSourceClockTruth()
	if truth.Generation != 1 || truth.SubscriptionID != 7 || truth.LastGapPrior != 10 ||
		truth.LastGapSequence != 12 || truth.Gaps != 1 || truth.PriorSequence != 10 {
		t.Fatalf("truth=%+v", truth)
	}
}

func TestTradeAndLifecycleReplayNeverInventMissingSourceTime(t *testing.T) {
	c := &Client{}
	var events []ResearchReplayEvent
	c.SetResearchReplayHandler(func(event ResearchReplayEvent) { events = append(events, event) })
	c.ingestTradeSession([]byte(`{"type":"trade","sid":2,"seq":1,"msg":{"trade_id":"t","market_ticker":"KX-T","count_fp":"0.01","yes_price_dollars":"0.4","no_price_dollars":"0.6","taker_outcome_side":"yes"}}`), 7)
	c.ingestLifecycleSession([]byte(`{"type":"market_lifecycle_v2","sid":3,"seq":5,"msg":{"event_type":"deactivated","market_ticker":"KX-L","ts_ms":1700000000123}}`), 7)
	if len(events) != 2 {
		t.Fatalf("events=%d", len(events))
	}
	if !events[0].SourceAt.IsZero() || events[0].SourceGeneration != 7 || events[0].Trade == nil || events[0].Trade.Count != .01 ||
		events[0].Trade.TakerOutcomeSide != "yes" {
		t.Fatalf("trade=%+v", events[0])
	}
	if events[1].SourceAt.IsZero() || events[1].SourceGeneration != 7 || events[1].Lifecycle == nil || events[1].Lifecycle.EventType != "deactivated" {
		t.Fatalf("lifecycle=%+v", events[1])
	}
}

func TestEverySubscribedTouchDeltaReachesMemoryObserverWithTopOnlyCopy(t *testing.T) {
	c, session := newBookTestClient(t)
	var events []ResearchReplayEvent
	c.SetResearchReplayHandler(func(event ResearchReplayEvent) { events = append(events, event) })
	c.ingestBook([]byte(`{"type":"orderbook_snapshot","sid":7,"seq":1,"msg":{"market_ticker":"KX-D","yes_dollars_fp":[["0.4000","10"]],"no_dollars_fp":[["0.5000","10"]]}}`), session)
	// Same price, same top, size-only depletion: this used to be dropped at source.
	c.ingestBook([]byte(`{"type":"orderbook_delta","sid":7,"seq":2,"msg":{"market_ticker":"KX-D","price_dollars":"0.5000","delta_fp":"-3","side":"no"}}`), session)
	if len(events) != 2 || events[1].Book == nil || events[1].Book.YesAsk != .5 || events[1].Book.YesAskSize != 7 ||
		len(events[1].Book.YesBids) != 1 || len(events[1].Book.YesAsks) != 1 {
		t.Fatalf("ordinary touch delta missing or not top-only: %+v", events)
	}
}
