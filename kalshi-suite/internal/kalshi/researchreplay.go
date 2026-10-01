package kalshi

import (
	"sort"
	"time"
)

// ResearchReplayEvent is a small normalized public-market envelope emitted from existing Kalshi
// WebSocket decoders. It never carries headers, credentials, account messages, or raw JSON and it
// does not create a subscription. The callback contract is enqueue-and-return; the venue read loop
// must never wait for disk or SQLite.
type ResearchReplayEvent struct {
	Kind, EntityID, FrameType string
	Channel                   string
	SubscriptionID            int64
	SourceGeneration          uint64
	ObservedAt, SourceAt      time.Time
	SourceSequence            *int64
	PriorSourceSequence       *int64
	SequenceGap               int64
	Book                      *ResearchReplayBook
	Trade                     *ResearchReplayTrade
	Lifecycle                 *ResearchReplayLifecycle
}

type ResearchReplayBook struct {
	Valid            bool
	YesBid, YesAsk   float64
	YesBidSize       float64
	YesAskSize       float64
	YesBids, YesAsks []OrderbookLevel
}

type ResearchReplayTrade struct {
	TradeID, Aggressor                 string
	TakerOutcomeSide, TakerBookSide    string
	LegacyTakerSide, LegacyOutcomeSide string
	Count                              float64
	YesPrice, NoPrice                  float64
	Block                              bool
}

type ResearchReplayLifecycle struct {
	EventType, CloseTime, SettledResult string
}

// SetResearchReplayHandler wires the already-running public feed to a bounded research selector.
// It grants no Paper/LIVE authority and must never be used for private account payloads.
func (c *Client) SetResearchReplayHandler(fn func(ResearchReplayEvent)) {
	c.txMu.Lock()
	c.researchReplayFn = fn
	c.txMu.Unlock()
}

func (c *Client) emitResearchReplay(ev ResearchReplayEvent) {
	c.txMu.Lock()
	fn := c.researchReplayFn
	c.txMu.Unlock()
	if fn != nil {
		fn(ev)
	}
}

func (c *Client) researchReplaySequence(channel string, current int64) (*int64, *int64, int64) {
	if current <= 0 {
		return nil, nil, 0
	}
	c.txMu.Lock()
	if c.researchReplaySeq == nil {
		c.researchReplaySeq = map[string]int64{}
	}
	prior := c.researchReplaySeq[channel]
	c.researchReplaySeq[channel] = current
	c.txMu.Unlock()
	return researchSequenceTruth(prior, current)
}

func researchSequenceTruth(prior, current int64) (*int64, *int64, int64) {
	if current <= 0 {
		return nil, nil, 0
	}
	cur := current
	if prior <= 0 {
		return &cur, nil, 0
	}
	prev := prior
	gap := int64(0)
	if current > prior+1 {
		gap = current - prior - 1
	}
	return &cur, &prev, gap
}

// researchReplayBookLocked copies at most the same ten executable levels exported by LiveBook.
// Caller holds books.mu. This is invoked only for snapshots, sequence faults, or top-price changes.
func researchReplayBookLocked(bk *wsBook) *ResearchReplayBook {
	if bk == nil || !bk.valid {
		return &ResearchReplayBook{Valid: false}
	}
	yesKeys := make([]int, 0, len(bk.yes))
	for key := range bk.yes {
		yesKeys = append(yesKeys, key)
	}
	noKeys := make([]int, 0, len(bk.no))
	for key := range bk.no {
		noKeys = append(noKeys, key)
	}
	sort.Sort(sort.Reverse(sort.IntSlice(yesKeys)))
	sort.Sort(sort.Reverse(sort.IntSlice(noKeys)))
	out := &ResearchReplayBook{Valid: true}
	for i, key := range yesKeys {
		if i >= bookDepthCap {
			break
		}
		out.YesBids = append(out.YesBids, OrderbookLevel{Price: float64(key) / 10000, Size: bk.yes[key]})
	}
	for i, key := range noKeys {
		if i >= bookDepthCap {
			break
		}
		out.YesAsks = append(out.YesAsks, OrderbookLevel{Price: 1 - float64(key)/10000, Size: bk.no[key]})
	}
	if len(out.YesBids) > 0 {
		out.YesBid, out.YesBidSize = out.YesBids[0].Price, out.YesBids[0].Size
	}
	if len(out.YesAsks) > 0 {
		out.YesAsk, out.YesAskSize = out.YesAsks[0].Price, out.YesAsks[0].Size
	}
	return out
}

// researchReplayTopBookLocked is the low-allocation event path for ordinary deltas. The
// event-driven research observer only needs the executable touch to recognize depletion/refill;
// selected snapshots, sequence faults, and top-price changes still use the full ten-level copy.
// Caller holds books.mu.
func researchReplayTopBookLocked(bk *wsBook) *ResearchReplayBook {
	if bk == nil || !bk.valid || bk.bestYes <= 0 || bk.bestNo <= 0 {
		return &ResearchReplayBook{Valid: false}
	}
	yesBid, yesAsk := float64(bk.bestYes)/10000, 1-float64(bk.bestNo)/10000
	yesBidSize, yesAskSize := bk.yes[bk.bestYes], bk.no[bk.bestNo]
	if yesBid <= 0 || yesAsk <= yesBid || yesAsk >= 1 || yesBidSize <= 0 || yesAskSize <= 0 {
		return &ResearchReplayBook{Valid: false}
	}
	return &ResearchReplayBook{Valid: true, YesBid: yesBid, YesAsk: yesAsk,
		YesBidSize: yesBidSize, YesAskSize: yesAskSize,
		YesBids: []OrderbookLevel{{Price: yesBid, Size: yesBidSize}},
		YesAsks: []OrderbookLevel{{Price: yesAsk, Size: yesAskSize}}}
}
