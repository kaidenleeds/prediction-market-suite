package kalshi

import (
	"fmt"
	"strings"
)

// R110/R111 — codified Kalshi MVE combo-legality rules, from the live-probed API (2026-07-07,
// both open collections KXMVESPORTSMULTIGAMEEXTENDED-R and KXMVECROSSCATEGORY-R) + docs:
//   https://docs.kalshi.com/api-reference/multivariate/get-multivariate-event-collections.md
//   https://docs.kalshi.com/api-reference/multivariate/get-multivariate-event-collection.md
//   https://docs.kalshi.com/api-reference/multivariate/create-market-in-multivariate-event-collection.md
// The venue's rule surface:
//   1. ONE collection per combo — every leg's event_ticker must be in that collection's
//      associated_events. Cross-collection combos do not exist as a concept.
//   2. Cross-SERIES legs are fine if the collection lists both events (the live cross-category
//      collection mixes ~50 series incl. sports + crypto).
//   3. Leg count: size_min ≤ len(legs) ≤ size_max (size_max 0 = unlimited; live size_min=2).
//   4. Per-event cap: at most associated_events[].size_max legs from one EVENT (null = NO cap).
//      ⚠ R111 correction of R110's prose: this is a per-EVENT cap, NOT a per-GAME cap. One game
//      spans many events (GAME/SPREAD/TOTAL/ADVANCE/GOAL/CORNERS/TCORNERS/1H…), so SAME-GAME
//      MULTI-LEG COMBOS ARE LEGAL — verified against the operator's live app combo (France
//      advances + 7+ corners + Doue 1+ + France 6+ team corners, one game, quoted 7.59x) whose
//      four legs are four distinct events in both open collections. Prop/corners/goal events
//      often carry size_max=null (97/430 uncapped at the R111 re-probe), so even two legs of ONE
//      such event are legal.
//   5. YES-only events (associated_events[].is_yes_only — moneylines/props; re-probed 2026-07-07
//      R111: 177/430): a side:"no" leg on THOSE events is refused. This is a
//      per-event COMBO constraint only — every Kalshi market still has a NO side for singles, and
//      NO legs on non-yes-only events (spreads/totals/corners) are legal. R110's "all-YES"
//      shortcut was stricter than the venue.
// Combos become tradeable via POST create-market (5,000 creations/user/week); the legacy PUT
// lookup path is deprecated in favor of RFQs.

// ComboLeg is one would-be combo leg for legality checking.
type ComboLeg struct {
	EventTicker string
	Side        string // "yes" | "no" (case-insensitive)
}

// ComboLegal checks a full leg set against ONE collection's rules. Returns "" when the combo is
// legal in this collection, else a human-readable reason (first violation found).
func (c MVCollection) ComboLegal(legs []ComboLeg) string {
	min := c.SizeMin
	if min <= 0 {
		min = 2 // a combo is ≥2 legs by definition; live collections pin size_min=2
	}
	if len(legs) < min {
		return fmt.Sprintf("%d legs < collection size_min %d", len(legs), min)
	}
	if c.SizeMax > 0 && len(legs) > c.SizeMax {
		return fmt.Sprintf("%d legs > collection size_max %d", len(legs), c.SizeMax)
	}
	perEvent := map[string]int{}
	for _, l := range legs {
		if _, ok := c.Events[l.EventTicker]; !ok {
			return "event " + l.EventTicker + " is not in collection " + c.CollectionTicker
		}
		if reason := c.LegOK(l.EventTicker, l.Side); reason != "" {
			return reason
		}
		perEvent[l.EventTicker]++
		if cfg, ok := c.EventCfg[l.EventTicker]; ok && cfg.SizeMax > 0 && perEvent[l.EventTicker] > cfg.SizeMax {
			return fmt.Sprintf("event %s allows at most %d leg(s) in this collection", l.EventTicker, cfg.SizeMax)
		}
	}
	return ""
}

// ComboLegalAny asks whether ANY of the given collections accepts the leg set (rule 1: a combo
// lives in exactly one collection, so legality = ∃ a collection that takes all legs). Returns the
// accepting collection ticker and "", or "" and the last rejection reason (or a generic one).
func ComboLegalAny(cols []MVCollection, legs []ComboLeg) (string, string) {
	reason := "no open multivariate collection contains all legs"
	for _, c := range cols {
		if r := c.ComboLegal(legs); r == "" {
			return c.CollectionTicker, ""
		} else if !strings.Contains(r, "is not in collection") {
			reason = r // a membership-passing collection rejected on rules — the more informative reason
		}
	}
	return "", reason
}
