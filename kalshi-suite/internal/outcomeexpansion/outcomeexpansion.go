// Package outcomeexpansion implements the frozen v1 survivor-redistribution counterfactual for
// complete exhaustive outcome sets. It is a pure research model: current target quotes are not an
// input to fair-value fitting, and route economics are evaluated only after the immutable model
// result exists.
package outcomeexpansion

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math"
	"sort"
	"strings"
	"time"
)

const (
	ModelVersion = "outcome-set-survivor-redistribution-v1"
	modelReserve = 0.01
)

type Member struct {
	ID     string  `json:"id"`
	Status string  `json:"status"`
	Bid    float64 `json:"yes_bid,omitempty"`
	Ask    float64 `json:"yes_ask,omitempty"`
}

type Frame struct {
	FrameID           int64     `json:"frame_id"`
	Observed          time.Time `json:"observed"`
	Venue             string    `json:"venue"`
	EventID           string    `json:"event_id"`
	MembershipHash    string    `json:"membership_hash"`
	Members           []Member  `json:"members"`
	MutuallyExclusive bool      `json:"mutually_exclusive"`
	Exhaustive        bool      `json:"exhaustive"`
	VoidPolicy        bool      `json:"void_policy_verified"`
}

type Change struct {
	Added         []string `json:"added,omitempty"`
	Removed       []string `json:"removed,omitempty"`
	StatusChanged []string `json:"status_changed,omitempty"`
	Class         string   `json:"class"`
}

type Projection struct {
	MemberID string  `json:"member_id"`
	Fair     float64 `json:"fair"`
	Lower    float64 `json:"lower"`
	Upper    float64 `json:"upper"`
}

type Result struct {
	ModelVersion         string       `json:"model_version"`
	PriorFrameID         int64        `json:"prior_frame_id"`
	CurrentFrameID       int64        `json:"current_frame_id"`
	EventDayCluster      string       `json:"event_day_cluster"`
	Change               Change       `json:"change"`
	Projections          []Projection `json:"projections,omitempty"`
	ModelReserve         float64      `json:"model_reserve"`
	SettlementCompatible bool         `json:"settlement_compatible"`
	Blocker              string       `json:"blocker,omitempty"`
}

type OneShareEconomics struct {
	Side      string  `json:"side"`
	Cost      float64 `json:"cost"`
	Fee       float64 `json:"fee"`
	Depth     float64 `json:"depth"`
	Tick      float64 `json:"tick"`
	QuoteAge  float64 `json:"quote_age_s"`
	BookClock string  `json:"book_clock"`
}

type ActionDecision struct {
	Side             string  `json:"side,omitempty"`
	Decision         string  `json:"decision"`
	Blocker          string  `json:"blocker,omitempty"`
	Fair             float64 `json:"fair"`
	FairLower        float64 `json:"fair_lower"`
	ExpectedNetLower float64 `json:"expected_net_lower"`
}

func normalizeStatus(v string) string {
	v = strings.ToLower(strings.TrimSpace(v))
	switch v {
	case "open", "active", "initialized", "trading", "unopened":
		return "active"
	case "closed", "settled", "resolved", "finalized":
		return "settled"
	case "removed", "withdrawn", "eliminated", "cancelled", "canceled", "inactive":
		return "inactive"
	default:
		return v
	}
}

func active(v string) bool { return normalizeStatus(v) == "active" }

func canonicalMembers(rows []Member) []Member {
	out := append([]Member(nil), rows...)
	for i := range out {
		out[i].ID = strings.TrimSpace(out[i].ID)
		out[i].Status = normalizeStatus(out[i].Status)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// MembershipDigest is the package's deterministic immutable-frame receipt. Stored venue hashes
// remain provenance; this digest makes the model input independently replayable.
func MembershipDigest(rows []Member) string {
	b, _ := json.Marshal(canonicalMembers(rows))
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

func validFrame(f Frame, prior bool) error {
	if f.FrameID <= 0 || f.Observed.IsZero() || strings.TrimSpace(f.Venue) == "" ||
		strings.TrimSpace(f.EventID) == "" || strings.TrimSpace(f.MembershipHash) == "" ||
		!f.MutuallyExclusive || !f.Exhaustive || len(f.Members) < 2 {
		return errors.New("frame is not a complete immutable exhaustive outcome set")
	}
	seen := map[string]bool{}
	for _, m := range f.Members {
		id := strings.TrimSpace(m.ID)
		if id == "" || seen[id] || strings.TrimSpace(normalizeStatus(m.Status)) == "" {
			return errors.New("frame has invalid or duplicate member")
		}
		seen[id] = true
		if prior && active(m.Status) && (math.IsNaN(m.Bid) || math.IsNaN(m.Ask) ||
			math.IsInf(m.Bid, 0) || math.IsInf(m.Ask, 0) || m.Bid < 0 || m.Ask <= 0 ||
			m.Bid > m.Ask || m.Ask > 1) {
			return errors.New("prior active member lacks bounded probability interval")
		}
	}
	return nil
}

func mapMembers(rows []Member) map[string]Member {
	out := make(map[string]Member, len(rows))
	for _, m := range rows {
		m.ID = strings.TrimSpace(m.ID)
		m.Status = normalizeStatus(m.Status)
		out[m.ID] = m
	}
	return out
}

func classify(added, removed, changed []string) string {
	switch {
	case len(added)+len(removed)+len(changed) == 0:
		return "unchanged"
	case len(added) > 0 && len(removed)+len(changed) == 0:
		return "expanded"
	case len(removed) > 0 && len(added)+len(changed) == 0:
		return "contracted"
	case len(changed) > 0 && len(added)+len(removed) == 0:
		return "status_change"
	default:
		return "mixed"
	}
}

// Analyze freezes the conditional redistribution implied by the prior probability intervals.
// Only prior prices are read. A newly active member is deliberately blocked because v1 has no
// independent pre-shock probability for that entrant; using its current quote would be target
// leakage. Removal and active-to-inactive shocks condition the prior distribution on survivors.
func Analyze(prior, current Frame) (Result, error) {
	if err := validFrame(prior, true); err != nil {
		return Result{}, err
	}
	if err := validFrame(current, false); err != nil {
		return Result{}, err
	}
	if !strings.EqualFold(prior.Venue, current.Venue) || prior.EventID != current.EventID ||
		!prior.Observed.Before(current.Observed) || prior.FrameID >= current.FrameID ||
		prior.MembershipHash == current.MembershipHash {
		return Result{}, errors.New("frames are not an ordered immutable same-event change")
	}
	p, c := mapMembers(prior.Members), mapMembers(current.Members)
	added, removed, changed := []string{}, []string{}, []string{}
	for id, member := range c {
		old, ok := p[id]
		if !ok {
			added = append(added, id)
		} else if old.Status != member.Status {
			changed = append(changed, id)
		}
	}
	for id := range p {
		if _, ok := c[id]; !ok {
			removed = append(removed, id)
		}
	}
	sort.Strings(added)
	sort.Strings(removed)
	sort.Strings(changed)
	result := Result{ModelVersion: ModelVersion, PriorFrameID: prior.FrameID,
		CurrentFrameID: current.FrameID, EventDayCluster: strings.ToLower(current.Venue) + "|" +
			current.EventID + "|" + current.Observed.UTC().Format("2006-01-02"),
		Change: Change{Added: added, Removed: removed, StatusChanged: changed,
			Class: classify(added, removed, changed)}, ModelReserve: modelReserve,
		SettlementCompatible: prior.VoidPolicy && current.VoidPolicy}

	for _, id := range added {
		if active(c[id].Status) {
			result.Blocker = "new active member has no independent pre-shock probability"
			// Preserve exact-book, terminal-grade controls for the pre-existing survivors without
			// inventing an entrant probability. Their point is the prior normalized composition, but
			// the bounds are deliberately [0,1], so no residual can become a candidate.
			priorMass := 0.0
			for memberID, old := range p {
				if cur, exists := c[memberID]; exists && active(old.Status) && active(cur.Status) {
					priorMass += (old.Bid + old.Ask) / 2
				}
			}
			if priorMass > 0 {
				for memberID, old := range p {
					if cur, exists := c[memberID]; exists && active(old.Status) && active(cur.Status) {
						result.Projections = append(result.Projections, Projection{MemberID: memberID,
							Fair: ((old.Bid + old.Ask) / 2) / priorMass, Lower: 0, Upper: 1})
					}
				}
				sort.Slice(result.Projections, func(i, j int) bool {
					return result.Projections[i].MemberID < result.Projections[j].MemberID
				})
			}
			return result, nil
		}
	}
	removedMass := false
	for id, old := range p {
		if !active(old.Status) {
			continue
		}
		cur, exists := c[id]
		if !exists || !active(cur.Status) {
			removedMass = true
		}
	}
	if !removedMass {
		result.Blocker = "no active probability mass left the exhaustive outcome set"
		return result, nil
	}
	survivors := []Member{}
	for id, old := range p {
		if cur, exists := c[id]; exists && active(old.Status) && active(cur.Status) {
			survivors = append(survivors, old)
		}
	}
	sort.Slice(survivors, func(i, j int) bool { return survivors[i].ID < survivors[j].ID })
	if len(survivors) == 0 {
		result.Blocker = "shock leaves no active survivor"
		return result, nil
	}
	midSum := 0.0
	for _, m := range survivors {
		midSum += (m.Bid + m.Ask) / 2
	}
	if midSum <= 0 {
		result.Blocker = "prior survivor probabilities have zero mass"
		return result, nil
	}
	for i, target := range survivors {
		otherBid, otherAsk := 0.0, 0.0
		for j, other := range survivors {
			if i == j {
				continue
			}
			otherBid += other.Bid
			otherAsk += other.Ask
		}
		lowerDenom := target.Bid + otherAsk
		upperDenom := target.Ask + otherBid
		lower := 0.0
		if lowerDenom > 0 {
			lower = target.Bid / lowerDenom
		}
		upper := 1.0
		if upperDenom > 0 {
			upper = target.Ask / upperDenom
		}
		lower = math.Max(0, lower-modelReserve)
		upper = math.Min(1, upper+modelReserve)
		fair := ((target.Bid + target.Ask) / 2) / midSum
		if lower > fair {
			lower = fair
		}
		if upper < fair {
			upper = fair
		}
		result.Projections = append(result.Projections, Projection{MemberID: target.ID,
			Fair: fair, Lower: lower, Upper: upper})
	}
	if !result.SettlementCompatible {
		result.Blocker = "void policy is not verified for both immutable frames"
	}
	return result, nil
}

// SelectAction applies a frozen projection to independently supplied current one-share route
// economics. The model never sees those prices. A candidate requires complete source-clock,
// depth, tick and exact-fee truth and a strictly positive conservative residual after all-in cost.
func SelectAction(p Projection, yes, no OneShareEconomics) ActionDecision {
	valid := func(v OneShareEconomics, side string) bool {
		return strings.EqualFold(v.Side, side) && v.Cost > 0 && v.Cost < 1 && v.Fee >= 0 &&
			v.Depth >= 1 && v.Tick > 0 && v.QuoteAge >= 0 && strings.TrimSpace(v.BookClock) != "" &&
			!math.IsNaN(v.Cost+v.Fee+v.Depth+v.Tick+v.QuoteAge) && !math.IsInf(v.Cost+v.Fee+v.Depth+v.Tick+v.QuoteAge, 0)
	}
	if !valid(yes, "YES") || !valid(no, "NO") {
		return ActionDecision{Decision: "control", Blocker: "current exact one-share YES/NO route truth is incomplete"}
	}
	yesNet := p.Lower - yes.Cost - yes.Fee
	noLower := 1 - p.Upper
	noNet := noLower - no.Cost - no.Fee
	if yesNet <= 0 && noNet <= 0 {
		return ActionDecision{Decision: "control", Blocker: "conservative residual does not clear all-in cost",
			Fair: p.Fair, FairLower: math.Max(p.Lower, noLower), ExpectedNetLower: math.Max(yesNet, noNet)}
	}
	if yesNet >= noNet {
		return ActionDecision{Side: "YES", Decision: "candidate", Fair: p.Fair,
			FairLower: p.Lower, ExpectedNetLower: yesNet}
	}
	return ActionDecision{Side: "NO", Decision: "candidate", Fair: 1 - p.Fair,
		FairLower: noLower, ExpectedNetLower: noNet}
}
