package outcomeexpansion

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestFrameAndChangeJSONSchemaRoundTrip(t *testing.T) {
	in := Result{ModelVersion: ModelVersion, PriorFrameID: 1, CurrentFrameID: 2,
		EventDayCluster: "kalshi|EVT|2026-07-12", Change: Change{Added: []string{"a"},
			Removed: []string{"b"}, StatusChanged: []string{"c"}, Class: "mixed"},
		Projections:  []Projection{{MemberID: "a", Fair: .5, Lower: .4, Upper: .6}},
		ModelReserve: .01, SettlementCompatible: true}
	b, err := json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	text := string(b)
	for _, key := range []string{`"added"`, `"removed"`, `"status_changed"`, `"prior_frame_id"`,
		`"current_frame_id"`, `"settlement_compatible"`} {
		if !strings.Contains(text, key) {
			t.Fatalf("schema key %s missing from %s", key, text)
		}
	}
	var out Result
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(in, out) {
		t.Fatalf("round trip changed schema:\n%+v\n%+v", in, out)
	}

	f := frame(3, time.Date(2026, 7, 12, 1, 2, 3, 0, time.UTC), "hash",
		Member{ID: "a", Status: "active", Bid: .4, Ask: .5}, Member{ID: "b", Status: "inactive"})
	fb, err := json.Marshal(f)
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{`"venue"`, `"event_id"`, `"membership_hash"`,
		`"mutually_exclusive"`, `"exhaustive"`, `"void_policy_verified"`} {
		if !strings.Contains(string(fb), key) {
			t.Fatalf("frame schema key %s missing from %s", key, fb)
		}
	}
	var round Frame
	if err := json.Unmarshal(fb, &round); err != nil || !reflect.DeepEqual(f, round) {
		t.Fatalf("frame round trip failed: %+v err=%v", round, err)
	}
}

func frame(id int64, at time.Time, hash string, members ...Member) Frame {
	return Frame{FrameID: id, Observed: at, Venue: "kalshi", EventID: "EVT", MembershipHash: hash,
		Members: members, MutuallyExclusive: true, Exhaustive: true, VoidPolicy: true}
}

func TestRemovalRedistributesPriorWithoutCurrentQuoteLeakage(t *testing.T) {
	t0 := time.Date(2026, 7, 12, 10, 0, 0, 0, time.UTC)
	prior := frame(10, t0, "prior",
		Member{ID: "a", Status: "active", Bid: .18, Ask: .22},
		Member{ID: "b", Status: "active", Bid: .28, Ask: .32},
		Member{ID: "c", Status: "active", Bid: .48, Ask: .52})
	current := frame(11, t0.Add(time.Minute), "current",
		Member{ID: "a", Status: "active", Bid: .01, Ask: .99},
		Member{ID: "b", Status: "active", Bid: .99, Ask: .999},
		Member{ID: "c", Status: "withdrawn", Bid: .49, Ask: .51})
	a, err := Analyze(prior, current)
	if err != nil {
		t.Fatal(err)
	}
	current.Members[0].Bid, current.Members[0].Ask = .77, .78
	current.Members[1].Bid, current.Members[1].Ask = .02, .03
	b, err := Analyze(prior, current)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(a, b) {
		t.Fatalf("current quotes leaked into pre-shock redistribution:\n%+v\n%+v", a, b)
	}
	if a.Blocker != "" || len(a.Projections) != 2 || a.Change.Class != "status_change" ||
		a.Projections[0].Lower > a.Projections[0].Fair || a.Projections[0].Fair > a.Projections[0].Upper {
		t.Fatalf("unexpected result: %+v", a)
	}
	if a.EventDayCluster != "kalshi|EVT|2026-07-12" {
		t.Fatalf("missing event/day cluster: %q", a.EventDayCluster)
	}
}

func TestNewActiveMemberBlocksRatherThanUsingItsQuote(t *testing.T) {
	t0 := time.Now().UTC()
	prior := frame(1, t0, "p", Member{ID: "a", Status: "active", Bid: .4, Ask: .45},
		Member{ID: "b", Status: "active", Bid: .55, Ask: .6})
	current := frame(2, t0.Add(time.Second), "c", Member{ID: "a", Status: "active"},
		Member{ID: "b", Status: "active"}, Member{ID: "new", Status: "active", Bid: .01, Ask: .02})
	got, err := Analyze(prior, current)
	if err != nil {
		t.Fatal(err)
	}
	if got.Blocker == "" || len(got.Projections) != 2 {
		t.Fatalf("unidentified entrant probability must block: %+v", got)
	}
	for _, projection := range got.Projections {
		if projection.Lower != 0 || projection.Upper != 1 {
			t.Fatalf("added-member controls must stay uninformative: %+v", projection)
		}
	}
}

func TestIncompleteOrUnorderedFramesAreRejected(t *testing.T) {
	t0 := time.Now().UTC()
	p := frame(2, t0, "same", Member{ID: "a", Status: "active", Bid: .4, Ask: .5},
		Member{ID: "b", Status: "active", Bid: .5, Ask: .6})
	c := frame(1, t0, "same", Member{ID: "a", Status: "active"}, Member{ID: "b", Status: "inactive"})
	if _, err := Analyze(p, c); err == nil {
		t.Fatal("unordered/non-changing frames must be rejected")
	}
	c = frame(3, t0.Add(time.Second), "next", Member{ID: "a", Status: "active"}, Member{ID: "b", Status: "inactive"})
	c.Exhaustive = false
	if _, err := Analyze(p, c); err == nil {
		t.Fatal("incomplete frames must be rejected")
	}
}

func TestActionRequiresExactRouteAndPositiveLowerResidual(t *testing.T) {
	p := Projection{MemberID: "a", Fair: .7, Lower: .65, Upper: .75}
	yes := OneShareEconomics{Side: "YES", Cost: .5, Fee: .01, Depth: 3, Tick: .01, QuoteAge: .2, BookClock: "q1"}
	no := OneShareEconomics{Side: "NO", Cost: .51, Fee: .01, Depth: 3, Tick: .01, QuoteAge: .2, BookClock: "q1"}
	got := SelectAction(p, yes, no)
	if got.Decision != "candidate" || got.Side != "YES" || got.ExpectedNetLower <= 0 {
		t.Fatalf("expected conservative YES residual: %+v", got)
	}
	yes.Cost = .8
	got = SelectAction(p, yes, no)
	if got.Decision != "control" || got.Blocker == "" {
		t.Fatalf("non-positive route must remain control: %+v", got)
	}
	no.BookClock = ""
	if got := SelectAction(p, yes, no); got.Decision != "control" || got.Blocker == "" {
		t.Fatalf("missing source clock must block: %+v", got)
	}
}
