package kalshi

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"
)

// RFQ / combo (multivariate event) live-trading. VERIFIED against docs.kalshi.com (openapi.yaml,
// v3.14.0) 2026-07-01. Combos are their OWN market priced by an RFQ auction, NOT a resting order book:
//
//	1. CreateMVEMarket — instantiate the combined market for a set of legs (must be hit once before trading).
//	2. CreateRFQ        — open an RFQ on that market_ticker for a target dollar cost ($1 combos).
//	3. GetQuotes        — poll for market-maker quotes on the RFQ (yes_bid/no_bid per contract).
//	4. AcceptQuote      — accept one side of the best quote; the MAKER then confirms within ~3s (HVM),
//	                      executes within ~1s, and the fill lands in GET /portfolio/fills.
//
// Fills are NOT guaranteed: if no maker quotes (or none confirm), nothing happens. All WRITES are gated
// by writeAllowed() — demo always, prod only when explicitly armed.

// MVELeg is one selected leg of a combo (a market + which side).
type MVELeg struct {
	MarketTicker string `json:"market_ticker"`
	EventTicker  string `json:"event_ticker"`
	Side         string `json:"side"` // "yes" | "no"
}

type createMVEMarketRequest struct {
	SelectedMarkets []MVELeg `json:"selected_markets"`
}
type createMVEMarketResponse struct {
	EventTicker  string `json:"event_ticker"`
	MarketTicker string `json:"market_ticker"`
}

// CreateMVEMarket instantiates (or fetches) the combined market for a set of legs in a multivariate event
// collection and returns its market ticker. Must be called at least once before an RFQ can reference it.
// Limited by Kalshi to 5000 creations/week.
func (c *Client) CreateMVEMarket(ctx context.Context, collectionTicker string, legs []MVELeg) (marketTicker, eventTicker string, err error) {
	if err = c.writeAllowed(); err != nil {
		return "", "", err
	}
	var resp createMVEMarketResponse
	path := "/multivariate_event_collections/" + url.PathEscape(collectionTicker)
	if err = c.doWithBody(ctx, http.MethodPost, path, createMVEMarketRequest{SelectedMarkets: legs}, &resp, true); err != nil {
		return "", "", err
	}
	return resp.MarketTicker, resp.EventTicker, nil
}

// CreateResearchMVEMarket performs the same authenticated public-listing mutation without granting
// order authority. It is reserved for the server's sealed Paper AUTO bridge: creating a combined
// market cannot move money, and the caller must not call AcceptQuote without the separate live arm.
func (c *Client) CreateResearchMVEMarket(ctx context.Context, collectionTicker string, legs []MVELeg) (marketTicker, eventTicker string, err error) {
	var resp createMVEMarketResponse
	path := "/multivariate_event_collections/" + url.PathEscape(collectionTicker)
	if err = c.doWithBody(ctx, http.MethodPost, path, createMVEMarketRequest{SelectedMarkets: legs}, &resp, true); err != nil {
		return "", "", err
	}
	return resp.MarketTicker, resp.EventTicker, nil
}

// Combo probe states (R113).
const (
	ComboLegal   = "legal"   // venue serves/instantiated the combined market
	ComboIllegal = "illegal" // venue refused with a reason (cacheable answer)
	ComboUnknown = "unknown" // lookup 404 not_found — NOT an answer (see LookupCombo)
)

// comboRefusal extracts the venue's 4xx reason from a client error. c.do surfaces refusals as
// "kalshi <METHOD> <path>: status 4xx: <body>" — a 4xx IS the venue's answer (cacheable);
// anything else is a transport failure (retry later). Returns (reason, true) for 4xx.
func comboRefusal(err error) (string, bool) {
	es := err.Error()
	if i := strings.Index(es, ": status 4"); i >= 0 {
		reason := es[i+2:]
		if j := strings.Index(reason, ": "); j >= 0 {
			reason = reason[j+2:]
		}
		return strings.TrimSpace(reason), true
	}
	return "", false
}

// LookupCombo (R113 REWRITE of the R112 probe) — authed, LOOKUP-ONLY:
// PUT /multivariate_event_collections/{ticker}/lookup reads the combined market for a leg
// selection WITHOUT creating anything. ⚠ DEPRECATED-ENDPOINT SEMANTICS (docs, verified live
// R113): a 404 not_found means "nobody has ever instantiated this exact combo", NOT "illegal"
// — R112 cached those 404s as illegal_probed and wrongly concluded crypto combos don't exist
// (refuted by operator app screenshots + R113 replication: the venue CREATED a 3-leg
// BTC+ETH+SOL cross-series combo and even a 4-leg double-BTC one). Returns:
//   - state=ComboLegal:   the combined market exists (cacheable yes)
//   - state=ComboUnknown: 404 not_found — inconclusive; escalate to CreateComboProbe
//   - state=ComboIllegal: other 4xx + the venue's reason (cacheable no)
//   - err != nil:         transport/5xx — not an answer, must not be cached
func (c *Client) LookupCombo(ctx context.Context, collectionTicker string, legs []MVELeg) (state string, venueReason string, err error) {
	state, _, _, venueReason, err = c.LookupComboFull(ctx, collectionTicker, legs)
	return state, venueReason, err
}

// LookupComboFull (R126) — LookupCombo plus the instantiated combined market's identifiers.
// The lookup 200 body is LookupTickersForMarketInMultivariateEventCollectionResponse:
// {"event_ticker": string, "market_ticker": string} (both required) — VERIFIED against
// docs.kalshi.com openapi v3.14.0, fetched 2026-07-09; same shape as the POST create response.
// State semantics are exactly LookupCombo's (legal / unknown-404 / illegal-4xx / transport err).
// Defensive: an empty 2xx body decodes to empty tickers (doWithBody skips empty bodies), and a
// 200 whose body fails to decode still reports ComboLegal with empty tickers — a decode bug must
// never mask the venue's yes.
func (c *Client) LookupComboFull(ctx context.Context, collectionTicker string, legs []MVELeg) (state, marketTicker, eventTicker, venueReason string, err error) {
	path := "/multivariate_event_collections/" + url.PathEscape(collectionTicker) + "/lookup"
	var resp createMVEMarketResponse
	err = c.doWithBody(ctx, http.MethodPut, path, createMVEMarketRequest{SelectedMarkets: legs}, &resp, true)
	if err == nil {
		return ComboLegal, resp.MarketTicker, resp.EventTicker, "", nil
	}
	if strings.HasPrefix(err.Error(), "decode response") {
		return ComboLegal, "", "", "", nil // HTTP was 2xx — the venue said yes; only our decode failed
	}
	if reason, is4xx := comboRefusal(err); is4xx {
		if strings.Contains(reason, "not_found") {
			return ComboUnknown, "", "", "", nil // never-created ≠ illegal (the R112 misread)
		}
		return ComboIllegal, "", "", reason, nil
	}
	return "", "", "", "", err
}

// CreateComboProbe (R140 current official contract) — POST the create-market-in-collection
// endpoint. This instantiates the combined MARKET (a public listing — NOT an order, NOT an RFQ;
// no position, no money moves; venue-documented, capped at 5000 creations/week). Verified live
// R113 against the operator's screenshot combos and rechecked against Kalshi's Jul-12 docs.
// Deliberately NOT writeAllowed-gated: it is the current documented way to get the venue's
// yes/no on a never-created combo, and callers enforce
// their own strict weekly budget (comboprobe.go) far below the venue cap.
// ok=true → legal; ok=false+reason → the venue's refusal; err → transport/5xx (not an answer).
func (c *Client) CreateComboProbe(ctx context.Context, collectionTicker string, legs []MVELeg) (ok bool, venueReason string, err error) {
	path := "/multivariate_event_collections/" + url.PathEscape(collectionTicker)
	err = c.doWithBody(ctx, http.MethodPost, path, createMVEMarketRequest{SelectedMarkets: legs}, nil, true)
	if err == nil {
		return true, "", nil
	}
	if reason, is4xx := comboRefusal(err); is4xx {
		return false, reason, nil
	}
	return false, "", err
}

type createRFQRequest struct {
	MarketTicker      string `json:"market_ticker"`
	TargetCostDollars string `json:"target_cost_dollars"`
	RestRemainder     bool   `json:"rest_remainder"`
}
type createRFQResponse struct {
	ID string `json:"id"`
}

// CreateRFQ opens an RFQ on a market for a target dollar cost (e.g. "1.00" for a $1 combo) and returns the
// RFQ id. rest_remainder=false ⇒ don't leave a resting order for any unfilled remainder. NOTE: RFQs are NOT
// idempotent — a network-retry could in theory open a second RFQ, but Kalshi returns 409 if an open RFQ
// already exists on the same market, so a dup surfaces as an error rather than a double trade.
func (c *Client) CreateRFQ(ctx context.Context, marketTicker, targetCostDollars string) (string, error) {
	if err := c.writeAllowed(); err != nil {
		return "", err
	}
	var resp createRFQResponse
	req := createRFQRequest{MarketTicker: marketTicker, TargetCostDollars: targetCostDollars, RestRemainder: false}
	if err := c.doWithBody(ctx, http.MethodPost, "/communications/rfqs", req, &resp, true); err != nil {
		return "", err
	}
	return resp.ID, nil
}

// CreateRFQContracts opens an RFQ sized in WHOLE CONTRACTS (R21: testing whether the venue's
// auto-maker quotes contract-sized RFQs while ignoring target-cost ones). rest=true rests any
// unfilled remainder on the public book after execution.
func (c *Client) CreateRFQContracts(ctx context.Context, marketTicker, contractsFP string, rest bool) (string, error) {
	if err := c.writeAllowed(); err != nil {
		return "", err
	}
	var resp createRFQResponse
	req := map[string]any{"market_ticker": marketTicker, "contracts_fp": contractsFP, "rest_remainder": rest}
	if err := c.doWithBody(ctx, http.MethodPost, "/communications/rfqs", req, &resp, true); err != nil {
		return "", err
	}
	return resp.ID, nil
}

// CreateResearchRFQContracts opens an authenticated quote request but cannot accept it. Unlike the
// order-authorized method, this is allowed while LIVE is disarmed so a sealed system can make an
// exact one-contract Paper decision. The server immediately deletes it unless the separately armed
// live path revalidates and accepts it.
func (c *Client) CreateResearchRFQContracts(ctx context.Context, marketTicker, contractsFP string, rest bool) (string, error) {
	var resp createRFQResponse
	req := map[string]any{"market_ticker": marketTicker, "contracts_fp": contractsFP, "rest_remainder": rest}
	if err := c.doWithBody(ctx, http.MethodPost, "/communications/rfqs", req, &resp, true); err != nil {
		return "", err
	}
	return resp.ID, nil
}

// Quote is a market-maker's response to an RFQ (read-only). Prices are per-contract dollar strings;
// yes_bid = price for the YES side, no_bid = price for the NO side. Either may be "0" to decline that side.
type Quote struct {
	ID             string `json:"id"`
	RFQID          string `json:"rfq_id"`
	MarketTicker   string `json:"market_ticker"`
	ContractsFp    string `json:"contracts_fp"`
	YesBidDollars  string `json:"yes_bid_dollars"`
	NoBidDollars   string `json:"no_bid_dollars"`
	YesContractsFp string `json:"yes_contracts_fp"`
	NoContractsFp  string `json:"no_contracts_fp"`
	// Current Kalshi quote responses expose lifecycle timestamps, not a status property. Status is
	// retained as a normalized convenience for callers and for compatibility with older responses.
	Status             string `json:"status"` // derived: open | accepted | confirmed | executed | cancelled
	CreatedTS          string `json:"created_ts"`
	UpdatedTS          string `json:"updated_ts"`
	AcceptedTS         string `json:"accepted_ts"`
	ConfirmedTS        string `json:"confirmed_ts"`
	ExecutedTS         string `json:"executed_ts"`
	CancelledTS        string `json:"cancelled_ts"`
	CancellationReason string `json:"cancellation_reason"`
	// Populated after the RFQ creator accepts. This immutable order identity is the only safe join
	// from the communication quote to /portfolio/fills; the MVE ticker alone may be reused.
	RFQCreatorOrderID string `json:"rfq_creator_order_id"`
}

func normalizeQuoteStatus(quote Quote) Quote {
	switch {
	case strings.TrimSpace(quote.CancelledTS) != "" || strings.TrimSpace(quote.CancellationReason) != "":
		quote.Status = "cancelled"
	case strings.TrimSpace(quote.ExecutedTS) != "":
		quote.Status = "executed"
	case strings.TrimSpace(quote.ConfirmedTS) != "":
		quote.Status = "confirmed"
	case strings.TrimSpace(quote.AcceptedTS) != "":
		quote.Status = "accepted"
	case strings.TrimSpace(quote.Status) == "":
		quote.Status = "open"
	default:
		quote.Status = strings.ToLower(strings.TrimSpace(quote.Status))
	}
	return quote
}

type getQuotesResponse struct {
	Quotes []Quote `json:"quotes"`
	Cursor string  `json:"cursor"`
}

// GetQuotes returns quotes responding to an RFQ created by this authenticated account. Kalshi's
// identity filters are directional: requester-side reads use rfq_user_filter=self.
func (c *Client) GetQuotes(ctx context.Context, rfqID string) ([]Quote, error) {
	return c.getQuotes(ctx, rfqID, quoteRFQCreatorSelf)
}

const (
	// Kalshi's communications quote list caps one response at 500 rows. The page ceiling bounds a
	// malformed/cyclic upstream cursor while still allowing a complete 50,000-quote read.
	quotePageLimit = 500
	quoteMaxPages  = 100
)

type quoteIdentityFilter string

const (
	// quoteRFQCreatorSelf means "quotes responding to an RFQ created by this authenticated user".
	// This is the identity required after CreateRFQ/CreateResearchRFQContracts.
	quoteRFQCreatorSelf quoteIdentityFilter = "rfq_user_filter"
	// quoteCreatorSelf means "quotes authored by this authenticated user". Keep this separate so a
	// maker-side reader cannot accidentally inherit requester-side semantics.
	quoteCreatorSelf quoteIdentityFilter = "user_filter"
)

// GetOwnQuotes is the distinct maker-side view: it returns quotes authored by this authenticated
// account for the named RFQ. Funded combo execution must use GetQuotes, not this method.
func (c *Client) GetOwnQuotes(ctx context.Context, rfqID string) ([]Quote, error) {
	return c.getQuotes(ctx, rfqID, quoteCreatorSelf)
}

func (c *Client) getQuotes(ctx context.Context, rfqID string, identity quoteIdentityFilter) ([]Quote, error) {
	rfqID = strings.TrimSpace(rfqID)
	if rfqID == "" {
		return nil, fmt.Errorf("kalshi get quotes: rfq id is required")
	}
	if identity != quoteRFQCreatorSelf && identity != quoteCreatorSelf {
		return nil, fmt.Errorf("kalshi get quotes: unsupported identity filter %q", identity)
	}

	const endpoint = "/communications/quotes"
	cursor := ""
	seenCursors := map[string]struct{}{}
	seenQuotes := map[string]Quote{}
	out := make([]Quote, 0)
	for page := 0; page < quoteMaxPages; page++ {
		q := url.Values{
			"rfq_id":         {rfqID},
			"limit":          {fmt.Sprint(quotePageLimit)},
			string(identity): {"self"},
		}
		path := portfolioPagePath(endpoint, q, cursor)
		var resp getQuotesResponse
		if err := c.do(ctx, http.MethodGet, path, &resp, true); err != nil {
			return nil, err
		}
		if resp.Quotes == nil {
			return nil, fmt.Errorf("kalshi %s response missing quotes array", endpoint)
		}
		for _, quote := range resp.Quotes {
			quote = normalizeQuoteStatus(quote)
			quote.ID = strings.TrimSpace(quote.ID)
			if quote.ID == "" {
				return nil, fmt.Errorf("kalshi %s returned quote without id", endpoint)
			}
			if quote.RFQID != "" && quote.RFQID != rfqID {
				return nil, fmt.Errorf("kalshi %s returned quote %q for rfq %q while reading %q",
					endpoint, quote.ID, quote.RFQID, rfqID)
			}
			if prior, duplicate := seenQuotes[quote.ID]; duplicate {
				if prior != quote {
					return nil, fmt.Errorf("kalshi %s returned conflicting duplicate quote %q", endpoint, quote.ID)
				}
				continue
			}
			seenQuotes[quote.ID] = quote
			out = append(out, quote)
		}

		next, done, err := portfolioNextCursor(endpoint, cursor, resp.Cursor, seenCursors)
		if err != nil {
			return nil, err
		}
		if done {
			return out, nil
		}
		cursor = next
	}
	return nil, fmt.Errorf("kalshi %s pagination exceeded %d pages", endpoint, quoteMaxPages)
}

// GetRFQQuote re-reads one quote through Kalshi's current RFQ-scoped resource. The quote-ID-only
// lookup is deprecated as of 2026-07-07. Money paths use this exact parent/child identity before
// acceptance and while reconciling accepted -> confirmed -> executed; a list-row snapshot is not
// proof that the same quote still belongs to the same open RFQ.
func (c *Client) GetRFQQuote(ctx context.Context, rfqID, quoteID string) (Quote, error) {
	var resp struct {
		Quote Quote `json:"quote"`
	}
	path := "/communications/rfqs/" + url.PathEscape(rfqID) + "/quotes/" + url.PathEscape(quoteID)
	if err := c.do(ctx, http.MethodGet, path, &resp, true); err != nil {
		return Quote{}, err
	}
	return normalizeQuoteStatus(resp.Quote), nil
}

type acceptQuoteRequest struct {
	AcceptedSide string `json:"accepted_side"` // "yes" | "no"
}

// AcceptQuote accepts one side of a maker's quote (side = "yes" to BUY the combo YES / "no" for NO). This
// puts the deal into the maker's confirmation window; it only fills once the maker confirms. Returns 204.
// ⚠ LEGACY PATH — Kalshi deprecated the /communications/quotes/{quote_id} resource family on
// 2026-07-09 (current official docs fetched 2026-07-12). Kept only as the rollout fallback for
// AcceptQuoteRFQ below.
func (c *Client) AcceptQuote(ctx context.Context, quoteID, side string) error {
	if err := c.writeAllowed(); err != nil {
		return err
	}
	path := "/communications/quotes/" + url.PathEscape(quoteID) + "/accept"
	return c.doWithBody(ctx, http.MethodPut, path, acceptQuoteRequest{AcceptedSide: side}, nil, true)
}

// AcceptQuoteRFQ (R122, auditor r56 DO-THIS 3) — the migrated accept: RFQ-scoped quote path
// PUT /communications/rfqs/{rfq_id}/quotes/{quote_id}/accept, per the venue's 2026-07-09
// deprecation of the /communications/quotes/{quote_id} family. A scoped 404/405 fails closed:
// retrying a parent-identity refusal through the deprecated quote-id-only path could accept a
// different or reused quote id. Transport errors remain ambiguous for the caller to reconcile.
func (c *Client) AcceptQuoteRFQ(ctx context.Context, rfqID, quoteID, side string) error {
	if err := c.writeAllowed(); err != nil {
		return err
	}
	newPath := "/communications/rfqs/" + url.PathEscape(rfqID) + "/quotes/" + url.PathEscape(quoteID) + "/accept"
	err := c.doWithBody(ctx, http.MethodPut, newPath, acceptQuoteRequest{AcceptedSide: side}, nil, true)
	if err != nil {
		return err // fail closed: never retry a scoped identity refusal on the deprecated global path
	}
	return nil
}

// DeleteRFQ cancels an open RFQ (cleanup so it doesn't count against the 100-open-RFQ limit).
func (c *Client) DeleteRFQ(ctx context.Context, rfqID string) error {
	if err := c.writeAllowed(); err != nil {
		return err
	}
	return c.do(ctx, http.MethodDelete, "/communications/rfqs/"+url.PathEscape(rfqID), nil, true)
}

// DeleteResearchRFQ closes a no-money Paper quote request while LIVE is disarmed. Cancellation can
// only remove a communication/resource reservation; it cannot create a position.
func (c *Client) DeleteResearchRFQ(ctx context.Context, rfqID string) error {
	return c.do(ctx, http.MethodDelete, "/communications/rfqs/"+url.PathEscape(rfqID), nil, true)
}
