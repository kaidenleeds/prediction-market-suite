package server

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strings"
)

// paperPortfolioSurface is the current money truth for one independently funded Paper book.
// Unknown ledger state deliberately omits money fields instead of presenting a fabricated $0.
type paperPortfolioSurface struct {
	ID                      string   `json:"id"`
	Label                   string   `json:"label"`
	Available               bool     `json:"available"`
	StartingGrantUSD        float64  `json:"starting_grant_usd"`
	EquityUSD               *float64 `json:"equity_usd,omitempty"`
	RealizedFeeNetUSD       *float64 `json:"realized_fee_net_usd,omitempty"`
	ReservedOpenExposureUSD *float64 `json:"reserved_open_exposure_usd,omitempty"`
	AvailableUSD            *float64 `json:"available_usd,omitempty"`
	FundedOpenPositions     *int     `json:"funded_open_positions,omitempty"`
	ReservedOrOpenCount     *int     `json:"reserved_or_open_count,omitempty"`
	ClosedBets              *int     `json:"closed_bets,omitempty"`
	ClosedUnits             *float64 `json:"closed_units,omitempty"`
	UnavailableReason       string   `json:"unavailable_reason,omitempty"`
}

// paperFundedPosition is a read-only projection of one current funded Genfollow lot. These rows
// intentionally remain separate from paper.Position: the existing /api/paper close endpoints own
// only SQLite paper_fills and must never pretend they can close a different ledger.
type paperFundedPosition struct {
	PositionID               string   `json:"position_id"`
	Ledger                   string   `json:"ledger"`
	SubBook                  string   `json:"sub_book"`
	Portfolio                string   `json:"portfolio"`
	Platform                 string   `json:"platform"`
	Ticker                   string   `json:"ticker"`
	Title                    string   `json:"title,omitempty"`
	Side                     string   `json:"side"`
	Contracts                float64  `json:"contracts"`
	EntryPrice               float64  `json:"entry_price"`
	EntryFeeUSD              float64  `json:"entry_fee_usd"`
	CostBasisUSD             float64  `json:"cost_basis_usd"`
	Opened                   string   `json:"opened,omitempty"`
	CurrentPrice             *float64 `json:"current_price,omitempty"`
	UnrealizedUSD            *float64 `json:"unrealized_usd,omitempty"`
	Route                    string   `json:"route,omitempty"`
	ExecutionShadowAttempt   string   `json:"execution_shadow_attempt_id,omitempty"`
	ExecutionGeneration      string   `json:"execution_generation,omitempty"`
	ExecutionBookSource      string   `json:"execution_book_source,omitempty"`
	Provenance               string   `json:"provenance"`
	State                    string   `json:"state"`
	ReadOnly                 bool     `json:"read_only"`
	CloseSupportedByPaperAPI bool     `json:"close_supported_by_paper_api"`
}

func paperFloat64Ptr(v float64) *float64 { return &v }
func paperIntPtr(v int) *int             { return &v }

func (s *Server) currentPaperPortfolioSurfaces(ctx context.Context) []paperPortfolioSurface {
	rows := make([]paperPortfolioSurface, 0, 5)
	for _, book := range []struct {
		id, label string
	}{
		{vbKalshi, "Kalshi"},
		{vbPolyus, "PolyUS"},
		{vbCombos, "System Combos"},
		{vbML, "ML"},
	} {
		row := paperPortfolioSurface{
			ID: book.id, Label: book.label, StartingGrantUSD: s.bookBankUSD(book.id),
		}
		metrics, metricsOK := s.bookSessionClosedMetrics(ctx, book.id)
		exposure, count, exposureOK := s.bookOpenExposure(ctx, book.id)
		if !metricsOK || !exposureOK {
			row.UnavailableReason = "one or more funded ledgers could not be read"
			rows = append(rows, row)
			continue
		}
		equity := compoundedPortfolioEquity(row.StartingGrantUSD, metrics.NetUSD)
		available := math.Max(0, equity-exposure)
		row.Available = true
		row.EquityUSD = paperFloat64Ptr(equity)
		row.RealizedFeeNetUSD = paperFloat64Ptr(metrics.NetUSD)
		row.ReservedOpenExposureUSD = paperFloat64Ptr(exposure)
		row.AvailableUSD = paperFloat64Ptr(available)
		row.ReservedOrOpenCount = paperIntPtr(count)
		row.ClosedBets = paperIntPtr(metrics.Bets)
		if metrics.OpenComplete {
			row.FundedOpenPositions = paperIntPtr(metrics.Open)
		}
		if metrics.UnitsComplete {
			row.ClosedUnits = paperFloat64Ptr(metrics.Units)
		}
		rows = append(rows, row)
	}

	mlCombo := paperPortfolioSurface{
		ID: "ml-combos", Label: "New-ML Combos", StartingGrantUSD: s.mlComboPaperGrant(),
	}
	status := newMLComboStatus()
	if !s.recomputeMLComboMoney(ctx, &status) {
		mlCombo.UnavailableReason = "the funded ML Combo ledger could not be read"
	} else {
		mlCombo.Available = true
		mlCombo.EquityUSD = paperFloat64Ptr(status.Equity)
		mlCombo.RealizedFeeNetUSD = paperFloat64Ptr(status.RealizedNet)
		mlCombo.ReservedOpenExposureUSD = paperFloat64Ptr(status.OpenCost)
		mlCombo.AvailableUSD = paperFloat64Ptr(status.Available)
		mlCombo.FundedOpenPositions = paperIntPtr(status.Open)
		mlCombo.ReservedOrOpenCount = paperIntPtr(status.Open)
		mlCombo.ClosedBets = paperIntPtr(status.Closed)
	}
	return append(rows, mlCombo)
}

func genfollowPositionID(sub string, p kfPos) string {
	for _, candidate := range []struct {
		kind, value string
	}{
		{"execution", p.ExecutionShadowAttemptID},
		{"placement", p.PlacementID},
		{"relation", p.RelationReceiptID},
	} {
		if id := strings.TrimSpace(candidate.value); id != "" {
			return "genfollow:" + sub + ":" + candidate.kind + ":" + id
		}
	}
	return fmt.Sprintf("genfollow:%s:legacy:%s:%s:%s", sub,
		strings.TrimSpace(p.Ticker), strings.ToUpper(strings.TrimSpace(p.Side)),
		firstNonEmpty(strings.TrimSpace(p.FillTS), strings.TrimSpace(p.TS)))
}

// currentGenfollowFundedPositions reads only current canonical funded lots. It does not inspect
// execution-shadow events, so a shadow PAPER-FILLED receipt without a committed Genfollow lot
// cannot become a displayed position.
func (s *Server) currentGenfollowFundedPositions() ([]paperFundedPosition, bool) {
	type fundedLot struct {
		sub string
		pos kfPos
	}
	var lots []fundedLot
	s.gfBookMu.Lock()
	book := s.gfLoadLocked()
	if s.gfBookPoisoned.Load() {
		s.gfBookMu.Unlock()
		return nil, false
	}
	for sub, funded := range book.Subs {
		if funded == nil {
			continue
		}
		for _, pos := range funded.Open {
			lots = append(lots, fundedLot{sub: sub, pos: pos})
		}
	}
	s.gfBookMu.Unlock()

	rows := make([]paperFundedPosition, 0, len(lots))
	for _, lot := range lots {
		p := lot.pos
		portfolio := vbKalshi
		if strings.HasSuffix(lot.sub, "-p") {
			portfolio = vbPolyus
		}
		// Older funded lots sometimes omitted Platform because their sub-book suffix was the venue
		// authority. Restore that authority before both marking and display; otherwise a legacy
		// PolyUS `-p` lot would be mislabeled and marked against Kalshi.
		if strings.TrimSpace(p.Platform) == "" {
			p.Platform = portfolio
		}
		platform := fiLotPlatform(p)
		opened := firstNonEmpty(strings.TrimSpace(p.FillTS), strings.TrimSpace(p.TS))
		provenance := "legacy-funded-ledger"
		if strings.TrimSpace(p.ExecutionShadowAttemptID) != "" {
			provenance = "funded-ledger-with-execution-link"
		}
		row := paperFundedPosition{
			PositionID: genfollowPositionID(lot.sub, p), Ledger: "genfollow_book.json",
			SubBook: lot.sub, Portfolio: portfolio, Platform: platform,
			Ticker: p.Ticker, Title: p.Title, Side: strings.ToUpper(strings.TrimSpace(p.Side)),
			Contracts: p.Contracts, EntryPrice: p.Price, EntryFeeUSD: p.Fee,
			CostBasisUSD: p.Contracts*p.Price + p.Fee, Opened: opened, Route: p.RouteReason,
			ExecutionShadowAttempt: p.ExecutionShadowAttemptID,
			ExecutionGeneration:    p.ExecutionGeneration, ExecutionBookSource: p.ExecutionBookSource,
			Provenance: provenance, State: "funded-current", ReadOnly: true,
			CloseSupportedByPaperAPI: false,
		}
		if mark, ok := s.fiSideMark(p); ok {
			unrealized := p.Contracts*(mark-p.Price) - p.Fee
			row.CurrentPrice, row.UnrealizedUSD = paperFloat64Ptr(mark), paperFloat64Ptr(unrealized)
		}
		rows = append(rows, row)
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].Opened != rows[j].Opened {
			return rows[i].Opened > rows[j].Opened
		}
		return rows[i].PositionID < rows[j].PositionID
	})
	return rows, true
}
