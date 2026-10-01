package server

import (
	"math"
	"strings"
)

func mlVenueMap(m map[string]any, key string) map[string]any {
	v, _ := m[key].(map[string]any)
	if v == nil {
		v = map[string]any{}
		m[key] = v
	}
	return v
}

func mlVenueMapFloat(m map[string]any, key string) float64 {
	v, _ := m[key].(float64)
	return v
}

func mlRoundMoney(v float64) float64 { return math.Round(v*100) / 100 }

func mlVenueLifetimeMaps(life map[string]any, key string) (map[string]any, map[string]any, bool) {
	current, cOK := life["venue_"+key].(map[string]any)
	base, bOK := life["venue_"+key+"_base"].(map[string]any)
	return current, base, cOK && bOK
}

// mlRepairVenueLifetimeFromCompleteEpoch repairs a stale per-venue split only when the retained
// detail is a complete, independently checkable account of the current epoch. The three receipts
// must agree: lifetime closed count, lifetime total net, and the detailed close rows. A trimmed or
// partially written ring is never used to rewrite durable accounting.
func mlRepairVenueLifetimeFromCompleteEpoch(life map[string]any, closed []any) bool {
	net, netBase, netOK := mlVenueLifetimeMaps(life, "net")
	closedCount, closedBase, closedCountOK := mlVenueLifetimeMaps(life, "closed")
	contracts, contractsBase, contractsOK := mlVenueLifetimeMaps(life, "contracts")
	_, totalOK := life["net"].(float64)
	_, closedOK := life["closed"].(float64)
	if !netOK || !closedCountOK || !contractsOK || !totalOK || !closedOK {
		return false
	}
	expected := currentMLFloat(life, "closed") - currentMLFloat(life, "closed_base")
	if expected < 0 || math.Abs(expected-math.Round(expected)) > 1e-6 {
		return false
	}

	cut := 0
	for i, raw := range closed {
		row, _ := raw.(map[string]any)
		if currentMLResetMarker(row) {
			cut = i + 1
		}
	}
	detailNet := map[string]float64{vbKalshi: 0, vbPolyus: 0}
	detailClosed := map[string]float64{vbKalshi: 0, vbPolyus: 0}
	detailContracts := map[string]float64{vbKalshi: 0, vbPolyus: 0}
	observed := 0
	for _, raw := range closed[cut:] {
		row, ok := raw.(map[string]any)
		if !ok || currentMLResetMarker(row) {
			continue
		}
		venue, _ := row["platform"].(string)
		venue = strings.ToLower(strings.TrimSpace(venue))
		pnl, pnlOK := row["pnl"].(float64)
		if (venue != vbKalshi && venue != vbPolyus) || !pnlOK || math.IsNaN(pnl) || math.IsInf(pnl, 0) {
			return false
		}
		detailNet[venue] = mlRoundMoney(detailNet[venue] + pnl)
		detailClosed[venue]++
		detailContracts[venue] += currentMLFloat(row, "contracts")
		observed++
	}
	if observed != int(math.Round(expected)) {
		return false
	}
	epochTotal := currentMLFloat(life, "net") - currentMLFloat(life, "net_base")
	detailTotal := detailNet[vbKalshi] + detailNet[vbPolyus]
	if math.Abs(detailTotal-epochTotal) > 0.015 {
		return false
	}
	needsRepair := false
	for _, venue := range []string{vbKalshi, vbPolyus} {
		if math.Abs((mlVenueMapFloat(net, venue)-mlVenueMapFloat(netBase, venue))-detailNet[venue]) > 0.015 ||
			math.Abs((mlVenueMapFloat(closedCount, venue)-mlVenueMapFloat(closedBase, venue))-detailClosed[venue]) > 1e-6 ||
			math.Abs((mlVenueMapFloat(contracts, venue)-mlVenueMapFloat(contractsBase, venue))-detailContracts[venue]) > 1e-6 {
			needsRepair = true
		}
	}
	if !needsRepair {
		return false
	}
	for _, venue := range []string{vbKalshi, vbPolyus} {
		net[venue] = mlRoundMoney(mlVenueMapFloat(netBase, venue) + detailNet[venue])
		closedCount[venue] = mlVenueMapFloat(closedBase, venue) + detailClosed[venue]
		contracts[venue] = mlVenueMapFloat(contractsBase, venue) + detailContracts[venue]
	}
	life["venue_accounting_version"] = 3.0
	return true
}

// mlEnsureVenueLifetime performs a one-time migration from the retained ring. Once present, the
// counters are incremented by both Go and Python settlers before ring trimming, so the venue split
// remains durable even when old detailed rows age out.
func mlEnsureVenueLifetime(life map[string]any, closed []any) {
	_, _, netOK := mlVenueLifetimeMaps(life, "net")
	_, _, closedOK := mlVenueLifetimeMaps(life, "closed")
	_, _, contractsOK := mlVenueLifetimeMaps(life, "contracts")
	if netOK && closedOK && contractsOK {
		mlRepairVenueLifetimeFromCompleteEpoch(life, closed)
		return
	}
	net, netBase, _ := mlVenueLifetimeMaps(life, "net")
	if !netOK {
		net = map[string]any{vbKalshi: 0.0, vbPolyus: 0.0}
		netBase = map[string]any{vbKalshi: 0.0, vbPolyus: 0.0}
	}
	closedCount := map[string]any{vbKalshi: 0.0, vbPolyus: 0.0}
	closedBase := map[string]any{vbKalshi: 0.0, vbPolyus: 0.0}
	contracts := map[string]any{vbKalshi: 0.0, vbPolyus: 0.0}
	contractsBase := map[string]any{vbKalshi: 0.0, vbPolyus: 0.0}
	for _, raw := range closed {
		row, _ := raw.(map[string]any)
		venue, _ := row["platform"].(string)
		venue = strings.ToLower(strings.TrimSpace(venue))
		if venue == vbKalshi || venue == vbPolyus {
			if !netOK {
				net[venue] = mlRoundMoney(mlVenueMapFloat(net, venue) + currentMLFloat(row, "pnl"))
			}
			closedCount[venue] = mlVenueMapFloat(closedCount, venue) + 1
			contracts[venue] = mlVenueMapFloat(contracts, venue) + currentMLFloat(row, "contracts")
		}
		if currentMLResetMarker(row) {
			if !netOK {
				netBase = map[string]any{vbKalshi: mlVenueMapFloat(net, vbKalshi), vbPolyus: mlVenueMapFloat(net, vbPolyus)}
			}
			closedBase = map[string]any{vbKalshi: mlVenueMapFloat(closedCount, vbKalshi), vbPolyus: mlVenueMapFloat(closedCount, vbPolyus)}
			contractsBase = map[string]any{vbKalshi: mlVenueMapFloat(contracts, vbKalshi), vbPolyus: mlVenueMapFloat(contracts, vbPolyus)}
		}
	}
	life["venue_net"], life["venue_net_base"] = net, netBase
	life["venue_closed"], life["venue_closed_base"] = closedCount, closedBase
	life["venue_contracts"], life["venue_contracts_base"] = contracts, contractsBase
	life["venue_accounting_version"] = 3.0
	mlRepairVenueLifetimeFromCompleteEpoch(life, closed)
}

// mlAddVenueClose records the same cent-booked result written to closed[]. Counts and contract
// totals live beside P&L so a 500-row diagnostic ring can never shrink funded sleeve accounting.
func mlAddVenueClose(life map[string]any, venue string, pnl, contracts float64) {
	venue = strings.ToLower(strings.TrimSpace(venue))
	if venue != vbKalshi && venue != vbPolyus {
		return
	}
	net := mlVenueMap(life, "venue_net")
	net[venue] = mlRoundMoney(mlVenueMapFloat(net, venue) + mlRoundMoney(pnl))
	closed := mlVenueMap(life, "venue_closed")
	closed[venue] = mlVenueMapFloat(closed, venue) + 1
	units := mlVenueMap(life, "venue_contracts")
	units[venue] = mlVenueMapFloat(units, venue) + contracts
}

func mlResetVenueLifetime(life map[string]any) {
	for _, key := range []string{"net", "closed", "contracts"} {
		current := mlVenueMap(life, "venue_"+key)
		life["venue_"+key+"_base"] = map[string]any{
			vbKalshi: mlVenueMapFloat(current, vbKalshi),
			vbPolyus: mlVenueMapFloat(current, vbPolyus),
		}
	}
}

func mlEpochVenueMetric(life map[string]any, key, venue string, fallback float64) float64 {
	current, base, ok := mlVenueLifetimeMaps(life, key)
	if !ok {
		return fallback
	}
	return mlVenueMapFloat(current, venue) - mlVenueMapFloat(base, venue)
}

// mlVenueSleeveView is the current-New-ML API/briefing truth. It recomputes fills and settlement
// from the filtered current epoch while preserving the Python router's still-resting reservations.
func mlVenueSleeveView(bank0 float64, life map[string]any, open, closed []any, existing any) map[string]any {
	// The current-v2 retained epoch is the strongest display receipt when all of its rows reconcile
	// to the lifetime total. This also repairs old durable splits that missed a settlement before
	// venue counters were introduced; incomplete retained history still falls back to durability.
	mlEnsureVenueLifetime(life, closed)
	grants := mlPaperVenueBanks(bank0)
	old, _ := existing.(map[string]any)
	out := map[string]any{}
	for _, venue := range []string{vbKalshi, vbPolyus} {
		fallbackNet, fallbackContracts, deployed, openN, fallbackClosedN := 0.0, 0.0, 0.0, 0, 0
		for _, raw := range closed {
			row, _ := raw.(map[string]any)
			platform, _ := row["platform"].(string)
			if strings.EqualFold(strings.TrimSpace(platform), venue) {
				fallbackNet += currentMLFloat(row, "pnl")
				fallbackContracts += currentMLFloat(row, "contracts")
				fallbackClosedN++
			}
		}
		for _, raw := range open {
			row, _ := raw.(map[string]any)
			platform, _ := row["platform"].(string)
			if strings.EqualFold(strings.TrimSpace(platform), venue) {
				deployed += currentMLFloat(row, "price")*currentMLFloat(row, "contracts") + currentMLFloat(row, "fee")
				openN++
			}
		}
		reserved := 0.0
		if oldRow, ok := old[venue].(map[string]any); ok {
			reserved = math.Max(0, currentMLFloat(oldRow, "reserved"))
		}
		net := mlEpochVenueMetric(life, "net", venue, fallbackNet)
		closedN := mlEpochVenueMetric(life, "closed", venue, float64(fallbackClosedN))
		contractN := mlEpochVenueMetric(life, "contracts", venue, fallbackContracts)
		grant := grants[venue]
		equity := math.Max(0, grant+net)
		// Independent compounding: only this sleeve's realized P&L moves its sizing balance.
		sizing := equity
		out[venue] = map[string]any{
			"grant": math.Round(grant*100) / 100, "net": math.Round(net*100) / 100,
			"equity": math.Round(equity*100) / 100, "sizing_balance": math.Round(sizing*100) / 100,
			"deployed": math.Round(deployed*100) / 100, "reserved": math.Round(reserved*100) / 100,
			"available": math.Round(math.Max(0, sizing-deployed-reserved)*100) / 100,
			"open":      float64(openN), "closed": closedN, "contracts": contractN,
		}
	}
	return out
}
