// Package config loads and validates runtime configuration.
package config

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"sort"
	"strings"
)

type Environment string

const (
	EnvDemo Environment = "demo"
	EnvProd Environment = "prod"
)

// KalshiConfig holds transport tuning for the Kalshi API client.
type KalshiConfig struct {
	RateLimitPerSec  float64 `json:"rate_limit_per_sec"`
	RequestTimeoutMs int     `json:"request_timeout_ms"`
	// R90 bug 134 (auditor edge 28): min interval between the ARMED stale-sweep's GET
	// /portfolio/orders polls. 0 → default 1000ms = 3.6k calls/h armed (the legacy 200ms tick
	// polled 18k/h against an endpoint whose rate cost Kalshi bumped in 2026). The WS 1¢-move
	// cancel path is unaffected (instant). Set ≤200 to restore the legacy per-tick behavior.
	OrdersPollMS float64 `json:"orders_poll_ms,omitempty"`
	// R72-B SUB-MARKETS → R73 FULL UNIVERSE: row budget for the WINDOWED markets pull (1000
	// rows/page). The 18-day close window held 14,218+ open rows on 2026-07-04 and the operator
	// directive is "every single thing that can be bet on needs to be included", so the default
	// now covers the WHOLE windowed universe with headroom: 0 = default 20000 (20 pages); clamped
	// in the client to [5000, 40000]. The cursor ends early when the universe is smaller, so the
	// real cost is ~1 request per 1000 OPEN rows per refresh (markets_refresh_s cadence).
	MarketsMaxPull int `json:"markets_max_pull,omitempty"`
	// R73: board refresh cadence in SECONDS (StartBoardRefresher tick + the client cache TTL rides
	// it at +0.5s). 0 = default 4; clamped to [2, 60]. Budget math at the defaults: ~15-20 windowed
	// pages / 4s ≈ 4-5 req/s — 25% of the 20/s Basic-tier budget (17% of 30/s Advanced), LESS than
	// R72-B's 71-request curated pull (~17.8 req/s), because the curated list is now only a
	// warm-boot seed. Slow this down before raising markets_max_pull past ~30k.
	MarketsRefreshS float64 `json:"markets_refresh_s,omitempty"`
}

// RiskConfig holds the safety limits.
//
// R70 (audit §c): the Phase-0 `trading_enabled` / `paper_mode` flags are REMOVED. They were read
// only for a status display — never consulted on any order path — so they LOOKED load-bearing but
// weren't (the audit's "safety flags that aren't"). The real controls are: the kill switch (halts
// everything), the persisted exec mode (paper vs live_sandbox vs live_prod routing), and session
// arming (liveArmed — real money never survives a restart). Stale keys in config.json are ignored.
type RiskConfig struct {
	MaxDailyLossUSD         float64 `json:"max_daily_loss_usd"`
	MaxPositionPerMarketUSD float64 `json:"max_position_per_market_usd"`
	MaxTotalExposureUSD     float64 `json:"max_total_exposure_usd,omitempty"` // 0 = no cap (removed from the UI; bankrolls bound exposure)
	MaxOrderSizeContracts   int     `json:"max_order_size_contracts"`
	// Real-money (live_prod) rails. Positive legacy dollar values remain optional HARDER caps;
	// -1 explicitly disables that fixed-dollar cap. R133 Adaptive Allocation Model uses the percentage rails below so
	// stake/exposure grow and shrink with the venue bankroll instead of choking at $2/order/$10 total.
	LiveMaxOrderUSD     float64 `json:"live_max_order_usd"`
	LiveMaxDailyLossUSD float64 `json:"live_max_daily_loss_usd"`
	LiveMaxOrderPct     float64 `json:"live_max_order_pct,omitempty"`    // max final order cost / that venue's ARM bankroll (default 5%)
	LiveExposureCapPct  float64 `json:"live_exposure_cap_pct,omitempty"` // max outstanding exposure / current intraday equity, combined + per venue (default 20%; Allocation may use less)
	LiveCryptoCapPct    float64 `json:"live_crypto_cap_pct,omitempty"`   // max aggregate crypto exposure / current intraday equity across enabled LIVE venues (default 10%)
	LiveDailyLossPct    float64 `json:"live_daily_loss_pct,omitempty"`   // realized daily-loss kill stop / total ARM bankroll (default 7.5%)
	LiveClusterCapPct   float64 `json:"live_cluster_cap_pct,omitempty"`  // one event/game/crypto cluster / venue ARM bankroll (default 3%)
	LiveKellyMaxFrac    float64 `json:"live_kelly_max_frac,omitempty"`   // live-only adaptive Kelly ceiling (default 0.25; hard-clamped to half-Kelly)
	// The activation/canary epoch is a deliberately explicit, operator-reviewed real-account
	// baseline. Unlike the UTC-day loss ledger it does not reset at midnight or on every ARM.
	// Turnover and fees are measured from immutable accepted-order receipts at/after the timestamp.
	LiveActivationAt          string  `json:"live_activation_at,omitempty"`
	LiveActivationBankrollUSD float64 `json:"live_activation_bankroll_usd,omitempty"`
	LiveActivationPeakUSD     float64 `json:"live_activation_peak_usd,omitempty"`
	// RiskEpoch* is the operator-reviewed restart point for the non-resetting loss stop. It is
	// deliberately separate from Activation*, which preserves full account history for reporting.
	LiveRiskEpochAt          string  `json:"live_risk_epoch_at,omitempty"`
	LiveRiskEpochBankrollUSD float64 `json:"live_risk_epoch_bankroll_usd,omitempty"`
	LiveRiskEpochPeakUSD     float64 `json:"live_risk_epoch_peak_usd,omitempty"`
	LiveRiskEpochDayPnLUSD   float64 `json:"live_risk_epoch_day_pnl_usd,omitempty"`
	LiveRiskEpochTurnoverUSD float64 `json:"live_risk_epoch_turnover_usd,omitempty"`
	LiveSessionLossPct       float64 `json:"live_session_loss_pct,omitempty"`
	LiveRollingLossPct       float64 `json:"live_rolling_loss_pct,omitempty"`
	// R147 prospective allocation is a deliberately separate authorization class from sealed
	// promotion. When explicitly enabled it may route Kalshi taker singles from a fresh
	// accepted Paper signal whose exact family+venue+side route has enough executable one-share
	// history and a positive fee-net confidence lower bound. Quantity uses the shared Adaptive
	// Allocation Model and every LIVE rail; it never promotes or seals the system.
	LiveProspectiveAllocation bool `json:"live_prospective_allocation,omitempty"`
	// Venue switches are an additional operator belt: the master switch above enables the
	// authorization class, while these decide which destination may consume it. They default OFF
	// so adding a technically complete venue route cannot silently put money behind it.
	LiveSystemKalshi bool `json:"live_system_kalshi,omitempty"`
	LiveSystemPolyUS bool `json:"live_system_polyus,omitempty"`
	// Additive multi-leg systems use a separate disabled-by-default staged-FOK belt. It cannot
	// widen or consume the six-identity singles canary; its own exact allowlist is mandatory.
	LiveStagedBundles bool `json:"live_staged_bundles,omitempty"`
	// Separate exact identities for staged additive bundles. This cannot consume or widen the
	// six-entry singles canary allowlist. Format remains venue|system|side|taker; no wildcards.
	LiveStagedBundleAllowlist string `json:"live_staged_bundle_allowlist,omitempty"`
	// Comma/newline-separated exact identities: venue|family|YES-or-NO|maker-or-taker.
	// Empty or invalid means no System may submit. Runtime accepts at most six identities.
	LiveSystemAllowlist string `json:"live_system_allowlist,omitempty"`
	// A deliberately smaller, experimental authority inside LiveSystemAllowlist. Runtime accepts
	// only the four operator-selected Kalshi taker lanes (Spot-lag YES/NO and raw Kalshi-flow
	// YES/NO). It may request one IOC contract when normal confidence proof does not pass; all
	// current signal, book, fee, account, conflict and risk checks still apply.
	LiveSystemCanaryAllowlist string `json:"live_system_canary_allowlist,omitempty"`
	// Research-only exact identities. This is deliberately separate from every LIVE/canary
	// authority field and can only schedule money-free execution-shadow observations.
	LiveSystemShadowAllowlist string `json:"live_system_shadow_allowlist,omitempty"`
	// New ML has its own per-venue switches because its evidence ledger and bankroll sleeves are
	// independent from System proof. These never bypass ARM, LIVE AUTO, sealed model validation,
	// exact route economics, or the venue handler's final book/account/risk checks.
	LiveNewMLKalshi                bool    `json:"live_new_ml_kalshi,omitempty"`
	LiveNewMLPolyUS                bool    `json:"live_new_ml_polyus,omitempty"`
	LiveAllocationMinMarkets       int     `json:"live_allocation_min_markets,omitempty"`        // distinct settled venue+ticker contracts (default 120; operator floor 60)
	LiveAllocationMinEventClusters int     `json:"live_allocation_min_event_clusters,omitempty"` // compatibility/diagnostic only; never LIVE authority
	LiveAllocationMinEdge          float64 `json:"live_allocation_min_edge,omitempty"`           // fee-net confidence lower bound per contract (default 0.005)
	// R105: LiveOrderUSD (live_order_usd, the R23 fixed $/bet knob) is REMOVED — operator, repeated
	// ask: live stakes come from the SAME engine as paper (per-venue wallets, livebank.go), with
	// ONLY the caps/rails below on top. A live_order_usd key in an old config.json is ignored.
	// A positive combined dollar ceiling is a legacy optional tightening cap; the Adaptive Allocation Model's percentage
	// ceiling is the active default and is not silently replaced by a $10 fallback.
	LiveExposureCapUSD float64 `json:"live_exposure_cap_usd,omitempty"`
	// Live-proposal gates, promoted from hardcoded consts in liveProposals: the min 24h-volume
	// floor (USD) and the acceptable price band in CENTS (applies to model AND live prices on
	// both venues). Zero/unset ⇒ the historical defaults ($20k, 2–98¢), so an existing
	// config.json without these keys behaves exactly as before.
	LiveMinVol24hUSD float64 `json:"live_min_vol_24h_usd,omitempty"`
	LivePxBandMinC   float64 `json:"live_px_band_min_c,omitempty"`
	LivePxBandMaxC   float64 `json:"live_px_band_max_c,omitempty"`
	// R103 PER-VENUE LIVE BANKROLLS (operator: "money cannot move between venues"). Each venue's
	// live sizing runs the SAME engine as the paper books but on ITS OWN bankroll:
	//   kalshi_live_bankroll: 0/unset = VENUE TRUTH (cash balance + deployed exposure, refreshed
	//     live; at arm time with nothing deployed this is exactly the account balance). >0 pins it.
	//   polyus_live_bankroll: the venue has NO balance API (client gap, documented) — 0/unset =
	//     UNKNOWN (the private WS balance supplies venue truth when healthy); R105: an unknown
	//     bankroll REFUSES to size (no flat fallback). >0 pins the bankroll. Never guessed.
	// Per-venue outstanding-exposure ceilings (0 = only the combined live_exposure_cap_usd
	// applies, which ALWAYS still applies on top — the R103 split can only tighten, never loosen).
	KalshiLiveBankroll float64 `json:"kalshi_live_bankroll,omitempty"`
	PolyusLiveBankroll float64 `json:"polyus_live_bankroll,omitempty"`
	LiveKalshiCapUSD   float64 `json:"live_kalshi_cap_usd,omitempty"`
	LivePolyusCapUSD   float64 `json:"live_polyus_cap_usd,omitempty"`
}

// BriefingConfig controls the app-generated market briefing, written to a folder the
// forwarder watches. It replaces the old external scheduled task so the briefing
// always uses the app's fresh, future-closing market data (never a stale snapshot).
type BriefingConfig struct {
	Enabled      bool   `json:"enabled"`
	Dir          string `json:"dir"`
	EveryMinutes int    `json:"every_minutes"`
}

// BackupConfig controls cross-PC portability. The suite writes a CONSISTENT snapshot of the SQLite
// history DB (via VACUUM INTO — safe while the suite is running) plus the ML state into Dir, which you
// point a cloud sync (e.g. Google Drive Desktop) at. On a fresh machine where the live data/ DB is
// missing but a synced snapshot exists, the suite auto-restores it on startup — so "clone + run" on a
// second PC comes up with all history/ML intact. The 718MB+ live DB is far too big for git; this is
// the supported way to carry the data. (Code, config.json, and docs travel via git as usual.)
type BackupConfig struct {
	Enabled      bool   `json:"enabled"`
	Dir          string `json:"dir"`           // snapshot folder — point Google Drive at this (default "ml/drive-sync", i.e. kalshi-suite/ml/drive-sync)
	EveryMinutes int    `json:"every_minutes"` // periodic snapshot cadence (0 = only on graceful shutdown / manual `backup`)
	OnShutdown   bool   `json:"on_shutdown"`   // also write a fresh snapshot on graceful shutdown (so switching PCs always carries the latest)
	AutoRestore  bool   `json:"auto_restore"`  // on startup, if the live DB is missing/empty but a snapshot exists, restore it (the magic that makes a fresh PC "just work")
}

// AutoConfig controls the PAPER auto-pilot (no real orders — ever). When On, a monitor
// auto-books paper buys on cross-platform arb gaps and strong one-sided Kalshi flow,
// respecting the risk limits. Default off; toggle live from the dashboard.
type AutoConfig struct {
	Enabled bool `json:"enabled"`
	// R70 (audit §c): the dead `gate_only` toggle is REMOVED — it had zero reads; Mode is the real
	// executor selector. A stale key in config.json is ignored by the JSON decoder.
	Mode      string  `json:"mode"` // autobet mode: ""/"gate"/"ai" (all remapped to "ai" in server.New) = autobet only SUGGESTS and the gate executes (approve/deny/edit); "autobet" = autobet executes its own bets directly (no gate). "off" is handled by the Enabled toggle. Default "ai".
	StakeUSD  float64 `json:"stake_usd"`
	ArbMinGap float64 `json:"arb_min_gap_pct"`
	// R100 (auditor r26 §3a + DO-THIS 7): TPCents (`tp_cents`) is REMOVED — the global fixed-¢
	// take-profit had been inert since 06-29 (live mode "auto" never read it) and the measured
	// exit study says a fixed profit-taker COSTS EV (74/74 auto-tp closes were settled winners
	// anyway; a 95¢ TP ≈ −0.3¢/ct). Ride-to-settlement is the endorsed default; the stop-loss
	// below stays exactly as-is (+1.7¢/ct keeper). A stale tp_cents key in config.json is ignored
	// by the JSON decoder (same convention as the R70 gate_only removal above). Per-order TPs on
	// gate/manual orders are unaffected (they never read this).
	SLCents        int     `json:"sl_cents"`
	SLTPRatio      float64 `json:"sltp_ratio"`       // SAFETYNET: when >0, auto bets use RATIO TP/SL instead of fixed ¢ — TP=p+r·(1−p), SL=p−r·p (r=this). Scales the stop/target to the price (right for sports/longer holds). 0 = use the fixed sl_cents (tp removed R100). AUTO-TUNED from post-entry paths when SLTPAuto is on.
	SLTPAuto       bool    `json:"sltp_auto"`        // SAFETYNET self-tuning: when on, the suite periodically re-learns SLTPRatio from REALIZED post-entry price paths (BREADCRUMB) — replays candidate ratios vs ride-to-resolve and applies the best (incl. 0=ride). Needs ≥50 resolved+filled signals with a post-path.
	SLTPMode       string  `json:"sltp_mode"`        // SL/TP MODE selector (STOPMODES). "ride" = no stops, ride to resolution. "offset" = SL a fixed ¢ DELTA below entry (sl_cents; the tp arm is removed R100 — gate per-order TPs still honor their own values). "abs" = SL at an ABSOLUTE price level (sl_cents read as the level). "ratio" = scaled to price: TP=p+r(1−p), SL=p−r·p with fixed r=sltp_ratio. "auto" = ratio, but r is self-tuned live from realized paths (SLTPAuto). Empty defaults to "auto".
	KellyFrac      float64 `json:"kelly_frac"`       // KELLYSIZE: 0 = flat % sizing (stake_pct). >0 = fractional-Kelly sizing — stake = bankroll × (edge/(1−price)) × this. ¼ (0.25) is the safe default; full Kelly (1.0) risks ruin. Bigger edge → bigger bet.
	KellyEdge      string  `json:"kelly_edge"`       // KELLYSIZE edge source: "ml" (the sidecar's calibrated win-prob − price), "realized" (the signal type's measured net/contract), "hybrid" (the more CONSERVATIVE of the two — guards the ML's known optimism). Empty defaults to "hybrid".
	AutoRetire     bool    `json:"auto_retire"`      // R79 PLACEMENT POLICY arm (operator: "all signals ON for auto, or retired only if BOTH direct AND inverted EV fail after maker fees"): when on, autoPlace SKIPS a family the 30-min policy marks retired — i.e. BOTH the direct and the inverted maker-net EV/ct 95% lower bounds are ≤0 at n≥30 resolved rows. Everything else places (inverted-only-positive places FLIPPED). The signal STILL logs + backtests and can earn its way back. The two-tier goal: trade winners, backtest everyone.
	MaxExposureUSD float64 `json:"max_exposure_usd"` // arb+signal combined budget (gate gets the rest)

	// Consensus copy-bets — three independent strategies, all gated by ConsensusEnabled:
	//   Kalshi  : follow one-sided aggressive Kalshi flow (the whale-consensus panel).
	//   Poly    : follow the Polymarket leaderboard when high-ranked, profitable traders
	//             cluster on a side AND it's a big slice of their bankroll (concentration).
	//   Cross   : follow Kalshi+Poly agreement — same event/side priced as a shared favorite
	//             on BOTH platforms (the strongest, two-venue confirmation).
	ConsensusEnabled       bool    `json:"consensus_enabled"`
	ConsensusKalshi        bool    `json:"consensus_kalshi"`
	ConsensusKalshiFlow    bool    `json:"consensus_kalshi_flow"` // R71 (QUANT_STUDY rec #2): "kflow" — the CLEAN flow-only family split out of the retired blended "kalshi" source (kalshi-flow feed only, whale prints excluded, crypto 15M excluded). Default TRUE = LOG the family (signal_log rows accrue evidence per the promotion ladder). Placement is NOT wired yet — when kflow earns paper placement, this flag becomes its placement gate and logging goes unconditional per the standing rule (retire = stop placement only).
	ConsensusPoly          bool    `json:"consensus_poly"`
	ConsensusCross         bool    `json:"consensus_cross"`
	ConsensusArb           bool    `json:"consensus_arb"`             // cross-platform two-leg ARB. R79: ON by default (operator: "all signals ON for auto") — the placement policy auto-retires it from evidence if both direct+inverted maker-net LBs fail; the old hardcoded OFF (only net-losing strategy, legs rarely both fill) is now the policy's call.
	ConsensusPolyFlow      bool    `json:"consensus_poly_flow"`       // poly: bet strong one-sided aggressive BUY flow on Poly's OWN live trades, counted only from RANKED/profitable leaderboard whales (rank+P&L gated, same MaxRank/MinTraderPnL bars) — crypto up/down + other near-term
	ConsensusBasket        bool    `json:"consensus_basket"`          // BASKET: log a signal when ≥ConsensusBasketMin distinct QUALITY-basket wallets (consistent, below-consensus winners from poly_trader_trades) buy the same side. LOG-ONLY (no autobet) — the Edge finder proves it before we ever bet it.
	ConsensusBasketMin     int     `json:"consensus_basket_min"`      // BASKET: min distinct basket wallets agreeing on a side to fire
	ConsensusPolyUSFlow    bool    `json:"consensus_polyus_flow"`     // Poly US: bet the dominant aggressor side on live/soon game moneylines from Poly US's OWN executed taker tape (WS), book-health gated. Paper on the polyus venue.
	ConsensusPolyUSMinFlow float64 `json:"consensus_polyus_min_flow"` // Poly US: min recent taker $ on a market before its one-sided flow is tradeable
	ConsensusMaxRank       int     `json:"consensus_max_rank"`        // poly: group must include a top-N trader (rank cap). R82: lb-api serves only the top 50/window (limit caps at 50, offset ignored) — 50 = "any ranked trader qualifies"; values >50 are unreachable slack
	ConsensusMinTraders    int     `json:"consensus_min_traders"`     // poly: require ≥ this many agreeing traders
	ConsensusMinConc       float64 `json:"consensus_min_conc"`        // poly: require group $ ÷ their portfolios ≥ this
	ConsensusMinTraderPnL  float64 `json:"consensus_min_trader_pnl"`  // poly: each scanned trader must have ≥ this all-time profit ($)
	// R70 (audit §c P3): the deprecated consensus_max_days_out / consensus_crypto_max_days_out
	// fields are REMOVED (0 reads since the hours fields replaced them; stale JSON keys are ignored).
	ConsensusMaxHoursOut       float64 `json:"consensus_max_hours_out"`        // LIVE-only auto gate: never AUTO-BET a market resolving further out than this many HOURS (live games end within a few h). Panels still SHOW them — this only blocks auto orders.
	ConsensusCryptoMaxHoursOut float64 `json:"consensus_crypto_max_hours_out"` // crypto-only, TIGHTER hours cap for BTC/ETH windows
	ConsensusExitLeewayCents   int     `json:"consensus_exit_leeway_cents"`    // auto-sell leeway: only close cross/poly-flow positions once price drops THIS many ¢ below entry (so a 1¢ wiggle doesn't trigger)
	HoldToSettle               bool    `json:"consensus_hold_to_settle"`       // EXITLEAK: ride auto positions to SETTLEMENT / take-profit instead of the EV-negative early exits (auto-sell-exit + auto-sl). Export 20: early exits net −$8.7k while settlement+TP make the money. true = disable auto-sell-exit + auto-sl (keep auto-tp + max-hours backstop + settlement). Default true.
	ConsensusMaxEntrySlipCents float64 `json:"consensus_max_entry_slip_cents"` // STALENESS GUARD: skip an auto/AI fill if the live ask has moved more than this many ¢ ABOVE the signal price (market ran away → don't chase a stale price). 0 disables.
	MakerFirst                 bool    `json:"maker_first"`                    // R77 LEGACY MIRROR — derived from FeeMakerShare (share>0) at boot + every settings save; no engine path reads it directly anymore (server.makerIntent()). Kept for config round-trips/old clients. History: post-only/maker entries → maker fee (Kalshi 25% of taker; Polymarket $0); OFF = cross the spread (taker).
	HonestFills                bool    `json:"honest_fills"`                   // HONEST-FILL HARNESS (audit Q5/Q6): maker-first PAPER entries on kalshi/polyus REST as pending orders and only fill when the touch trades THROUGH the posted price (fill-rate + adverse selection logged to maker_fill_stats). OFF = legacy instant bid-fills (the fantasy that manufactured ~half the pcrypto edge). Default ON.
	MLDrivenDirectional        bool    `json:"ml_driven_directional"`          // ONE PORTFOLIO (operator directive): directional auto-cons-* sources LOG ONLY (the ML keeps learning from them); the ML executor ("auto-ml") is the single directional bettor in the main paper book, sized by calibrated p_win through the same gates/caps/honest-fill pipeline. AUTO keeps only what the ML can't express: two-leg arb locks, combos, gate/manual. Default ON.
	SharplineEnabled           bool    `json:"sharpline_enabled"`              // SHARP ANCHOR (audit Q7-C1, log-only): de-vigged Pinnacle h2h fair values vs Kalshi moneylines → "sharpline" signals. Needs odds_api_key.
	OddsAPIKey                 string  `json:"odds_api_key"`                   // The Odds API key (the-odds-api.com; ~$30–99/mo for this cadence). Empty = sharpline inert.
	SharplineMinutes           float64 `json:"sharpline_minutes"`              // minutes between sharpline sweeps (default 30; each sweep = one request per tracked league)
	FeeMakerShare              float64 `json:"fee_maker_share"`                // R77: THE fee-mode key (Settings toggle: Taker=0 · Maker=1 · Hybrid=measured share). Fraction of maker-intent fills that actually rest as MAKER; effective fee = share·makerFee + (1−share)·takerFee. Drives paper fees, ML books, backtests + the sidecar mirror; maker_first derives from it.
	ConsensusCrypto            bool    `json:"consensus_crypto"`               // Kalshi crypto (kcrypto): bet Kalshi's OWN 15M mid-priced favorite, cross-confirmed by Poly (rebuilt from the old flow-chasing xcrypto)
	ConsensusXMatch            bool    `json:"consensus_xmatch"`               // crypto CROSS-MATCH (xmatch): use Poly's 15m signal (proven predictor) to bet the same side on Kalshi when Kalshi underprices it (cross-venue divergence)
	InvertXmatch               bool    `json:"invert_xmatch"`                  // per-signal INVERT for the crypto cross-match
	ConsensusKThresh           bool    `json:"consensus_kthresh"`              // Kalshi THRESHOLD ladder (kthresh): bet the KX{COIN}D strike whose YES is out of line with its neighbors (the market's own implied distribution)
	ConsensusFavLong           bool    `json:"consensus_favlong"`              // FAVLONG (A2): structural favorite-longshot bias — bet the FAVORITE side (price 0.60–0.92) of liquid non-crypto markets, but ONLY when the ML confirms it's underpriced (EV ≥ min_ev_per_contract). All categories.
	InvertKthresh              bool    `json:"invert_kthresh"`                 // per-signal INVERT for the Kalshi threshold-ladder signal
	ConsensusPMatch            bool    `json:"consensus_pmatch"`               // CONSENSUS BRIDGE (pmatch): Poly smart-money consensus at 5–10% conviction (export 7: ~86% win) → bet the SAME event on KALSHI (the legal, tradeable expression of the untradeable Poly-int edge). Title/date-matched to its Kalshi twin.
	InvertPMatch               bool    `json:"invert_pmatch"`                  // per-signal INVERT for the consensus bridge
	ConsensusBridgeMinConc     float64 `json:"consensus_bridge_min_conc"`      // bridge: only bet when Poly consensus conviction (group $ ÷ their portfolios) is ≥ this — below ~5% it's a coinflip (export 7)
	ConsensusBridgeMaxConc     float64 `json:"consensus_bridge_max_conc"`      // bridge: …and ≤ this — above ~10% it collapses (lone-whale YOLO), so cap it
	BridgeMinGapCents          float64 `json:"bridge_min_gap_cents"`           // pmatch GAP GATE (R26): only bridge when the tradeable twin is ≥ this many ¢ CHEAPER than Poly-int's live price for the SAME side. If the twin already priced the conviction there's no unpriced information left — parity-bridging is exactly what made pbridge ~zero-gross (53.5% win @ 53.3¢). Same principle as xmatch's 4¢ and pbridge's hardcoded 3¢. 0 = off.
	BridgeMaxAgeMin            float64 `json:"bridge_max_age_min"`             // pmatch FRESHNESS GATE (R26): refuse to bridge when the Poly-int conviction snapshot (polyRowsAt) is older than this many minutes — a stalled scrape means we'd bridge conviction the market may have already resolved. 0 = off.
	ConsensusConfluence        bool    `json:"consensus_confluence"`           // CONFLUENCE: when ≥2 of {Kalshi price, Poly-int price, Poly US price} favor the SAME side, bet that side on the CHEAPEST TRADEABLE venue (Kalshi or Poly US; Poly-int is info-only), sized up by how many platforms agree
	InvertConfluence           bool    `json:"invert_confluence"`              // per-signal INVERT for the confluence engine
	ConsensusXVLag             bool    `json:"consensus_xvlag"`                // XVLAG (A3): cross-venue LAG. When Kalshi & Poly US price the SAME outcome and one venue's price just MOVED (the leader) while the other hasn't caught up (the laggard, still cheaper), bet the laggard's side to converge to the leader. EV-gated. Generalizes the crypto cross-match to ALL categories (sports/politics/etc). Runs inside the confluence matched-set (needs consensus_confluence on).
	XVLagLeadCents             float64 `json:"xvlag_lead_cents"`               // XVLAG: the leader venue must have moved ≥ this many ¢ toward the side since the last tick to count as leading (default 3)
	XVLagMinGapCents           float64 `json:"xvlag_min_gap_cents"`            // XVLAG: …and the laggard must still be ≥ this many ¢ cheaper than the leader (room to converge) (default 3)
	XvgapMaxEpisodes           int     `json:"xvgap_max_episodes"`             // R126 Part 2: max lots per market for the xvgap book under episode re-entry (0 ⇒ 3)
	XvgapEpisodeReopenC        float64 `json:"xvgap_episode_reopen_c"`         // R126 Part 2: a closed gap (≤1¢) must REOPEN ≥ this many ¢ to count as a NEW episode (0 ⇒ xvlag_min_gap_cents)
	XvgapComboOverlay          bool    `json:"xvgap_combo_overlay"`            // R126 Part 4.3: paper combo-overlay on Kalshi gap candidates with a live comboable partner (tag combo_overlay; PAPER ledger only; default true)
	ROIBandRankArm             bool    `json:"roi_band_rank_arm"`              // R126 Part 5: ARM the ROI-per-dollar band nudge in candidate ranking (default false — the weight is computed + logged only)
	EVDayRankExp               float64 `json:"ev_day_rank_exp,omitempty"`      // R129 C4 (operator: MED-HIGH): EV-per-capital-day RANKING MULTIPLIER exponent — candidates sort by evNet × (6h / horizon)^exp, so quicker-resolving markets rank higher at equal EV (capital turns over faster = compounding). 0 = off (bit-identical ranking). Med-high = 0.65 (1h beats 6h ~3.2× at equal EV). This is a ranking preference, NOT a gate — distinct from the (still-off) ev_capital_day_gate.
	ConsensusMeanRev           bool    `json:"consensus_meanrev"`              // MEANREV (A4): LOG-ONLY discovery — flag a side that just SPIKED to a price extreme on a sharp move (a momentum OVERSHOOT that tends to mean-revert). NOT auto-bet (the fade leg is sub-35¢ = blocked by the longshot filter, and unproven) — logged so the Edge finder proves it before we ever trade it.
	MeanRevExtremeCents        float64 `json:"meanrev_extreme_cents"`          // MEANREV: "extreme" = price ≤ this many ¢ (low) or ≥ 100−this (high). Default 8 (≤8¢ / ≥92¢).
	MeanRevMoveCents           float64 `json:"meanrev_move_cents"`             // MEANREV: …reached via a move ≥ this many ¢ since the last tick (a sharp spike, not slow drift). Default 5.
	LiqMaxSpreadCents          float64 `json:"liq_max_spread_cents"`           // LIQGATE (A5): skip an AUTO bet on a Kalshi market whose top-of-book spread is wider than this many ¢ — a wide book = slippage that eats the edge. 0 = off. Default 10 (only blocks genuinely illiquid books).
	LiqMinDepth                float64 `json:"liq_min_depth"`                  // LIQGATE: …and/or require ≥ this much top-of-book depth (contracts) on Kalshi. 0 = off (depth units vary; spread is the primary gate).
	AutoInvertOnLoss           bool    `json:"auto_invert_on_loss"`            // AUTO-INVERT: when a signal category's SESSION win rate drops below the low band, flip its future bets to the opposite side until it recovers to the high band
	AutoInvertMinTrades        int     `json:"auto_invert_min_trades"`         // min closed trades in a category this session before auto-invert can flip it (noise filter; default 8)
	AutoInvertBanded           bool    `json:"auto_invert_banded"`             // R76 BANDED AUTO-INVERT (bandinvert.go): arm the PLACEMENT flip for (family, price band) pairs whose measured EV is CI-negative and whose inverted EV is CI-positive at n≥max(auto_invert_min_trades,30). Default false = LOG-ONLY (evaluation + audit + inverted_band badges run regardless).
	// R81 (operator decision): the min_confidence* knobs and their autoPlace gate are REMOVED —
	// the measured confidence→WR relationship is NON-MONOTONIC (weather's best band was LOW
	// confidence), so a floor on it filters the wrong bets. `confidence` survives as a logged
	// signal column (ML feature + Edge-finder bins); the EV gate below is the profitability gate.
	// Stale min_confidence keys in config.json surface as unknown-key WARNs and are ignored.
	MinEntryPrice            float64 `json:"min_entry_price"`     // LONGSHOT FILTER: skip any auto bet whose entry price (on the side we'd buy) is below this. Edge finder: sub-35¢ entries win 11–22% across every signal = pure bleed. 0 = off. ~0.35 recommended.
	MinEVPerContract         float64 `json:"min_ev_per_contract"` // EVGATE: when the ML model has scored a market, skip auto bets whose fee-adjusted ML edge (p_win − price − per-contract fee) is below this. THE profitability gate (R81: the old min_confidence backstop is deleted). 0 = off. ~0.01 (+1¢) recommended.
	ExitScratchEV            float64 `json:"exit_scratch_ev"`     // EXITS E9: dynamic-EV scratch — close an open (non-crypto) auto position when the ML now re-scores it to (p_win − cur) < −this. Cuts positions the model no longer believes in. 0 = off. ~0.06 default.
	ExitTrailCents           float64 `json:"exit_trail_cents"`    // EXITS E10: trailing take-profit — once a (non-crypto) position is ≥+10¢ in profit, close if it retraces this many cents from its peak mark. Locks gains on swingy sports. 0 = off.
	ConsensusPCrypto         bool    `json:"consensus_pcrypto"`   // POLY crypto: bet Polymarket's OWN BTC/ETH 15m up/down (cross-confirmed by Kalshi, or Poly-solo on a strong lean)
	InvertPcrypto            bool    `json:"invert_pcrypto"`      // per-signal INVERT for poly crypto
	InvertCrypto             bool    `json:"invert_crypto"`       // per-signal INVERT: take the opposite side of this signal's bets
	InvertKalshi             bool    `json:"invert_kalshi"`
	InvertPoly               bool    `json:"invert_poly"`
	InvertCross              bool    `json:"invert_cross"`
	InvertPflow              bool    `json:"invert_pflow"`
	InvertPusflow            bool    `json:"invert_pusflow"`
	InvertArb                bool    `json:"invert_arb"`
	InvertKflow              bool    `json:"invert_kflow,omitempty"`      // R79: kflow is placement-eligible now — per-signal manual INVERT like every family (the policy's evidence-driven invert works on top)
	ConsensusFlowStrength    float64 `json:"consensus_flow_strength"`     // kalshi: min one-sided flow strength (0..1)
	ConsensusKalshiNeedsPoly bool    `json:"consensus_kalshi_needs_poly"` // kalshi: only fire when Poly agrees on the same side (cross-confirm)
	ConsensusKalshiAgreePct  float64 `json:"consensus_kalshi_agree_pct"`  // kalshi: Poly must give the flow's side ≥ this % to count as agreement
	ConsensusCrossMaxGapPct  float64 `json:"consensus_cross_max_gap_pct"` // cross: both platforms within this = agreement
	ConsensusCrossMinPct     float64 `json:"consensus_cross_min_pct"`     // cross: require the side ≥ this on both (favorite)

	// Position sizing. When StakePct > 0, each auto bet is that fraction of the venue's paper
	// bankroll (BankrollKalshiUSD / BankrollPolyIntUSD / BankrollPolyUSUSD, compounded by realized
	// net — see platformBankroll). StakeUSD is the fallback when StakePct is 0.
	StakePct         float64 `json:"stake_pct"`
	MinStakeUsd      float64 `json:"min_stake_usd"`      // Phase 2b: floor per bet so we stop placing tiny fee-heavy bets (sub-$5 bled ~5.8% on Kalshi vs ~3.5% at $15+). Capped at 25% of the venue bankroll so a small bankroll can't be over-concentrated.
	DrawdownFloorPct float64 `json:"drawdown_floor_pct"` // COMPOUND floor: sizing equity is never taken below this fraction of the base bankroll after paper losses (default 0.10 = 10%). Set to 1.0 to size off the FULL bankroll always (bets never shrink on a losing run; also disables the drawdown breaker). 0 = use the 0.10 default.
	// VARIABLE SHARE SELLING (auto scale-out): sell slices of a winning AUTO position as it runs,
	// banking profit instead of all-or-nothing at TP. Crypto 15m is excluded (rides to settlement).
	ScaleOutEnabled   bool    `json:"scaleout_enabled"`    // turn the auto scale-out ladder on
	ScaleOutGainCents float64 `json:"scaleout_gain_cents"` // sell another slice every this-many ¢ of gain above entry (default 10)
	ScaleOutFraction  float64 `json:"scaleout_fraction"`   // fraction of the contracts still held to sell at each step (0..1, default 0.25)
	ScaleOutMaxSteps  int     `json:"scaleout_max_steps"`  // cap on how many slices to sell (default 3)
	ScaleOutFreeRoll  bool    `json:"scaleout_freeroll"`   // once up enough, auto-sell exactly enough to RECOVER the full cost basis → the remainder rides at zero net cost (can't lose)
	// PARLAYS / COMBOS (paper, per-platform, own bankroll). The engine bundles the ML model's top +EV
	// single-market picks into multi-leg all-or-nothing combos (same venue only). With AUTO on it places
	// the best combo per platform itself, drawing from the canonical BookCombosUSD portfolio.
	ParlayEnabled               bool    `json:"parlay_enabled"`                   // build + (with AUTO on) auto-place parlays
	ParlayBankrollUSD           float64 `json:"parlay_bankroll_usd"`              // legacy compatibility field; funded combos use BookCombosUSD exclusively
	ParlayStakeUSD              float64 `json:"parlay_stake_usd"`                 // $ staked per auto-placed parlay (default 20)
	ParlayMaxLegs               int     `json:"parlay_max_legs"`                  // configurable 2-6; 0 is normalized by the server to the hard six-leg ceiling
	ParlayMinLegPWin            float64 `json:"parlay_min_leg_pwin"`              // min calibrated p_win per leg (default 0.55 — audit Q1#10 turned the hard-coded hit-rate filter into a knob)
	ParlayMaxHoursOut           float64 `json:"parlay_max_hours_out"`             // final LIVE combo LEG horizon (default/hard max 4h; crypto remains capped at 2h). Unknown or elapsed clocks fail closed.
	PaperComboMaxHoursOut       float64 `json:"paper_combo_max_hours_out"`        // funded Paper Combo regular-leg collection window (default/hard max 24h)
	PaperComboCryptoMaxHoursOut float64 `json:"paper_combo_crypto_max_hours_out"` // funded Paper Combo crypto-leg collection window (default/hard max 6h)
	PaperMLMaxHoursOut          float64 `json:"paper_ml_max_hours_out"`           // book-native-v2 Paper singles regular collection window (default/hard max 24h)
	PaperMLCryptoMaxHoursOut    float64 `json:"paper_ml_crypto_max_hours_out"`    // book-native-v2 Paper singles crypto collection window (default/hard max 6h)
	// R109 PARLAY LAB (log-only research collector — parlaylab.go). Entirely separate from the
	// archived paper parlay book above: it never bets; it logs would-be combos and grades them.
	ParlayLabEnabled     bool    `json:"parlay_lab_enabled"`       // enumerate + grade would-be combos (default true — log-only)
	ParlayLabMinEVNet    float64 `json:"parlay_lab_min_ev_net"`    // per-leg net-EV floor ($/contract, default 0.005) — EV is the verdict, never a win-rate gate
	ParlayLabMaxPerCycle int     `json:"parlay_lab_max_per_cycle"` // cap on new candidates per lab tick (default 400 since R122)
	// Combo Lab shares the project-wide hard six-leg ceiling with Paper and LIVE routes. The
	// branch-and-bound enumerator's configured ceiling (0 ⇒ 6) and the COMBO-level fee-net EV/$1
	// floor a combo must clear to be logged (default 0 = any +EV after fees; negative admits
	// study space below breakeven). The floor is the ONLY cut — never sampling.
	ParlayLabMaxLegs    int     `json:"parlay_lab_max_legs,omitempty"`
	ParlayLabComboFloor float64 `json:"parlay_lab_combo_floor,omitempty"`
	// R111/R112 correlated-exposure cluster cap: max OPEN stake ($) a paper book may hold across
	// all CORRELATED-DISTINCT variants of one underlying. R112 operator order: exposure is
	// INFORMATION, not a brake — 0/absent = OFF (unlimited; the metric stays visible). Set a
	// positive dollar value to re-arm the brake. (API-verified identical-payoff twins are
	// hard-blocked separately, any size — twins.go.)
	ClusterExposureCap float64 `json:"cluster_exposure_cap"`
	// R70 (audit §c): the legacy `bankroll_usd` fallback is REMOVED — it had zero reads (the
	// per-venue bankrolls below are what sizing actually uses); its comments falsely claimed it fed
	// StakePct sizing. A stale key in config.json is ignored.
	// R78 HISTORY/API COMPATIBILITY. These fields still preserve the old NAV epoch and wire shape,
	// but Paper money is the five fixed Book*USD portfolios below; these fractions no longer
	// divide those portfolio banks. Keep them decodable so old config files and reports survive.
	PaperTotalStart float64 `json:"paper_total_start"`
	AllocKalshi     float64 `json:"alloc_kalshi"`
	AllocPolyus     float64 `json:"alloc_polyus"`
	AllocML         float64 `json:"alloc_ml"`
	// R103 RAWFLOW BOOK (signals-vs-ML experiment, epoch-3): allocation fraction for the RawFlow
	// paper book — kalshi-flow signals ONLY, fixed algorithmic rules (10–50¢ band, family flow bar,
	// Kelly off the family's REALIZED edge), NO ML gate/haircut/borders by design. 0/unset = book
	// OFF and the remaining fractions normalize exactly as before (back-compat). Its net joins NAV;
	// its bank seeds at alloc_rawflow × paper_total_start on each reset and compounds on its own.
	AllocRawFlow float64 `json:"alloc_rawflow,omitempty"`
	// R106 WEATHER BOOK (operator ask): allocation fraction for the Weather paper book — wxedge
	// signals ONLY (NWS-anchored Kalshi temp ladders), tunable 20–40¢ default band, shared Kelly
	// engine on the book's own equity with the BOOK'S OWN realized edge (no ML, no other family's
	// stats). 0/unset = book OFF and the remaining fractions normalize exactly as before. Its net
	// joins NAV; its bank seeds at alloc_weather × paper_total_start on each reset.
	AllocWeather float64 `json:"alloc_weather,omitempty"`
	// R106: the Weather book's entry band, CENTS (0/unset ⇒ the 20/40 defaults — the auditor's
	// edge-24 endorsement). Config-adjustable per the operator's spec.
	WeatherBandLoC float64 `json:"weather_band_lo_c,omitempty"`
	WeatherBandHiC float64 `json:"weather_band_hi_c,omitempty"`
	// R106 RFQ WOULD-QUOTE SIMULATOR (edge 36, log-only): the margin we'd quote combos at —
	// max(floor, per_leg × legs) cents off leg-fair, both sides; size capped by max_usd. 0/unset ⇒
	// the conservative defaults (3¢ floor / 1.5¢ per leg / $5 — the live combo path's numbers).
	RFQSimMarginC       float64 `json:"rfq_sim_margin_c,omitempty"`
	RFQSimMarginPerLegC float64 `json:"rfq_sim_margin_per_leg_c,omitempty"`
	RFQSimMaxUSD        float64 `json:"rfq_sim_max_usd,omitempty"`
	// R78 EV-SHARE AUTO-ALLOCATION: families with no significant positive EV lower bound still get
	// this fraction of an equal-split stake (exploration — data keeps accruing). 0/unset = 0.10.
	ExploreStakeFrac float64 `json:"explore_stake_frac,omitempty"`
	// Deprecated flat venue values kept for backwards compatibility. The operator-facing R133 money
	// settings are BookKalshiUSD/BookPolyusUSD; Poly-int alone keeps this research-only notional.
	BankrollKalshiUSD  float64 `json:"bankroll_kalshi_usd"`
	BankrollPolyIntUSD float64 `json:"bankroll_polyint_usd"`
	BankrollPolyUSUSD  float64 `json:"bankroll_polyus_usd"`
	ResetOnStart       bool    `json:"reset_on_start"` // R30 (operator): at every suite start, run Reset P&L FIRST (close all paper/ML positions at marks, zero P&L + graphs) before any trading loop starts. Header switch next to the kill switch. Default ON.

	// Kalshi-flow soft-bonus + anti-churn. SoloStrength: a flow signal with NO Poly confirm must
	// clear this higher one-sided bar to bet alone (confirmed ones use ConsensusFlowStrength and get
	// 2x size). MaxOpenPositions caps concurrent auto positions so the bot can't churn into hundreds
	// of thin-edge bets and bleed fees (0 = unlimited).
	ConsensusKalshiSoloStrength float64 `json:"consensus_kalshi_solo_strength"`
	MaxOpenPositions            int     `json:"max_open_positions"`

	// R72-B KFLOW TWIN BOOKS (quant follow-up to R71's kflow family): two Go-side PAPER
	// mini-books, sidecar-independent — "kflow_pre" ($500) takes every kflow signal logged
	// PRE-GAME (is_live=0), "kflow_live" ($500) takes the IN-PLAY ones (is_live=1; unknown −1
	// is skipped). $10 flat per signal, maker-fee model, settled by the normal settlement
	// sweep off signal_log resolutions. Pure experiment books: never real money, never the
	// main paper bankrolls. Default ON (they only accrue while kflow logging is on).
	KflowBooksEnabled bool `json:"kflow_books_enabled"`
	// R98 (operator restructure): FREEZE the kflow twins — no NEW placements while true; existing
	// lots settle out on the normal sweep, history/stats stay visible, header lines render while
	// lots remain then drop off flat ("frozen"). The R72-B experiment concluded; flip back to
	// resume it. Distinct from KflowBooksEnabled=false, which also stops display/settlement paths.
	KflowBooksFrozen bool `json:"kflow_books_frozen,omitempty"`
	// R98 (operator decision: "remove shadow — ML auto-trains on all bets even ones not taken"):
	// RETIRE the shadow control book — the sidecar stops opening NEW shadow lots (flag rides
	// data/ml_alloc.json), existing lots settle naturally, historical stats stay readable, the
	// header marks the line "(retired)" while lots remain then drops it, and Reset P&L leaves the
	// retired book un-rebased (its record freezes instead of zeroing). All code paths kept —
	// set false to re-enable. Zero-value false = shadow ACTIVE (back-compat for old configs).
	ShadowBookRetired bool `json:"shadow_book_retired,omitempty"`

	// R72-B MODEL-EV HAIRCUT (QUANT_STUDY rec #10): realized ≈ 0.70× model-predicted EV
	// (ML book ratio 0.696; OOS pred 4.87¢ vs realized 3.13¢ on gated picks). When set in
	// (0,1), the ML edge consumed by the EV gates + Kelly sizing is scaled by this factor.
	// Default 0 (= 1.0, OFF) because the study conditions the change on live-fill
	// confirmation and the live sample is still too small — flip to ~0.7 once it confirms.
	ModelEVHaircut float64 `json:"model_ev_haircut,omitempty"`

	// R79 (operator: REMOVE the model-implausibility guard): master flag for the "p_win ≥ 3× a
	// sub-10¢ live price → skip" rule — the ML sidecar's buy loops (real book + the shadow book's
	// identical copy) and the live-proposal near-expiry guard part (c) all gate on it. Default
	// FALSE = guard OFF per the operator's instruction; one Settings click (true) restores it.
	// History the guard encodes: the ML book once bought $25 of a FINISHED game at 1¢ on garbage
	// p_win (R44), and the quant study measured sub-10¢ edges as ~2× optimistic. The 5–95¢
	// live-price sanity BAND is a separate data-integrity guard and stays unconditional.
	MLImplausibilityGuard bool `json:"ml_implausibility_guard"`

	// R84 ML p_win FLOOR (operator-confirmed, REVERSING the R83 "no standalone floor" note): the ML
	// paper book's high-EV longshots were cooking the bankroll — book autopsy 2026-07-05: picks with
	// p_win<0.50 were 55% of ML book losses and realized a 14% win rate vs the 35% their prices
	// implied. The sidecar's REAL-book buy loop and the live-proposals ML-pick funnel both refuse any
	// pick whose model win probability is below this floor (rejections logged "p_win below floor").
	// The SHADOW book stays ungated by design — it is the all-gross control. Default 0.50 (set in
	// Default(), so an absent key means 0.50); an EXPLICIT 0 disables the floor. The sidecar re-reads
	// config.json every cycle (same handshake as ml_implausibility_guard) — no sidecar restart needed.
	MLMinPWin float64 `json:"ml_min_p_win"`

	// R90 ML BORDERS (operator ask, dovetails auditor DO-THIS 5): a sanity band + EV window on
	// every ML-scored pick, applied at the common gates AFTER the per-family realization haircut.
	//   ml_pwin_min / ml_pwin_max — model probabilities outside [min,max] are implausible →
	//     REJECT (the existing "≥3× sub-10¢" implausibility rule stays independently on top).
	//   ml_ev_min_cents — net-of-fees EV floor at the LIVE price, cents/contract.
	//   ml_ev_max_cents — ABOVE this ceiling the pick is too good to be true → QUARANTINE: logged
	//     to the audit trail (category "mlquarantine") and the sidecar's ml_quarantine.jsonl for
	//     review; never bet. Defaults (set in Default()): 0.05 / 0.95 / 2 / 20. An explicit 0
	//     disables that single border. The sidecar re-reads config.json every cycle — no restart.
	MLPWinMin    float64 `json:"ml_pwin_min"`
	MLPWinMax    float64 `json:"ml_pwin_max"`
	MLEVMinCents float64 `json:"ml_ev_min_cents"`
	MLEVMaxCents float64 `json:"ml_ev_max_cents"`
	// R90 REALIZATION HAIRCUT (auditor DO-THIS 5i / edge 29): scale gate-input EV by the
	// per-family realization ratio (actual/predicted from settled ML-book lots, recomputed with
	// the 30-min alloc sweep, shrunk toward 1 at low n, floor 0, cap 1). true = applied at the
	// gates; false = compute + stamp only (log-only validation mode). Measured at r22:
	// kalshi-flow realizes .10 of predicted, kcrypto .38, pbridge NEGATIVE — fantasy-EV lots.
	RealizationHaircut bool `json:"realization_haircut"`
	// R90 ADVERSE-POSTING GUARD (auditor DO-THIS 5iii / edge 30): skip paper maker posts when
	// the book shows ZERO depth or ~5m momentum runs against the side (mfs.adverse_5m measured
	// −1.6…−3.2¢ on filled posts — winner's-cursed passive fills). Every post row is stamped
	// would_gate 0/1 regardless, so the guard's counterfactual stays measurable when it's off.
	// R105 (operator option c): the guard now DIVERTS unsafe posts to the taker path (take or
	// skip per existing gates) instead of skipping outright, and its thresholds are tunable:
	MakerAdverseGuard bool `json:"maker_adverse_guard"`
	// MakerMinDepth: minimum visible touch depth (contracts/shares) to allow a resting post —
	// 0/unset ⇒ default 10. mfs rows log the observed depth, so tune this from real distributions.
	MakerMinDepth float64 `json:"maker_min_depth,omitempty"`
	// MakerMomGateC: ~5m momentum (¢) against the posted side that blocks a post — 0/unset ⇒ the
	// R101 default 1.0¢.
	MakerMomGateC float64 `json:"maker_mom_gate_c,omitempty"`
	// MLMakerMinPxC (R114 maker autopsy — auditor r45 DO-THIS 8, band leg): minimum ML-book maker
	// post price in ¢. E3 evidence: settled sub-50¢ ml-book maker fills won only 12.5–25% (near
	// pure adverse selection; 71% of fills saw adverse 5m drift). 0/unset ⇒ default 50; <0 ⇒ off.
	MLMakerMinPxC float64 `json:"ml_maker_min_px_c,omitempty"`
	// R122 QUEUE-AWARE ROUTING (router.go — operator: "taker usually, maker when queue allows"):
	// at placement, expected-time-to-fill = queue-ahead ÷ tape trade-through rate, compared to the
	// strategy's edge horizon. RouteQueueEnabled false ⇒ legacy maker-first behavior (router
	// returns maker:router-off). RouteHorizonFrac: unmeasured families' horizon = time-to-resolve
	// × this (0 ⇒ 0.25). RouteETAMargin: maker only when ETA ≤ margin × horizon (0 ⇒ 1.0).
	RouteQueueEnabled bool    `json:"route_queue_enabled"`
	RouteHorizonFrac  float64 `json:"route_horizon_frac,omitempty"`
	RouteETAMargin    float64 `json:"route_eta_margin,omitempty"`
	// MLMakerCalibGateECE (R122 — auditor r56 decision item 6 + bug 407; MONEY POLICY, flagged):
	// ML-book maker posting is PAUSED (router diverts to taker) while the sidecar's ece_gated
	// sensor (10-bin ECE on OOS rows passing the live gates — the 407 sensor) is ABSENT or above
	// this threshold. 0 ⇒ default 0.025 (the ML-CALIB-TRANSFER bar); <0 ⇒ gate off (maker resumes
	// unconditionally). Post-boot maker fill-EV was CI-negative ×2 (−15.22¢ n=50) at ship time.
	MLMakerCalibGateECE float64 `json:"ml_maker_calib_gate_ece,omitempty"`
	// MLDecayHalfLifeDays (R114): exponential recency weighting half-life for sidecar training
	// (w = 0.5^(age_days/hl)); the sidecar hot-reads this each cycle. 0/unset ⇒ sidecar default 14
	// (data-picked: hl=14 OOS-AUC .7769 vs unweighted .7750 on the 07-08 copy).
	MLDecayHalfLifeDays float64 `json:"ml_decay_half_life_days,omitempty"`
	// GoEvalEnabled (R123 Part 1): the Go-side tick re-scorer — evaluates the sidecar's exported
	// model (data/ml_model_export.json, parity-gated 1e-6 on load) on every WS price tick so a
	// held lot's p_win refreshes in microseconds instead of the sidecar's ~6-min cycle. False ⇒
	// every consumer falls back to the file-based p_win exactly as pre-R123.
	GoEvalEnabled bool `json:"go_eval_enabled"`
	// GoEvalDriftWarn (R123): alarm threshold on the MEDIAN |go-eval − sidecar| SAME-VECTOR
	// drift (the model_drift health metric — ≈0 when export/version are in sync; growth means
	// version skew). 0 ⇒ 0.02. Trips a WARN audit + the go_eval /api/ready component.
	GoEvalDriftWarn float64 `json:"go_eval_drift_warn,omitempty"`
	// R107 (operator): EVERY paper book (ML / RawFlow / Weather — the auto books already did)
	// places via the maker sim: post at entryBid, fill only on ≥1¢ touch-through, R106 cancel
	// parity, option-c divert to taker. False = the pre-R107 instant-booking behavior.
	MakerSimBooks bool `json:"maker_sim_books"`
	// PolyusMakerBlocked — the Part-1 venue-honesty exception: TRUE forces PolyUS paper
	// placements onto the taker path tagged px_src=taker_venue_rule. The R107 live retest
	// PROVED retail maker orders rest at the venue, so this defaults FALSE; flip it only if
	// the venue changes its participation rules (paper must predict live, not flatter it).
	PolyusMakerBlocked bool `json:"polyus_maker_blocked,omitempty"`
	// R107 latency-health thresholds (Part 5). Zero ⇒ defaults (2000ms REST · 120s WS age ·
	// 500ms DB p95). A metric must breach for 3 consecutive 30s samples to go RED + warn once.
	LatWarnRESTMs  float64 `json:"latency_warn_rest_ms,omitempty"`
	LatWarnWSAgeS  float64 `json:"latency_warn_ws_age_s,omitempty"`
	LatWarnDBP95Ms float64 `json:"latency_warn_db_p95_ms,omitempty"`
	// R117 AUTO-PROMOTION PIPELINE (promotion.go) — PAPER ONLY, FOREVER: the pipeline only ever
	// spawns/funds PAPER experiment books from a paper reserve; it never touches live_* config or
	// any real-money path (LIVE arming stays operator-only). Zero/unset values fall back to the
	// code defaults noted per key (promoCfg in promotion.go).
	PromotionEnabled          *bool   `json:"promotion_enabled,omitempty"`            // nil = ON (default true)
	PromotionBankrollUSD      float64 `json:"promotion_bankroll_usd,omitempty"`       // seed per promoted paper book (default 100)
	PromotionMaxBooks         int     `json:"promotion_max_books,omitempty"`          // max concurrently-promoted books (default 4)
	PromotionMinRows          int     `json:"promotion_min_rows,omitempty"`           // DEPRECATED R118 (parsed, ignored): eligibility is the PROVEN+ verdict alone
	PromotionMinDays          float64 `json:"promotion_min_days,omitempty"`           // DEPRECATED R118 (parsed, ignored): eligibility is the PROVEN+ verdict alone
	PromotionGrowCapUSD       float64 `json:"promotion_grow_cap_usd,omitempty"`       // per-book allocation ceiling across doublings (default 800). R127: this caps a SUB-STRATEGY's share within its venue book now
	PromotionReserveUSD       float64 `json:"promotion_reserve_usd,omitempty"`        // DEPRECATED R127 (parsed, journal-only): promotion no longer moves reserve money — a promoted strategy is a sub-strategy share of its venue book
	PromotionDrawdownPausePct float64 `json:"promotion_drawdown_pause_pct,omitempty"` // no new promotions/grows past this portfolio drawdown % (default 20)
	// R133 FOUR-PORTFOLIO BANKS (operator order): paper money consolidates into exactly FOUR
	// portfolios — Kalshi $600 · PolyUS $600 · Combos $600 · ML $600. Every old per-strategy book
	// folds into its venue book as a TAGGED SUB-STRATEGY (its ledger file, tags and verdict-engine
	// grading are unchanged; only the MONEY consolidates: placement checks venue-book available =
	// bank − Σ open exposure of ALL subs in that book). paper_total_start + alloc_* stay parseable
	// for history but are SUPERSEDED for money — equityalloc now produces these fixed banks
	// (venueBudgetUSD / writeMLAllocFile read the book_* keys). 0/unset ⇒ the operator defaults.
	BookKalshiUSD   float64 `json:"book_kalshi_usd,omitempty"`    // Kalshi paper portfolio bank (default 600): shared-auto-kalshi + kflow-pre + kflow-live + weather + freshinv
	BookPolyusUSD   float64 `json:"book_polyus_usd,omitempty"`    // PolyUS paper portfolio bank (default 600): shared-auto-polyus + xvgap
	BookCombosUSD   float64 `json:"book_combos_usd,omitempty"`    // Combos paper portfolio bank (default 600): positive-system conjunctions
	BookMLUSD       float64 `json:"book_ml_usd,omitempty"`        // ML singles bank (default 600), handed to the sidecar via the ml_alloc.json handshake
	BookMLCombosUSD float64 `json:"book_ml_combos_usd,omitempty"` // New-ML Combo Paper bank (default 600): independently compounded book-native ML conjunctions only
	// R127 POLY-INT SPORTS ANCHORING (server/pintsports.go — the parked R125 recall gap).
	// PintSportsAnchor: nil/absent = ON (the budgeted gamma-metadata sweep that joins poly-int
	// sports markets to canonical games is log/DB only — zero-risk); explicit false stops it.
	// PintSportsConsume: default FALSE — consumers (the log-only xvlock scanner, anything reading
	// venue='polyint' market_game joins) see NOTHING until this is armed; flip it only after the
	// 30-sample hand-verification of /api/xvpairs pint_sports samples passes. Poly-int stays
	// venue-locked for betting regardless — matching is for gap/lock SCANNING only.
	PintSportsAnchor  *bool `json:"pint_sports_anchor,omitempty"`
	PintSportsConsume bool  `json:"pint_sports_consume,omitempty"`
	// These money-policy settings produce audit previews only. Both are off by default.
	// ev_capital_day_gate checks expected value against capital and holding time.
	// cluster_kelly_arm checks half-Kelly sizing against a per-cluster bankroll cap.
	EVCapitalDayGate bool    `json:"ev_capital_day_gate,omitempty"`
	EVCapitalDayMin  float64 `json:"ev_capital_day_min,omitempty"`
	ClusterKellyArm  bool    `json:"cluster_kelly_arm,omitempty"`
	ClusterCapPct    float64 `json:"cluster_cap_pct,omitempty"`
	// R128 FAVLONG80 (fix-list 4): the >80¢-favorites maker-preferred Kalshi book (B1 study:
	// +5.4¢/ct maker at n=10,072). nil/absent = ON (paper book on the scoreboard-weight share);
	// explicit false stops NEW lots (settlement continues).
	Favlong80Enabled *bool `json:"favlong80_enabled,omitempty"`
	// R129 CHEAPBAND: the cheap-price-band paper book (R126 band study: Kalshi 0-10¢ +3.75¢/ct,
	// PolyUS <15¢ ≈1.0 ROI/$ — WITHIN measured-positive families only; raw cheap loses). nil/absent
	// = ON; explicit false stops NEW lots (settlement continues).
	CheapbandEnabled *bool `json:"cheapband_enabled,omitempty"`
	// R129 LOCKSTACK: paper-stake the depth-verified slice of the xvlock scanner's K↔PUS locks
	// (margin ≥2¢, both legs ≥1 contract at the ask — 67 settled locks, +4.2¢ mean, 0 mismatches).
	// nil/absent = ON; explicit false stops NEW stakes (grading continues).
	LockStackEnabled *bool `json:"lockstack_enabled,omitempty"`
	// R130 GENERIC FOLLOWER: the automatic-roster signal follower (genfollow.go) — every
	// measured-positive, venue-eligible scoreboard family without a dedicated executor gets its
	// detections placed as paper lots. nil/absent = ON; explicit false stops NEW lots
	// (settlement continues).
	GenfollowEnabled *bool `json:"genfollow_enabled,omitempty"`
	// R154: wait this many milliseconds after a generic-follower Paper taker decision, then re-read
	// the executable book as an IOC at the original limit. 0/unset = 750ms; runtime clamps 25-5000.
	// This affects Paper only and runs after the LIVE-first signal handoff.
	PaperTakerDelayMS int `json:"paper_taker_delay_ms,omitempty"`
}

// PintSportsAnchorOn reports whether the R127 poly-int sports anchor sweep runs (nil = default
// TRUE — anchoring/logging is zero-risk; only an explicit false stops it).
func (a AutoConfig) PintSportsAnchorOn() bool {
	return a.PintSportsAnchor == nil || *a.PintSportsAnchor
}

type Config struct {
	Environment Environment    `json:"environment"`
	ServerAddr  string         `json:"server_addr"`
	DataDir     string         `json:"data_dir"`
	LogLevel    string         `json:"log_level"`
	OpenBrowser bool           `json:"open_browser"`
	Kalshi      KalshiConfig   `json:"kalshi"`
	Risk        RiskConfig     `json:"risk"`
	Briefing    BriefingConfig `json:"briefing"`
	Backup      BackupConfig   `json:"backup"`
	Auto        AutoConfig     `json:"auto"`
	// R74 LIVE BOOKS: how many Kalshi markets the orderbook_delta WS may track at once — the dynamic
	// working set (held live positions + resting orders + proposal candidates + combo legs, then
	// top-24h-volume fill), NEVER the whole 14k universe. 0 = default 600; clamped in the server to
	// [50, 1000]. Self-imposed: the venue's AsyncAPI documents no per-connection market cap, but a
	// bounded set keeps the post-gap/post-failover resnapshot storm cheap.
	KalshiBookWSCap int `json:"kalshi_book_ws_cap,omitempty"`
	// R133 PolyUS full-depth tier. MARKET_DATA_LITE BBO + TRADE still cover the complete proven-open
	// board; this controls only the expensive priority MARKET_DATA prefix. Default 600, clamped by
	// the socket/planner to [50, 1000]. Applied at process boot so both warm-standby sockets agree.
	PolyUSBookWSCap int `json:"polyus_book_ws_cap,omitempty"`
	// R65 TELEGRAM (operator: "let's do telegram"): when BOTH are set, every ntfy-published message
	// (the briefing-dir .md drops, incl. the SHADOW section) is ALSO posted to this Telegram chat via
	// the Bot API. Either empty = Telegram off; ntfy behavior is unchanged in all cases. No defaults —
	// paste the token (@BotFather) + chat id from Settings (/api/settings) or config.json.
	TelegramBotToken string `json:"telegram_bot_token,omitempty"`
	TelegramChatID   string `json:"telegram_chat_id,omitempty"`
	// R82 MESSENGER MODE (operator: "telegram only"): which messenger channel(s) carry the briefings.
	//   "telegram" (default) — Telegram's own 2-min loop is the sole messenger; the briefing/parlay/
	//                          ML-book loops stop writing ntfy .md drops (so a running forwarder has
	//                          nothing to send), and build-suite.bat no longer builds/starts the
	//                          forwarder. Briefing TEXT generation is untouched.
	//   "ntfy"               — legacy: .md drops on, Telegram sends suppressed (TgSend no-ops).
	//   "both"               — .md drops AND Telegram sends (the pre-R82 behavior).
	// Unknown/empty values normalize to "telegram" (server.MessengerMode).
	MessengerMode string `json:"messenger_mode,omitempty"`
	// R83: when a 2-min Telegram briefing tick has nothing to send, still send a minimal "suite up"
	// heartbeat line at most every 30 minutes — a silent phone then MEANS the suite is down, never
	// "the loop was quietly skipping" (the 2026-07-05 failure mode). nil/absent = DEFAULT TRUE
	// (server.TelegramHeartbeatOn); set false to silence. Togglable live from Settings.
	TelegramHeartbeat *bool `json:"telegram_heartbeat,omitempty"`
	// Optional file-based credentials. Paths are empty by default so a checkout never carries a
	// machine-specific credential location.
	// KalshiKeyFile: path to the Kalshi RSA private key (PEM with BEGIN/END markers; an optional
	// leading "key_id:"/"keyid=" line inside the file is honored). When the file EXISTS it is the
	// credential source and WINS over the encrypted store + KALSHI_SUITE_PASSPHRASE; when absent
	// the store path is unchanged. KalshiKeyID: the Kalshi API key UUID — REQUIRED alongside a
	// PEM-only key file (no embedded key_id line); without it auth boots RED with the fix text.
	// PolyUSKeyFile: path to the Polymarket US secret (the base64 one-liner from the developer
	// portal); when it exists it WINS over POLY_US_SECRET(_FILE) env. PolyUSKeyID (R88): the
	// PolyUS key UUID — config wins over the POLY_US_KEY_ID env var, so a headless boot never
	// depends on process-env inheritance again (2026-07-05 red state: the User-scope env var
	// existed but the launching parent's env predated the setx, so the suite saw nothing).
	// All four are Settings-wired text fields, consumed at BOOT (restart to
	// apply). Clearing a path to "" disables the file source explicitly (defaults only apply when
	// the key is ABSENT from config.json). Key MATERIAL is never logged — only a sha256 prefix;
	// key IDs are non-secret labels (the suite already logs them at boot).
	KalshiKeyFile string `json:"kalshi_key_file"`
	KalshiKeyID   string `json:"kalshi_key_id"`
	PolyUSKeyFile string `json:"polyus_key_file"`
	PolyUSKeyID   string `json:"polyus_key_id"`
}

// Default returns a safe baseline configuration.
func Default() Config {
	return Config{
		Environment:     EnvDemo,
		ServerAddr:      "127.0.0.1:8787",
		DataDir:         "./data",
		LogLevel:        "info",
		OpenBrowser:     true,
		PolyUSBookWSCap: 600,
		Kalshi:          KalshiConfig{RateLimitPerSec: 20, RequestTimeoutMs: 8000, OrdersPollMS: 1000}, // 20/s = Basic-tier cap (max before 429s); set 30 in config.json if on Advanced. orders_poll_ms: R90 bug 134
		Risk: RiskConfig{
			MaxDailyLossUSD:         50,
			MaxPositionPerMarketUSD: 25,
			MaxTotalExposureUSD:     0, // 0 = no total-exposure cap (deleted from config/UI; bankrolls bound it)
			MaxOrderSizeContracts:   0, // no flat contract cap; bankroll/edge/depth/exposure rails size the order
			// R133 Adaptive Allocation Model: proportional real-money rails; fixed-dollar fields are explicitly disabled.
			LiveMaxOrderUSD:     -1,
			LiveMaxDailyLossUSD: -1,
			LiveMaxOrderPct:     0.05,
			LiveExposureCapPct:  0.20,
			LiveCryptoCapPct:    0.10,
			LiveDailyLossPct:    0.075,
			LiveClusterCapPct:   0.03,
			LiveKellyMaxFrac:    0.25,
			LiveSessionLossPct:  0.075,
			LiveRollingLossPct:  0.075,
			// Fail closed by default. A production operator must deliberately enable prospective
			// allocation after reviewing the currently qualified exact route cells.
			LiveProspectiveAllocation:      false,
			LiveSystemKalshi:               false,
			LiveSystemPolyUS:               false,
			LiveStagedBundles:              false,
			LiveStagedBundleAllowlist:      "",
			LiveSystemAllowlist:            "",
			LiveSystemCanaryAllowlist:      "",
			LiveNewMLKalshi:                false,
			LiveNewMLPolyUS:                false,
			LiveAllocationMinMarkets:       120,
			LiveAllocationMinEventClusters: 30,
			LiveAllocationMinEdge:          0.005,
		},
		Briefing: BriefingConfig{Enabled: false, EveryMinutes: 10},
		Backup:   BackupConfig{Enabled: true, Dir: "ml/drive-sync", EveryMinutes: 360, OnShutdown: true, AutoRestore: true},
		// R82 (operator: "telegram only"): Telegram is the sole messenger by default — the 2-min
		// Telegram loop sends every briefing; ntfy .md drops (and the forwarder) are off unless
		// messenger_mode is set to "ntfy"/"both".
		MessengerMode: "telegram",
		// Credential paths and key IDs are account-specific and must be configured locally.
		KalshiKeyFile: "",
		PolyUSKeyFile: "",
		Auto: AutoConfig{
			// R76 (auditor bug 19, the "Default is 10× the live tuning" trap): the money knobs below
			// are aligned to the OPERATED live file's conservatism — kelly_frac 0.025 (was 0.25 = 10×),
			// stake_usd 10 (was 25), max_exposure_usd 75 (was 2,500 = 33×). One lost/typo'd key must
			// never silently re-arm an aggressive default. Tune UP deliberately in config.json.
			Enabled: false, Mode: "ai", StakeUSD: 10, ArbMinGap: 8, SLCents: 0, SLTPRatio: 0, SLTPAuto: true, SLTPMode: "auto", KellyFrac: 0.025, KellyEdge: "hybrid", AutoRetire: true, MaxExposureUSD: 75,
			// R79 (operator: "all signals ON for auto"): every family placement-enabled by default —
			// arb + favlong join the trues below; the 30-min placement policy (not a hardcoded default)
			// auto-retires whatever fails BOTH the direct and inverted maker-net LB bars at n≥30.
			ConsensusEnabled: true, ConsensusKalshi: true, ConsensusKalshiFlow: true, ConsensusPoly: true, ConsensusCross: true, ConsensusPolyFlow: true, ConsensusPolyUSFlow: true, // ConsensusKalshiFlow (R71→R79): kflow logs unconditionally; this flag now gates its PLACEMENT (autoTick 3a-KFLOW)
			ConsensusArb: true, ConsensusFavLong: true,
			ConsensusBasket: true, ConsensusBasketMin: 3, // BASKET log-only signal: ≥3 quality-basket wallets agreeing
			// Poly leaderboard pool — R82 RANK-DEPTH PROBE (2026-07-05): lb-api.polymarket.com/profit
			// now serves EXACTLY the top 50 per window (limit>50 silently caps at 50 rows; offset is
			// IGNORED — every page returns the same head — on 1d/7d/30d/all alike). Max visible rank
			// is therefore 50, so MaxRank=50 covers EVERYTHING the API exposes (it degrades to a pure
			// "must include a ranked trader" bar; the old 500 was unreachable). EACH trader must still
			// have ≥$100k all-time profit — a proven track record, not a one-week fluke. Then 2+ of
			// them must agree, with their combined $ on the side being ≥4% of those traders' combined
			// total portfolio value (the concentration gate). AND the auto-pilot won't BET anything
			// resolving more than ~48h out — catches today's + tomorrow's games, but no week+ futures /
			// year-end politics. Crypto stays tighter (1 day, intraday). AUTO-WIDE: blocks every auto
			// strategy (consensus, arb, cross, gate) via the autoPlace gate. Panels still SHOW these.
			ConsensusMaxRank: 50, ConsensusMinTraders: 3, ConsensusMinConc: 0, ConsensusMinTraderPnL: 100000, // MinConc=0: NO concentration gate for auto (UI still shows conv%); 3-trader agreement is the bar
			ConsensusMaxHoursOut: 4, ConsensusCryptoMaxHoursOut: 2, ConsensusExitLeewayCents: 12, ConsensusMaxEntrySlipCents: 3, // LIVE/Paper hard ceilings: regular 4h, crypto 2h; skip fills >3¢ off the signal
			HoldToSettle:        true,                                                                        // EXITLEAK: ride to settlement/TP by default (early exits net −$8.7k in export 20)
			MakerFirst:          true,                                                                        // default ON: minimize fees with post-only/maker entries across all 3 venues
			HonestFills:         true,                                                                        // default ON (audit Q5/Q6): paper maker entries fill only on touch-through — no more fantasy bid-fills
			MLDrivenDirectional: true,                                                                        // ONE PORTFOLIO (operator directive): ML places the directional bets; auto keeps arb/combos
			FeeMakerShare:       1.0,                                                                         // R67h MIGRATION NOTE (operator: "we are genuinely doing maker only" — the WS 1¢-cancel discipline): default flipped 0.5 → 1.0 so paper fees, replay/backtests and the ML fee mirror all price MAKER. Live config.json instances carrying the old taker default (0) were migrated to 1 in this round; set it back below 1 only if the execution style changes.
			ConsensusCrypto:     true, ConsensusPCrypto: true, ConsensusXMatch: true, ConsensusKThresh: true, // kcrypto + pcrypto + cross-match + threshold-ladder on by default
			ConsensusPMatch: true, ConsensusBridgeMinConc: 0.05, ConsensusBridgeMaxConc: 0.10, // consensus BRIDGE: Poly 5–10% conviction → bet the Kalshi twin (the tradeable edge)
			BridgeMinGapCents: 3, BridgeMaxAgeMin: 10, // R26 pmatch gates: twin must be ≥3¢ cheaper than Poly's same-side price + conviction snapshot ≤10 min old
			ConsensusConfluence: true,                                         // CONFLUENCE: ≥2 platforms agree on a side → bet the cheapest tradeable venue, sized by agreement
			ConsensusXVLag:      true, XVLagLeadCents: 3, XVLagMinGapCents: 3, // XVLAG: cross-venue lag (leader moved ≥3¢, laggard ≥3¢ cheaper)
			XvgapMaxEpisodes: 3, XvgapComboOverlay: true, // R126: episode re-entry cap ≤3 lots/market; paper combo-overlay on (reopen threshold 0 ⇒ min-gap; ROI band rank stays log-only)
			ConsensusMeanRev: true, MeanRevExtremeCents: 8, MeanRevMoveCents: 5, // MEANREV: log-only overshoot discovery (≤8¢/≥92¢ on a ≥5¢ spike)
			LiqMaxSpreadCents: 10, LiqMinDepth: 0, // LIQGATE: skip Kalshi auto bets when the book spread is wider than 10¢ (slippage bleed); depth gate off
			ScaleOutEnabled: false, ScaleOutGainCents: 10, ScaleOutFraction: 0.25, ScaleOutMaxSteps: 3, ScaleOutFreeRoll: false, // SCALEOUT: auto scale-out ladder off by default; sensible ladder ready when toggled on
			ParlayEnabled: false, ParlayBankrollUSD: 500, ParlayStakeUSD: 20, ParlayMaxLegs: 6,
			ParlayMaxHoursOut: 4, PaperComboMaxHoursOut: 24, PaperComboCryptoMaxHoursOut: 6,
			PaperMLMaxHoursOut: 24, PaperMLCryptoMaxHoursOut: 6,
			ParlayLabMaxLegs: 6,
			ParlayLabEnabled: true, ParlayLabMinEVNet: 0.005, ParlayLabMaxPerCycle: 25000, // R128 operator order: no cap — the acceptability floor is the only FILTER (was 400 since R122); 25k/cycle bounds tick cost, the dedup ladder streams full coverage across cycles, nothing is evicted ClusterExposureCap: 0, // R112 operator order: cluster exposure is a METRIC, not a brake — cap defaults OFF (<=0 = unlimited); set a positive dollar value to re-arm // R109 PARLAY LAB: log-only research (no betting) — operator's data-first parlay revival // PARLAYS: ARCHIVED R101 (auditor r28 rotation-f verdict: combo-level −$10.90/combo CI-negative even pre-fee, no positive subset, PolyUS 0/37 — dead end). OFF by default: no suggestions, no auto-places, no briefing section; history stays readable (dashboard Combos tab + /api/parlay), open combos still settle via MonitorParlays — the shadow-retire pattern.

			// Kalshi flow now trades on its OWN signal (NeedsPoly=false): the raw flow backtests ~59%
			// and the hard Poly-confirm was filtering out every Kalshi-only market (most sports/crypto).
			// Crypto still cross-confirms against Poly's 15m up/down when that market exists.
			ConsensusFlowStrength: 0.85, ConsensusKalshiNeedsPoly: false, ConsensusKalshiAgreePct: 55,
			ConsensusCrossMaxGapPct: 4, ConsensusCrossMinPct: 62, ConsensusPolyUSMinFlow: 500,
			// R76 (bug 19): 0.5% of the venue's paper bankroll per position (was 3% = 6× the live
			// tuning), with the paper gates ON by default — min_ev/min_entry were ABSENT here
			// (0 = gates OFF), so a fresh boot ran gate-less at 10× Kelly. Values = the live
			// file's: EV floor +3¢, 40¢ longshot floor. (R81: min_confidence deleted — see struct.)
			StakePct: 0.005, MinStakeUsd: 10, DrawdownFloorPct: 0.10,
			MinEVPerContract: 0.03, MinEntryPrice: 0.4,
			AutoInvertMinTrades: 8, // min closed trades before the EV/fee-based auto-invert may change state (noise filter)
			// R78 historical NAV/API defaults. R133 does not use these fractions to divide paper money;
			// explore_stake_frac still feeds the scoreboard allocator's evidence-collection floor.
			PaperTotalStart: 1000, AllocKalshi: 0.25, AllocPolyus: 0.25, AllocML: 0.5, ExploreStakeFrac: 0.10,
			// Five fixed $600 paper portfolios — Kalshi/PolyUS/System Combos/ML singles/New-ML Combos.
			// These are THE paper money now; paper_total_start/alloc_* above are kept for history.
			BookKalshiUSD: 600, BookPolyusUSD: 600, BookCombosUSD: 600, BookMLUSD: 600, BookMLCombosUSD: 600,
			// R127-D C4/C5 money-policy knobs: SHIPPED OFF (gate/arm bools zero-default false,
			// ev_capital_day_min 0). Only cluster_cap_pct carries its documented default here.
			ClusterCapPct:     22,
			PaperTakerDelayMS: 750,
			BankrollKalshiUSD: 125, BankrollPolyIntUSD: 125, BankrollPolyUSUSD: 125, // Deprecated K/US compatibility values; Poly-int remains a RESEARCH notional (excluded from tradeable math).
			ResetOnStart: true, // R30 (operator): "when I start it up, it resets pnl, makes sure all trades are closed, then starts. every time."

			ConsensusKalshiSoloStrength: 0.92, MaxOpenPositions: 25, // selectivity + anti-churn (last night: 597 trades, 49% win, fees ate half)
			KflowBooksEnabled:     true,  // R72-B: kflow twin paper books (pre/live split) on by default — paper-only experiment
			MLImplausibilityGuard: false, // R79 (operator): "p_win ≥ 3× sub-10¢" guard OFF by default — flip to true to restore it (the 5–95¢ band stays on regardless)
			MLMinPWin:             0.50,  // R84 (operator, evidence-confirmed): ML book + live ML proposals refuse picks with model p_win below 0.50 (p_win<0.50 = 55% of ML book losses, 14% realized vs 35% implied). Explicit 0 disables.
			// R90 ML borders (operator ask): plausibility band 5–95% + EV window 2–20¢ net at live
			// price; above the ceiling = quarantine (audit cat "mlquarantine"), never bet.
			MLPWinMin: 0.05, MLPWinMax: 0.95, MLEVMinCents: 2, MLEVMaxCents: 20,
			RealizationHaircut: true, // R90 DO-THIS 5i / edge 29: family realization ratio scales gate EV (shrunk, floor 0, cap 1)
			MakerAdverseGuard:  true, // R90 DO-THIS 5iii / edge 30 → R105 option (c): unsafe posts DIVERT to taker (was skip)
			MakerMinDepth:      10,   // R105: min visible touch depth to allow a resting post (mfs logs depth — tune from real distributions)
			MakerMomGateC:      1.0,  // R105: ¢ against the posted side over ~5m that blocks a post (the R101 threshold, now tunable)
			MakerSimBooks:      true, // R107 (operator order): ML/RawFlow/Weather books fill via the maker sim (auto books already did)
			RouteQueueEnabled:  true, // R122: queue-aware maker/taker routing (operator: "taker usually, maker when queue allows")
			GoEvalEnabled:      true, // R123: tick-fresh p_win re-scoring (operator: "as fast as possible, closest to 0 seconds")
		},
	}
}

// Load reads JSON config from path (a missing file falls back to defaults), then
// applies environment-variable overrides:
//
//	KALSHI_SUITE_ENV       demo|prod
//	KALSHI_SUITE_ADDR      listen address
//	KALSHI_SUITE_DATA_DIR  data directory
func Load(path string) (Config, error) {
	cfg, _, _, err := LoadWithReport(path)
	return cfg, err
}

// LoadWithReport is Load plus boot transparency. It returns, alongside the config:
//   - defaulted: every leaf key the FILE DID NOT SET, i.e. every knob running on Default()'s
//     value (R76, auditor bug 19) — main logs it at boot so effective-vs-file drift is one
//     glance, not archaeology;
//   - unknown: every file key the schema does NOT decode. R79 (boot robustness — replaces the
//     R76 fail-stop): unknown keys are a loud WARN + audit + CONTINUE, no longer a boot refusal.
//     Rationale: the R76 hard error existed so a typo'd money knob couldn't silently re-arm a
//     default, but the defaults-diff above already names the real key as defaulted — the typo is
//     visible TWICE (unknown + defaulted) while a LIVE-money suite keeps running instead of
//     fail-stopping invisibly in a background window. Truly invalid JSON (syntax / wrong types)
//     still fails hard.
//
// R79 also strips a leading UTF-8 BOM before parsing: Windows editors (Notepad's "UTF-8 with
// BOM", some PowerShell redirects) prepend EF BB BF, which encoding/json rejects — that BOM
// fail-stopped the suite invisibly. writeConfigAtomic always writes BOM-less UTF-8, so a BOM can
// only arrive from an outside editor; it is now harmless either way.
func LoadWithReport(path string) (Config, []string, []string, error) {
	cfg := Default()
	var defaulted, unknown []string
	if path != "" {
		b, err := os.ReadFile(path)
		switch {
		case err == nil:
			b = bytes.TrimPrefix(b, []byte{0xEF, 0xBB, 0xBF}) // R79: tolerate a UTF-8 BOM (see doc comment)
			dec := json.NewDecoder(bytes.NewReader(b))
			dec.DisallowUnknownFields()
			if derr := dec.Decode(&cfg); derr != nil {
				// Strict decode failed. If the ONLY problem is unknown keys, re-decode leniently
				// (fresh defaults — the strict pass may have half-applied fields before stopping),
				// report the unknown keys, and continue. Any other failure (bad syntax, wrong
				// types) fails the boot exactly as before.
				cfg = Default()
				if lerr := json.Unmarshal(b, &cfg); lerr != nil {
					return cfg, nil, nil, fmt.Errorf("parse config %s: %w", path, lerr)
				}
				unknown = unknownKeys(b)
				if len(unknown) == 0 {
					// Lenient pass succeeded yet no unknown keys found — a decode disagreement we
					// don't understand. Surface the original strict error rather than guessing.
					return cfg, nil, nil, fmt.Errorf("parse config %s: %w", path, derr)
				}
			}
			defaulted = defaultedKeys(b, cfg)
		case !os.IsNotExist(err):
			return cfg, nil, nil, fmt.Errorf("read config %s: %w", path, err)
		default:
			defaulted = []string{"(no config file — running entirely on defaults)"}
		}
	}
	if v := os.Getenv("KALSHI_SUITE_ENV"); v != "" {
		cfg.Environment = Environment(strings.ToLower(v))
	}
	if v := os.Getenv("KALSHI_SUITE_ADDR"); v != "" {
		cfg.ServerAddr = v
	}
	if v := os.Getenv("KALSHI_SUITE_DATA_DIR"); v != "" {
		cfg.DataDir = v
	}
	if cfg.Environment != EnvDemo && cfg.Environment != EnvProd {
		return cfg, defaulted, unknown, fmt.Errorf("invalid environment %q (want demo|prod)", cfg.Environment)
	}
	return cfg, defaulted, unknown, nil
}

// knownPaths returns every dot-path the Config schema decodes (json struct tags, recursive) —
// object paths and leaves alike. Reflection over the TYPE, not a marshal of a value, so omitempty
// fields at their zero value are still (correctly) known.
func knownPaths() map[string]bool {
	out := map[string]bool{}
	var walk func(pre string, t reflect.Type)
	walk = func(pre string, t reflect.Type) {
		for i := 0; i < t.NumField(); i++ {
			f := t.Field(i)
			tag := strings.Split(f.Tag.Get("json"), ",")[0]
			if tag == "" || tag == "-" {
				continue
			}
			key := tag
			if pre != "" {
				key = pre + "." + tag
			}
			out[key] = true
			ft := f.Type
			if ft.Kind() == reflect.Pointer {
				ft = ft.Elem()
			}
			if ft.Kind() == reflect.Struct {
				walk(key, ft)
			}
		}
	}
	walk("", reflect.TypeOf(Config{}))
	return out
}

// unknownKeys names every key in the file's JSON that the Config schema does not decode (dot
// paths, sorted). An unknown OBJECT is reported once at its own path — its children are implied.
func unknownKeys(fileJSON []byte) []string {
	var fileM map[string]any
	if json.Unmarshal(fileJSON, &fileM) != nil {
		return nil
	}
	known := knownPaths()
	var out []string
	var walk func(pre string, m map[string]any)
	walk = func(pre string, m map[string]any) {
		for k, v := range m {
			key := k
			if pre != "" {
				key = pre + "." + k
			}
			if !known[key] {
				out = append(out, key)
				continue
			}
			if sub, ok := v.(map[string]any); ok {
				walk(key, sub)
			}
		}
	}
	walk("", fileM)
	sort.Strings(out)
	return out
}

// defaultedKeys flattens the file's JSON against the EFFECTIVE config and returns the effective
// leaf keys the file left unset (dot paths, sorted) — those are the knobs running on Default().
// omitempty fields at their zero value don't marshal and therefore can't be reported; that's the
// safe direction (they ARE the default).
func defaultedKeys(fileJSON []byte, eff Config) []string {
	var fileM, effM map[string]any
	if json.Unmarshal(fileJSON, &fileM) != nil {
		return nil
	}
	eb, err := json.Marshal(eff)
	if err != nil || json.Unmarshal(eb, &effM) != nil {
		return nil
	}
	var out []string
	var walk func(pre string, eff, file map[string]any)
	walk = func(pre string, eff, file map[string]any) {
		for k, ev := range eff {
			key := k
			if pre != "" {
				key = pre + "." + k
			}
			var fv any
			ok := false
			if file != nil {
				fv, ok = file[k]
			}
			if sub, isMap := ev.(map[string]any); isMap {
				fsub, _ := fv.(map[string]any)
				walk(key, sub, fsub)
				continue
			}
			if !ok {
				out = append(out, key)
			}
		}
	}
	walk("", effM, fileM)
	sort.Strings(out)
	return out
}
