# r99_placement_test.py — R99 regression pins for the ML paper book placement path
# (auditor r24/r25: the book placed ZERO lots for 6+ cycles after the R98 $1k reset, and the
# both-sides picker carried 429 dual-side markets in one cycle).
#
#  1. bug 176: a below-floor SIGNAL-time-EV row at the top of the live-EV-sorted preds list must
#     SKIP, not end the whole buy pass (the old loop-head `break` starved the book).
#  2. bug 193: closed[] rows from BEFORE the last reset_close marker must not block re-entry
#     (the dup set spanned epochs → ~500 tickers refused post-reset).
#  3. bug 192: the book gate applies the R98 crash clamp itself (live price collapsed vs signal
#     price ⇒ never a bet) — it used to recompute raw live EV and buy the clamped rows.
#  4. ONE-SIDE-PER-MARKET: any open lot blocks new lots on that market, whatever the side
#     vocabulary (the R97 _OPP_SIDE map missed exotic labels), and the existing position WINS
#     (no flip-flop churn).
#  5. CROSS-BOOK: an auto-book open lot (ml_alloc.json auto_open handshake) blocks the ML book's
#     OPPOSITE side of that market; the SAME side stays allowed.
#  6. _collapse_dual_sides: dual-side preds collapse to the better-ranked side only; the pair-sum
#     calibration-health metric counts |p_yes+p_no−1| > 0.10 markets.
#
# Run: python ml/r99_placement_test.py  (standalone, tempdir-hermetic, no suite required)
import json
import os
import sys
import tempfile
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import live_ml  # noqa: E402

PASS = 0
FAIL = 0


def check(name, cond, detail=""):
    global PASS, FAIL
    if cond:
        PASS += 1
        print("  ok  %s" % name)
    else:
        FAIL += 1
        print("  FAIL %s %s" % (name, detail))


def pred(ticker, side="YES", price=0.50, p_win=0.65, ev_net=0.13, plat="kalshi", **kw):
    x = {"ticker": ticker, "side": side, "signal_type": "kalshi-flow", "platform": plat,
         "title": "t " + ticker, "price": price, "p_win": p_win,
         "ev_per_contract": round(p_win - price, 4), "ev_net": ev_net, "resolve_hours": 2.0}
    x.update(kw)
    return x


def lpx(preds, px=None):
    return {live_ml._pxkey(x): (px if px is not None else x["price"]) for x in preds}


def run_book(td, preds, livepx, book=None, alloc=None):
    db = os.path.join(td, "signals.db")
    if book is not None:
        with open(os.path.join(td, "ml_paper.json"), "w", encoding="utf-8") as f:
            json.dump(book, f)
    if alloc is not None:
        with open(os.path.join(td, "ml_alloc.json"), "w", encoding="utf-8") as f:
            json.dump(alloc, f)
    old = live_ml.ML_PAPER_NEW_ENTRIES_ENABLED
    live_ml.ML_PAPER_NEW_ENTRIES_ENABLED = True
    try:
        return live_ml.manage_portfolio(db, preds, {}, livepx=livepx)
    finally:
        live_ml.ML_PAPER_NEW_ENTRIES_ENABLED = old


def rejections(td):
    try:
        with open(os.path.join(td, "ml_rejections.json"), encoding="utf-8") as f:
            return [r.get("reason") or "" for r in json.load(f).get("rows", [])]
    except Exception:
        return []


def open_tickers(td):
    with open(os.path.join(td, "ml_paper.json"), encoding="utf-8") as f:
        return [(p["ticker"], p["side"]) for p in json.load(f).get("open", [])]


print("1. bug 176 — below-floor sig-EV row must not end the buy pass")
with tempfile.TemporaryDirectory() as td:
    bad = pred("T-SLIPPED", ev_net=-0.50)          # signal-time EV cratered (price ran) — old code: break
    good = pred("T-GOOD")                          # admissible row right below it
    st = run_book(td, [bad, good], lpx([bad, good]))
    opens = open_tickers(td)
    check("pass continues past the slipped row", ("T-GOOD", "YES") in opens, str(opens))
    check("slipped row itself not bought", all(t != "T-SLIPPED" for t, _ in opens), str(opens))

print("2. bug 193 — pre-epoch closes must not block re-entry")
with tempfile.TemporaryDirectory() as td:
    book = {"open": [], "closed": [
        {"ticker": "T-OLD", "side": "YES", "price": 0.4, "contracts": 10, "pnl": 1.0},   # pre-reset epoch
        {"ticker": "T-RESET", "side": "YES", "price": 0.4, "contracts": 5, "pnl": 0.0, "reset_close": True},
    ], "lifetime": {"net": 1.0, "wins": 1, "closed": 2, "net_base": 1.0, "bank_ver": 3}}
    st = run_book(td, [pred("T-OLD")], lpx([pred("T-OLD")]), book=book)
    check("old-epoch ticker tradeable again", ("T-OLD", "YES") in open_tickers(td), str(open_tickers(td)))
with tempfile.TemporaryDirectory() as td:
    book = {"open": [], "closed": [
        {"ticker": "T-RESET", "side": "YES", "price": 0.4, "contracts": 5, "pnl": 0.0, "reset_close": True},
        {"ticker": "T-NEW", "side": "YES", "price": 0.4, "contracts": 10, "pnl": 1.0},   # THIS epoch
    ], "lifetime": {"net": 1.0, "wins": 1, "closed": 2, "net_base": 1.0, "bank_ver": 3}}
    st = run_book(td, [pred("T-NEW")], lpx([pred("T-NEW")]), book=book)
    check("same-epoch close still dedups", ("T-NEW", "YES") not in open_tickers(td), str(open_tickers(td)))

print("3. bug 192 — crash clamp runs at the book gate")
with tempfile.TemporaryDirectory() as td:
    crash = pred("T-CRASH", price=0.61, p_win=0.61, ev_net=0.05)
    st = run_book(td, [crash], lpx([crash], px=0.30))  # live 31c below signal — market says it's dying
    check("crashed pick refused", ("T-CRASH", "YES") not in open_tickers(td), str(open_tickers(td)))
    check("crash reason logged", any("crashed" in r for r in rejections(td)), str(rejections(td)))

print("4. one side per market — open lot blocks the market, existing position wins")
with tempfile.TemporaryDirectory() as td:
    book = {"open": [{"ticker": "T-HELD", "side": "Above 3.5", "price": 0.5, "contracts": 10,
                      "platform": "kalshi", "opened": int(time.time()), "p_win": 0.6,
                      "ev": 0.1, "signal_type": "kalshi-flow", "title": "held"}],
            "closed": [], "lifetime": {"net": 0.0, "wins": 0, "closed": 0, "net_base": 0.0, "bank_ver": 3}}
    st = run_book(td, [pred("T-HELD", side="YES")], lpx([pred("T-HELD", side="YES")]), book=book)
    opens = open_tickers(td)
    check("exotic-side open lot blocks any new side", opens == [("T-HELD", "Above 3.5")], str(opens))
    check("hedge-block reason logged", any("one side per market" in r for r in rejections(td)), str(rejections(td)))

print("5. cross-book — auto book's opposite side blocks; same side allowed")
with tempfile.TemporaryDirectory() as td:
    alloc = {"ml_bank_usd": 400.0, "shadow_enabled": False,
             "auto_open": [{"platform": "kalshi", "ticker": "T-AUTO", "side": "YES"}]}
    st = run_book(td, [pred("T-AUTO", side="NO", price=0.50, p_win=0.65)],
                  lpx([pred("T-AUTO", side="NO")]), alloc=alloc)
    check("opposite side of auto lot refused", ("T-AUTO", "NO") not in open_tickers(td), str(open_tickers(td)))
    check("cross-book reason logged", any("auto paper book" in r for r in rejections(td)), str(rejections(td)))
with tempfile.TemporaryDirectory() as td:
    alloc = {"ml_bank_usd": 400.0, "shadow_enabled": False,
             "auto_open": [{"platform": "kalshi", "ticker": "T-AUTO", "side": "YES"}]}
    st = run_book(td, [pred("T-AUTO", side="YES")], lpx([pred("T-AUTO", side="YES")]), alloc=alloc)
    check("same side as auto lot still allowed", ("T-AUTO", "YES") in open_tickers(td), str(open_tickers(td)))

print("6. _collapse_dual_sides — one side per market at the preds layer")
rows = [pred("X", side="YES", p_win=0.70), pred("X", side="NO", p_win=0.60),
        pred("X", side="YES", p_win=0.66, signal_type="kalshi-whale"), pred("Y", side="NO", p_win=0.55)]
kept, dropped, viol = live_ml._collapse_dual_sides(rows)
check("winner side keeps all its rows", [(k["ticker"], k["side"]) for k in kept] ==
      [("X", "YES"), ("X", "YES"), ("Y", "NO")], str([(k["ticker"], k["side"]) for k in kept]))
check("loser side dropped", dropped == 1, "dropped=%d" % dropped)
check("pair-sum violation counted (0.70+0.60)", viol == 1, "viol=%d" % viol)
kept2, dropped2, viol2 = live_ml._collapse_dual_sides([pred("Z", side="YES", p_win=0.55), pred("Z", side="NO", p_win=0.45)])
check("consistent pair (sum=1) not a violation", viol2 == 0 and dropped2 == 1, "viol=%d dropped=%d" % (viol2, dropped2))

print("7. current Paper horizon — regular 24h and crypto 6h, recomputed from signal age")
with tempfile.TemporaryDirectory() as td:
    now = int(time.time())
    aged_in = pred("T-AGED-IN", resolve_hours=30.0,
                   ts=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(now - 10 * 3600)))
    crypto_aged_in = pred("KXBTC15M-IN", resolve_hours=20.0,
                          ts=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(now - 17 * 3600)))
    crypto_still_far = pred("KXBTC15M-AGED", resolve_hours=20.0,
                            ts=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(now - 13 * 3600)))
    still_far = pred("T-STILL-FAR", resolve_hours=30.0,
                     ts=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(now - 4 * 3600)))
    rows = [aged_in, crypto_aged_in, crypto_still_far, still_far]
    st = run_book(td, rows, lpx(rows))
    opens = open_tickers(td)
    check("regular signal now inside 24-hour Paper window is tradeable", ("T-AGED-IN", "YES") in opens, str(opens))
    check("crypto signal now inside six-hour Paper window is tradeable", ("KXBTC15M-IN", "YES") in opens, str(opens))
    check("crypto keeps its stricter six-hour Paper window", ("KXBTC15M-AGED", "YES") not in opens, str(opens))
    check("regular market still 26 hours away remains blocked", ("T-STILL-FAR", "YES") not in opens, str(opens))
    check("horizon rejection is visible", any("outside current" in r for r in rejections(td)), str(rejections(td)))

print("8. Paper horizon caps widen collection; LIVE still rechecks its separate 4h/2h contract")
with tempfile.TemporaryDirectory() as td:
    db = os.path.join(td, "data", "kalshi.db")
    os.makedirs(os.path.dirname(db), exist_ok=True)
    with open(os.path.join(td, "config.json"), "w", encoding="utf-8") as f:
        json.dump({"auto": {"paper_ml_max_hours_out": 99,
                            "paper_ml_crypto_max_hours_out": 12}}, f)
    check("regular config cannot widen the 24-hour Paper hard cap", live_ml.load_max_hours(db) == 24.0)
    check("crypto config cannot widen the six-hour Paper hard cap", live_ml.load_crypto_max_hours(db) == 6.0)
check("Kalshi BTC ticker is crypto", live_ml._is_crypto_contract({"ticker": "KXBTC-26JUL15"}))
check("explicit crypto category is crypto", live_ml._is_crypto_contract({"ticker": "market-1", "category": "crypto"}))
check("Marina Bassols is not crypto", not live_ml._is_crypto_contract({"ticker": "marina-bassols-win", "title": "Marina Bassols"}))
check("Solary Eclipse is not crypto", not live_ml._is_crypto_contract({"ticker": "solary-eclipse-map-1", "title": "Solary Eclipse"}))

print("\n%d passed, %d failed" % (PASS, FAIL))
sys.exit(1 if FAIL else 0)
