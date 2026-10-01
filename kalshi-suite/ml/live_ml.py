#!/usr/bin/env python3
"""
live_ml.py — live edge-scorer for the kalshi-suite signal_log.

What it does, every cycle (so it keeps "learning" as new bets resolve):
  1. Reads signal_log straight out of the live SQLite DB (read-only).
  2. De-dups to ONE row per market (ticker|side|signal_type, earliest ts) so the
     5x per-10-min-slot inflation doesn't leak.
  3. Retrains a gradient-boosted model after enough new RESOLVED rows (or 15 min).
     It prints an honest walk-forward out-of-sample AUC (train on the older 70%,
     test the newer 30%)
     so you can watch whether the edge is real, then retrains on ALL resolved
     rows to score the open markets.
  4. Scores every OPEN (resolved=0) market: model win-prob p_win, and the thing
     you actually care about ->  EV/contract = p_win - price  (the Kelly edge).
  5. Writes ml_predictions.json next to the DB (so the Go side can read it later)
     and prints the top +EV open markets.

This is PAPER/decision-support only. It never places a bet. It just scores.

Run:
  pip install -r requirements.txt           (one time)
  python live_ml.py --db ../data/kalshi.db --interval 10     (loop every 10 min)
  python live_ml.py --db ../data/kalshi.db --once            (one pass, then exit)
"""
import argparse, bisect, contextlib, json, math, os, shutil, sqlite3, sys, threading, time  # R123: bisect for the flat-export isotonic emulation
import urllib.request, urllib.parse  # R108 (auditor 310): module-level — _ml_pending/_ml_post referenced urllib without it, freezing the book
from datetime import datetime, timezone, timedelta


# R133 GPU/CPU BUDGET: CUDA owns the expensive tree fits, but XGBoost, NumPy/BLAS and the
# sklearn CPU fallback still create CPU helper pools. Let an operator request FEWER helpers with
# KALSHI_ML_CPU_THREADS, honor any stricter inherited library cap, and never allow more than four.
# This must run before numpy/sklearn/xgboost import so their native runtimes see the bound at boot.
ML_CPU_HELPER_THREAD_CAP = 4
_ML_THREAD_ENV = ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS", "NUMEXPR_NUM_THREADS")


def _configure_helper_threads(env):
    try:
        requested = int(env.get("KALSHI_ML_CPU_THREADS", ML_CPU_HELPER_THREAD_CAP))
    except (TypeError, ValueError):
        requested = ML_CPU_HELPER_THREAD_CAP
    requested = max(1, min(ML_CPU_HELPER_THREAD_CAP, requested))
    # A pre-existing smaller cap is deliberate; preserve it. Larger inherited defaults are
    # clamped so a CUDA failure cannot suddenly fan the sidecar across all 24 logical CPUs.
    for name in _ML_THREAD_ENV:
        try:
            inherited = int(env.get(name, 0))
        except (TypeError, ValueError):
            inherited = 0
        if inherited > 0:
            requested = min(requested, inherited)
    for name in _ML_THREAD_ENV:
        env[name] = str(requested)
    return requested


ML_CPU_HELPER_THREADS = _configure_helper_threads(os.environ)


def _atomic_dump(obj, path):
    """Write JSON atomically: dump to a temp file then os.replace() (atomic on the same filesystem).
    Prevents torn/half-written files that corrupt ml_paper.json / ml_predictions.json if two writers
    ever overlap (last-writer-wins, but NEVER a broken file)."""
    tmp = path + ".tmp"
    # R80 ENCODING: explicit utf-8 — Windows open() defaults to the ANSI code page (cp1252), which
    # can't even round-trip what json.dump escapes today if ensure_ascii ever changes, and every
    # READER in this file is utf-8 now. One rule, both directions: all text I/O here is utf-8.
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=1)
    os.replace(tmp, path)


def _json_read(path):
    """R80 ENCODING (operator screenshot: "CORRUPT BOOK: ml_paper.json failed to parse ('charmap'
    codec can't decode byte 0x8f in position 1714)"): EVERY JSON read goes through THIS helper with
    encoding='utf-8'. Windows open() defaults to cp1252 ('charmap'), which explodes on the UTF-8
    emoji the Go side writes into the books (friendly titles) — the file was NEVER corrupt, the
    default codec was wrong. Also mirrors the Go loader's R79 tolerance: strips a UTF-8 BOM and
    trailing NUL padding (the live config.json shipped a 1.8KB \\x00 tail after a crash — Go's
    json.Decoder ignores trailing garbage, so strict json.load() here silently threw every
    Settings read back to defaults). An empty/blank file still RAISES like json.load did, so the
    corrupt-book guards keep aborting instead of treating garbage as an empty book."""
    with open(path, "r", encoding="utf-8") as f:
        s = f.read()
    return json.loads(s.lstrip(chr(0xFEFF)).strip("\x00 \t\r\n"))


def _append_model_history(db, rec):
    """R72-A #3 MODEL-QUALITY HISTORY: one JSONL line per FULL retrain ({ts, oos_auc, log loss,
    brier, brier_raw, ece, cal_method, n_resolved, n_features, backend}) -> data/ml_model_history.jsonl, so the dashboard
    can chart model drift across weeks. The append is one small single write() (atomic enough for
    the single writer); the 5k-line cap trims via tmp + os.replace (atomic on the same filesystem),
    so the Go loose reader never sees a torn file. Best-effort: never fails a cycle."""
    path = os.path.join(os.path.dirname(os.path.abspath(db)), "ml_model_history.jsonl")
    try:
        with open(path, "a", encoding="utf-8") as f:
            f.write(json.dumps(rec, separators=(",", ":")) + "\n")
        with open(path, "r", encoding="utf-8") as f:
            lines = f.readlines()
        if len(lines) > 5000:
            tmp2 = path + ".trim"
            with open(tmp2, "w", encoding="utf-8") as f:
                f.writelines(lines[-5000:])
            os.replace(tmp2, path)
    except Exception as e:
        print("  (could not append ml_model_history: %s)" % e)


@contextlib.contextmanager
def _book_lock(book_path, wait=3.0, stale=15.0):
    """Cross-process advisory lock for ml_paper.json (audit #6). The Go side (settleMLBook /
    acquireBookLock in the suite) uses the IDENTICAL protocol on <dataDir>/ml_paper.lock:
    O_CREAT|O_EXCL create (atomic on Windows+POSIX), break locks older than `stale` seconds
    (crashed holder), give up after `wait` seconds. Both writers wrap their ENTIRE
    read-modify-write so updates serialize — before this, last-writer-wins silently dropped
    the other side's settlements. Yields True when the lock was acquired; callers MUST skip
    their write when it yields False (retry next cycle — never block, never clobber)."""
    lock = os.path.join(os.path.dirname(os.path.abspath(book_path)), "ml_paper.lock")
    deadline = time.time() + wait
    acquired = False
    while True:
        try:
            fd = os.open(lock, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            os.write(fd, ("%d %s" % (os.getpid(), datetime.now(timezone.utc).isoformat())).encode())
            os.close(fd)
            acquired = True
            break
        except FileExistsError:
            try:
                if time.time() - os.path.getmtime(lock) > stale:
                    os.remove(lock)  # crashed holder — break the stale lock
                    continue
            except OSError:
                pass
            if time.time() >= deadline:
                break
            time.sleep(0.05)
        except OSError:
            break
    try:
        yield acquired
    finally:
        if acquired:
            try:
                os.remove(lock)
            except OSError:
                pass

try:
    import numpy as np
    # HistGradientBoosting = the modern histogram-based boosting model. Unlike the old
    # GradientBoosting* (single-threaded, no GPU), it runs MULTI-THREADED across all CPU cores
    # (OpenMP), so a full retrain on ~48k rows is ~10-50x faster on this 12-core/24-thread machine.
    from sklearn.ensemble import HistGradientBoostingClassifier, HistGradientBoostingRegressor
    from sklearn.calibration import CalibratedClassifierCV
    from sklearn.isotonic import IsotonicRegression
    from sklearn.linear_model import LogisticRegression
    from sklearn.metrics import roc_auc_score
except ImportError:
    sys.exit("Missing deps. Run:  pip install scikit-learn numpy")

# R52 CUDA (operator: "mk do cuda"): train on the GPU when one exists. xgboost's hist trees on the
# RTX are a drop-in for sklearn's HGB (same depth/iters/lr, sklearn-compatible API, so
# CalibratedClassifierCV wraps it unchanged). Probed with a real tiny fit+predict — if xgboost
# is missing, the wheel has no GPU, or the driver is unhappy, everything falls back to the exact
# sklearn CPU path that has been running all along. Side benefit beyond speed: training stops
# competing with the Go engine for CPU during live hours (the operator's latency mandate).
# GPU RE-PROBE (audit #5): the probe is no longer once-at-boot-forever — every FULL retrain calls
# _model_backend(refresh=True), so a GPU that was merely busy at boot gets adopted once free, and a
# GPU that dies mid-run falls back to sklearn-cpu instead of erroring every retrain. _mk_clf/_mk_reg
# rebuild their params off the fresh probe each call. Backend transitions are logged once, not spammed.
_BACKEND = None  # (label, XGBClassifier|None, XGBRegressor|None) after the last probe


def _model_backend(refresh=False):
    global _BACKEND
    if _BACKEND is not None and not refresh:
        return _BACKEND
    prev = _BACKEND[0] if _BACKEND else None
    try:
        from xgboost import XGBClassifier, XGBRegressor
        probe = XGBClassifier(n_estimators=2, max_depth=2, tree_method="hist", device="cuda",
                              n_jobs=ML_CPU_HELPER_THREADS, verbosity=0)
        probe.fit(np.random.rand(64, 3), np.random.randint(0, 2, 64))
        probe.predict_proba(np.random.rand(4, 3))  # exercise inference too — fit alone can pass where predict warns/fails
        _BACKEND = ("xgboost-cuda", XGBClassifier, XGBRegressor)
        if prev != "xgboost-cuda":
            print("[ml] model backend: xgboost-cuda (GPU training active)")
    except Exception as e:  # no xgboost / no NVIDIA GPU / driver trouble — never fatal
        _BACKEND = ("sklearn-cpu", None, None)
        if prev != "sklearn-cpu":
            print("[ml] model backend: sklearn-cpu (CUDA unavailable: %s)" % str(e)[:200])
    return _BACKEND


def _mk_clf():
    """Boosted classifier at the suite's standard hyperparams — GPU when available, else sklearn."""
    label, XC, _ = _model_backend()
    if XC is not None:
        return XC(max_depth=3, n_estimators=150, learning_rate=0.05,
                  tree_method="hist", device="cuda", n_jobs=ML_CPU_HELPER_THREADS,
                  eval_metric="logloss", verbosity=0)
    return HistGradientBoostingClassifier(max_depth=3, max_iter=150, learning_rate=0.05)


def _mk_reg():
    label, _, XR = _model_backend()
    if XR is not None:
        return XR(max_depth=3, n_estimators=150, learning_rate=0.05,
                  tree_method="hist", device="cuda", n_jobs=ML_CPU_HELPER_THREADS, verbosity=0)
    return HistGradientBoostingRegressor(max_depth=3, max_iter=150, learning_rate=0.05)

# R114 DEAD-FEATURE REMOVAL (recon over 1.17M signal_log rows + the 07-08 retrain importances):
# best_rank / trader_count / trader_pnl / trader_skill / holders_hhi / holders_skill were ALWAYS
# their sentinel on every non-poly-int row (0 informative rows; poly-int is excluded from training)
# and carried ~zero importance — pure column bloat. REMOVED from the model input only: the DB
# columns keep logging (a future poly-int-inclusive model can re-add them by re-listing here).
# NUM shrinking changes n_num, which invalidates the persisted bundle → clean full retrain.
BOOK_FEATURE_SCHEMA = "book-native-v2"
BOOK_FEATURE_VERSION = 1
ML_LABEL_VERSION = "kalshi_start_clock_v2"
# Stable compatibility identity for LIVE evidence. ``model_version`` changes on every fitted
# generation; that is correct provenance for the current forecast but it starves a settlement
# cohort when the online model refits before markets close. The lineage changes only when the
# feature, executable-book, label, or calibration contract changes. Paper lots still retain their
# exact model_version as well, so a current signal is never confused with an older generation.
MODEL_LINEAGE = "book-native-v2|book-features:1|label:kalshi_start_clock_v2|calibration:v1"
# R143: Paper is the proving ground, LIVE is the promotion target.  A provisional, event/day-
# separated fit may open PAPER observations so the executable route can accumulate evidence.  It
# never authorizes LIVE. LIVE remains fail-closed until a separately replicated untouched holdout
# contract says otherwise (the rolling five-day diagnostic below is necessary, not sufficient).
# The execution seam is implemented, but the proof receipt—not this switch—owns authority.
ML_PAPER_EXECUTION_AUTHORITY = True
ML_LIVE_EXECUTION_AUTHORITY = True

# R133 BOOK-NATIVE MODEL. `legacy_signal_price` remains context, while route-specific executable
# prices are primary. Historical entry_price rows are never promoted into the new fields.
NUM = ["legacy_signal_price", "strength", "notional",
       "concentration", "momentum", "spread_cents",
       "resolve_hours", "imbalance", "confidence", "underlying", "book_depth",
       "is_live",       # R38 (operator): in-play (1) vs pre/stable (0) vs unknown (-1) at signal time
       # R65 feature families (NULL on pre-R65 rows BY DESIGN — sentinels applied in _LEAN_COLS):
       "book_imb3", "book_depth3",  # top-3-level book shape: bid share of top-3 depth (0..1) + summed top-3 depth
       "mom_1h", "mom_4h",          # YES-price change over ~1h / ~4h in cents (ring-scope, side-agnostic)
       "dist_hi", "dist_lo",        # cents below/above the RING-scope session high/low (not venue 24h)
       "secs_to_start",             # SIGNED secs until event start (negative = underway; huge = unknown)
       # R67l taker-flow family (from the tapes the suite already ingests; NULL pre-R67 by design):
       "flow_ratio_15m",            # taker BUY-YES share of taker $ on the ticker, last 15m (0..1; -1 = no tape)
       "flow_n_15m",                # taker trade count behind the ratio (0 = no tape)
       # R70-B SCHEMA_AUDIT families (NULL until the feeds warm; sentinels in _LEAN_COLS)
       # (holders_hhi / holders_skill removed R114 — poly-int-only, dead on the training set):
       "in_play",                   # polyus venue-reported game state: 1 live, 0 pre-game, -1 unknown
       "score_margin",              # polyus |lead| while in-play (-1 = unknown; 0 = tied is meaningful)
       "oi_delta_1h",               # kalshi open-interest change over ~1h, contracts (0 = flat/unknown)
       "px_age_s"]                  # R114: decision-price age s at signal time (-1 = unmeasured) — stale-price loss driver

# FEE MODEL (audit F11): the sidecar's whole EV layer ran GROSS of fees — the book gate's
# ev_floor (1.5¢) sat BELOW the ~1.75¢ mid-price fee, Kelly sized on gross edge, the output was
# sorted gross, and every venue settled at Kalshi's flat 7% formula (Poly US actually charges
# 0.06 taker and PAYS −0.0125 maker rebate). These helpers are the venue-true rates; maker share
# mirrors the suite's fee_maker_share setting (loaded at startup like MLHOURS).
FEE_MAKER_SHARE = 1.0  # R67h: maker-only default (operator: "we are genuinely doing maker only" — WS 1¢-cancel discipline); config.json fee_maker_share still overrides at startup
# R79 (operator: REMOVE the model-implausibility guard): the "p_win >=3x a sub-10c live price ->
# skip" rule on BOTH buy loops (real book + the shadow's identical copy) is now OPT-IN via
# config.json ml_implausibility_guard (Settings toggle; same handshake family as fee_maker_share,
# re-read every cycle so the toggle applies without a sidecar restart). Default False = guard OFF
# per operator — one click restores it. History it encodes: the book once bought $25 of a FINISHED
# game at 1c on garbage p_win (R44), and the quant study measured sub-10c edges ~2x optimistic.
# The 5–95c live-price sanity BAND is a separate data-integrity guard and stays unconditional.
ML_IMPLAUSIBILITY_GUARD = False
# R84 ML p_win FLOOR (operator-confirmed, REVERSING the R83 "no standalone floor" note): high-EV
# longshots were cooking the bankroll — book autopsy 2026-07-05: p_win<0.50 picks = 55% of ML book
# losses, realized 14% win rate vs the 35% their prices implied. The REAL book's buy loop refuses
# any pick with p_win below this floor ("p_win below floor" in ml_rejections). The SHADOW book is
# deliberately UNGATED — it stays the all-gross control. config.json ml_min_p_win overrides
# (re-read every cycle, same handshake as ml_implausibility_guard); explicit 0 disables.
ML_MIN_P_WIN = 0.50
# R90 ML BORDERS (operator ask): plausibility band + EV window on every pick, applied AFTER the
# edge-29 realization haircut. Hot-reloaded each cycle from config.json (ml_pwin_min/ml_pwin_max/
# ml_ev_min_cents/ml_ev_max_cents; explicit 0 disables a single border). Above the too-good
# ceiling -> QUARANTINE (data/ml_quarantine.jsonl + a rejection row), never bet.
ML_BORDERS = {"pwin_min": 0.05, "pwin_max": 0.95, "ev_min_c": 2.0, "ev_max_c": 20.0}
# R90 edge 29 (realization haircut): per-family actual/predicted ratios computed by the Go side
# from this book's own settled lots (data/ml_realization.json, ~30min cadence; shrunk toward 1 at
# low n, floor 0, cap 1). Scales ev_net at the buy gate when realization_haircut=true in config.
REALIZATION_RATIOS = {}
REALIZATION_ENABLED = True


# Exact outcome-space book features. Latency remains nullable; its explicit missing flag prevents
# unknown from reading as a physically impossible zero-latency observation.
NUM += ["book_feature_ver", "book_maker_price", "book_taker_price",
        "book_maker_depth", "book_taker_depth", "book_quote_age_s",
        "book_maker_tick", "book_taker_tick", "book_maker_fee_pc", "book_taker_fee_pc",
        "book_spread_cents", "book_touch_imbalance", "book_maker_all_in", "book_taker_all_in",
        "legacy_to_taker_gap_cents", "book_is_tapered",
        "book_latency_ms", "book_latency_missing"]


def _fee_rate(platform, share=None):
    """Blended fee RATE r so fee = r · C · p · (1−p), venue-true (see audit F11).
    R67h: `share` overrides the blend — pass 1.0 for a pure-MAKER rate (shadow book)."""
    p = (platform or "").lower()
    s = min(max(FEE_MAKER_SHARE if share is None else share, 0.0), 1.0)
    if p == "polyus":
        return (1 - s) * 0.06 + s * (-0.0125)  # taker 0.06 / maker REBATE −0.0125 (Jul-2026 schedule)
    if p in ("polymarket", "poly", "polyint"):
        return (1 - s) * 0.05                   # per-category taker ~0.05 modal, maker $0
    return (1 - s) * 0.07 + s * 0.0175          # kalshi taker/maker ≈ ¼ taker bound (≈0 on many series)


_SUITE_BASE = None
_CONFIG_PATH = None


def _config_path(anyfile):
    """Return the exact config path supplied by the Go parent, with the legacy adjacent lookup
    retained for standalone/test launches that do not pass --config."""
    if _CONFIG_PATH:
        return _CONFIG_PATH
    return os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(anyfile)), "..", "config.json"))


def _suite_base(db):
    """R32: the suite's local HTTP base (for /api/livepx) — server_addr from config.json (repo root)."""
    global _SUITE_BASE
    if _SUITE_BASE is not None:
        return _SUITE_BASE
    addr = "127.0.0.1:8787"
    try:
        cfgp = _config_path(db)
        v = (_json_read(cfgp).get("server_addr") or "").strip()  # R80: BOM/NUL-tolerant like the Go loader
        if v:
            addr = v
    except Exception:
        pass
    if addr.startswith(":"):
        addr = "127.0.0.1" + addr
    _SUITE_BASE = "http://" + addr
    return _SUITE_BASE


def _maker_sim_on(anyfile):
    """R107: the suite's maker_sim_books knob (auto.*, default TRUE when a real config exists) —
    read per cycle like MLHOURS. anyfile = any path inside the data dir (db or ml_paper.json).
    NO config.json (hermetic tests / standalone runs) = FALSE: the maker sim needs a live suite
    to route through, so absent-suite environments keep the legacy local booking."""
    try:
        cfgp = _config_path(anyfile)
        if not os.path.exists(cfgp):
            return False
        auto = (_json_read(cfgp).get("auto") or {})
        v = auto.get("maker_sim_books")
        return True if v is None else bool(v)
    except Exception:
        return False  # unreadable config = behave standalone (legacy booking), never wedge the book


def _suite_token(anyfile):
    """R107: the local API token (data/api-token.txt) — POST /api/mlpost is mutating and token-authed."""
    try:
        p = os.path.join(os.path.dirname(os.path.abspath(anyfile)), "api-token.txt")
        with open(p, "r", encoding="utf-8") as f:
            return f.read().strip()
    except Exception:
        return ""


def _lock_heartbeat(book_path):
    """R108 (auditor 312): refresh ml_paper.lock mtime before slow in-lock work (HTTP, 8s timeout)
    so the 15s stale-break can never fire on a HEALTHY holder mid-read-modify-write."""
    try:
        lock = os.path.join(os.path.dirname(os.path.abspath(book_path)), "ml_paper.lock")
        os.utime(lock, None)
    except OSError:
        pass


def _ml_pending(anyfile):
    """R107: GET /api/mlpending — the book's resting maker posts (count toward deployed + one-lot).
    R108 (auditor 317): returns None on failure (fail-closed at the caller) — an empty list now
    genuinely means 'no pending', not 'suite unreachable'."""
    try:
        with urllib.request.urlopen(_suite_base(anyfile) + "/api/mlpending", timeout=8) as resp:
            return (json.loads(resp.read().decode("utf-8")) or {}).get("pending") or []
    except Exception as e:
        print("  ML book: /api/mlpending unreachable (%s) — pausing new entries this cycle (fail-closed)" % e)
        return None


def _ml_post(anyfile, row, ref_price):
    """R107 MAKER SIM: submit a buy decision to the suite. Returns the response dict
    ({"status":"pending"|"taker"|"reject", ...}) or {"status":"reject","reason":"mlpost-unreachable"}.
    Fail-CLOSED: an unreachable suite means no /api/livepx either, so the book pauses new entries
    rather than inventing instant fills the market never proved."""
    try:
        body = json.dumps({"row": row, "ref_price": ref_price}).encode("utf-8")
        req = urllib.request.Request(_suite_base(anyfile) + "/api/mlpost", data=body, method="POST",
                                     headers={"Content-Type": "application/json",
                                              "X-Api-Token": _suite_token(anyfile)})
        with urllib.request.urlopen(req, timeout=8) as resp:
            return json.loads(resp.read().decode("utf-8")) or {"status": "reject", "reason": "empty response"}
    except Exception as e:
        return {"status": "reject", "reason": "mlpost-unreachable: %s" % e}


def _funded_relation_outcome(anyfile, row):
    """Commit one already-durable New-ML Paper settlement to the funded relation ledger.

    The endpoint is idempotent. False means leave the row unsynced so a later model/settlement
    cycle retries; it never changes the Paper book's own settlement truth.
    """
    receipt_id = str((row or {}).get("relation_receipt_id") or "").strip()
    if len(receipt_id) != 64:
        return False
    try:
        body = json.dumps({"receipt_id": receipt_id,
                           "pnl_dollars": float(row.get("pnl") or 0.0),
                           "closed_ts": int(row.get("closed_ts") or time.time()),
                           "source": "new-ml-paper-settlement"}).encode("utf-8")
        req = urllib.request.Request(_suite_base(anyfile) + "/api/funded-relations/outcome",
                                     data=body, method="POST",
                                     headers={"Content-Type": "application/json",
                                              "X-Api-Token": _suite_token(anyfile)})
        with urllib.request.urlopen(req, timeout=8) as resp:
            out = json.loads(resp.read().decode("utf-8")) or {}
            return bool(out.get("ok"))
    except Exception:
        return False


def _sync_funded_relation_outcomes(anyfile, rows):
    """Retry only unsynced relation rows; successful rows are marked in the next atomic save."""
    changed = 0
    for row in rows or []:
        if not isinstance(row, dict) or row.get("relation_outcome_synced"):
            continue
        if not row.get("relation_receipt_id"):
            continue
        if _funded_relation_outcome(anyfile, row):
            row["relation_outcome_synced"] = True
            changed += 1
    return changed


def _ingest_maker_fills(path, pf, openpos):
    """R107 MAKER SIM: ingest Go-side touch-through fills queued in data/ml_maker_fills.jsonl
    (+ the rotated .1). Go is the only writer.

    R122 (auditor 379): fills land in FILL order, not mfs-id order — a slow fill whose id sits
    below an already-ingested faster fill's was skipped FOREVER by the old max-seq watermark
    (posting surge => more concurrent posts => more out-of-order fills => a silently growing lost
    cohort, i.e. survivorship bias inside the maker-EV evidence). Dedup is now a bounded SEEN-SET
    over a GRACE window under the max ingested seq; the watermark survives as the hard floor.
    R122 (auditor 453): the live file is read incrementally by byte offset (readline keeps tell()
    legal). R132 makes that cursor restart-proof and rotation-proof: size shrink OR a changed
    first sequence identifies replacement, and rows/cursor state commit together only after the
    whole pass succeeds. A mid-read exception therefore cannot add rows while leaving an old
    cursor that re-adds them next cycle."""
    ing = 0
    GRACE = 5000  # ids of out-of-orderness tolerated (>= a day of posting at current rates)
    try:
        wm = int(pf.get("maker_fill_wm", 0) or 0)
        legacy = "maker_fill_seen" not in pf  # transition: everything <= wm was ingested-or-lost pre-R122
        seen = set(int(x) for x in (pf.get("maker_fill_seen") or []))
        floor = wm if legacy else max(0, wm - GRACE)
        base = os.path.join(os.path.dirname(os.path.abspath(path)), "ml_maker_fills.jsonl")
        off = int(pf.get("maker_fill_off", 0) or 0)
        try:
            sz = os.path.getsize(base)
        except OSError:
            sz = 0
        # A rotated file can already be larger than the old offset; size-only detection then seeks
        # into the middle and drops its first fills. The first valid seq is a stable file identity
        # while the writer only appends. Store it with the cursor and compare on every pass.
        head = 0
        if sz > 0:
            try:
                with open(base, "r", encoding="utf-8") as hf:
                    for _ in range(32):
                        hline = hf.readline()
                        if not hline:
                            break
                        try:
                            head = int((json.loads(hline) or {}).get("seq") or 0)
                        except Exception:
                            head = 0
                        if head > 0:
                            break
            except OSError:
                head = 0
        old_head = int(pf.get("maker_fill_head", 0) or 0)
        rotated = sz < off or (off > 0 and old_head > 0 and head > 0 and head != old_head)
        read_plan = []  # (path, start_offset)
        if rotated or off == 0:  # full pass is idempotent via the persisted grace-window seen set
            if os.path.exists(base + ".1"):
                read_plan.append((base + ".1", 0))
            read_plan.append((base, 0))
        else:
            read_plan.append((base, off))
        new_wm, new_off = wm, (0 if rotated else off)
        staged_rows = []
        staged_seen = set(seen)
        epoch_id = str(pf.get("epoch_id") or "")
        reset_unix = _sig_unix(pf.get("reset_at")) if pf.get("reset_at") else 0
        for fp, start in read_plan:
            if not os.path.exists(fp):
                continue
            with open(fp, "r", encoding="utf-8") as f:
                if start:
                    f.seek(start)
                while True:
                    line = f.readline()
                    if not line:
                        break
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        rec = json.loads(line)
                    except Exception:
                        continue
                    seq = int(rec.get("seq") or 0)
                    row = rec.get("row")
                    if seq <= floor or seq in staged_seen or not isinstance(row, dict) or not row.get("ticker"):
                        continue
                    # R143 epoch/cohort contract: a queued maker fill can arrive after RESET even
                    # though it was posted before RESET. Advance the durable journal cursor, but
                    # never resurrect that older position into the new $600 epoch. Current fills
                    # inherit the epoch here because Go cannot safely read/lock the sidecar book
                    # while appending its separate maker journal.
                    staged_seen.add(seq)
                    if seq > new_wm:
                        new_wm = seq
                    try:
                        fill_unix = int(float(row.get("fill_ts") or row.get("opened") or 0))
                    except (TypeError, ValueError):
                        fill_unix = 0
                    if row.get("model_cohort") != BOOK_FEATURE_SCHEMA:
                        continue
                    if reset_unix and (not fill_unix or fill_unix <= reset_unix):
                        continue
                    row = dict(row)
                    row["epoch_id"] = epoch_id
                    staged_rows.append(row)
                if fp == base:
                    new_off = f.tell()
        # Commit rows + cursor as one in-memory transaction. The caller's one atomic book dump
        # persists all four keys below, so restart resumes at the exact byte with the exact dedup.
        openpos.extend(staged_rows)
        ing = len(staged_rows)
        pf["maker_fill_wm"] = new_wm
        pf["maker_fill_off"] = new_off
        pf["maker_fill_seen"] = sorted(x for x in staged_seen if x > max(0, new_wm - GRACE))
        pf["maker_fill_head"] = head
    except Exception as e:
        print("  ML book: maker-fill ingest failed (%s) — retrying next cycle" % e)
    return ing


def _maker_fill_checkpoint(pf):
    """The complete restart cursor for the Go→Python maker-fill journal (R132 auditor 496/499)."""
    return {
        "maker_fill_wm": int(pf.get("maker_fill_wm", 0) or 0),
        "maker_fill_off": int(pf.get("maker_fill_off", 0) or 0),
        "maker_fill_seen": [int(x) for x in (pf.get("maker_fill_seen") or [])],
        "maker_fill_head": int(pf.get("maker_fill_head", 0) or 0),
    }


def _carry_book_epoch_contract(source, target):
    """Preserve the Go-owned reset/cohort boundary across a Python book rewrite.

    The sidecar rebuilds ``ml_paper.json`` from an explicit output allow-list each cycle. These
    fields are not derived state: Go stamps them atomically when the operator resets the book and
    every current-cohort reader uses them to exclude pre-reset/legacy lots. Copy only the three
    contract strings instead of merging arbitrary old keys, so a normal cycle cannot erase the
    boundary or accidentally resurrect unrelated stale state.
    """
    for key in ("epoch_id", "reset_at", "current_model_cohort",
                "settlement_repair_blocked", "settlement_repair_blocked_at",
                "settlement_epoch", "settlement_epoch_at",
                "settlement_legacy_archive_sha256", "settlement_legacy_excluded"):
        value = source.get(key)
        if isinstance(value, str) and value.strip():
            target[key] = value
        elif key == "settlement_legacy_excluded" and isinstance(value, bool):
            target[key] = value
    return target


_LIVE_BOOK = {}


def _live_px(db, picks):
    """R32 (operator: "fix it asap to be same exact price"): CURRENT side prices for picks from the
    suite's in-memory feeds (Kalshi WS / PolyUS book) via GET /api/livepx — so the book BUYS at the
    venue's live price, never the (possibly minutes-old) signal-log price. Best-effort: {} on any
    failure and the caller keeps its fallback.
    R67h: CHUNKED — the old [:200] cap silently priced only the top-200 preds, which (because the
    shadow book refuses to buy without a live price) capped the 'buys EVERY +EV candidate' book to
    top-N. Now every pick gets priced, 200 per request (URL-length bound)."""
    if not picks:
        return {}
    out = {}
    _LIVE_BOOK.clear()
    # R98 FIX (found live: EVERY pred went px_src=sig-stale after the restructure): the old
    # single try wrapped the WHOLE chunk loop, so ONE slow chunk threw away every already-priced
    # chunk AND all remaining ones — livemap={} for the entire cycle. Post-reset the pick set
    # skews to tickers outside the WS book set, so /api/livepx legitimately needs its REST-miss
    # batches (>3s per 200-chunk) and the 3s timeout tripped constantly. Now: per-chunk
    # isolation (a slow chunk costs only its own 200 picks) + an 8s timeout matching the
    # handler's own REST budget. Partial coverage is fine — unpriced picks mark sig-stale.
    try:
        import urllib.request
        import urllib.parse
    except Exception:
        return out
    # R99 bug 197 (auditor r25): the chunks run SERIAL, so the worst case was 8s × n/200 — one
    # slow venue window could stretch a cycle by minutes. Total budget 30s: plenty for the normal
    # all-cached case (ms/chunk), and a degraded venue now costs seconds, not the whole cadence.
    # Chunks beyond the budget stay unpriced → their picks mark sig-stale (the book refuses
    # blind buys anyway), and the next cycle retries with warm handler caches.
    _t0 = time.time()
    _skipped = 0
    for i0 in range(0, len(picks), 200):
        if time.time() - _t0 > 30.0:
            _skipped = len(picks) - i0
            break
        chunk = picks[i0:i0 + 200]
        try:
            q = ",".join("%s|%s|%s" % (p.get("platform") or "", p.get("ticker") or "", p.get("side") or "") for p in chunk)
            url = _suite_base(db) + "/api/mlbookpx?q=" + urllib.parse.quote(q, safe="")
            with urllib.request.urlopen(url, timeout=8) as resp:
                d = json.loads(resp.read().decode("utf-8"))
            if isinstance(d, dict):
                for key, snap in d.items():
                    if not isinstance(snap, dict) or snap.get("feature_schema") != BOOK_FEATURE_SCHEMA:
                        continue
                    try:
                        if int(snap.get("book_feature_version") or 0) != BOOK_FEATURE_VERSION:
                            continue
                        px = float(snap.get("taker_price") or 0)
                        fee = float(snap.get("taker_fee_pc") or 0)
                    except (TypeError, ValueError):
                        continue
                    if 0 < px < 1 and math.isfinite(fee):
                        out[key] = px
                        _LIVE_BOOK[key] = snap
        except Exception:
            continue
    if _skipped:
        print("  (_live_px: 30s budget hit — %d picks left unpriced this cycle, they rank sig-stale)" % _skipped)
    return out


def _board_rows(db):
    """R75: the suite's FULL tradeable board (kalshi + polyus, in-memory caches only) via
    GET /api/mlboard — the universe for the every-market scoring pass. Best-effort: [] on
    any failure (the cycle then just skips the board pass; signal scoring is untouched)."""
    try:
        import urllib.request
        with urllib.request.urlopen(_suite_base(db) + "/api/mlboard", timeout=15) as resp:
            d = json.loads(resp.read().decode("utf-8"))
        rows = d.get("rows") or []
        return rows if isinstance(rows, list) else []
    except Exception:
        return []


# R75 EVERY-MARKET SCORING: throttle state (the board is ~15-20k rows; once per ~5 min is plenty
# and keeps the 10s fast-rescore path light) + the last computed list so cycles between board
# passes republish it instead of dropping the section.
_BOARD = {"t": 0.0, "scores": []}


def _score_board(db, model, vocab, calib_brake):
    """R75: score EVERY open tradeable market (the /api/mlboard universe) with MARKET-CONTEXT
    features only — price, volume, book spread, momentum, kind, secs_to_start, venue, liveness;
    signal_type='none' (absent from the trained vocab → one-hots to all-zeros, the natural
    'no signal' encoding). Returns rows capped to +EV (fee-net) only, ranked by ev_net.
    SCORING ≠ PROPOSING: this feeds the market_scores display/log section and NOTHING else —
    preds / paper books / live proposals keep their own gated paths untouched."""
    # Book-v1 refuses the old full-board pseudo rows: /api/mlboard carries visible price/spread but
    # not an internally consistent side book, touch depth and exact fee receipt for every row.
    # Signal scoring remains complete; broad market-only scoring resumes only after that endpoint
    # publishes the same versioned contract. Never fill missing book state with legacy mids.
    return []
    board = _board_rows(db)
    if not board:
        return []
    now_utc = time.time()
    brows, meta = [], []
    for b in board:
        try:
            price = float(b.get("price") or 0)
        except Exception:
            continue
        plat = str(b.get("venue") or "")
        if price <= 0.01 or price >= 0.99 or plat not in TRADEABLE_PLATFORMS:
            continue
        rh = 0.0
        cts = str(b.get("close_ts") or "")
        if cts:
            try:
                from datetime import datetime as _dt
                rh = max(0.0, (_dt.fromisoformat(cts.replace("Z", "+00:00")).timestamp() - now_utc) / 3600.0)
            except Exception:
                rh = 0.0
        sts = b.get("secs_to_start")
        live = b.get("live")
        brows.append({
            # market-context features; every signal-only column stays at its sentinel/neutral value
            "ticker": b.get("ticker"), "side": "YES", "signal_type": "none", "platform": plat,
            "category": "", "market_type": "", "kind": str(b.get("kind") or ""),
            "title": str(b.get("title") or ""), "ts": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(now_utc)),
            "entry_price": price,
            "notional": float(b.get("volume_24h") or 0),  # $-activity magnitude (board volume)
            "spread_cents": float(b.get("spread_cents") or 0),
            "momentum": float(b.get("move_cents") or 0),
            "resolve_hours": rh,
            "secs_to_start": (float(sts) if isinstance(sts, (int, float)) else 10000000.0),
            "is_live": (float(live) if isinstance(live, (int, float)) else -1.0),
            "in_play": (float(live) if isinstance(live, (int, float)) else -1.0),
            "book_imb3": -1.0, "flow_ratio_15m": -1.0, "holders_hhi": -1.0, "score_margin": -1.0,
        })
        meta.append((plat, price))
    if not brows:
        return []
    try:
        Xb = featurize(brows, vocab)
        pb = model.predict_proba(Xb)[:, 1]
    except Exception as e:
        print("  (R75 board scoring skipped: %s)" % e)
        return []
    scores = []
    for r, (plat, price), pw in zip(brows, meta, pb):
        ev_net = (float(pw) - price) * calib_brake - _fee_pc(plat, price)
        if ev_net <= 0:
            continue  # spec: market_scores carries +EV rows only
        scores.append({"ticker": r["ticker"], "venue": plat, "side": "YES", "price": round(price, 3),
                       "p_win": round(float(pw), 3), "ev_net": round(ev_net, 4),
                       "kind": r["kind"], "title": r["title"][:140]})
    scores.sort(key=lambda x: -x["ev_net"])
    return scores[:500]


def _pxkey(p):
    return "%s|%s|%s" % (p.get("platform") or "", p.get("ticker") or "", p.get("side") or "")


def _fee_pc(platform, price, share=None):
    """Per-contract entry fee in $ at `price` (can be NEGATIVE on Poly US maker rebates)."""
    if price <= 0 or price >= 1:
        return 0.0
    return _fee_rate(platform, share) * price * (1 - price)


def _entry_fee(platform, contracts, price, share=None):
    """Total entry fee for a lot — the venue-true replacement for the flat 0.07 Kalshi formula."""
    return _fee_pc(platform, price, share) * contracts


_OPP_SIDE = {"YES": "NO", "NO": "YES", "UP": "DOWN", "DOWN": "UP"}


def _collapse_dual_sides(preds):
    """R99 ONE-SIDE-PER-MARKET at the PREDS layer (auditor r24 §3c: 429/1,407 tickers carried BOTH
    sides in one cycle, 124 pairs violated p_yes+p_no≈1 by >0.10, and flipped rows averaged
    NEGATIVE live EV — the picker was manufacturing EV out of calibration inconsistency between
    rows of the same market). Both sides stay SCORED (all_scored / shadow_cands are untouched);
    the ACTIONABLE list keeps, per (platform, ticker) market, only rows of ONE side: the side of
    the best-ranked row. The list arrives sorted by fee-net EV at the CURRENT price, so that side
    is exactly "the better fee-net EV at the live price" — and it still has to clear every border
    floor downstream before a single contract is bought. Same-side rows from other families stay
    (the book dedups (ticker, side) itself). Returns (kept, dual_dropped, pair_sum_violations)."""
    won, pair_p = {}, {}
    kept, dropped = [], 0
    for x in preds:
        mk = (x.get("platform") or "", x.get("ticker") or "")
        sd = (x.get("side") or "").strip().upper()
        b = pair_p.setdefault(mk, {})
        if sd in ("YES", "UP"):
            b.setdefault("y", float(x.get("p_win") or 0))
        elif sd in ("NO", "DOWN"):
            b.setdefault("n", float(x.get("p_win") or 0))
        w = won.get(mk)
        if w is None:
            won[mk] = sd
            kept.append(x)
        elif sd == w:
            kept.append(x)
        else:
            dropped += 1  # opposite side of a market whose better-priced side ranks above it
    viol = sum(1 for b in pair_p.values() if "y" in b and "n" in b and abs(b["y"] + b["n"] - 1.0) > 0.10)
    return kept, dropped, viol


def _pick_trade_side(sd, price, pw, plat, brake=1.0):
    """R97 BOTH-SIDES PICKER (operator: "why is ml buying a bunch of yes's but not a lot of nos").

    Signals are stored side-canonical (R90 made the producers largely YES-form), and the actionable
    preds list used to carry ONLY the signal's side — so whenever the model favored the OPPOSITE
    outcome (p_win < 0.5) the pick died at the book's p_win floor instead of flipping to the side
    the model actually likes (315 such invisible candidates in one live prediction file, measured
    2026-07-06). For a binary market the complement is exact: p(opp) = 1-p, price(opp) = 1-price.
    Returns (side, price, p_win, ev_gross, ev_net, flipped) for whichever side has the better
    fee-NET edge (ties keep the signal side; the calibration brake applies symmetrically).
    Non-binary/degenerate side labels pass through untouched — never guess a complement for them.
    """
    pw = float(pw)
    ev = (pw - price) * brake
    ev_net = ev - _fee_pc(plat, price)
    opp = _OPP_SIDE.get((sd or "").strip().upper(), "")
    if not opp or not (0.0 < price < 1.0):
        return sd, price, pw, ev, ev_net, False
    o_price = 1.0 - price
    o_pw = 1.0 - pw
    o_ev = (o_pw - o_price) * brake
    o_ev_net = o_ev - _fee_pc(plat, o_price)
    if o_ev_net > ev_net:
        return opp, o_price, o_pw, o_ev, o_ev_net, True
    return sd, price, pw, ev, ev_net, False


def load_maker_share(db, default=1.0):
    """Read fee_maker_share from the suite's config.json (nested under "auto") so both sides
    blend fees identically."""
    try:
        cfg = _config_path(db)
        c = _json_read(cfg)  # R80: utf-8 + BOM/NUL-tolerant (was cp1252-default open + strict json.load)
        for v in (c.get("fee_maker_share"), (c.get("auto") or {}).get("fee_maker_share")):
            if isinstance(v, (int, float)) and 0 <= v <= 1:
                return float(v)
    except Exception:
        pass
    return default


def load_implausibility_guard(db, default=False):
    """R79: read ml_implausibility_guard from the suite's config.json (nested under "auto") — the
    same settings handshake as load_maker_share/load_max_hours, but refreshed EVERY cycle (see
    cycle()) so the Settings toggle applies without a sidecar restart. Default False = the
    implausibility guard is OFF (operator: remove the rule; one click restores it)."""
    try:
        cfg = _config_path(db)
        c = _json_read(cfg)  # R80: utf-8 + BOM/NUL-tolerant (was cp1252-default open + strict json.load)
        for v in (c.get("ml_implausibility_guard"), (c.get("auto") or {}).get("ml_implausibility_guard")):
            if isinstance(v, bool):
                return v
            if isinstance(v, (int, float)):
                return v != 0
    except Exception:
        pass
    return default


def load_decay_half_life(db, default=14.0):
    """R114 (operator/Gemini idea, data-picked default): exponential recency sample weighting for
    training — w = 0.5 ** (age_days / half_life). Config key ml_decay_half_life_days (top-level or
    under "auto"), hot-read like the other knobs. Offline comparison on the 07-08 DB copy
    (301,227 resolved, chronological 70/30): none AUC .7750/Brier .1903 · hl=30 .7757/.1901 ·
    hl=14 .7769/.1895 (best) · hl=7 .7756/.1902 · hl=1 .7712/.1928 (too aggressive). Default 14;
    an EXPLICIT 0 disables weighting. Re-tune as history grows past the current ~16-day span."""
    try:
        cfg = _config_path(db)
        c = _json_read(cfg)
        for v in (c.get("ml_decay_half_life_days"), (c.get("auto") or {}).get("ml_decay_half_life_days")):
            if isinstance(v, (int, float)) and not isinstance(v, bool):
                return min(max(float(v), 0.0), 365.0)
    except Exception:
        pass
    return default


def _decay_weights(res, half_life_days):
    """R114: per-row recency weights over the ts-sorted resolved rows; None when disabled or
    timestamps are unusable (fit falls back to unweighted). Scale-invariant, so one vector serves
    both the walk-forward train slice and the full final fit."""
    if not half_life_days or half_life_days <= 0:
        return None
    try:
        def _pts(v):
            if isinstance(v, (int, float)) and not isinstance(v, bool):
                return float(v)
            try:
                from datetime import datetime
                return datetime.fromisoformat(str(v).replace("Z", "+00:00")).timestamp()
            except Exception:
                return 0.0
        ts = np.array([_pts(r.get("ts", 0)) for r in res], dtype=float)
        if (ts > 0).sum() < len(ts) * 0.9:
            return None
        t0 = ts[ts > 0].max()
        age_days = np.clip((t0 - ts) / 86400.0, 0.0, None)
        return np.power(0.5, age_days / float(half_life_days))
    except Exception:
        return None


def load_min_p_win(db, default=0.50):
    """R84: read ml_min_p_win from the suite's config.json (top-level or nested under "auto") — the
    same handshake family as load_implausibility_guard, refreshed EVERY cycle (see cycle()) so the
    Settings knob applies without a sidecar restart. Absent key = the 0.50 default (the Go side's
    Default() writes 0.50 too); an EXPLICIT 0 disables the floor. Values clamp to [0, 0.99]."""
    try:
        cfg = _config_path(db)
        c = _json_read(cfg)  # utf-8 + BOM/NUL-tolerant, like every other config read here
        for v in (c.get("ml_min_p_win"), (c.get("auto") or {}).get("ml_min_p_win")):
            if isinstance(v, (int, float)) and not isinstance(v, bool):
                return min(max(float(v), 0.0), 0.99)
    except Exception:
        pass
    return default


def load_ml_borders(db):
    """R90 ML BORDERS (operator ask): read ml_pwin_min/ml_pwin_max/ml_ev_min_cents/ml_ev_max_cents
    from the suite's config.json (top-level or under "auto"), refreshed EVERY cycle like
    ml_min_p_win. Absent keys inherit the Go Default() values (0.05/0.95/2/20); an explicit 0
    disables that single border."""
    out = {"pwin_min": 0.05, "pwin_max": 0.95, "ev_min_c": 2.0, "ev_max_c": 20.0}
    try:
        cfg = _config_path(db)
        c = _json_read(cfg)
        a = (c.get("auto") or {})
        for key, k2 in (("pwin_min", "ml_pwin_min"), ("pwin_max", "ml_pwin_max"),
                        ("ev_min_c", "ml_ev_min_cents"), ("ev_max_c", "ml_ev_max_cents")):
            for v in (c.get(k2), a.get(k2)):
                if isinstance(v, (int, float)) and not isinstance(v, bool):
                    out[key] = float(v)
                    break
    except Exception:
        pass
    return out


def load_realization(db):
    """R90 edge 29: (ratios, enabled) — the Go side's per-family realization ratios
    (data/ml_realization.json beside the DB) + the realization_haircut config flag. {} / True
    defaults when absent (ratio 1 = no haircut; flag default matches the Go Default())."""
    ratios, enabled = {}, True
    try:
        d = _json_read(os.path.join(os.path.dirname(os.path.abspath(db)), "ml_realization.json"))
        r = d.get("ratios")
        if isinstance(r, dict):
            ratios = {str(k): float(v) for k, v in r.items()
                      if isinstance(v, (int, float)) and not isinstance(v, bool)}
    except Exception:
        pass
    try:
        cfg = _config_path(db)
        c = _json_read(cfg)
        for v in (c.get("realization_haircut"), (c.get("auto") or {}).get("realization_haircut")):
            if isinstance(v, bool):
                enabled = v
                break
            if isinstance(v, (int, float)):
                enabled = v != 0
                break
    except Exception:
        pass
    return ratios, enabled


def _quarantine(datadir, x, live_px, ev_net):
    """R90: append a too-good pick to data/ml_quarantine.jsonl — the reviewable quarantine list
    (the Go gates mirror this into the audit trail under category "mlquarantine")."""
    try:
        with open(os.path.join(datadir, "ml_quarantine.jsonl"), "a", encoding="utf-8") as f:
            f.write(json.dumps({"ts": int(time.time()), "ticker": x.get("ticker"), "side": x.get("side"),
                                "signal_type": x.get("signal_type"), "p_win": x.get("p_win"),
                                "live_px": round(float(live_px), 4), "ev_net": round(float(ev_net), 4)}) + "\n")
    except Exception:
        pass


def load_ml_bank(datadir, default=None):
    """R78 EQUITY-FRACTION BANK (operator: "kalshi 1/4, polyus 1/4, ml 1/2 of total equity, AT ALL
    TIMES, compounding"): read the Go side's ml_alloc.json handshake — {"ml_bank_usd": alloc_ml x
    total paper equity (NAV)} — refreshed ~30s by the suite's equity sweep (the same handshake
    family as load_maker_share/load_max_hours, but a data-dir file because NAV is dynamic, not a
    config constant). Returns None when the file is absent/unreadable, which keeps the legacy $250
    anchor. A stale-but-present file still wins over the flat fallback: the last-known allocation
    beats snapping back to $250 mid-run when the suite is briefly down."""
    try:
        d = _json_read(os.path.join(datadir, "ml_alloc.json"))  # R80: utf-8, never the cp1252 default
        v = d.get("ml_bank_usd")
        if isinstance(v, (int, float)) and v > 0:
            return float(v)
    except Exception:
        pass
    return default


def load_ml_venue_banks(datadir, total_bank=None):
    """Return the two funded New-ML Paper sleeves without minting extra money.

    ``ml_bank_usd`` remains the one durable ML grant.  The Go allocator now also writes
    ``ml_venue_banks`` whose Kalshi + PolyUS values add up to that grant.  Old handshakes and
    hermetic tests fall back to an exact 50/50 split of the total; malformed/over-allocated maps
    fail back to the same split instead of letting two venue sleeves each spend the full bank.
    """
    if total_bank is None:
        total_bank = load_ml_bank(datadir)
    try:
        total_bank = float(total_bank)
    except (TypeError, ValueError):
        total_bank = 0.0
    if not math.isfinite(total_bank) or total_bank <= 0:
        return {}
    fallback_k = round(total_bank / 2.0, 2)
    fallback = {"kalshi": fallback_k, "polyus": round(total_bank - fallback_k, 2)}
    try:
        d = _json_read(os.path.join(datadir, "ml_alloc.json"))
        raw = d.get("ml_venue_banks") or {}
        k, p = float(raw.get("kalshi")), float(raw.get("polyus"))
        if (not math.isfinite(k) or not math.isfinite(p) or k < 0 or p < 0
                or abs((k + p) - total_bank) > 0.011):
            return fallback
        return {"kalshi": round(k, 2), "polyus": round(p, 2)}
    except Exception:
        return fallback


def _money_round(value):
    """Round booked dollars exactly like Go's math.Round(v*100)/100 (half away from zero)."""
    value = float(value or 0.0)
    cents = math.floor(abs(value) * 100.0 + 0.5)
    return math.copysign(cents / 100.0, value) if cents else 0.0


def _ml_venue_maps(life, key):
    current = life.get("venue_" + key)
    base = life.get("venue_" + key + "_base")
    return current, base, isinstance(current, dict) and isinstance(base, dict)


def _repair_ml_venue_lifetime_from_complete_epoch(life, closed):
    """Repair a stale venue split only when current-epoch detail is provably complete."""
    net, net_base, net_ok = _ml_venue_maps(life, "net")
    counts, count_base, count_ok = _ml_venue_maps(life, "closed")
    contracts, contract_base, contract_ok = _ml_venue_maps(life, "contracts")
    if not net_ok or not count_ok or not contract_ok:
        return False
    if "net" not in life or "closed" not in life:
        return False
    try:
        expected_raw = float(life["closed"]) - float(life.get("closed_base", 0.0) or 0.0)
        epoch_total = float(life["net"]) - float(life.get("net_base", 0.0) or 0.0)
    except (TypeError, ValueError):
        return False
    if expected_raw < 0 or abs(expected_raw - round(expected_raw)) > 1e-6:
        return False
    cut = 0
    for i, row in enumerate(closed or []):
        if isinstance(row, dict) and row.get("reset_close"):
            cut = i + 1
    detail = {venue: 0.0 for venue in TRADEABLE_PLATFORMS}
    detail_counts = {venue: 0.0 for venue in TRADEABLE_PLATFORMS}
    detail_contracts = {venue: 0.0 for venue in TRADEABLE_PLATFORMS}
    observed = 0
    for row in (closed or [])[cut:]:
        if not isinstance(row, dict) or row.get("reset_close"):
            continue
        venue = str(row.get("platform") or "").lower()
        if venue not in detail or not isinstance(row.get("pnl"), (int, float)):
            return False
        pnl = float(row["pnl"])
        if not math.isfinite(pnl):
            return False
        detail[venue] = _money_round(detail[venue] + pnl)
        detail_counts[venue] += 1.0
        try:
            detail_contracts[venue] += float(row.get("contracts") or 0.0)
        except (TypeError, ValueError):
            return False
        observed += 1
    if observed != int(round(expected_raw)):
        return False
    if abs(sum(detail.values()) - epoch_total) > 0.015:
        return False
    needs_repair = any(
        (abs((float(net.get(v, 0.0) or 0.0) - float(net_base.get(v, 0.0) or 0.0)) - detail[v]) > 0.015
         or abs((float(counts.get(v, 0.0) or 0.0) - float(count_base.get(v, 0.0) or 0.0))
                - detail_counts[v]) > 1e-6
         or abs((float(contracts.get(v, 0.0) or 0.0) - float(contract_base.get(v, 0.0) or 0.0))
                - detail_contracts[v]) > 1e-6)
        for v in TRADEABLE_PLATFORMS)
    if not needs_repair:
        return False
    for venue in TRADEABLE_PLATFORMS:
        net[venue] = _money_round(float(net_base.get(venue, 0.0) or 0.0) + detail[venue])
        counts[venue] = float(count_base.get(venue, 0.0) or 0.0) + detail_counts[venue]
        contracts[venue] = float(contract_base.get(venue, 0.0) or 0.0) + detail_contracts[venue]
    life["venue_accounting_version"] = 3
    return True


def _ensure_ml_venue_lifetime(life, closed):
    """Migrate/preserve durable per-venue net counters inside the one ML lifetime ledger."""
    venues = TRADEABLE_PLATFORMS
    net, net_base, net_ok = _ml_venue_maps(life, "net")
    counts, count_base, count_ok = _ml_venue_maps(life, "closed")
    contracts, contract_base, contract_ok = _ml_venue_maps(life, "contracts")
    if net_ok and count_ok and contract_ok:
        for venue in venues:
            net[venue] = float(net.get(venue, 0.0) or 0.0)
            net_base[venue] = float(net_base.get(venue, 0.0) or 0.0)
            counts[venue] = float(counts.get(venue, 0.0) or 0.0)
            count_base[venue] = float(count_base.get(venue, 0.0) or 0.0)
            contracts[venue] = float(contracts.get(venue, 0.0) or 0.0)
            contract_base[venue] = float(contract_base.get(venue, 0.0) or 0.0)
        _repair_ml_venue_lifetime_from_complete_epoch(life, closed)
        return
    # One-time migration. The retained ring may not allocate ancient lifetime P&L, but the current
    # reset epoch is exact; every future close increments these durable counters before trimming.
    if not net_ok:
        net = {venue: 0.0 for venue in venues}
        net_base = {venue: 0.0 for venue in venues}
    counts = {venue: 0.0 for venue in venues}
    count_base = {venue: 0.0 for venue in venues}
    contracts = {venue: 0.0 for venue in venues}
    contract_base = {venue: 0.0 for venue in venues}
    for row in closed or []:
        venue = str(row.get("platform") or "").lower()
        if venue in net:
            if not net_ok:
                net[venue] = _money_round(net[venue] + float(row.get("pnl") or 0.0))
            counts[venue] += 1.0
            contracts[venue] += float(row.get("contracts") or 0.0)
        if row.get("reset_close"):
            if not net_ok:
                net_base = dict(net)
            count_base = dict(counts)
            contract_base = dict(contracts)
    life["venue_net"], life["venue_net_base"] = net, net_base
    life["venue_closed"], life["venue_closed_base"] = counts, count_base
    life["venue_contracts"], life["venue_contracts_base"] = contracts, contract_base
    life["venue_accounting_version"] = 3
    _repair_ml_venue_lifetime_from_complete_epoch(life, closed)


def _ml_venue_epoch_net(life):
    return _ml_venue_epoch_metric(life, "net")


def _ml_venue_epoch_metric(life, key):
    current = life.get("venue_" + key) or {}
    base = life.get("venue_" + key + "_base") or {}
    return {venue: float(current.get(venue, 0.0) or 0.0) - float(base.get(venue, 0.0) or 0.0)
            for venue in TRADEABLE_PLATFORMS}


def _ml_venue_life_add(life, venue, pnl, contracts=None):
    venue = str(venue or "").lower()
    if venue not in TRADEABLE_PLATFORMS:
        return
    net = life.setdefault("venue_net", {})
    net[venue] = _money_round(float(net.get(venue, 0.0) or 0.0) + _money_round(pnl))
    if contracts is not None:
        counts = life.setdefault("venue_closed", {})
        counts[venue] = float(counts.get(venue, 0.0) or 0.0) + 1.0
        units = life.setdefault("venue_contracts", {})
        units[venue] = float(units.get(venue, 0.0) or 0.0) + float(contracts or 0.0)


def _ml_venue_sleeves(openpos, epoch_closed, pending, venue_banks, venue_epoch_net=None,
                      venue_epoch_closed=None, venue_epoch_contracts=None):
    """Build the executable balance/exposure truth for each ML destination venue.

    The sleeve is an accounting/risk wall inside the single durable New-ML ledger, not a second
    bankroll.  Only current-epoch, actually placed lots contribute.  Pending maker posts reserve
    their destination sleeve.  Unknown/Poly-int rows are deliberately excluded from both funded
    sleeves and therefore cannot gain sizing authority.
    """
    out = {}
    for venue in TRADEABLE_PLATFORMS:
        grant = max(0.0, float(venue_banks.get(venue, 0.0) or 0.0))
        rows_open = [p for p in (openpos or []) if str(p.get("platform") or "").lower() == venue]
        rows_closed = [p for p in (epoch_closed or [])
                       if str(p.get("platform") or "").lower() == venue and not p.get("reset_close")]
        rows_pending = [p for p in (pending or []) if str(p.get("platform") or "").lower() == venue]
        net = (float(venue_epoch_net.get(venue, 0.0) or 0.0)
               if isinstance(venue_epoch_net, dict)
               else sum(float(p.get("pnl") or 0.0) for p in rows_closed))
        closed_n = (int(round(float(venue_epoch_closed.get(venue, 0.0) or 0.0)))
                    if isinstance(venue_epoch_closed, dict) else len(rows_closed))
        contract_n = (float(venue_epoch_contracts.get(venue, 0.0) or 0.0)
                      if isinstance(venue_epoch_contracts, dict)
                      else sum(float(p.get("contracts") or 0.0) for p in rows_closed))
        deployed = sum(float(p.get("price") or 0.0) * float(p.get("contracts") or 0.0)
                       + float(p.get("fee") or 0.0)
                       for p in rows_open)
        reserved = sum(float(p.get("post_px") or 0.0) * float(p.get("contracts") or 0.0)
                       + float(p.get("fee") or 0.0)
                       for p in rows_pending)
        equity = max(0.0, grant + net)
        # Each sleeve compounds independently: its own realized gains expand it and its own losses
        # shrink it. The other venue's P&L never enters this number.
        sizing = equity
        out[venue] = {
            "grant": round(grant, 2), "net": round(net, 2), "equity": round(equity, 2),
            "sizing_balance": round(sizing, 2),
            "deployed": round(deployed, 2), "reserved": round(reserved, 2),
            "available": round(max(0.0, sizing - deployed - reserved), 2),
            "open": len(rows_open), "closed": closed_n, "contracts": contract_n,
        }
    return out


def _ml_sleeve_order_dollars(sizing_balance, deployed, total_remaining, kelly_fraction,
                             drawdown_brake=1.0):
    """Bound one proposed order by its venue sleeve and the parent ML wall."""
    try:
        sizing_balance = max(0.0, float(sizing_balance))
        venue_remaining = max(0.0, sizing_balance - float(deployed))
        total_remaining = max(0.0, float(total_remaining))
        kelly_fraction = max(0.0, float(kelly_fraction))
        drawdown_brake = min(1.0, max(0.0, float(drawdown_brake)))
    except (TypeError, ValueError):
        return 0.0
    return max(0.0, min(sizing_balance * kelly_fraction * drawdown_brake,
                        0.10 * sizing_balance, venue_remaining, total_remaining))


def load_ml_epoch_anchor(datadir, default=None):
    """R106 (auditor r34, bug 280 migration): the epoch GRANT = alloc_ml x paper_total_start from
    the same handshake file — used once to repair a bank0 that the pre-R106 plug destroyed (the
    plug held bank0 == ml_bank_usd - net by construction; the true anchor is the grant)."""
    try:
        d = _json_read(os.path.join(datadir, "ml_alloc.json"))
        a, s = d.get("alloc_ml"), d.get("paper_total_start")
        if isinstance(a, (int, float)) and isinstance(s, (int, float)) and a > 0 and s > 0:
            return round(float(a) * float(s), 2)
    except Exception:
        pass
    return default


def _reanchor_bank0(bank0, incoming_bank):
    """R127 OPERATOR HANDOFF — upward-only bank re-anchor. Returns (new_bank0, delta).

    The R127 four-book restructure hands this book a FIXED bank via the handshake
    (ml_bank_usd = book_ml_usd, $600 per the R143 operator order) instead of the old alloc x NAV
    share. bank0 historically re-anchored ONLY at a Go reset, and reset_on_start is false —
    so a RAISED grant sat unused forever (sizing stayed clamped at min(alloc, old own
    equity)). This helper moves the anchor UP by exactly the raise: bank0 += delta —
    positions, closed history and the epoch net are all untouched (equity stays
    bank0 + net, so past losses are never erased). DOWNWARD changes keep the old
    conservative behavior: the sizing min() already clamps to the lower alloc and the
    anchor never shrinks itself. The $0.50 dead-band mirrors the Go writer's churn guard.
    Tested in ml/r127_bank_test.py."""
    try:
        if bank0 > 0 and incoming_bank is not None and float(incoming_bank) > bank0 + 0.5:
            delta = round(float(incoming_bank) - bank0, 2)
            return round(bank0 + delta, 2), delta
    except Exception:
        pass
    return bank0, 0.0


def _book_equity_peak(stored_peak, bank0, book_curve):
    """Keep the drawdown high-water mark anchored at least to the epoch's starting bank.

    A legacy or interrupted file can carry eq_peak below bank0. Accepting that value understates
    drawdown and can disable the brake after losses. The repair is upward-only and does not change
    P&L, positions, sizing authority, or any research label.
    """
    vals = []
    for value in (stored_peak, bank0, book_curve):
        try:
            value = float(value)
            if math.isfinite(value):
                vals.append(value)
        except (TypeError, ValueError):
            pass
    return max(vals) if vals else 0.0


def load_shadow_enabled(datadir):
    """R98 SHADOW RETIREMENT (operator decision): the Go side stamps shadow_enabled into the same
    ml_alloc.json handshake. False = stop OPENING new shadow lots (settlement of existing lots and
    every display path keep running so the book winds down naturally). Absent file/key = True —
    old suites and hermetic tests keep the pre-R98 behavior."""
    try:
        d = _json_read(os.path.join(datadir, "ml_alloc.json"))
        v = d.get("shadow_enabled")
        if isinstance(v, bool):
            return v
    except Exception:
        pass
    return True


def load_auto_open(datadir):
    """R99 CROSS-BOOK HEDGE BLOCK (both-sides redesign): the Go equity sweep stamps the auto paper
    books' OPEN (platform, ticker, side) lots into the same ml_alloc.json handshake (auto_open).
    The ML book refuses the OPPOSITE side of anything a sibling book holds — the books share one
    bankroll, so a cross-book YES+NO is the same guaranteed fees+spread loss as an in-book hedge.
    Absent file/key (old suite, hermetic tests) = empty set = no cross-book blocking."""
    out = set()
    try:
        d = _json_read(os.path.join(datadir, "ml_alloc.json"))
        for r in d.get("auto_open") or []:
            if not isinstance(r, dict):
                continue
            tk = r.get("ticker") or ""
            if tk:
                out.add(((r.get("platform") or ""), tk, (r.get("side") or "").strip().upper()))
    except Exception:
        pass
    return out
CATS = ["signal_type", "platform", "category", "side", "book_source",
        "market_type",  # TYPE LOGGING (audit Q3): granular market type — spreads/totals/props stop being invisible
        "kind"]  # R72-B SUB-MARKETS: stable 5-value coarse kind (winner/total/spread/prop/unknown) — categorical vocab handles it like signal_type (no NUM one-hot needed); pre-R72 rows one-hot as "" until re-logged
# TRADEABLE: the ML paper book only opens positions on US-legal, actually-tradeable venues. Poly-int
# ("polymarket") markets are still scored + shown (for research) but never paper-traded by the book.
TRADEABLE_PLATFORMS = ("kalshi", "polyus")
# R166: predictions and settlement keep running, but the old book-native portfolio cannot open a
# new modeled fill. It becomes eligible again only after it shares the delayed two-book executor.
ML_PAPER_NEW_ENTRIES_ENABLED = False
# MLSOON: the paper book only opens markets resolving within this many hours (bets "ending soonish" —
# no months-out World Cup Final / Game-of-the-Year holds). Overridden at startup from config.json so the
# Paper New ML uses its dedicated 24h/6h collection settings; LIVE remains 4h/2h.
MAX_RESOLVE_HOURS = 24.0  # Paper New ML collection; every LIVE handoff still rechecks <=4h
MAX_CRYPTO_RESOLVE_HOURS = 6.0  # Paper New ML collection; every LIVE handoff still rechecks <=2h
_NCYCLE = 0  # CALIBEXP throttle: run the heavy isotonic A/B + residual experiment only every Nth cycle


def load_max_hours(db, default=24.0):
    """Read the dedicated Paper New-ML regular horizon, capped at 24h."""
    try:
        cfg = _config_path(db)
        c = _json_read(cfg)  # R80: utf-8 + BOM/NUL-tolerant (was cp1252-default open + strict json.load)
        auto = c.get("auto") or {}  # the suite nests these under "auto" — the top-level read always missed them
        for k in ("paper_ml_max_hours_out", "ml_max_hours"):
            for v in (c.get(k), auto.get(k)):
                if isinstance(v, (int, float)) and v > 0:
                    return min(24.0, float(v))
    except Exception:
        pass
    return min(24.0, default)


def load_crypto_max_hours(db, default=6.0):
    """Read the dedicated Paper New-ML crypto horizon, capped at 6h."""
    try:
        c = _json_read(_config_path(db))
        auto = c.get("auto") or {}
        for k in ("paper_ml_crypto_max_hours_out", "ml_crypto_max_hours"):
            for v in (c.get(k), auto.get(k)):
                if isinstance(v, (int, float)) and v > 0:
                    return min(6.0, float(v))
    except Exception:
        pass
    return min(6.0, default)


def _is_crypto_contract(row):
    """Use venue category and ticker grammar, never a raw title substring.

    Raw substring matching called Marina BasSOLs and Solary Eclipse crypto because their names
    contain ``SOL``. Short symbols count only as ticker/slug tokens (or Kalshi's KX prefix).
    """
    category = str(row.get("category") or "").strip().lower().replace("_", "-")
    if category in ("crypto", "cryptocurrency", "digital-assets", "digital-asset"):
        return True
    ticker = str(row.get("ticker") or "").strip().upper()
    normalized = "".join(ch if ch.isalnum() else " " for ch in ticker)
    tokens = set(normalized.split())
    short = {"BTC", "ETH", "SOL", "DOGE", "XRP", "BNB", "HYPE"}
    if tokens & short:
        return True
    compact = "".join(ch for ch in ticker if ch.isalnum())
    if any(compact.startswith("KX" + coin) for coin in short):
        return True
    text = (ticker + " " + str(row.get("title") or "")).upper()
    words = set("".join(ch if ch.isalnum() else " " for ch in text).split())
    return bool(words & {"BITCOIN", "ETHEREUM", "ETHER", "SOLANA", "DOGECOIN", "CRYPTO", "CRYPTOCURRENCY"})


def _resolve_horizon_limit(row):
    return MAX_CRYPTO_RESOLVE_HOURS if _is_crypto_contract(row) else MAX_RESOLVE_HOURS


def time_feats(ts):
    """Cyclical time-of-day (sin/cos so 23:59≈00:00) + day-of-week from the signal timestamp —
    lets the model learn intraday / weekday patterns (crypto windows, game schedules)."""
    try:
        dt = datetime.fromisoformat(str(ts).replace("Z", "")[:19])
        h = dt.hour + dt.minute / 60.0
        return (math.sin(2 * math.pi * h / 24), math.cos(2 * math.pi * h / 24), float(dt.weekday()))
    except Exception:
        return (0.0, 0.0, 0.0)


# Title keyword flags — let the model see the market SUBTYPE (asset / sport / politics / threshold wording)
# that the coarse category one-hot misses. Cheap, pure-text, no schema change.
TITLE_KW = ["bitcoin", "btc", "eth", "ethereum", "solana", "sol", "doge", "xrp", "crypto", "price",
            "nfl", "nba", "mlb", "nhl", "soccer", "tennis", "golf", "ufc", "f1", "cup", "league",
            "game", "match", "vs", "halftime", "draw", "shutout", "goal",
            "trump", "election", "president", "senate", "governor", "fed", "rate", "cpi", "gdp",
            "above", "below", "or more", "reach", "between", "win", "by ",
            # audit Q3: the vocabulary of exactly the market types in question was MISSING
            "over", "under", "spread", "total", "score", "scorer", "corner"]


def extra_feats(r):
    """Derived numerics the model can't easily get from raw columns: log-scaled flow $, distance from a
    coin-flip, a few price interactions, + title keyword flags. All from already-logged data."""
    def f(k):
        try:
            return float(r.get(k) or 0)
        except Exception:
            return 0.0
    # Book-v1 derives every price interaction from the executable taker ask. The old visible
    # signal price remains one explicitly named raw feature, never the primary price.
    p = f("book_taker_price")
    notl, strg, imb, mom, rh = f("notional"), f("strength"), f("imbalance"), f("momentum"), f("resolve_hours")
    sp, dep, conc, tc = f("spread_cents"), f("book_depth"), f("concentration"), f("trader_count")
    out = [math.log1p(max(0.0, notl)), abs(p - 0.5), p * strg, p * imb, mom * rh,
           # MLFEATS (B): more derived signal from already-logged columns
           p * (1 - p),                        # price uncertainty (max at 50¢)
           min(p, 1 - p),                      # longshot-ness (distance to nearest extreme)
           math.log1p(max(0.0, rh)),           # log resolve-horizon (compress long tails)
           mom * imb,                          # momentum × book imbalance (aligned pressure)
           strg * math.log1p(max(0.0, notl)),  # conviction × size
           sp / 100.0, 1.0 if (0 < sp <= 3) else 0.0,  # spread + tight-book flag (liquidity regime)
           math.log1p(max(0.0, dep)),          # log book depth (liquidity)
           tc * conc]                          # agreeing-traders × concentration (consensus strength)
    # LADDER DISTANCE (audit Q3): for threshold tickers (…-T60999.99 / …-B63125), the distance from
    # spot to strike is the single most informative number — `underlying` alone was bimodal noise.
    # dist = ln(spot/strike), clamped; 0 for non-threshold markets (harmless constant).
    dist = 0.0
    tk = str(r.get("ticker") or "")
    u = f("underlying")
    if u > 0:
        for sep in ("-T", "-B"):
            i = tk.rfind(sep)
            if i > 0:
                try:
                    strike = float(tk[i + 2:])
                    if strike > 0:
                        dist = max(-1.0, min(1.0, math.log(u / strike)))
                except ValueError:
                    pass
                break
    out.append(dist)
    t = str(r.get("title") or "").lower()
    out += [1.0 if kw in t else 0.0 for kw in TITLE_KW]
    return out


def path_feats(s):
    """slope, vol, range, len, trend-strength from the price_path sparkline — models the signal's
    MOVEMENT + its REGIME (G17): trend = |slope|/vol, high ⇒ trending, low ⇒ choppy/mean-reverting."""
    try:
        a = [float(x) for x in json.loads(s) if x is not None]
    except Exception:
        a = []
    if len(a) < 2:
        return (0.0, 0.0, 0.0, 0.0, 0.0)
    df = np.diff(a)
    slope = a[-1] - a[0]
    vol = float(np.std(df))
    rng = float(max(a) - min(a))
    trend = abs(slope) / (vol + 1e-6)  # MLFEATS/G17 regime proxy
    return (slope, vol, rng, float(len(a)), trend)


# LEAN COLUMNS (R10 — the "4-minute ML warmup"): SELECT * dragged price_path/post_path — ~560MB of
# text across 620k rows that the model NEVER reads — into Python dicts every cycle. Explicit
# columns cut the load from minutes to seconds. Keep this list in sync with featurize()/CATS.
_LEAN_COLS = ("id, ts, day, slot, platform, ticker, title, side, signal_type, entry_price, strength, "
              "notional, best_rank, trader_count, trader_pnl, concentration, momentum, spread_cents, "
              "resolve_hours, imbalance, underlying, category, resolved, won, resolved_at, "
              # R122 (auditor 458): price_path IS loaded now — featurize() called path_feats() on a
              # column this SELECT never fetched, so the G17 movement family (path_0..4) was
              # silently constant-zero. The R10-era "560MB drag" is gone: measured on the 07-09 DB
              # copy, price_path is 6,904 rows / 0.46 MiB total (sparse by design — only
              # path-stamping producers fill it), so the honest fix is to fetch it.
              "COALESCE(price_path,'') AS price_path, "
              "COALESCE(confidence,0) AS confidence, COALESCE(book_depth,0) AS book_depth, "
              "COALESCE(fill_price,0) AS fill_price, COALESCE(settle_val,-1) AS settle_val, "
              "COALESCE(trader_skill,0) AS trader_skill, COALESCE(market_type,'') AS market_type, "
              "COALESCE(is_live,-1) AS is_live, "  # R38: live-vs-pre feature
              # R65 sentinels (columns are NULL on pre-R65 rows by design — trees split the
              # sentinels off as labeled rows accrue):
              #   moms/dists/depth3 -> 0: the neutral prior (flat move / at-the-extreme / no depth),
              #     matching the schema's legacy 0-defaults (book_depth, momentum, ...);
              #   book_imb3 -> -1: it's a 0..1 ratio, so unknown must be OUT-OF-BAND (0 would read
              #     "all-ask book" and 0.5 "perfectly balanced" — both meaningful);
              #   secs_to_start -> 1e7 (~116 days): "no start on the horizon", keeping the
              #     meaningful pre-start/in-play boundary at 0 clean (negative = underway).
              "COALESCE(book_imb3,-1) AS book_imb3, COALESCE(book_depth3,0) AS book_depth3, "
              "COALESCE(mom_1h,0) AS mom_1h, COALESCE(mom_4h,0) AS mom_4h, "
              "COALESCE(dist_hi,0) AS dist_hi, COALESCE(dist_lo,0) AS dist_lo, "
              "COALESCE(secs_to_start,10000000) AS secs_to_start, "
              # R67l taker-flow sentinels: the ratio is a 0..1 share → unknown must be OUT-OF-BAND
              # (-1, mirroring book_imb3); the count's neutral prior is 0 trades.
              "COALESCE(flow_ratio_15m,-1) AS flow_ratio_15m, COALESCE(flow_n_15m,0) AS flow_n_15m, "
              # R70-B sentinels: holders_hhi is a 0..1 ratio → -1 out-of-band (0 would read "perfectly
              # dispersed crowd"); holders_skill is a signed lean → 0 neutral prior; in_play mirrors
              # is_live's -1 unknown; score_margin -1 (0 = genuinely tied); oi_delta 0 = flat prior.
              "COALESCE(holders_hhi,-1) AS holders_hhi, COALESCE(holders_skill,0) AS holders_skill, "
              "COALESCE(in_play,-1) AS in_play, COALESCE(score_margin,-1) AS score_margin, "
              "COALESCE(oi_delta_1h,0) AS oi_delta_1h, "
              # R114: decision-price age (s); -1 out-of-band = unmeasured (0 would read "perfectly fresh")
              "COALESCE(px_age_s,-1) AS px_age_s, "
              # R133 book-v1 fields stay nullable except the version. Never reconstruct these
              # from entry_price/spread or fill_price: the exclusive cohort filter below owns
              # eligibility and preserves legacy rows as version 0.
              "COALESCE(book_feature_ver,0) AS book_feature_ver, "
              "book_bid AS book_maker_price, book_ask AS book_taker_price, "
              "book_bid_depth AS book_maker_depth, book_ask_depth AS book_taker_depth, "
              "book_quote_age_s, book_maker_tick, book_taker_tick, "
              "book_maker_fee_pc, book_taker_fee_pc, book_latency_ms, "
              "COALESCE(book_source,'') AS book_source, "
              # R72-B: coarse market kind (winner/total/spread/prop/unknown) — a CATS one-hot like
              # signal_type. Pre-R72 rows COALESCE to '' (their own vocab bucket) until re-logged.
              "COALESCE(kind,'') AS kind, "
              # R138 prospective provenance. Current canonical game identity wins; the catalog's
              # venue event key is the structural fallback; exact ticker isolation is applied in
              # Python when neither mapping exists.
              "COALESCE(label_version,'') AS label_version, "
              "COALESCE(pricing_version,'') AS pricing_version, "
              "COALESCE((SELECT event_id FROM research_instrument_specs ris WHERE ris.venue=signal_log.platform AND ris.ticker=signal_log.ticker ORDER BY ris.version DESC LIMIT 1),"
              "(SELECT 'sports:' || game_id FROM market_game mg WHERE mg.venue=signal_log.platform AND mg.ticker=signal_log.ticker LIMIT 1),'') AS canonical_event_id, "
              "COALESCE((SELECT event_key FROM market_catalog mc WHERE mc.venue=signal_log.platform AND mc.ticker=signal_log.ticker LIMIT 1),'') AS catalog_event_key")


# R24 (operator): poly-int is READ-ONLY research — we can't trade it, so the model must not train
# on it or spend cycles scoring it. Excluding it at the SQL layer keeps the DB intact (bridges and
# venue-signal research still read poly-int rows through their own queries) while the ML sees a
# tradeable-only world. Also ~halves the training set → faster retrains.
_ML_PLATFORM_FILTER = (" WHERE platform != 'polymarket' AND book_feature_ver = 1"
                       " AND pricing_version='book-native-v2'"
                       " AND label_version='kalshi_start_clock_v2'"
                       " AND (platform!='kalshi' OR secs_to_start IS NOT NULL)")


_BOOK_REQUIRED = ("book_maker_price", "book_taker_price", "book_maker_depth", "book_taker_depth",
                  "book_quote_age_s", "book_maker_tick", "book_taker_tick",
                  "book_maker_fee_pc", "book_taker_fee_pc")
_BOOK_SOURCES = ("kalshi-ws-depth", "polyus-ws-full")


def _finite_number(v):
    try:
        return math.isfinite(float(v))
    except (TypeError, ValueError):
        return False


def _is_book_native(r):
    """True only for a complete R133 book-v1 observation. Strict version equality is deliberate:
    a future schema must opt in with its own model instead of silently entering this cohort."""
    try:
        if int(r.get("book_feature_ver") or 0) != BOOK_FEATURE_VERSION:
            return False
    except (TypeError, ValueError):
        return False
    if (r.get("pricing_version") or "") != BOOK_FEATURE_SCHEMA:
        return False
    if (r.get("label_version") or "") != ML_LABEL_VERSION:
        return False
    if (r.get("platform") or "").lower() not in TRADEABLE_PLATFORMS:
        return False
    if (r.get("book_source") or "") not in _BOOK_SOURCES:
        return False
    if not all(_finite_number(r.get(k)) for k in _BOOK_REQUIRED):
        return False
    try:
        legacy = float(r.get("entry_price") or 0)
        bid, ask = float(r["book_maker_price"]), float(r["book_taker_price"])
        bd, ad = float(r["book_maker_depth"]), float(r["book_taker_depth"])
        age = float(r["book_quote_age_s"])
        mt, tt = float(r["book_maker_tick"]), float(r["book_taker_tick"])
    except (TypeError, ValueError, KeyError):
        return False
    if not (0 < legacy < 1 and 0 < bid < ask < 1 and bd > 0 and ad > 0 and age >= 0 and
            0 < mt <= 0.10 and 0 < tt <= 0.10):
        return False
    # The v2 Kalshi label contract is start-clock first. Future events cannot be live; a started
    # event may be live or unknown depending on fresh activity, but never fabricated PRE history.
    if (r.get("platform") or "").lower() == "kalshi":
        try:
            sts = float(r.get("secs_to_start"))
            live = int(r.get("is_live"))
        except (TypeError, ValueError):
            return False
        if sts > 0 and live != 0:
            return False
        if sts <= 0 and live not in (-1, 1):
            return False
    return True


def _book_native_rows(rows):
    """Exclusive cohort selector. Filter BEFORE dedup so an early legacy row cannot hide a later
    book-v1 observation with the same (ticker, side, family) key."""
    return [r for r in rows if _is_book_native(r)]


def _publish_book_warming(db, resolved_n, open_n, min_train, raw_n=0,
                          split_receipt=None, reason=""):
    """Fresh fail-closed sidecar receipt while the new cohort accrues. Consumers see zero picks,
    and explicit authority=false; the old bundle/export can remain on disk as labeled history but
    cannot be loaded as book-v1."""
    out = {
        "generated_at": int(time.time()),
        "model_status": "WARMING_SPLIT" if reason else "WARMING",
        "model_cohort": BOOK_FEATURE_SCHEMA,
        "forecast_source": "ml-book-v2-calibrated",
        "model_version": "warming",
        "feature_schema": BOOK_FEATURE_SCHEMA,
        "book_feature_version": BOOK_FEATURE_VERSION,
        "book_v2_resolved": int(resolved_n),
        "book_v2_open": int(open_n),
        "book_v2_raw": int(raw_n),
        # Compatibility aliases for pre-R143 readers.  New code uses book_v2_* exclusively.
        "book_v1_resolved": int(resolved_n),
        "book_v1_open": int(open_n),
        "book_v1_raw": int(raw_n),
        "n_resolved": int(resolved_n),
        "min_train_required": int(min_train),
        "authority": False,
        "execution_enabled": False,
        "paper_authority": False,
        "live_authority": False,
        "actionable_predictions": 0,
        "warmup_remaining": max(0, int(min_train) - int(resolved_n)),
        "metric_scope": "none", "model_backend": "warming",
        "oos_auc": None, "oos_brier": None, "oos_log_loss": None, "oos_ece": None,
        "provisional_auc": None, "provisional_brier": None,
        "provisional_log_loss": None, "provisional_ece": None,
        "reliability": [], "predictions": [], "all_scored": [], "market_scores": [],
        "split_receipt": split_receipt or {},
        "note": (reason or "Book-v2 is collecting exact executable-book outcomes; legacy models are reference-only."),
    }
    _atomic_dump(out, os.path.join(os.path.dirname(os.path.abspath(db)), "ml_predictions.json"))
    _publish_no_execution_vectors(db)
    return out


def _publish_no_execution_vectors(db):
    """Actively revoke the Go tick evaluator's execution seam.

    An empty/mismatched legacy file used to leave the prior in-memory scores alive. The Go loader
    now treats this explicit authority receipt as a command to clear every vector and score.
    """
    out = {"generated_at": int(time.time()), "version": str(_EXPORT_VER.get("v") or ""),
           "authority": False, "execution_role": "research-ranking-risk-only",
           "n": 0, "rows": []}
    _atomic_dump(out, os.path.join(os.path.dirname(os.path.abspath(db)), "ml_eval_vectors.json"))


def load(db):
    # R98 WAL-PIN FIX: this full reload is the sidecar's longest DB read (900k+ rows, every full
    # retrain ≈ every 6 min). The old shape iterated the LIVE cursor inside a Python dict()
    # comprehension, holding the read snapshot open for the whole conversion (10-20s) — those are
    # the "long read txns" R97 root-caused as pinning the WAL reader mark (blocking truncate
    # checkpoints and inflating every reader's WAL-index lookups). Now: fetchall() materializes at
    # C speed, the connection closes (snapshot released in ~2-4s), and the Python-side dict
    # conversion happens against dead rows. sqlite3.Row keeps its data after close.
    con = sqlite3.connect("file:%s?mode=ro" % db, uri=True)
    con.row_factory = sqlite3.Row
    try:
        raw = con.execute("SELECT %s FROM signal_log%s" % (_LEAN_COLS, _ML_PLATFORM_FILTER)).fetchall()
    finally:
        con.close()
    return [dict(r) for r in raw]


# INCREMENTAL CACHE (R14 — "picks are still old"): the rescore's cost was the full 620k-row reload
# (~8-10s), which forced a 30s+ cadence. Keep the row set RESIDENT and per-pass fetch only the
# DELTA: new rows (id > max_id) + rows resolved since the last pass (2-min overlap window against
# clock skew). A delta pass is ~1-2s, enabling a 10s rescore. The FULL retrain cycle re-primes the
# cache from scratch every interval, bounding any drift (e.g. late fill_price updates on old rows).
# R138 cache: retain exact dedup winners, every open row, and a bounded resolved tail. Full fits
# still read the complete prospective dataset directly before this fast-rescore cache is primed.
# The compact id index points only to retained winners, not every signal observation.
ML_INC_MAX_RESOLVED = 50000
_INC = {"winners": None, "id_to_key": {}, "max_id": 0,
        "last_res": "1970-01-01T00:00:00", "dropped_resolved": 0}


def _inc_key(r):
    return (r.get("ticker"), r.get("side"), r.get("signal_type"))


def _inc_compact(max_resolved=ML_INC_MAX_RESOLVED):
    """Keep all open dedup winners plus a bounded newest resolved tail.

    Full fits never train from this tail: they call load() against the complete prospective cohort.
    This cache exists only for ten-second rescoring and held-lot resolution, whose horizon is hours.
    """
    winners = _INC.get("winners") or {}
    opens = [(k, r) for k, r in winners.items() if int(r.get("resolved") or 0) == 0]
    resolved = [(k, r) for k, r in winners.items() if int(r.get("resolved") or 0) != 0]
    resolved.sort(key=lambda kr: (str(kr[1].get("resolved_at") or kr[1].get("ts") or ""),
                                  int(kr[1].get("id") or 0)), reverse=True)
    limit = max(0, int(max_resolved))
    kept = dict(opens + resolved[:limit])
    _INC["dropped_resolved"] = max(0, len(resolved) - limit)
    _INC["winners"] = kept
    _INC["id_to_key"] = {int(r.get("id") or 0): k for k, r in kept.items()
                         if int(r.get("id") or 0) > 0}


def _prime_inc(rows, max_id=None):
    winners = {}
    for r in rows:
        k = _inc_key(r)
        cur = winners.get(k)
        if cur is None or str(r.get("ts") or "") < str(cur.get("ts") or ""):
            winners[k] = r
    _INC["winners"] = winners
    _INC["max_id"] = int(max_id if max_id is not None else
                         max((r.get("id", 0) for r in rows), default=0))
    _INC["last_res"] = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(time.time() - 120))
    _inc_compact()


def load_incremental(db):
    if _INC["winners"] is None:
        rows = load(db)
        native = _book_native_rows(rows)
        _prime_inc(native, max((r.get("id", 0) for r in rows), default=0))
        return list(_INC["winners"].values())
    # R98 WAL-PIN FIX (same as load()): fetch at C speed, release the snapshot, convert after.
    con = sqlite3.connect("file:%s?mode=ro" % db, uri=True)
    con.row_factory = sqlite3.Row
    try:
        # Freeze an ID ceiling before reading the delta. Rows committed after this receipt stay
        # above the ceiling for the next cycle, while irrelevant non-v2 rows below it cannot make
        # every fast rescore scan the same ever-growing tail.
        db_max_id = int(con.execute("SELECT COALESCE(MAX(id),0) FROM signal_log").fetchone()[0])
        raw_new = con.execute(
            "SELECT %s FROM signal_log WHERE id > ? AND id <= ? AND platform != 'polymarket' AND book_feature_ver = 1 AND pricing_version='book-native-v2' AND label_version='kalshi_start_clock_v2' AND (platform!='kalshi' OR secs_to_start IS NOT NULL)" % _LEAN_COLS,
            (_INC["max_id"], db_max_id)).fetchall()
        raw_upd = con.execute(
            "SELECT %s FROM signal_log WHERE resolved != 0 AND resolved_at >= ? AND platform != 'polymarket' AND book_feature_ver = 1 AND pricing_version='book-native-v2' AND label_version='kalshi_start_clock_v2' AND (platform!='kalshi' OR secs_to_start IS NOT NULL)" % _LEAN_COLS,
            (_INC["last_res"],)).fetchall()
    finally:
        con.close()
    _INC["max_id"] = max(int(_INC["max_id"]), db_max_id)
    new = _book_native_rows([dict(r) for r in raw_new])
    upd = _book_native_rows([dict(r) for r in raw_upd])
    for r in new:
        k = _inc_key(r)
        cur = _INC["winners"].get(k)
        if cur is None or str(r.get("ts") or "") < str(cur.get("ts") or ""):
            if cur is not None:
                _INC["id_to_key"].pop(int(cur.get("id") or 0), None)
            _INC["winners"][k] = r
            _INC["id_to_key"][int(r["id"])] = k
    for r in upd:
        k = _INC["id_to_key"].get(int(r["id"]))
        if k is not None:
            _INC["winners"][k] = r
    _INC["last_res"] = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(time.time() - 120))
    _inc_compact()
    return list(_INC["winners"].values())


def _bundle_path(db):
    # Keep earlier bundles as auditable reference files; v2 never overwrites/loads them.
    return os.path.join(os.path.dirname(os.path.abspath(db)), "ml_model_book_v2.pkl")


_MODEL_VER = {"v": "", "nonce": ""}


def _forecast_model_version(bundle):
    """Stable identity for the calibrated forecast model itself.

    This is deliberately separate from ``_EXPORT_VER`` below: the optional flat Go copy may
    refuse its parity gate while the sidecar's fitted model remains perfectly valid for research
    forecasts. Hash the exact bundle payload before adding the version field, then persist that
    identity in the bundle so a restart republishes the same provenance.
    """
    import hashlib
    import pickle
    material = dict(bundle)
    material.pop("model_version", None)
    digest = hashlib.sha256(pickle.dumps(material, protocol=pickle.HIGHEST_PROTOCOL)).hexdigest()
    return "book-v2-" + digest[:16]


def _ensure_forecast_model_version(bundle):
    """Return one non-empty identity for the fitted forecast model, independent of flat export.

    Normally the exact pickle payload supplies the stable digest. If a valid in-memory estimator
    cannot be pickled (which also prevents bundle persistence), retain a process-stable emergency
    identity instead of publishing a usable PAPER_PROVISIONAL/ACTIVE_RESEARCH manifest with an
    empty provenance field. The nonce is minted once for that in-memory fit and never borrows
    ``_EXPORT_VER``: the optional Go flat copy has a separate parity/authority lifecycle.
    """
    current = str(_MODEL_VER.get("v") or "").strip()
    if current:
        return current
    try:
        current = _forecast_model_version(bundle)
    except Exception:
        import hashlib
        import json
        nonce = str(_MODEL_VER.get("nonce") or "")
        if not nonce:
            nonce = "%d-%d" % (time.time_ns(), os.getpid())
            _MODEL_VER["nonce"] = nonce
        material = {
            "feature_schema": BOOK_FEATURE_SCHEMA,
            "model_class": type(bundle.get("model")).__module__ + "." +
                           type(bundle.get("model")).__qualname__,
            "metric_scope": str(bundle.get("metric_scope") or "none"),
            "split_receipt": bundle.get("split_receipt") or {},
            "vocab": bundle.get("vocab") or {},
            "nonce": nonce,
        }
        payload = json.dumps(material, sort_keys=True, separators=(",", ":"), default=str)
        current = "book-v2-runtime-" + hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]
    _MODEL_VER["v"] = current
    return current


def _save_bundle(db, bundle):
    """Persist the fitted model + vocab + last metrics (R10) so a restart can SCORE IMMEDIATELY
    with yesterday's model instead of spending minutes retraining before the first prediction."""
    import pickle
    path = _bundle_path(db)
    tmp = path + ".tmp"
    _MODEL_VER["v"] = ""
    _MODEL_VER["nonce"] = ""
    bundle = dict(bundle)
    bundle["model_version"] = _ensure_forecast_model_version(bundle)
    try:
        with open(tmp, "wb") as f:
            pickle.dump(bundle, f, protocol=pickle.HIGHEST_PROTOCOL)
        os.replace(tmp, path)
    except Exception as e:
        print("  (could not save model bundle: %s)" % e)


def _load_bundle(db, max_age_h=6.0):
    """Load the persisted model bundle if it exists, is fresh, and unpickles under THIS sklearn.
    Any failure → None (fall back to a full train). Version drift is the main hazard; the
    try/except makes it a soft miss, never a crash."""
    import pickle
    path = _bundle_path(db)
    _MODEL_VER["v"] = ""
    _MODEL_VER["nonce"] = ""
    try:
        if (time.time() - os.path.getmtime(path)) > max_age_h * 3600:
            return None
        with open(path, "rb") as f:
            b = pickle.load(f)
        if b.get("feature_schema") != BOOK_FEATURE_SCHEMA:
            return None
        split = b.get("split_receipt") or {}
        if (not split.get("valid") or split.get("test_role") != "rolling-event-day-final-slice" or
                int(split.get("test_prediction_calls") or 0) != 1 or
                not split.get("group_disjoint") or not split.get("ticker_disjoint")):
            return None  # no restart shortcut around the per-fit rolling final-slice contract
        # R65: the raw feature list can grow between releases — a bundle trained on the old width
        # passes the zeros smoke test below (it stores its OWN n_features) and then blows up on
        # freshly-featurized rows. Any NUM-width drift => stale bundle, fall through to full retrain.
        if int(b.get("n_num", -1)) != len(NUM):
            return None
        # smoke-test the model so a version-drifted pickle fails HERE, not mid-scoring
        b["model"].predict_proba(np.zeros((1, b["n_features"])))
        ver = str(b.get("model_version") or "").strip()
        if not ver:
            # Compatibility for the pre-R137 bundle already on disk: its immutable file bytes are
            # an exact, restart-stable identity even though the old payload lacked a named version.
            import hashlib
            with open(path, "rb") as f:
                ver = "book-v2-legacy-" + hashlib.sha256(f.read()).hexdigest()[:16]
            b["model_version"] = ver
        _MODEL_VER["v"] = ver
        return b
    except Exception:
        return None


# ---------------------------------------------------------------------------------------------
# R123 FLAT MODEL EXPORT (Part 1: near-zero re-scoring). After every full retrain the sidecar
# dumps the calibrated model to data/ml_model_export.json in a flat, dependency-free format the
# Go suite evaluates in microseconds: the 3 CalibratedClassifierCV folds, each = boosted trees
# (float32 sequential-sum walk — REQUIRED: the r123 probe measured f32 margin parity 1.8e-7 vs
# f64 drifting to 1.4e-6) + that fold's calibrator (isotonic np.interp semantics or sigmoid),
# probabilities averaged across folds. PARITY IS THE CONTRACT, in two honest layers:
#   1. IMPLEMENTATION parity: the export carries up to 1,000 (real feature-vector → the
#      sidecar's own FLAT evaluation) pairs; the Go loader refuses any export it cannot replay
#      within 1e-6 (it lands ~1e-15 — same arithmetic, different language). Go can never
#      silently drift from the exported model.
#   2. MODEL-COPY fidelity: |flat − predict_proba| is MEASURED on the same 1,000 real rows and
#      PUBLISHED (median/p95/p99/max in the export + /api/mleval). Gate: p99 ≤ 2.5e-7, max ≤
#      5e-5 — a real extraction bug measures ~1e-2 and refuses loudly; the measured live tail
#      (max 4.2e-6 on 3/1000 rows) is XGBoost's own GPU/SIMD float32 margin order landing on
#      steep isotonic segment boundaries — irreducible portably, and worth 4e-6 of a
#      probability point at worst (documented deviation from the flat 1e-6-to-proba reading).
# Refusal keeps the previous export on disk — the Go side keeps its old model + the file-based
# fallback. The sidecar stays the trainer and the ground truth; the Go evaluator is a fast
# copy, never a fork.
_EXPORT_VER = {"v": ""}


def _export_path(db):
    return os.path.join(os.path.dirname(os.path.abspath(db)), "ml_model_export.json")


def _flat_fold_xgb(est):
    """XGBClassifier → flat trees. Verified on the live pkl (r123 probe, xgb 3.3.0): leaf value
    lives in split_conditions at leaf nodes (left_children == -1); bias = logit(base_score);
    base_score serializes as '[5.834963E-1]' in 3.x. Walk: float32(x[f]) < float32(t) → left."""
    booster = est.get_booster()
    raw = json.loads(bytearray(booster.save_raw(raw_format="json")))
    learner = raw["learner"]
    bs = float(str(learner["learner_model_param"]["base_score"]).strip("[]").split(",")[0])
    if not (0.0 < bs < 1.0):
        raise ValueError("base_score out of range: %r" % bs)
    trees = []
    for t in learner["gradient_booster"]["model"]["trees"]:
        trees.append({"f": [int(v) for v in t["split_indices"]],
                      "t": [float(np.float32(v)) for v in t["split_conditions"]],
                      "l": [int(v) for v in t["left_children"]],
                      "r": [int(v) for v in t["right_children"]]})
    return {"cmp": "lt", "acc": "f32", "bias": math.log(bs / (1.0 - bs)), "trees": trees}


def _flat_fold_hgb(est):
    """HistGradientBoostingClassifier fallback → flat trees from _predictors (float64 math
    end-to-end; walk: x[f] <= threshold → left). bias = _baseline_prediction. Same encoding as
    the xgb fold: leaf ⇔ l[i] < 0, leaf value stored in t[i]."""
    trees = []
    for it in est._predictors:
        pred = it[0]  # binary classification: one predictor per iteration
        f, t, l, r = [], [], [], []
        for nd in pred.nodes:
            leaf = bool(nd["is_leaf"])
            f.append(-1 if leaf else int(nd["feature_idx"]))
            t.append(float(nd["value"]) if leaf else float(nd["num_threshold"]))
            l.append(-1 if leaf else int(nd["left"]))
            r.append(-1 if leaf else int(nd["right"]))
        trees.append({"f": f, "t": t, "l": l, "r": r})
    return {"cmp": "le", "acc": "f64", "bias": float(np.ravel(est._baseline_prediction)[0]),
            "trees": trees}


def _flat_calibrator(cal):
    tn = type(cal).__name__
    if tn == "IsotonicRegression":
        return {"method": "isotonic",
                "x": [float(v) for v in cal.X_thresholds_],
                "y": [float(v) for v in cal.y_thresholds_]}
    if hasattr(cal, "a_") and hasattr(cal, "b_"):
        return {"method": "sigmoid", "a": float(cal.a_), "b": float(cal.b_)}
    raise ValueError("unsupported calibrator %s" % tn)


def _flat_calibrate(cal, p):
    """np.interp semantics for isotonic (clip at the end thresholds; within a segment
    y = slope·(p−x0) + y0) — the Go evaluator implements this formula verbatim."""
    if cal["method"] == "sigmoid":
        return 1.0 / (1.0 + math.exp(cal["a"] * p + cal["b"]))
    cx, cy = cal["x"], cal["y"]
    if p <= cx[0]:
        return cy[0]
    if p >= cx[-1]:
        return cy[-1]
    j = bisect.bisect_right(cx, p) - 1
    if j >= len(cx) - 1:
        return cy[-1]
    slope = (cy[j + 1] - cy[j]) / (cx[j + 1] - cx[j])
    return slope * (p - cx[j]) + cy[j]


def _flat_pwin(folds, x):
    """The exact arithmetic the Go evaluator runs: per fold, sequential float32 (xgb) or
    float64 (hgb) leaf-sum → +bias → calibrator input → calibrator; folds averaged.
    cal_input pins WHAT sklearn fed the fold's calibrator (r123 test finding): estimators
    with a decision_function (HGB) calibrate on the raw MARGIN — no sigmoid anywhere;
    estimators without one (XGBClassifier) calibrate on predict_proba, which XGBoost emits
    in FLOAT32 — so the sigmoid output is cast to float32 before calibration (steep isotonic
    segments amplify the f32 ulp ~35x; skipping the cast measured 2.3e-6 vs 1e-6 target)."""
    x32 = np.asarray(x, dtype=np.float32)
    x64 = np.asarray(x, dtype=np.float64)
    tot = 0.0
    for fold in folds:
        if fold["acc"] == "f32":
            s = np.float32(0.0)
            for t in fold["trees"]:
                f, th, l, r = t["f"], t["t"], t["l"], t["r"]
                i = 0
                while l[i] >= 0:
                    i = l[i] if x32[f[i]] < np.float32(th[i]) else r[i]
                s = np.float32(s + np.float32(th[i]))
            margin = float(s) + fold["bias"]
        else:
            s = 0.0
            for t in fold["trees"]:
                f, th, l, r = t["f"], t["t"], t["l"], t["r"]
                i = 0
                while l[i] >= 0:
                    i = l[i] if x64[f[i]] <= th[i] else r[i]
                s += th[i]
            margin = s + fold["bias"]
        if fold.get("cal_input", "proba") == "margin":
            p_in = margin
        else:
            p_in = float(np.float32(1.0 / (1.0 + math.exp(-margin))))
        tot += _flat_calibrate(fold["calib"], p_in)
    return tot / len(folds)


def _flat_folds(model):
    """Assemble the flat folds from a fitted CalibratedClassifierCV — shared by the exporter
    and the r123 tests so the two can never drift."""
    folds = []
    if hasattr(model, "heldout_base_"):
        est = model.heldout_base_
        fold = _flat_fold_xgb(est) if type(est).__name__ == "XGBClassifier" else _flat_fold_hgb(est)
        # The heldout calibrator is explicitly fit on base probabilities from validation.
        fold["cal_input"] = "proba"
        fold["calib"] = _flat_calibrator(model.heldout_calibrator_)
        return [fold]
    for cc in model.calibrated_classifiers_:
        est = getattr(cc, "estimator", None)
        if est is None:
            est = getattr(cc, "base_estimator", None)  # older sklearn spelling
        fold = _flat_fold_xgb(est) if type(est).__name__ == "XGBClassifier" else _flat_fold_hgb(est)
        # sklearn's _CalibratedClassifier prefers decision_function over predict_proba as the
        # calibrator input — HGB has one (margin domain), XGBClassifier does not (proba domain).
        fold["cal_input"] = "margin" if hasattr(est, "decision_function") else "proba"
        fold["calib"] = _flat_calibrator(cc.calibrators[0])
        folds.append(fold)
    return folds


def _export_flat_model(db, model, vocab, sample_row, cal_method, X_parity):
    """Write data/ml_model_export.json (see block comment above). X_parity = REAL featurized
    rows (last ≤1000 used for the parity pairs). Refuses to publish on self-parity failure."""
    import hashlib
    # A vector publish may name only the export produced by THIS exact model. Clear the process
    # latch before extraction so any refusal/exception leaves subsequent vectors unversioned and
    # therefore unusable by Go, rather than silently pairing a new model's p_win with an old file.
    _EXPORT_VER["v"] = ""
    t0 = time.time()
    folds = _flat_folds(model)
    if X_parity is None or len(X_parity) == 0:
        print("  R123 flat export skipped: no parity rows available")
        return None
    V = np.asarray(X_parity, dtype=np.float64)
    if V.shape[0] > 1000:
        V = V[-1000:]
    true_p = model.predict_proba(V)[:, 1].astype(np.float64)
    flat_p = np.array([_flat_pwin(folds, V[i]) for i in range(V.shape[0])])
    d = np.abs(flat_p - true_p)
    fid = {"median": float(np.median(d)), "p95": float(np.percentile(d, 95)),
           "p99": float(np.percentile(d, 99)), "max": float(np.max(d))}
    # FIDELITY GATE (measured 2026-07-09 on 1,000 REAL rows: median 0 · p95 2.0e-8 · p99 4.0e-8
    # · max 4.2e-6 on 3 rows). The tail is IRREDUCIBLE: XGBoost accumulates margins in its own
    # (GPU/SIMD) float32 order; a ~1.8e-7 margin difference landing exactly on a steep isotonic
    # segment boundary flips the segment. No portable walk can be bit-exact to that, so the gate
    # is DISTRIBUTIONAL: p99 must stay ≤ 2.5e-7 (a real extraction bug — wrong leaf field, wrong
    # bias — measures ~1e-2 here and refuses loudly) and max ≤ 5e-5 (sanity). The full fidelity
    # table is PUBLISHED in the export and on /api/mleval — nobody gets to claim the copy is
    # closer to predict_proba than it measured. Worst-case decision impact of the tail: 4e-6 of
    # a probability point, ~4 orders below any border/EV threshold.
    if fid["p99"] > 2.5e-7 or fid["max"] > 5e-5:
        print("  R123 flat export REFUSED: fidelity p99 %.3e / max %.3e beyond gate (n=%d) — keeping previous export"
              % (fid["p99"], fid["max"], V.shape[0]))
        return None
    # PARITY PAIRS = (vector → the sidecar's OWN FLAT evaluation): the Go loader must replay
    # these within 1e-6 (it lands ~1e-15 — same arithmetic, different language), which makes
    # implementation drift impossible. Flat-vs-predict_proba fidelity is the block above.
    names = _feature_names(vocab, sample_row)
    body = {"folds": folds, "feature_names": names, "calib_method": cal_method}
    ver = "%d-%s" % (int(time.time()),
                     hashlib.sha256(json.dumps(body, sort_keys=True).encode()).hexdigest()[:12])
    out = {"version": ver, "generated_at": int(time.time()),
           "feature_schema": BOOK_FEATURE_SCHEMA, "book_feature_version": BOOK_FEATURE_VERSION,
           "backend": _model_backend()[0], "calib_method": cal_method,
           "n_features": int(V.shape[1]), "feature_names": names,
           "folds": folds,
           "parity": {"n": int(V.shape[0]), "self_max_diff": fid["max"],
                      "fidelity_vs_predict_proba": fid,
                      "vectors": [[float(v) for v in row] for row in V],
                      "pwin": [float(v) for v in flat_p]}}
    _atomic_dump(out, _export_path(db))
    _EXPORT_VER["v"] = ver
    print("  R123 flat export: %s (%d folds, fidelity p99 %.2e max %.2e, %d pairs, %.1fs)"
          % (ver, len(folds), fid["p99"], fid["max"], V.shape[0], time.time() - t0))
    return ver


def _held_lot_keys(db):
    """(ticker, SIDE) set of the ML paper book's open lots — the rows the Go tick re-scorer
    must always have vectors for."""
    try:
        book = _json_read(os.path.join(os.path.dirname(os.path.abspath(db)), "ml_paper.json")) or {}
        lots = book.get("open") or book.get("positions") or []
        return {((l.get("ticker") or ""), str(l.get("side") or "").upper()) for l in lots}
    except Exception:
        return set()


def _held_price_requests(db):
    """Open ML/shadow lots that need an exact current exit bid/fee receipt each score cycle."""
    out = []
    root = os.path.dirname(os.path.abspath(db))
    for name in ("ml_paper.json", "ml_shadow.json"):
        try:
            book = _json_read(os.path.join(root, name)) or {}
            for lot in (book.get("open") or book.get("positions") or []):
                if lot.get("ticker") and lot.get("platform") and lot.get("side"):
                    out.append({"ticker": lot["ticker"], "platform": lot["platform"],
                                "side": str(lot["side"]).upper()})
        except Exception:
            pass
    return out


_OPP_SIDE = {"YES": "NO", "NO": "YES", "UP": "DOWN", "DOWN": "UP"}


def _dump_eval_vectors(db, opn, Xo, p, preds):
    """R123: publish the SIGNAL-SIDE feature vectors (exactly as scored this cycle) for every
    row the Go tick re-scorer cares about: held book lots + actionable preds. Go patches the
    live features (price/momentum/spread/depth/horizon/…) into the vector and re-walks the
    exported trees on every price tick; features Go cannot faithfully reconstruct stay FROZEN
    at these values (the hybrid is flagged per-feature in /api/mleval). p_win here is the
    sidecar's own fresh score → the Go side's drift ground truth. Complement sides are exact
    binary complements (p_opp = 1 − p), mirroring all_scored — Go complements, no extra rows."""
    if not ML_LIVE_EXECUTION_AUTHORITY:
        _publish_no_execution_vectors(db)
        return
    if not opn or Xo is None or p is None:
        return
    held = _held_lot_keys(db)
    predt = {str(x.get("ticker") or "") for x in preds}
    rows, seen = [], set()
    order = []
    for i, r in enumerate(opn):
        tk = str(r.get("ticker") or "")
        sd = str(r.get("side") or "").upper()
        if not tk or (tk, sd) in seen:
            continue
        seen.add((tk, sd))
        pri = 0
        if (tk, sd) in held or (tk, _OPP_SIDE.get(sd, "")) in held:
            pri = 2
        elif tk in predt:
            pri = 1
        if pri:
            order.append((pri, i))
    order.sort(key=lambda t: (-t[0], t[1]))
    now_utc = time.time()
    for _pri, i in order[:600]:
        r = opn[i]
        rh = float(r.get("resolve_hours") or 0)
        ts_str = str(r.get("ts") or "")
        t0 = 0.0
        try:
            t0 = datetime.fromisoformat(ts_str.replace("Z", "+00:00")).timestamp()
        except Exception:
            pass
        close_ts = int(t0 + rh * 3600) if (t0 > 0 and rh > 0) else 0
        try:
            sts = float(r.get("secs_to_start") or 0)
        except Exception:
            sts = 0.0
        start_ts = int(t0 + sts) if (t0 > 0 and sts < 9.9e6) else 0
        rows.append({"ticker": r.get("ticker"), "side": str(r.get("side") or "").upper(),
                     "platform": r.get("platform"), "p_win": float(p[i]),
                     "sig_price": float(r.get("entry_price") or 0),
                     "sig_ts": int(t0), "close_ts": close_ts, "start_ts": start_ts,
                     "held": _pri == 2,
                     "x": [round(float(v), 6) for v in Xo[i]]})
    out = {"generated_at": int(now_utc), "version": _EXPORT_VER["v"],
           "authority": bool(ML_LIVE_EXECUTION_AUTHORITY), "n": len(rows), "rows": rows}
    _atomic_dump(out, os.path.join(os.path.dirname(os.path.abspath(db)), "ml_eval_vectors.json"))


def dedup(rows):
    """One earliest book-v1 row per (ticker, side, signal_type). A later fill is post-signal
    selection information and never chooses the feature row."""
    best = {}
    for r in rows:
        k = (r.get("ticker"), r.get("side"), r.get("signal_type"))
        cur = best.get(k)
        if cur is None or r.get("ts", 0) < cur.get("ts", 0):
            best[k] = r
    return list(best.values())


def _row_utc_day(r):
    raw = str(r.get("ts") or r.get("day") or "").strip()
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).date()
    except Exception:
        try:
            return datetime.strptime(str(r.get("day") or "")[:10], "%Y-%m-%d").date()
        except Exception:
            return None


def _event_group(r):
    canonical = str(r.get("canonical_event_id") or "").strip()
    if canonical:
        return "canonical:" + canonical
    venue = str(r.get("platform") or "").strip().lower()
    catalog = str(r.get("catalog_event_key") or "").strip()
    if catalog:
        return "catalog:%s:%s" % (venue, catalog)
    # Fail closed on identity uncertainty: the exact ticker remains indivisible. We never title-
    # match or strip a suffix here because a false merge is as contaminating as a false split.
    return "ticker:%s:%s" % (venue, str(r.get("ticker") or "").strip().upper())


def _nested_event_day_split(rows, embargo_days=1):
    """Chronological train/validation/rolling-final split by indivisible event groups.

    A group's anchor is its latest UTC observation day, so a multi-day event is assigned to the
    latest partition it touches. One full calendar day is purged before validation and test.
    """
    groups = {}
    for r in rows:
        d = _row_utc_day(r)
        if d is None:
            continue
        g = _event_group(r)
        ent = groups.setdefault(g, {"rows": [], "day": d, "tickers": set()})
        ent["rows"].append(r)
        ent["day"] = max(ent["day"], d)
        ent["tickers"].add((str(r.get("platform") or "").lower(), str(r.get("ticker") or "").upper()))
    days = sorted({v["day"] for v in groups.values()})
    if len(days) < 5:
        return [], [], [], {"valid": False, "reason": "need-at-least-5-utc-days",
                            "days": len(days), "groups": len(groups), "purged_groups": len(groups)}
    n_test_days = max(1, int(math.ceil(len(days) * 0.20)))
    test_start = days[-n_test_days]
    test_embargo_start = test_start - timedelta(days=max(1, int(embargo_days)))
    pre_test_days = [d for d in days if d < test_embargo_start]
    if len(pre_test_days) < 3:
        return [], [], [], {"valid": False, "reason": "insufficient-days-before-test-embargo",
                            "days": len(days), "groups": len(groups), "purged_groups": len(groups)}
    n_val_days = max(1, int(math.ceil(len(pre_test_days) * 0.25)))
    val_start = pre_test_days[-n_val_days]
    val_embargo_start = val_start - timedelta(days=max(1, int(embargo_days)))
    train, val, test, purged = [], [], [], 0
    split_groups = {"train": set(), "validation": set(), "test": set()}
    split_tickers = {"train": set(), "validation": set(), "test": set()}
    for g, ent in groups.items():
        d = ent["day"]
        target = None
        if d >= test_start:
            target = "test"
        elif val_start <= d < test_embargo_start:
            target = "validation"
        elif d < val_embargo_start:
            target = "train"
        if target is None:
            purged += 1
            continue
        split_groups[target].add(g)
        split_tickers[target].update(ent["tickers"])
        {"train": train, "validation": val, "test": test}[target].extend(ent["rows"])
    group_disjoint = not ((split_groups["train"] & split_groups["validation"]) or
                          (split_groups["train"] & split_groups["test"]) or
                          (split_groups["validation"] & split_groups["test"]))
    ticker_disjoint = not ((split_tickers["train"] & split_tickers["validation"]) or
                           (split_tickers["train"] & split_tickers["test"]) or
                           (split_tickers["validation"] & split_tickers["test"]))
    valid = bool(train and val and test and group_disjoint and ticker_disjoint)
    receipt = {
        "valid": valid, "reason": "ok" if valid else "empty-or-overlapping-partition",
        "embargo_days": max(1, int(embargo_days)), "days": len(days), "groups": len(groups),
        "train_rows": len(train), "validation_rows": len(val), "test_rows": len(test),
        "train_groups": len(split_groups["train"]), "validation_groups": len(split_groups["validation"]),
        "test_groups": len(split_groups["test"]), "purged_groups": purged,
        "validation_start_utc": val_start.isoformat(), "test_start_utc": test_start.isoformat(),
        "group_disjoint": group_disjoint, "ticker_disjoint": ticker_disjoint,
    }
    return train, val, test, receipt


def _provisional_event_day_split(rows):
    """Three-day, group-disjoint chronological split for PAPER exploration only.

    This is deliberately weaker than `_nested_event_day_split`: it has no spare calendar day for
    an embargo.  It therefore can never satisfy LIVE validation.  It still keeps canonical events
    and tickers indivisible, trains on earlier UTC days, calibrates on the penultimate day, and
    evaluates once on the latest day.  The explicit receipt prevents a provisional score from ever
    being mistaken for the five-day LIVE diagnostic.
    """
    groups = {}
    for r in rows:
        d = _row_utc_day(r)
        if d is None:
            continue
        g = _event_group(r)
        ent = groups.setdefault(g, {"rows": [], "day": d, "tickers": set()})
        ent["rows"].append(r)
        ent["day"] = max(ent["day"], d)
        ent["tickers"].add((str(r.get("platform") or "").lower(),
                            str(r.get("ticker") or "").upper()))
    days = sorted({v["day"] for v in groups.values()})
    base_receipt = {
        "valid": False, "provisional": True, "live_eligible": False,
        "days": len(days), "days_required_live": 5, "groups": len(groups),
        "embargo_days": 0, "test_role": "provisional-latest-utc-day",
    }
    if len(days) < 3:
        return [], [], [], dict(base_receipt, reason="need-at-least-3-utc-days-for-provisional-paper")
    validation_day, test_day = days[-2], days[-1]
    train, val, test = [], [], []
    split_groups = {"train": set(), "validation": set(), "test": set()}
    split_tickers = {"train": set(), "validation": set(), "test": set()}
    for g, ent in groups.items():
        d = ent["day"]
        target = "test" if d == test_day else ("validation" if d == validation_day else "train")
        split_groups[target].add(g)
        split_tickers[target].update(ent["tickers"])
        {"train": train, "validation": val, "test": test}[target].extend(ent["rows"])
    group_disjoint = not ((split_groups["train"] & split_groups["validation"]) or
                          (split_groups["train"] & split_groups["test"]) or
                          (split_groups["validation"] & split_groups["test"]))
    ticker_disjoint = not ((split_tickers["train"] & split_tickers["validation"]) or
                           (split_tickers["train"] & split_tickers["test"]) or
                           (split_tickers["validation"] & split_tickers["test"]))
    valid = bool(train and val and test and group_disjoint and ticker_disjoint)
    receipt = dict(base_receipt,
                   valid=valid, reason="ok" if valid else "empty-or-overlapping-provisional-partition",
                   train_rows=len(train), validation_rows=len(val), test_rows=len(test),
                   train_groups=len(split_groups["train"]),
                   validation_groups=len(split_groups["validation"]),
                   test_groups=len(split_groups["test"]),
                   validation_start_utc=validation_day.isoformat(),
                   test_start_utc=test_day.isoformat(),
                   group_disjoint=group_disjoint, ticker_disjoint=ticker_disjoint,
                   purged_groups=0)
    return train, val, test, receipt


def _validation_calibration_indices(rows):
    """Indivisible chronological halves inside validation: calibrator-fit then method-select."""
    grouped = {}
    for i, r in enumerate(rows):
        g = _event_group(r)
        d = _row_utc_day(r)
        ent = grouped.setdefault(g, {"idx": [], "day": d})
        ent["idx"].append(i)
        if d is not None and (ent["day"] is None or d > ent["day"]):
            ent["day"] = d
    order = sorted(grouped.values(), key=lambda e: (e["day"] or datetime.min.date(), min(e["idx"])))
    cut = len(order) // 2
    if cut < 1 or len(order) - cut < 1:
        return np.array([], dtype=int), np.array([], dtype=int)
    fit = np.array([i for e in order[:cut] for i in e["idx"]], dtype=int)
    select = np.array([i for e in order[cut:] for i in e["idx"]], dtype=int)
    return fit, select


class _SigmoidCalibrator:
    def fit(self, p, y, sample_weight=None):
        self.model = LogisticRegression(solver="lbfgs", max_iter=500)
        self.model.fit(np.asarray(p).reshape(-1, 1), np.asarray(y), sample_weight=sample_weight)
        # Flat-export compatibility uses sklearn's historical 1/(1+exp(a*p+b)) convention.
        self.a_ = -float(self.model.coef_[0][0])
        self.b_ = -float(self.model.intercept_[0])
        return self

    def predict(self, p):
        return self.model.predict_proba(np.asarray(p).reshape(-1, 1))[:, 1]


class HeldoutCalibratedModel:
    """Base fit on train; calibrator fit/selected only in validation; test is prediction-only."""
    def __init__(self, estimator, calibrator, method):
        self.heldout_base_ = estimator
        self.heldout_calibrator_ = calibrator
        self.heldout_method_ = method

    def predict_with_raw(self, X):
        raw = self.heldout_base_.predict_proba(X)[:, 1]
        p = np.asarray(self.heldout_calibrator_.predict(raw), dtype=float)
        p = np.clip(p, 0.0, 1.0)
        return raw, p

    def predict_proba(self, X):
        _, p = self.predict_with_raw(X)
        return np.column_stack((1.0 - p, p))


def _fit_with_weights(est, X, y, w=None):
    if w is not None:
        try:
            est.fit(X, y, sample_weight=w)
            return est
        except TypeError:
            pass
    est.fit(X, y)
    return est


def _new_calibrator(method):
    return IsotonicRegression(out_of_bounds="clip") if method == "isotonic" else _SigmoidCalibrator()


def _fit_heldout_calibrated(Xtr, ytr, Xv, yv, val_rows, train_weights=None, val_weights=None):
    base = _fit_with_weights(_mk_clf(), Xtr, ytr, train_weights)
    raw_v = base.predict_proba(Xv)[:, 1]
    fit_idx, select_idx = _validation_calibration_indices(val_rows)
    method_scores = {}
    if (len(fit_idx) >= 10 and len(select_idx) >= 10 and
            len(set(yv[fit_idx])) > 1 and len(set(yv[select_idx])) > 1):
        for method in ("sigmoid", "isotonic"):
            cal = _new_calibrator(method)
            sw = val_weights[fit_idx] if val_weights is not None else None
            try:
                cal.fit(raw_v[fit_idx], yv[fit_idx], sample_weight=sw)
            except TypeError:
                cal.fit(raw_v[fit_idx], yv[fit_idx])
            pv = np.asarray(cal.predict(raw_v[select_idx]), dtype=float)
            method_scores[method] = float(np.mean((pv - yv[select_idx]) ** 2))
        chosen = min(method_scores, key=method_scores.get)
    else:
        chosen = "sigmoid"
    final_cal = _new_calibrator(chosen)
    try:
        final_cal.fit(raw_v, yv, sample_weight=val_weights)
    except TypeError:
        final_cal.fit(raw_v, yv)
    return HeldoutCalibratedModel(base, final_cal, chosen), chosen, method_scores



def _feature_names(vocab, sample_row):
    # R103 (auditor 252): column names in featurize()'s EXACT order — NUM, then the synthetic
    # families (widths measured at runtime off a sample row so drift can never mislabel), then the
    # one-hots per CATS in vocab order.
    r = sample_row or {}
    names = list(NUM)
    names += ["path_%d" % i for i in range(len(list(path_feats(r.get("price_path") or ""))))]
    names += ["time_%d" % i for i in range(len(list(time_feats(r.get("ts")))))]
    names += ["extra_%d" % i for i in range(len(list(extra_feats(r))))]
    for c in CATS:
        names += ["%s=%s" % (c, (u if u else "~empty~")) for u in vocab[c]]
    return names


def _model_importances(model, vocab, sample_row, top_n=20):
    # R103 (auditor 252): per-retrain feature importances into ml_model_history.jsonl so the
    # auditor can watch WHICH features carry the model (nf alone says only how many). Averaged
    # across the CalibratedClassifierCV folds' fitted base estimators; None when the backend has
    # no feature_importances_ (e.g. some CPU fallbacks) — telemetry must never break a retrain.
    try:
        imps = []
        if hasattr(model, "heldout_base_"):
            fi = getattr(model.heldout_base_, "feature_importances_", None)
            if fi is not None:
                imps.append(np.asarray(fi, dtype=float))
        for cc in (getattr(model, "calibrated_classifiers_", None) or []):
            est = getattr(cc, "estimator", None)
            if est is None:
                est = getattr(cc, "base_estimator", None)  # older sklearn spelling
            fi = getattr(est, "feature_importances_", None)
            if fi is not None:
                imps.append(np.asarray(fi, dtype=float))
        if not imps:
            return None
        mean_fi = np.mean(imps, axis=0)
        names = _feature_names(vocab, sample_row)
        if len(names) != len(mean_fi):
            names = ["f%d" % i for i in range(len(mean_fi))]  # width drift → positional, never wrong labels
        order = np.argsort(mean_fi)[::-1][:top_n]
        return {names[i]: round(float(mean_fi[i]), 5) for i in order if mean_fi[i] > 0}
    except Exception:
        return None


def _num_value(r, name):
    """Feature aliases/derivations for book-v1. Missing optional latency is represented by a
    separate flag; required book fields are guaranteed by _is_book_native before this is called."""
    if name == "legacy_signal_price":
        name = "entry_price"
    if name == "book_latency_missing":
        return 0.0 if _finite_number(r.get("book_latency_ms")) else 1.0
    try:
        bid = float(r.get("book_maker_price") or 0)
        ask = float(r.get("book_taker_price") or 0)
        bd = float(r.get("book_maker_depth") or 0)
        ad = float(r.get("book_taker_depth") or 0)
        mf = float(r.get("book_maker_fee_pc") or 0)
        tf = float(r.get("book_taker_fee_pc") or 0)
        legacy = float(r.get("entry_price") or 0)
        if name == "book_spread_cents":
            return (ask - bid) * 100.0
        if name == "book_touch_imbalance":
            return (bd - ad) / (bd + ad) if bd + ad > 0 else 0.0
        if name == "book_maker_all_in":
            return bid + mf
        if name == "book_taker_all_in":
            return ask + tf
        if name == "legacy_to_taker_gap_cents":
            return (ask - legacy) * 100.0
        if name == "book_is_tapered":
            return 1.0 if min(float(r.get("book_maker_tick") or 1), float(r.get("book_taker_tick") or 1)) < .01 else 0.0
        v = r.get(name)
        return float(v) if v is not None and _finite_number(v) else 0.0
    except (TypeError, ValueError):
        return 0.0


def featurize(rows, vocab):
    X = []
    for r in rows:
        row = []
        for n in NUM:
            row.append(_num_value(r, n))
        row += list(path_feats(r.get("price_path") or ""))
        row += list(time_feats(r.get("ts")))  # intraday + weekday signal
        row += extra_feats(r)                 # log-$ / distance-from-50 / interactions / title keywords
        for c in CATS:
            v = str(r.get(c) or "")
            row += [1.0 if v == u else 0.0 for u in vocab[c]]
        X.append(row)
    return np.array(X, dtype=float)


def _calibration_metrics(pp, y, rows):
    reliability, ece_num = [], 0.0
    for i in range(10):
        lo, hi = i / 10.0, (i + 1) / 10.0
        mask = (pp >= lo) & (pp <= hi if i == 9 else pp < hi)
        nb = int(mask.sum())
        if nb:
            pm, am = float(pp[mask].mean()), float(y[mask].mean())
            reliability.append({"lo": round(lo, 2), "pred": round(pm, 3),
                                "actual": round(am, 3), "n": nb})
            ece_num += abs(pm - am) * nb
    ece = float(ece_num / len(y)) if len(y) else float("nan")
    gated = np.zeros(len(rows), dtype=bool)
    for i, r in enumerate(rows):
        try:
            px = float(r.get("book_taker_price") or 0)
            fee = float(r.get("book_taker_fee_pc") or 0)
            rh = float(r.get("resolve_hours") or 0)
            ev = float(pp[i]) - px - fee
            gated[i] = (float(pp[i]) >= .50 and .02 <= ev <= .20 and 0 < rh <= 4.0 and
                        str(r.get("platform") or "") in TRADEABLE_PLATFORMS)
        except Exception:
            pass
    gated_n = int(gated.sum())
    gated_ece = float("nan")
    if gated_n >= 30:
        pg, yg = pp[gated], y[gated]
        total = 0.0
        for i in range(10):
            lo, hi = i / 10.0, (i + 1) / 10.0
            m = (pg >= lo) & (pg <= hi if i == 9 else pg < hi)
            n = int(m.sum())
            if n:
                total += abs(float(pg[m].mean()) - float(yg[m].mean())) * n
        gated_ece = float(total / gated_n)
    return reliability, ece, gated_ece, gated_n


def _binary_log_loss(probabilities, outcomes, eps=1e-15):
    """Mean binary cross-entropy with explicit tail clipping.

    A calibrated model can still emit exact 0/1 on a small holdout. Clipping keeps the diagnostic
    finite without hiding the mistake: a confidently wrong endpoint still incurs about 34.5 nats.
    """
    p = np.asarray(probabilities, dtype=float).reshape(-1)
    y = np.asarray(outcomes, dtype=float).reshape(-1)
    if len(p) != len(y):
        raise ValueError("probability/outcome length mismatch")
    if not len(y):
        return float("nan")
    p = np.clip(p, float(eps), 1.0 - float(eps))
    return float(-np.mean(y * np.log(p) + (1.0 - y) * np.log1p(-p)))


def _holdout_history_metrics(metric_scope, auc, brier, brier_raw, ece, log_loss):
    """Build mutually exclusive strict/provisional fields for the retrain history ledger."""
    if metric_scope == "provisional_paper":
        return {
            "oos_auc": None, "oos_log_loss": None,
            "brier": None, "brier_raw": None, "ece": None,
            "provisional_auc": round(auc, 4),
            "provisional_brier": round(brier, 5),
            "provisional_log_loss": round(log_loss, 5),
            "provisional_brier_raw": round(brier_raw, 5),
            "provisional_ece": round(ece, 5),
        }
    return {
        "oos_auc": round(auc, 4), "oos_log_loss": round(log_loss, 5),
        "brier": round(brier, 5), "brier_raw": round(brier_raw, 5),
        "ece": round(ece, 5),
        "provisional_auc": None, "provisional_brier": None,
        "provisional_log_loss": None, "provisional_brier_raw": None,
        "provisional_ece": None,
    }


def _train_nested_model(res, db):
    train, validation, test, split = _nested_event_day_split(res, embargo_days=1)
    if not split.get("valid"):
        raise ValueError("nested split invalid: %s" % split.get("reason"))
    if min(len(train), len(validation), len(test)) < 20:
        raise ValueError("nested split cells too small: %s" % split)
    ytr = np.asarray([r.get("won") for r in train], dtype=int)
    yv = np.asarray([r.get("won") for r in validation], dtype=int)
    yt = np.asarray([r.get("won") for r in test], dtype=int)
    if any(len(set(y)) < 2 for y in (ytr, yv, yt)):
        raise ValueError("nested split needs both outcomes in train/validation/test")

    # Vocabulary/model structure is selected without the rolling final slice. Rare market types are
    # capped from train only; test-only categories naturally map to the all-zero unknown bucket.
    vocab = {c: sorted({str(r.get(c) or "") for r in train}) for c in CATS}
    try:
        from collections import Counter
        freq = Counter(str(r.get("market_type") or "") for r in train)
        keep = {v for v, _ in freq.most_common(15)}
        vocab["market_type"] = sorted(v for v in vocab["market_type"] if v in keep)
    except Exception:
        pass
    Xtr, Xv, Xt = featurize(train, vocab), featurize(validation, vocab), featurize(test, vocab)
    decay_hl = load_decay_half_life(db)
    wtr, wv = _decay_weights(train, decay_hl), _decay_weights(validation, decay_hl)
    model, method, val_scores = _fit_heldout_calibrated(Xtr, ytr, Xv, yv, validation, wtr, wv)

    # The rolling final slice is consumed exactly once for this fit, after every model/calibration
    # choice. A later retrain may move older groups into training, so this is a walk-forward
    # diagnostic and never the immutable untouched replication required for promotion.
    raw_test, pp = model.predict_with_raw(Xt)
    auc = float(roc_auc_score(yt, pp))
    brier = float(np.mean((pp - yt) ** 2))
    log_loss = _binary_log_loss(pp, yt)
    brier_raw = float(np.mean((raw_test - yt) ** 2))
    reliability, ece, ece_gated, ece_gated_n = _calibration_metrics(pp, yt, test)
    split.update({
        "calibration_fit_rows": int(len(_validation_calibration_indices(validation)[0])),
        "calibration_select_rows": int(len(_validation_calibration_indices(validation)[1])),
        "calibration_method_scores": {k: round(v, 6) for k, v in val_scores.items()},
        "selected_calibration": method, "test_prediction_calls": 1,
        "test_role": "rolling-event-day-final-slice",
    })
    return {
        "model": model, "vocab": vocab, "base": float(ytr.mean()),
        "auc": auc, "brier": brier, "log_loss": log_loss,
        "brier_raw": brier_raw, "ece": ece,
        "ece_gated": ece_gated, "ece_gated_n": ece_gated_n,
        "reliability": reliability, "cal_method": method, "split": split,
        "fit_rows": train, "validation_rows": validation, "test_rows": test,
        "parity_X": np.vstack((Xtr, Xv)), "decay_half_life": decay_hl,
        "metric_scope": "rolling_five_day_holdout", "paper_authority": True,
        # This rolling slice is a required LIVE diagnostic, not the separately frozen replication
        # contract.  Keep LIVE fail-closed even when the five-day diagnostic is available.
        "live_authority": False,
    }


def _train_provisional_model(res, db):
    """Fit the explicitly PAPER-only three-day model and compute honest provisional metrics."""
    train, validation, test, split = _provisional_event_day_split(res)
    if not split.get("valid"):
        raise ValueError("provisional split invalid: %s" % split.get("reason"))
    if min(len(train), len(validation), len(test)) < 20:
        raise ValueError("provisional split cells too small: %s" % split)
    ytr = np.asarray([r.get("won") for r in train], dtype=int)
    yv = np.asarray([r.get("won") for r in validation], dtype=int)
    yt = np.asarray([r.get("won") for r in test], dtype=int)
    if any(len(set(y)) < 2 for y in (ytr, yv, yt)):
        raise ValueError("provisional split needs both outcomes in train/validation/test")

    vocab = {c: sorted({str(r.get(c) or "") for r in train}) for c in CATS}
    try:
        from collections import Counter
        freq = Counter(str(r.get("market_type") or "") for r in train)
        keep = {v for v, _ in freq.most_common(15)}
        vocab["market_type"] = sorted(v for v in vocab["market_type"] if v in keep)
    except Exception:
        pass
    Xtr, Xv, Xt = featurize(train, vocab), featurize(validation, vocab), featurize(test, vocab)
    decay_hl = load_decay_half_life(db)
    wtr, wv = _decay_weights(train, decay_hl), _decay_weights(validation, decay_hl)
    model, method, val_scores = _fit_heldout_calibrated(Xtr, ytr, Xv, yv, validation, wtr, wv)
    raw_test, pp = model.predict_with_raw(Xt)
    auc = float(roc_auc_score(yt, pp))
    brier = float(np.mean((pp - yt) ** 2))
    log_loss = _binary_log_loss(pp, yt)
    brier_raw = float(np.mean((raw_test - yt) ** 2))
    reliability, ece, ece_gated, ece_gated_n = _calibration_metrics(pp, yt, test)
    fit_idx, select_idx = _validation_calibration_indices(validation)
    split.update({
        "calibration_fit_rows": int(len(fit_idx)),
        "calibration_select_rows": int(len(select_idx)),
        "calibration_method_scores": {k: round(v, 6) for k, v in val_scores.items()},
        "selected_calibration": method, "test_prediction_calls": 1,
        "metric_scope": "provisional_paper",
    })
    return {
        "model": model, "vocab": vocab, "base": float(ytr.mean()),
        "auc": auc, "brier": brier, "log_loss": log_loss,
        "brier_raw": brier_raw, "ece": ece,
        "ece_gated": ece_gated, "ece_gated_n": ece_gated_n,
        "reliability": reliability, "cal_method": method, "split": split,
        "fit_rows": train, "validation_rows": validation, "test_rows": test,
        "parity_X": np.vstack((Xtr, Xv)), "decay_half_life": decay_hl,
        "metric_scope": "provisional_paper", "paper_authority": True,
        "live_authority": False,
    }


def _cluster(ticker):
    # F14: correlated-cluster key — all durations/strikes of a coin move together (one leveraged bet).
    # Returns None outside crypto (sports/politics are diverse → no diversification cap needed).
    up = (ticker or "").upper()
    for c in ("BTC", "ETH", "SOL", "DOGE", "XRP", "BNB", "HYPE"):
        if c in up:
            return "crypto:" + c.lower()
    return None


# R84 (operator-confirmed, REVERSES the R83 no-floor note): the book now ALSO gates entries on a
# standalone p_win floor (ML_MIN_P_WIN, config ml_min_p_win, default 0.50) — the EV floor alone let
# high-EV longshots cook the bankroll (2026-07-05 autopsy: p_win<0.50 = 55% of ML book losses, 14%
# realized vs 35% implied). ev_floor + the 5-95c live band still apply on top. Shadow stays ungated.
def manage_portfolio(db, preds, outcome, stake=10.0, ev_floor=0.015, max_open=30, livepx=None):
    """The ML's OWN paper book: each cycle it 'buys' its top +EV picks, settles any whose market has
    resolved (won/lost from the DB), and tracks running P&L. Persisted in ml_paper.json so it compounds
    across cycles. Pure decision-support — never places a real order.
    The whole read-modify-write runs under the cross-process book lock (audit #6) shared with the
    suite's Go settler; when the lock is busy this cycle's book update is skipped (never clobber)."""
    path = os.path.join(os.path.dirname(os.path.abspath(db)), "ml_paper.json")
    with _book_lock(path) as got:
        if not got:
            raise RuntimeError("ml_paper.lock busy (Go settler active) — book update skipped this cycle")
        return _manage_portfolio_locked(path, preds, outcome, stake, ev_floor, max_open, livepx)


def _sig_unix(ts):
    """R120 provenance: signal_log.ts (RFC3339) -> unix seconds; 0 = unparseable/absent."""
    try:
        return int(datetime.strptime(str(ts)[:19], "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc).timestamp())
    except Exception:
        return 0


def _remaining_hours(row, now=None):
    """Return current hours to the close implied by the immutable signal-time horizon.

    ``resolve_hours`` is stamped when a signal is created. Comparing that frozen value directly
    with the four-hour funding horizon permanently excluded a market first observed five or more
    hours out, even after it later entered the funded window. Missing timestamps retain the
    conservative legacy value; expired rows return a non-positive value.
    """
    try:
        horizon = float(row.get("resolve_hours") or 0)
    except (TypeError, ValueError):
        return 0.0
    if horizon <= 0:
        return 0.0
    signal_ts = _sig_unix(row.get("ts"))
    if signal_ts <= 0:
        return horizon
    return (signal_ts + horizon * 3600.0 - float(now or time.time())) / 3600.0


def _stamp_mark(lot, px, src="mark", now=None):
    """R120 provenance: append one (ts, px, src) trajectory mark under the documented resolution
    rule — first mark always; then only when the side price moved >=1c since the last mark, OR
    >=60s elapsed AND it moved at all. Cap 240 points (older half decimated 2:1); settle trims
    closed lots to <=60 (see the settle loop)."""
    if not px or px <= 0 or px >= 1:
        return
    now = int(now or time.time())
    mk = lot.setdefault("marks", [])
    if mk:
        dpx = abs(float(px) - float(mk[-1][1]))
        if dpx < 0.01 and (now - int(mk[-1][0]) < 60 or dpx < 0.0005):
            return
    mk.append([now, round(float(px), 3), src])
    if len(mk) > 240:
        half = len(mk) // 2
        lot["marks"] = mk[:half:2] + mk[half:]


def _stamp_entry_provenance(lot):
    """Give every book-native-v2 decision explicit, stable economic field names.

    Compact legacy keys remain canonical for compatibility. These aliases keep predicted edge
    separate from realized P&L and preserve the decision-time market price when a maker later
    fills at a different price.
    """
    try:
        lot["model_probability"] = round(float(lot.get("p_win")), 8)
    except (TypeError, ValueError):
        lot["model_probability"] = None
    try:
        value = lot.get("market_price_at_decision", lot.get("price"))
        lot["market_price_at_decision"] = round(float(value), 8)
    except (TypeError, ValueError):
        lot["market_price_at_decision"] = None
    try:
        lot["predicted_fee_net_edge"] = round(float(lot.get("ev_net")), 8)
    except (TypeError, ValueError):
        lot["predicted_fee_net_edge"] = None
    try:
        lot["entry_price"] = round(float(lot.get("price")), 8)
    except (TypeError, ValueError):
        lot["entry_price"] = None
    return lot


def _stamp_terminal_provenance(lot, payout=None, close_market_price=None, reason="settlement"):
    """Record actual result, per-share P&L, and honest close-price CLV.

    The final genuine side-price mark is the close. Settlement payout is never substituted for a
    missing close; in that case CLV remains null. An exact horizon-exit bid is itself a real close.
    """
    _stamp_entry_provenance(lot)
    try:
        contracts = float(lot.get("contracts") or 0)
        pnl = float(lot.get("pnl"))
        lot["realized_cents_per_share"] = round(100.0 * pnl / contracts, 6) if contracts > 0 else None
    except (TypeError, ValueError):
        lot["realized_cents_per_share"] = None
    if payout is not None:
        try:
            lot["settlement_payout"] = round(float(payout), 8)
        except (TypeError, ValueError):
            lot["settlement_payout"] = None
    won = lot.get("won")
    if reason == "horizon_exit":
        lot["final_result"] = "horizon_exit_profit" if won == 1 else "horizon_exit_loss"
    else:
        lot["final_result"] = "won" if won == 1 else "lost"
    if close_market_price is None:
        for mark in reversed(lot.get("marks") or []):
            try:
                src = str(mark[2] if len(mark) > 2 else "")
                px = float(mark[1])
            except (TypeError, ValueError, IndexError):
                continue
            if src not in ("settle", "horizon_exit") and 0 < px < 1:
                close_market_price = px
                break
    try:
        close_px = float(close_market_price)
        entry = float(lot.get("entry_price"))
        if 0 < close_px < 1 and 0 < entry < 1:
            lot["close_market_price"] = round(close_px, 8)
            lot["clv"] = round(close_px - entry, 8)
        else:
            lot["close_market_price"], lot["clv"] = None, None
    except (TypeError, ValueError):
        lot["close_market_price"], lot["clv"] = None, None
    lot["terminal_reason"] = reason
    return lot


def _horizon_exit_lot(lot, now, livepx):
    """Close an aged unresolved lot only from an exact executable exit receipt.

    Missing bid/fee or missing stored entry fee means RETAIN with an explicit pending reason; an
    unresolved position is never scratched or deleted merely because its expected horizon elapsed.
    """
    key = _pxkey(lot)
    snap = _LIVE_BOOK.get(key) or {}
    try:
        exit_px = float(snap.get("exit_price") or 0)
        exit_fee = float(snap.get("exit_taker_fee_pc"))
        known = bool(snap.get("exit_fee_known"))
    except (TypeError, ValueError):
        exit_px, exit_fee, known = 0.0, 0.0, False
    if not known or not (0 < exit_px < 1) or not math.isfinite(exit_fee):
        lot["horizon_exit_pending"] = "fresh executable bid/exact exit fee unavailable"
        return False
    if lot.get("fee") is None:
        lot["horizon_exit_pending"] = "exact entry fee unavailable"
        return False
    try:
        entry_fee = float(lot["fee"])
    except (TypeError, ValueError):
        lot["horizon_exit_pending"] = "invalid entry fee receipt"
        return False
    try:
        entry, contracts = float(lot["price"]), float(lot["contracts"])
    except (TypeError, ValueError, KeyError):
        lot["horizon_exit_pending"] = "invalid legacy lot economics"
        return False
    lot.pop("horizon_exit_pending", None)
    lot["horizon_exit"] = True
    lot["close_reason"] = "horizon_exit"
    lot["exit_price"] = round(exit_px, 6)
    lot["exit_fee"] = round(exit_fee * contracts, 6)
    lot["exit_fee_source"] = snap.get("exit_fee_source") or ""
    lot["total_fees"] = round(entry_fee + exit_fee * contracts, 6)
    lot["pnl"] = _money_round(contracts * (exit_px - entry) - lot["total_fees"])
    lot["won"] = 1 if lot["pnl"] > 0 else 0
    lot["closed_ts"] = int(now)
    _stamp_terminal_provenance(lot, close_market_price=exit_px, reason="horizon_exit")
    lot.setdefault("marks", []).append([int(now), round(exit_px, 6), "horizon_exit"])
    return True


def _manage_portfolio_locked(path, preds, outcome, stake, ev_floor, max_open, livepx=None):
    pf = {"open": [], "closed": []}
    if os.path.exists(path):
        try:
            pf = _json_read(path)  # R80: utf-8 — cp1252-default open() misread the Go side's emoji titles as 'corrupt'
        except Exception as e:
            # CORRUPT-BOOK GUARD (audit): present-but-unparseable ≠ absent. Starting empty here
            # meant the _atomic_dump at the end OVERWROTE the whole live book (positions, lifetime
            # P&L, equity history) on one torn/partial read. Keep the evidence (.bak, best-effort)
            # and ABORT this pass — the caller's try/except logs it and the next cycle retries
            # against the intact file.
            print("  CORRUPT BOOK: ml_paper.json exists but failed to parse (%s) — aborting book pass, file left untouched" % e)
            try:
                shutil.copyfile(path, path + ".bak")
            except Exception:
                pass
            raise RuntimeError("ml_paper.json unparseable — book pass aborted")
    repair_block = str(pf.get("settlement_repair_blocked") or "").strip()
    if repair_block:
        raise RuntimeError("ml_paper.json settlement repair is blocked: %s" % repair_block)
    openpos, closed = pf.get("open", []), pf.get("closed", [])
    life = pf.get("lifetime", {"net": 0.0, "wins": 0, "closed": 0})  # running totals — survive closed[] truncation
    _ensure_ml_venue_lifetime(life, closed)
    # R25 PNL REBASE (operator: "$600 book, reset the P&L/graph, KEEP the data"): one-time on first
    # run after this deploy — remember lifetime net as the baseline and restart the equity graph.
    # Lifetime counters, closed history and open positions are untouched.
    # R28/R30 (operator): book re-anchored $600→$300→$250 (total portfolio $500 = k125+pus125+ml250);
    # bank_ver=3 marks the one-time rebase (fresh net baseline + equity restart at the new anchor).
    if "net_base" not in life or int(life.get("bank_ver", 1)) < 3:
        life["net_base"] = float(life.get("net", 0.0))
        life["bank_ver"] = 3
        pf["equity"] = []
    net_base = float(life.get("net_base", 0.0))
    # R107 MAKER SIM: ingest Go-side touch-through fills BEFORE settling — a fill queued since the
    # last cycle is a real open lot (it may even settle this very pass).
    _ing = _ingest_maker_fills(path, pf, openpos)
    if _ing:
        print("  ML book: ingested %d maker fill(s) from the suite queue (fill_kind=maker)" % _ing)
    # settle any open ML position whose market has now resolved
    now = int(time.time())
    still, horizon_closed = [], 0
    for p in openpos:
        horizon_s = _resolve_horizon_limit(p) * 3600.0
        _v = outcome.get(p["ticker"])  # per-ticker YES value; side-adjust for this position's side
        payout = None if _v is None else (_v if (p.get("side") or "").upper() in ("YES", "UP") else (1.0 - _v))
        if payout is None:
            op = p.get("opened")
            if not op or (now - op) > horizon_s:
                if _horizon_exit_lot(p, now, livepx):
                    closed.append(p)
                    life["net"] = _money_round(float(life.get("net", 0.0) or 0.0) + p["pnl"])
                    _ml_venue_life_add(life, p.get("platform"), p["pnl"], p.get("contracts"))
                    life["closed"] += 1
                    if p["won"] == 1:
                        life["wins"] += 1
                    horizon_closed += 1
                else:
                    still.append(p)  # retain until exact executable exit truth exists
                continue
            still.append(p)
            continue
        e, c = p["price"], p["contracts"]
        fee = _entry_fee(p.get("platform"), c, e)  # venue-true fee (audit F11: was flat Kalshi 7% for every venue)
        if p.get("fee") is not None:
            fee = float(p.get("fee") or 0.0)  # R107: maker-sim lots carry their EXACT entry fee (pure maker / taker divert)
        win = 1 if payout > e else 0
        p["won"] = win
        p["pnl"] = round(c * (payout - e) - fee, 2)  # SCALARPAY: exact (settle − entry)/contract — scalar markets pay their true value, not binary 1/0
        p["pnl"] = _money_round(c * (payout - e) - fee)
        p["closed_ts"] = int(time.time())  # R90 DO-THIS 14: close timestamp on every settled lot
        _stamp_terminal_provenance(p, payout=payout, reason="settlement")
        # R120 provenance: terminal trajectory mark + trim the closed lot's marks to <=60 points
        # (closed[] is a 500-lot ring; coarse shape suffices for history).
        p.setdefault("marks", []).append([int(time.time()), round(float(payout), 3), "settle"])
        while len(p["marks"]) > 60:
            p["marks"] = p["marks"][:-1:2] + [p["marks"][-1]]
        closed.append(p)
        life["net"] = _money_round(float(life.get("net", 0.0) or 0.0) + p["pnl"])
        _ml_venue_life_add(life, p.get("platform"), p["pnl"], p.get("contracts"))
        life["closed"] += 1
        if win == 1:
            life["wins"] += 1
    openpos = still
    if horizon_closed:
        print("  ML book: closed %d aged unresolved lot(s) at exact executable horizon exits" % horizon_closed)
    # R120 PROVENANCE: stamp every open lot's P&L trajectory off this cycle's re-scored live
    # prices (preds carry live_price for their re-scored side — same source the chute prices at).
    _pxmap = {}
    for _x in preds or []:
        _lp = _x.get("live_price")
        if _lp:
            _pxmap[(_x.get("ticker"), (_x.get("side") or "").upper())] = float(_lp)
    _mnow = int(time.time())
    for p in openpos:
        _stamp_mark(p, _pxmap.get((p.get("ticker"), (p.get("side") or "").upper())), "mark", _mnow)
    # R99 bug 193 (auditor r25): the dup/"traded" set must NOT span reset epochs — closed[] keeps
    # pre-reset rows for history, and keying dups on them made the fresh epoch refuse re-entry into
    # ~500 tickers the OLD epoch had traded (a real placement-famine input post-reset). Epoch =
    # after the LAST reset_close marker (the R92 convention the EV-quality window below also uses).
    _ep_cut = 0
    for _i, _c in enumerate(closed):
        if _c.get("reset_close"):
            _ep_cut = _i + 1
    epoch_closed = [p for p in closed[_ep_cut:] if not p.get("reset_close")]
    traded = {(p["ticker"], p["side"]) for p in openpos} | {(p["ticker"], p["side"]) for p in epoch_closed}
    # R99 ONE-SIDE-PER-MARKET (redesign of the R97 hedge guard; auditor r24 §3c): ANY open lot on a
    # market blocks new lots on that market, whatever the side label — the R97 _OPP_SIDE map only
    # knew YES/NO/UP/DOWN, so exotic side vocabularies bypassed the guard entirely. On conflicting
    # signals the book KEEPS the existing position (no sell-and-rebuy churn); once the lot
    # settles/evicts the market is tradeable again.
    open_tickers = {p["ticker"] for p in openpos}
    # R99 CROSS-BOOK HEDGE BLOCK: the Go auto books share the one bankroll — holding the opposite
    # side of THEIR open lot burns fees+spread portfolio-wide just like an in-book hedge. The suite
    # stamps its open auto lots into the ml_alloc.json handshake (writeMLAllocFile, ~30s sweep).
    auto_open_sides = load_auto_open(os.path.dirname(os.path.abspath(path)))
    # MLKELLY: the ML book sizes each bet by its OWN edge (quarter-Kelly off current equity) — a real
    # self-sizing portfolio, not flat $10. equity = the book's bank + lifetime net.
    # R78 EQUITY-FRACTION BANK: the bank is DYNAMIC — the Go equity sweep writes alloc_ml x total
    # paper equity (NAV) to ml_alloc.json (~30s cadence) and this book anchors to it each cycle, so
    # Kelly sizing compounds with the WHOLE portfolio ("don't flat-limit ML to $500 when total
    # equity is $4000"). NAV already contains this book's own net, so the sizing equity IS the
    # target (no double count) and bank0 = target - net keeps equity = bank0 + net = target for
    # every display/ROI consumer. No handshake file -> the legacy $250 anchor (R30), exactly as before.
    net_now = float(life.get("net", 0.0)) - net_base
    _bank_target = load_ml_bank(os.path.dirname(os.path.abspath(path)))
    _venue_banks = load_ml_venue_banks(os.path.dirname(os.path.abspath(path)), _bank_target)
    # R106 (auditor r34, bug 280 — paste-ready fix applied): bank0 was REWRITTEN every cycle as
    # `target - net` — a plug that made file bank0 == equity - epoch_net by construction (it
    # drifted 350->268.88 in a day, faked a "giveback" in every equity read, and let the book
    # deploy 109-118% of its true bank). bank0 is now the EPOCH ANCHOR: the Go reset stamps it;
    # this code only seeds it when a legacy file carries none. Kelly keeps sizing on the alloc
    # share (kelly_bank — new observability key in the dump), and display equity = bank0 + net —
    # the invariant every consumer (header anchors, ROI, auditors) expects.
    kelly_bank = None
    if _bank_target is not None:
        kelly_bank = max(1.0, _bank_target)
        bank0 = float(pf.get("bank0", 0.0) or 0.0)
        # ONE-SHOT MARKER MIGRATION (bug 280): pre-R106 files carry a PLUG in bank0 (the code
        # rewrote it as target−net every cycle, so equity==target by construction). Drift-window
        # detection proved fragile against an actively-settling book (net moved >$2 between the
        # plug's last write and the check — two live misses at the R106 deploy), so the migration
        # is now deterministic: it runs EXACTLY ONCE per file (bank0_migrated marker, written on
        # every dump from here on) and repairs bank0 to the epoch grant when the file still has
        # the plug SHAPE — equity(=bank0+net) within $25 of the alloc target (true for every plug
        # file; a legitimately-custom anchor differs by its own epoch net) while bank0 sits far
        # from the grant.
        if not pf.get("bank0_migrated"):
            _grant = load_ml_epoch_anchor(os.path.dirname(os.path.abspath(path)))
            if (_grant and _grant > 0 and bank0 > 0
                    and abs(bank0 - _grant) > 2.0
                    and abs((bank0 + net_now) - _bank_target) < 25.0):
                bank0 = _grant
        # R127 OPERATOR HANDOFF (upward re-anchor — helper + rationale at _reanchor_bank0): an
        # incoming bank HIGHER than the anchored bank re-anchors bank0 UP by the delta so the
        # $600 four-book grant actually reaches Kelly sizing (min() below stops clamping at the
        # stale anchor). Positions/history/net untouched; downward keeps the old conservative
        # path. Runs BEFORE the <=0 seed on purpose: a fresh legacy file keeps its exact
        # target−net seed semantics. One journal line per actual re-anchor (bank0 persists in
        # the dump, so the delta is zero on every following cycle).
        if bank0 > 0:
            bank0, _r127_delta = _reanchor_bank0(bank0, kelly_bank)
            if _r127_delta > 0:
                print("  ML book: bank re-anchored +$%.2f to $%.2f per R127 operator order" % (_r127_delta, bank0))
        if bank0 <= 0:
            bank0 = max(1.0, round(_bank_target - net_now, 2))  # legacy file without an anchor: seed once
        # R115 KELLY ANCHOR (auditor EPOCH3 URGENT-2, specced fix): sizing bank = the LESSER of the
        # alloc share and the book's OWN equity (bank0 + epoch net). Pre-R115 sizing used the alloc
        # share alone, so a losing book kept deploying ~135% of what it actually had (kelly_bank
        # 373 vs equity 270 measured r38). kelly_bank stays in the dump as the observability key.
        ml_equity = min(kelly_bank, max(1.0, bank0 + net_now))
    else:
        # R108 (auditor 313): a TRANSIENT ml_alloc.json read miss used to force bank0=250 and
        # persist it under bank0_migrated=True — unrepairable. Prefer the book's own anchor.
        bank0 = float(pf.get("bank0", 0.0) or 0.0) or 250.0  # 250 = R30 legacy seed, last resort only
        ml_equity = bank0 + net_now
    if not _venue_banks:
        # Legacy/no-handshake mode still gets two bounded sleeves inside its existing one bank.
        _venue_banks = load_ml_venue_banks(
            os.path.dirname(os.path.abspath(path)), max(0.0, float(bank0)))
    # D5 drawdown breaker — R106 (auditor r34, bug 288): the brake now reads the BOOK-OWN epoch
    # curve (bank0 + net) instead of alloc x NAV, so sibling-book losses/NAV dips can no longer
    # read as ML drawdown, and the Go reset's eq_peak re-anchor (= fresh bank0) is consistent
    # with this curve by construction. (r34 also measured that braking on THIS epoch's path
    # destroyed $56/46% of profit — with the 280 plug gone the false triggers go with it.)
    _book_curve = bank0 + net_now
    peak = _book_equity_peak(pf.get("eq_peak", _book_curve), bank0, _book_curve)
    dd_brake = 0.5 if (peak > 0 and _book_curve < peak * 0.80) else 1.0
    # F14 diversification cap: at most CLUSTER_CAP positions per correlated cluster (same coin) so the book
    # is not 30 BTC coin-flips. Non-crypto markets are diverse → uncapped. Seed counts from open positions.
    CLUSTER_CAP = 4
    cl_count = {}
    for _p in openpos:
        _ck = _cluster(_p.get("ticker"))
        if _ck:
            cl_count[_ck] = cl_count.get(_ck, 0) + 1
    # MLCAP: NO LEVERAGE. The book was deploying more than its bankroll (e.g. $1,860 of open cost on a
    # ~$600 book) because each of up to 30 picks could take 10% of equity with no cumulative check.
    # Track total deployed cost basis and stop once the whole equity is invested.
    deployed = sum((p.get("price", 0) or 0) * (p.get("contracts", 0) or 0)
                   + (p.get("fee", 0) or 0) for p in openpos)
    max_deploy = ml_equity  # at most 100% of equity across all open positions (no borrowing)
    # R107 MAKER SIM: resting posts are RESERVED capital + block their market (one lot per market
    # includes not-yet-filled posts — a fill can arrive any sweep tick).
    _mk_sim = _maker_sim_on(path)
    _pend = []
    if _mk_sim:
        _lock_heartbeat(path)  # R108 (312): HTTP inside the book lock — keep the lock visibly alive
        _pend = _ml_pending(path)
        if _pend is None:
            # R108 (auditor 317): pending state UNKNOWN — capital reservation + same-market
            # blocking would be silently lost. Fail-closed: no new entries this cycle.
            max_deploy = -1.0
            _pend = []
        for _pp in _pend:
            deployed += ((_pp.get("post_px") or 0) * (_pp.get("contracts") or 0)
                         + (_pp.get("fee") or 0))
            if _pp.get("ticker"):
                open_tickers.add(_pp.get("ticker"))
    # One durable ML grant, two destination risk sleeves. The default is a strict 50/50 split;
    # their grants always add back to ml_bank_usd, so this cannot create two full-size ML banks.
    _venue_epoch_net = _ml_venue_epoch_net(life)
    _venue_epoch_closed = _ml_venue_epoch_metric(life, "closed")
    _venue_epoch_contracts = _ml_venue_epoch_metric(life, "contracts")
    venue_sleeves = _ml_venue_sleeves(
        openpos, epoch_closed, _pend, _venue_banks, _venue_epoch_net,
        _venue_epoch_closed, _venue_epoch_contracts)
    deployed_by_venue = {
        venue: float(row.get("deployed") or 0.0) + float(row.get("reserved") or 0.0)
        for venue, row in venue_sleeves.items()
    }
    max_deploy_by_venue = {
        venue: float(row.get("sizing_balance") or 0.0) for venue, row in venue_sleeves.items()
    }
    max_deploy = sum(max_deploy_by_venue.values())
    _new_pending = []
    # open NEW top +EV picks not already traded (preds are sorted by NET EV desc)
    mlrej = []  # R49 (operator): log WHY the ML book skips picks — served at /api/mlrejections
    mlrej_counts = {}
    open_before = len(openpos)

    def _rej(x, reason):
        # Preserve the complete current-cycle funnel even when the human-readable sample reaches
        # 200 rows. Horizon/dedup/capacity gates used to disappear silently, making a healthy
        # scored universe look like a dead executor.
        mlrej_counts[reason] = mlrej_counts.get(reason, 0) + 1
        if len(mlrej) < 200:
            mlrej.append({"ticker": x.get("ticker"), "side": x.get("side"), "price": x.get("price"),
                          "p_win": x.get("p_win"), "reason": reason, "ts": int(time.time())})

    if not ML_PAPER_NEW_ENTRIES_ENABLED:
        for x in preds:
            _rej(x, "book-native ML Paper is log-only; realistic delayed execution not connected")
        preds = []
    for x in preds:
        if len(openpos) >= max_open:
            _rej(x, "portfolio open-position cap reached")
            break
        if deployed >= max_deploy:
            _rej(x, "portfolio deployment cap reached")
            break  # MLCAP: book is fully invested — do not open more (would exceed bankroll)
        # audit F11: gate on the fee-NET edge — the old gross gate's 1.5¢ floor sat BELOW the
        # ~1.75¢ mid-price fee, so the book systematically bought −EV-after-fees picks.
        # R99 bug 176 (auditor r24/r25 URGENT): preds are sorted by ev_net_live since R98 —
        # signal-time ev_net is NOT the sort key anymore, so a below-floor sig-EV row must SKIP
        # (continue), never end the whole pass. The old `break` here died at the first
        # price-slipped row (~rank 19 of 3,548) and the book placed NOTHING for 6+ cycles after
        # the R98 reset. Only max_open/deployed (true monotone caps) may break.
        if x.get("ev_net", x["ev_per_contract"]) < ev_floor:
            _rej(x, "signal-time fee-net EV below floor")
            continue
        if (x.get("platform") or "") not in TRADEABLE_PLATFORMS:
            _rej(x, "venue has no funded execution route")
            continue  # TRADEABLE: only paper-trade US-legal venues (Kalshi + Poly US); poly-int stays scored, not traded
        _venue = str(x.get("platform") or "").lower()
        _venue_equity = max_deploy_by_venue.get(_venue, 0.0)
        _venue_remaining = _venue_equity - deployed_by_venue.get(_venue, 0.0)
        if _venue_equity <= 0 or _venue_remaining <= 0:
            _rej(x, "%s ML sleeve has no available balance" % _venue)
            continue
        rh = _remaining_hours(x, now)
        # MLHOURS/MLSOON: the book only trades markets with a KNOWN, soon resolution (0 < rh <= max).
        # rh==0 means unknown horizon — that's how months-out World Cup / iPhone / Game-of-Year markets
        # leaked into the book before. Excluding unknown-horizon markets keeps the book "ending-soonish".
        horizon_limit = _resolve_horizon_limit(x)
        if rh <= 0 or rh > horizon_limit:
            _rej(x, "outside current %.1fh horizon" % horizon_limit)
            continue
        k = (x["ticker"], x["side"])
        if k in traded:
            _rej(x, "market-side already sampled this reset epoch")
            continue
        if x["ticker"] in open_tickers:
            _rej(x, "market already held — one side per market (R99, ex-R97 hedge guard)")
            continue
        _osd = _OPP_SIDE.get((x.get("side") or "").strip().upper(), "")
        if _osd and ((x.get("platform") or ""), x["ticker"], _osd) in auto_open_sides:
            _rej(x, "opposite side held by the auto paper book (shared bankroll, R99)")
            continue
        ck = _cluster(x["ticker"])
        if ck and cl_count.get(ck, 0) >= CLUSTER_CAP:
            _rej(x, "correlated-cluster position cap reached")
            continue  # F14: this coin-cluster is already full
        # R84 p_win FLOOR (operator-confirmed): refuse the pick outright when the model itself says
        # it loses more often than it wins — whatever the EV math claims. Evidence (2026-07-05):
        # p_win<0.50 picks = 55% of ML book losses, 14% realized win rate vs 35% implied. ml_min_p_win
        # (default 0.50, hot-reloaded each cycle; explicit 0 disables). Shadow book stays UNGATED.
        if ML_MIN_P_WIN > 0 and float(x.get("p_win") or 0) < ML_MIN_P_WIN:
            _rej(x, "p_win below floor")
            continue
        # R32/R33 BUY AT THE LIVE PRICE — MANDATORY (operator verified 14/18 rows booked at stale
        # signal prices under the soft version): NO live venue price → NO bet, period. The pick is
        # rescored ~10s later anyway, so a cold feed only delays it. With a live price, RE-GATE the
        # net edge there — a pick that's only +EV at a stale price is a mirage.
        sig_price = float(x["price"])
        px_src = "live"
        lp = (livepx or {}).get(_pxkey(x))
        if not lp or not (0.0 < float(lp) < 1.0):
            _rej(x, "no live venue price")
            continue  # R33: refuse to book blind — stale entries poisoned the book once already
        lp = float(lp)
        # R44 (operator: "wtf is this bet" — $25 of a 1¢ team that had already LOST): the live price
        # is the truth serum. (a) SANITY BAND: calibration is junk at the tails — never book below
        # 5¢ or above 95¢ at the LIVE price, whatever the model claims. (b) IMPLAUSIBILITY: p_win
        # ≥3× a sub-10¢ live price means the market is SCREAMING the model is wrong (near-settled
        # games etc.) — same guard the live proposals use. R79: (b) is now OPT-IN via config
        # ml_implausibility_guard (default OFF per operator); the band (a) stays unconditional.
        if lp < 0.05 or lp > 0.95:
            _rej(x, "live price outside 5-95c band")
            continue
        if ML_IMPLAUSIBILITY_GUARD and lp < 0.10 and float(x.get("p_win") or 0) > 3 * lp:
            _rej(x, "model-implausible (p_win >=3x sub-10c live price)")
            continue
        # R99 bug 192 (auditor r25): the R98 crash clamp ran in the RANKING only — this book gate
        # recomputed a raw ev_net_live and happily bought the very rows the ranking had clamped
        # (tonight's only post-reset open: sig 61¢ → live 37.5¢, a "bargain" that is really the
        # market declaring the pick dying while signal-time p_win can't see it). Same rule as the
        # ranking (live_ml ranking block): ≥25¢ absolute drop from the signal price OR a ≥60%
        # relative collapse (8¢ floor) ⇒ never a bet, whatever the raw math claims.
        _px_drop = sig_price - lp
        if _px_drop >= 0.25 or (_px_drop >= 0.08 and lp < 0.4 * sig_price):
            _rej(x, "live price crashed vs signal — market says the pick is dying (R99/R98 clamp)")
            continue
        ev_live = float(x.get("p_win") or 0) - lp
        _live_fee_pc = float(x.get("live_taker_fee_pc") or 0)
        ev_net_live = ev_live - _live_fee_pc
        # R90 edge 29 (realization haircut): scale the net edge by this family's MEASURED
        # actual/predicted ratio (Go writes ml_realization.json from this book's settled lots) —
        # fantasy-EV families (pbridge-class) shrink toward what they actually realize.
        _hc = float(REALIZATION_RATIOS.get(x.get("signal_type") or "", 1.0))
        if REALIZATION_ENABLED and ev_net_live > 0 and 0 <= _hc < 1:
            ev_net_live = ev_net_live * _hc
        if ev_net_live < ev_floor:
            _rej(x, "net EV below floor at live price")
            continue  # edge gone at the REAL price → not a bet
        # R90 ML BORDERS (operator): plausibility band + EV window AFTER the haircut. The
        # existing 5-95c live-price band + implausibility rule above stay independently intact.
        _pw = float(x.get("p_win") or 0)
        if ML_BORDERS["pwin_min"] > 0 and _pw < ML_BORDERS["pwin_min"]:
            _rej(x, "R90 border: p_win below plausibility floor")
            continue
        if ML_BORDERS["pwin_max"] > 0 and _pw > ML_BORDERS["pwin_max"]:
            _rej(x, "R90 border: p_win above plausibility ceiling")
            continue
        if ML_BORDERS["ev_min_c"] > 0 and ev_net_live * 100 < ML_BORDERS["ev_min_c"]:
            _rej(x, "R90 border: net EV below border floor")
            continue
        if ML_BORDERS["ev_max_c"] > 0 and ev_net_live * 100 > ML_BORDERS["ev_max_c"]:
            _rej(x, "R90 border: net EV above too-good ceiling -> quarantined (never bet)")
            _quarantine(os.path.dirname(os.path.abspath(path)), x, lp, ev_net_live)
            continue
        x = dict(x, price=round(lp, 3), ev_per_contract=round(ev_live, 4), ev_net=round(ev_net_live, 4))
        pr = max(x["price"], 0.01)
        kf = max(0.0, float(x.get("ev_net", x.get("ev_per_contract", 0.0))) / max(1e-6, 1 - pr)) * 0.25  # quarter-Kelly on the NET edge (audit F11)
        # Quarter-Kelly, the 10% entry cap, and remaining capital use the destination venue
        # sleeve. The total ML wall remains a second, never-looser cap.
        dollars = _ml_sleeve_order_dollars(
            _venue_equity, deployed_by_venue.get(_venue, 0.0), max_deploy - deployed,
            kf, dd_brake)
        if dollars < pr:
            _rej(x, "sizing below one executable contract")
            continue  # MLCAP: not enough remaining bankroll for even one contract of this pick
        c = max(1, int(dollars / pr))
        deployed += c * pr  # MLCAP: track cumulative cost basis so the book stops at 100% invested
        deployed_by_venue[_venue] = deployed_by_venue.get(_venue, 0.0) + c * pr
        _ = px_src
        _fee_snap = _LIVE_BOOK.get(_pxkey(x)) or {}
        row = {"ticker": x["ticker"], "side": x["side"], "signal_type": x["signal_type"],
               "platform": x.get("platform"), "title": x["title"], "price": x["price"], "contracts": c,
               "p_win": x["p_win"], "ev": x["ev_per_contract"], "ev_net": x.get("ev_net"), "opened": now,
               "px_src": "live",
               # R120 provenance: signal_ts = when the originating signal_log row fired;
               # decision_ts = this chute decision; fill_ts = now for instant fills (the maker
               # path's Go router overwrites fill_ts at the real touch-through and adds post_ts).
               "signal_ts": _sig_unix(x.get("ts")) or None,
               "decision_ts": now, "fill_ts": now,
               "fee": round(c * _live_fee_pc, 4),
               "fee_known": bool(_fee_snap.get("fee_known")),
               "fee_source": _fee_snap.get("taker_fee_source") or "",
               # The no-maker-simulator path is an immediate taker fill. A later resting-maker
               # acceptance overwrites these fields at its real touch-through.
               "fill_kind": "taker", "fill_rule": "direct",
               "model_cohort": BOOK_FEATURE_SCHEMA,
               "model_lineage": MODEL_LINEAGE,
               # Exact fitted-generation identity. LIVE proof must never borrow closed results
               # from an older model that happens to share the same feature schema.
               "model_version": str(x.get("model_version") or ""),
               "epoch_id": str(pf.get("epoch_id") or ""),
               # Paper may deliberately collect farther out than today's LIVE contract. Persist
               # the actual decision-time horizon so future LIVE proof can use only the identical
               # <=4h regular / <=2h crypto cohort rather than borrowing 24h/6h research rows.
               "entry_resolve_hours": round(float(rh), 6),
               "entry_is_crypto": bool(_is_crypto_contract(x))}
        if px_src == "live" and abs(sig_price - float(x["price"])) >= 0.001:
            row["sig_price"] = round(sig_price, 3)  # visibility: what the signal said vs what we PAID
        if x.get("flipped_from"):
            # R99 (auditor grading tag): the lot carries the flip marker into closed[] at settle,
            # so flipped picks' win-rate is separately measurable from the book rings.
            row["flipped_from"] = x["flipped_from"]
        # R107 MAKER SIM (operator: every paper book fills honestly): the lot no longer books
        # instantly at the live price. The suite decides: gate-safe ⇒ a resting post (fill
        # arrives later via ml_maker_fills.jsonl, ONLY on real touch-through); gate-unsafe ⇒ an
        # instant TAKER quote we book ourselves at the ask with taker fees, tagged. Reject ⇒
        # not a bet this cycle (deployed reservation rolled back).
        if _mk_sim and (x.get("platform") or "").lower() in ("kalshi", "polyus"):
            _stamp_entry_provenance(row)
            _lock_heartbeat(path)  # R108 (312): each post is up to 8s of HTTP inside the lock
            _resp = _ml_post(path, row, float(x["price"]))
            _st = _resp.get("status")
            if _st == "pending":
                _post_px = float(_resp.get("post_px") or x["price"])
                _post_fee = float(_resp.get("fee") or 0.0)
                _new_pending.append({"platform": _venue, "post_px": _post_px,
                                     "contracts": c, "fee": _post_fee})
                # Replace the pre-route ask reservation with the actual resting principal+fee.
                _post_delta = c * (_post_px - pr) + _post_fee
                deployed += _post_delta
                deployed_by_venue[_venue] = deployed_by_venue.get(_venue, 0.0) + _post_delta
                traded.add(k)
                open_tickers.add(x["ticker"])  # a resting post blocks the market like an open lot
                if ck:
                    cl_count[ck] = cl_count.get(ck, 0) + 1
                # deployed stays incremented: the post reserves its stake (post_px ≤ lp)
                continue
            if _st == "taker":
                row = dict(row, price=round(float(_resp.get("px") or x["price"]), 3),
                           fee=round(float(_resp.get("fee") or 0.0), 4),
                           fee_known=bool(_resp.get("fee_known")),
                           fee_source=_resp.get("fee_source") or "",
                           fill_kind="taker", fill_rule=_resp.get("rule") or "divert",
                           px_src=_resp.get("px_src") or "live",
                           relation_receipt_id=_resp.get("relation_receipt_id") or "")
                # The route may divert at a different ask than the pre-route quote. Keep both the
                # total wall and the destination sleeve reserved at the actual fill cost.
                _fill_delta = c * (float(row["price"]) - pr)
                deployed += _fill_delta
                deployed_by_venue[_venue] = deployed_by_venue.get(_venue, 0.0) + _fill_delta
            else:
                deployed -= c * pr  # roll back the reservation — not a bet this cycle
                deployed_by_venue[_venue] = max(
                    0.0, deployed_by_venue.get(_venue, 0.0) - c * pr)
                _rej(x, "maker-sim: %s" % (_resp.get("reason") or "rejected"))
                continue
        # Filled entry fees (including a negative maker rebate) consume/release the same sleeve as
        # principal. This cycle's later decisions therefore see the exact all-in exposure.
        _entry_capital_fee = float(row.get("fee") or 0.0)
        deployed += _entry_capital_fee
        deployed_by_venue[_venue] = deployed_by_venue.get(_venue, 0.0) + _entry_capital_fee
        _stamp_entry_provenance(row)
        openpos.append(row)
        traded.add(k)
        open_tickers.add(x["ticker"])  # R99: a freshly-opened lot blocks the whole market this pass (one side per market)
        if ck:
            cl_count[ck] = cl_count.get(ck, 0) + 1
    try:  # R49: persist this cycle's skip reasons for the Logs → Rejects: ML sub-tab
        _atomic_dump({"rows": mlrej, "counts": mlrej_counts,
                      "input_predictions": len(preds), "open_before": open_before,
                      "filled_open_added": max(0, len(openpos) - open_before),
                      "maker_posts_added": len(_new_pending), "updated": int(time.time())},
                     os.path.join(os.path.dirname(os.path.abspath(path)), "ml_rejections.json"))
    except Exception:
        pass
    # R77 item 1: displayed stats are SINCE the last RESET epoch. The Go reset (resetMLBook) stamps
    # closed_base/wins_base alongside net_base; absent bases read 0, so a never-reset book still
    # shows lifetime totals (and closed[] truncation still never loses history — life[] accrues).
    n = max(0, int(life["closed"]) - int(life.get("closed_base", 0) or 0))
    w = max(0, int(life["wins"]) - int(life.get("wins_base", 0) or 0))
    net = round(float(life["net"]) - net_base, 2) # R25: displayed net is SINCE the rebase; lifetime stays in life["net"]
    # R78: bank0 was set above (dynamic alloc_ml x NAV bank, or the legacy $250 anchor) — the file's
    # stats/equity/bank0 all carry the SAME dynamic bank, so the Go liveMarkBook header anchors track it.
    equity = bank0 + net
    hist = pf.get("equity", [])
    hist.append({"t": int(time.time()), "eq": round(equity, 2), "closed": n})
    hist = hist[-2000:]  # cap the equity curve so the file can't grow unbounded over a long run
    # EV-QUALITY: measure the model's EV calibration — mean PREDICTED ev/contract at entry vs mean
    # REALIZED pnl/contract on closed picks. NET-vs-NET (audit F11): realized pnl is fee-inclusive, so
    # the predicted side must be the fee-net ev too — comparing gross-pred vs net-real conflated fee
    # drag with model optimism. Uses the retained closed[] window (legacy rows fall back to gross ev).
    # R92 (operator: "reset must ACTUALLY reset"): the window is SINCE THE RESET EPOCH. closed[]
    # keeps pre-reset rows for history and the Go reset stamps its force-closes reset_close=True —
    # so the epoch is "after the last reset_close marker", and the markers themselves
    # (mark-to-market force-closes, not realizations) are excluded. Before this, ev_pred_mean /
    # ev_real_mean blended every retained close across resets.
    cut = 0
    for i, c in enumerate(closed):
        if c.get("reset_close"):
            cut = i + 1
    epoch_closed = [c for c in closed[cut:] if not c.get("reset_close")]
    ev_pred = [c.get("ev_net", c["ev"]) for c in epoch_closed if c.get("ev") is not None and c.get("contracts")]
    ev_real = [(c["pnl"] / c["contracts"]) for c in epoch_closed if c.get("pnl") is not None and c.get("contracts")]
    ml_ev_pred = round(sum(ev_pred) / len(ev_pred), 4) if ev_pred else None
    ml_ev_real = round(sum(ev_real) / len(ev_real), 4) if ev_real else None
    ml_ev_ratio = round(ml_ev_real / ml_ev_pred, 3) if (ml_ev_pred and ml_ev_real is not None and ml_ev_pred != 0) else None
    # R115 ICIR: per-day rank correlation (Spearman IC) between predicted edge (ev_net) and realized
    # $/contract for lots RESOLVING that day, then ICIR = mean(IC)/std(IC) over the trailing 30 days.
    # Plain reading: IC answers "on a given day, did the lots we ranked higher actually do better?";
    # ICIR answers "is that skill steady or noise?". |ICIR| > 0.5 is decent for daily horizons;
    # near 0 = the ranking carries no reliable signal. Uses ALL retained closes in the 30d window
    # (rank correlation is epoch-agnostic — resets move the bank, not the model's ordering skill).
    def _r115_spearman(x, y):
        def rk(v):
            order = sorted(range(len(v)), key=lambda i: v[i])
            r = [0.0] * len(v)
            i = 0
            while i < len(order):
                j = i
                while j + 1 < len(order) and v[order[j + 1]] == v[order[i]]:
                    j += 1
                for k in range(i, j + 1):
                    r[order[k]] = (i + j) / 2.0
                i = j + 1
            return r
        rx, ry = rk(x), rk(y)
        nn = len(x)
        mx, my = sum(rx) / nn, sum(ry) / nn
        sxy = sum((a - mx) * (b - my) for a, b in zip(rx, ry))
        sxx = sum((a - mx) ** 2 for a in rx)
        syy = sum((b - my) ** 2 for b in ry)
        return (sxy / (sxx * syy) ** 0.5) if sxx > 0 and syy > 0 else None
    _ic_by_day = {}
    _now_t = int(time.time())
    for c in closed:
        if c.get("reset_close") or c.get("pnl") is None or not c.get("contracts"):
            continue
        _cts = int(c.get("closed_ts", 0) or 0)
        _cev = c.get("ev_net", c.get("ev"))
        if _cts <= 0 or _now_t - _cts > 30 * 86400 or _cev is None:
            continue
        _ic_by_day.setdefault(_cts // 86400, []).append((float(_cev), float(c["pnl"]) / float(c["contracts"])))
    _ics = []
    for _d in sorted(_ic_by_day):
        _pairs = _ic_by_day[_d]
        if len(_pairs) >= 5:  # need a few lots to rank meaningfully
            _ic = _r115_spearman([p[0] for p in _pairs], [p[1] for p in _pairs])
            if _ic is not None:
                _ics.append(_ic)
    ic_last = round(_ics[-1], 3) if _ics else None
    icir = None
    if len(_ics) >= 5:
        _m = sum(_ics) / len(_ics)
        _sd = (sum((v - _m) ** 2 for v in _ics) / (len(_ics) - 1)) ** 0.5
        if _sd > 1e-9:
            icir = round(_m / _sd, 2)
    # Recompute after this cycle's fills/posts so every API consumer sees the balance that the
    # next order decision will actually use.
    venue_sleeves = _ml_venue_sleeves(
        openpos, epoch_closed, list(_pend) + _new_pending, _venue_banks,
        _ml_venue_epoch_net(life), _ml_venue_epoch_metric(life, "closed"),
        _ml_venue_epoch_metric(life, "contracts"))
    stats = {"closed": n, "wins": w, "win_rate": round(w / n, 3) if n else 0,
             "net": round(net, 2),
             "contracts": sum(_ml_venue_epoch_metric(life, "contracts").values()),
             "open_n": len(openpos), "bank0": bank0,
             "equity": round(equity, 2), "roi": round(net / bank0, 3) if bank0 else 0,
             "kelly_bank": kelly_bank,  # R106 (bug 280): the alloc share Kelly actually sizes on — observability, never the anchor
             "venue_sleeves": venue_sleeves,
             "ev_pred_mean": ml_ev_pred, "ev_real_mean": ml_ev_real, "ev_calib_ratio": ml_ev_ratio,
             "ic_last": ic_last, "ic_days": len(_ics), "icir": icir}  # R115: daily rank-IC + trailing-30d ICIR
    book_out = {"open": openpos, "closed": closed[-500:], "lifetime": life, "stats": stats, "equity": hist,
                "bank0": bank0, "kelly_bank": kelly_bank, "bank0_migrated": True,
                "venue_sleeves": venue_sleeves,
                "eq_peak": round(peak, 2), "updated": int(time.time())}
    _carry_book_epoch_contract(pf, book_out)
    book_out.update(_maker_fill_checkpoint(pf))
    _atomic_dump(book_out, path)
    # The Paper settlement is durable before its funded-relation outcome is reported. A lost HTTP
    # response leaves the marker absent, so the next cycle retries the idempotent endpoint.
    if _sync_funded_relation_outcomes(path, book_out.get("closed")):
        _atomic_dump(book_out, path)
    return stats


# SHADOW-BOOK LOCK (audit: manage_shadow RMW race): ONE module-level gate for every in-process
# reader/writer of the ml_shadow.json book state. manage_shadow's ENTIRE read-modify-write runs
# inside it (mirroring manage_portfolio's lock-then-work split), so a concurrent caller — another
# thread, a future settle path — can never interleave mid-RMW and resurrect settled positions.
# The save itself is already torn-proof (_atomic_dump: tmp then os.replace).
_SHADOW_LOCK = threading.Lock()


def manage_shadow(db, preds, outcome, stake=20.0, livepx=None, allow_new=True):
    """SHADOW book: the all gross-+EV control book — measures what the gates earn.
    R64: takes EVERY resolvable, tradeable scored market whose GROSS edge at the LIVE price
    (p_win − live px) is positive, at a FLAT stake — no net-EV floor, no model-edge floor. Only the
    data-integrity guards remain (mandatory live price, 5–95¢ live band, and — R79, because the
    shadow copy of the implausibility rule is IDENTICAL to the real book's — the implausibility
    check, now gated on the same config ml_implausibility_guard flag, default OFF per operator).
    R67h (operator: maker-only): entry stays GROSS-gated, but the LEDGER now charges the venue's
    MAKER fee (share=1.0 — kalshi ≈ ¼-taker bound, Poly US a −1.25% REBATE) on each entry, stored
    on the position and subtracted at settle. R67h also widened the candidate source to the FULL
    scored file (both sides) — it was silently top-200-of-preds via the livepx cap.
    Persisted in ml_shadow.json. Pure analysis; never a real order.
    The whole read-modify-write is serialized under _SHADOW_LOCK (audit: RMW race)."""
    path = os.path.join(os.path.dirname(os.path.abspath(db)), "ml_shadow.json")
    with _SHADOW_LOCK:
        # R99 bug 177 (auditor r24/r25, frozen-record protection): _SHADOW_LOCK only serializes
        # THREADS in this process — the Go side (resetMLBook / settleMLBook) writes ml_shadow.json
        # under the cross-PROCESS ml_paper.lock (one dataDir-wide lock for both books). Without
        # taking that same lock here, a concurrent Go reset could be erased by this RMW's save —
        # or a stale in-flight shadow write could resurrect a retired book's lots. Busy ⇒ skip
        # this cycle (the caller's try/except logs it), exactly like the real book.
        with _book_lock(path) as got:
            if not got:
                raise RuntimeError("ml_paper.lock busy (Go writer active) — shadow book update skipped this cycle")
            return _manage_shadow_locked(path, preds, outcome, stake, livepx, allow_new)


def _manage_shadow_locked(path, preds, outcome, stake, livepx=None, allow_new=True):
    pf = {}
    if os.path.exists(path):
        try:
            pf = _json_read(path)  # R80: utf-8 — cp1252-default open() misread the Go side's emoji titles as 'corrupt'
        except Exception as e:
            # CORRUPT-BOOK GUARD (audit): present-but-unparseable ≠ absent. Falling through to {}
            # meant the save at the end OVERWROTE the whole shadow book on one torn/partial read.
            # Keep the evidence (.bak, best-effort) and ABORT this pass — the caller's try/except
            # logs it and the next cycle retries against the intact file.
            print("  CORRUPT BOOK: ml_shadow.json exists but failed to parse (%s) — aborting shadow pass, file left untouched" % e)
            try:
                shutil.copyfile(path, path + ".bak")
            except Exception:
                pass
            raise RuntimeError("ml_shadow.json unparseable — shadow book pass aborted")
    openpos, closed = pf.get("open", []), pf.get("closed", [])
    life = pf.get("lifetime", {"net": 0.0, "wins": 0, "closed": 0})
    # R25 PNL REBASE (operator): shadow book P&L/graph restart, data kept — same one-time rebase
    # as the ML book. R30: re-anchored to $250 (bank_ver=3) so shadow-vs-ML stays apples-to-apples.
    if "net_base" not in life or int(life.get("bank_ver", 1)) < 3:
        life["net_base"] = float(life.get("net", 0.0))
        life["bank_ver"] = 3
        pf["equity"] = []
    # R64 SHADOW REBASE (operator: "set balance to $800 for shadow"): one-time shadow-ONLY re-anchor
    # to the $800 bank — same mechanism as the bank_ver<3 rebase (net baseline + equity restart) but
    # keyed by its OWN stamp, so the ML book stays $250 and the Go reset's bank_ver=3 stamp can't
    # re-fire or suppress it.
    if int(life.get("shadow_bank_ver", 0)) < 4:
        life["net_base"] = float(life.get("net", 0.0))
        life["shadow_bank_ver"] = 4
        pf["equity"] = []
    net_base = float(life.get("net_base", 0.0))
    now = int(time.time())
    still, horizon_closed = [], 0
    for p in openpos:  # settle (same rules as the real book)
        horizon_s = _resolve_horizon_limit(p) * 3600.0
        _v = outcome.get(p["ticker"])
        payout = None if _v is None else (_v if (p.get("side") or "").upper() in ("YES", "UP") else (1.0 - _v))
        if payout is None:
            op = p.get("opened")
            if not op or (now - op) > horizon_s:
                if _horizon_exit_lot(p, now, livepx):
                    closed.append(p)
                    life["net"] += p["pnl"]
                    life["closed"] += 1
                    if p["won"] == 1:
                        life["wins"] += 1
                    horizon_closed += 1
                else:
                    still.append(p)
                continue
            still.append(p)
            continue
        e, c = p["price"], p["contracts"]
        win = 1 if payout > e else 0
        p["won"] = win
        # R67h: MAKER fees only — subtract the entry fee stored at open (pre-R67 rows carry none,
        # so legacy positions settle gross exactly as they were booked).
        p["pnl"] = round(c * (payout - e) - float(p.get("fee") or 0.0), 2)
        p["closed_ts"] = int(time.time())  # R90 DO-THIS 14
        _stamp_terminal_provenance(p, payout=payout, reason="settlement")
        closed.append(p)
        life["net"] += p["pnl"]
        life["closed"] += 1
        if win == 1:
            life["wins"] += 1
    openpos = still
    if horizon_closed:
        print("  ML shadow: closed %d aged unresolved lot(s) at exact executable horizon exits" % horizon_closed)
    traded = {(p["ticker"], p["side"]) for p in openpos} | {(p["ticker"], p["side"]) for p in closed}
    if not allow_new:  # R98 SHADOW RETIREMENT: settle-only pass — the buy loop below never runs
        preds = []
    for x in preds:  # R64: take EVERY gross-+EV candidate — no fee subtraction, no net-EV floor, no model-edge floor
        if (x.get("platform") or "") not in TRADEABLE_PLATFORMS:
            continue
        rh = _remaining_hours(x, now)
        if rh <= 0 or rh > _resolve_horizon_limit(x):
            continue  # must resolve soon so it actually settles (else the ledger never gets an outcome)
        k = (x["ticker"], x["side"])
        if k in traded:
            continue
        # R32/R33: shadow buys at the LIVE price too — and like the real book it REFUSES to book
        # without one (a control sample priced off stale signals measures the wrong thing).
        px_src = "live"
        lp = (livepx or {}).get(_pxkey(x))
        if not lp or not (0.0 < float(lp) < 1.0):
            continue
        lp = float(lp)
        # Data-integrity guards (NOT EV gates), kept identical to the real book's R44 pair: (a) the
        # 5–95¢ live-price sanity band — calibration is junk at the tails; (b) implausibility —
        # p_win ≥3× a sub-10¢ live price means the market is screaming the model is wrong. R79:
        # because (b) is the SAME rule as the real book's, it rides the same config flag
        # (ml_implausibility_guard, default OFF per operator); the band (a) stays unconditional.
        if lp < 0.05 or lp > 0.95:
            continue
        if ML_IMPLAUSIBILITY_GUARD and lp < 0.10 and float(x.get("p_win") or 0) > 3 * lp:
            continue
        # R84 NOTE: the REAL book's new p_win floor (ML_MIN_P_WIN) is deliberately NOT applied here —
        # the shadow book is the ALL-GROSS control (buys every gross-positive candidate) so the
        # floor's effect stays measurable against it. Do not "fix" this asymmetry.
        # R64 — THE entry criterion (operator: "bet on everything positive ev, dont count fees"):
        # GROSS EV at the live price must be positive: p_win − live px > 0. Nothing else gates entry.
        gross = float(x.get("p_win") or 0) - lp
        if gross <= 0:
            continue
        # Book the gross edge AT THE PRICE PAID (live), so ev_pred_mean vs ev_real_mean compares
        # like-with-like on this gross ledger (the signal-price ev would overstate the entry edge).
        x = dict(x, price=round(lp, 3), ev_per_contract=round(gross, 4))
        pr = max(x["price"], 0.01)
        c = max(1, int(stake / pr))
        openpos.append({"ticker": x["ticker"], "side": x["side"], "signal_type": x["signal_type"],
                        "platform": x.get("platform"), "title": x["title"], "price": x["price"], "contracts": c,
                        "p_win": x["p_win"], "ev": x["ev_per_contract"], "opened": now, "px_src": px_src,
                        "signal_ts": _sig_unix(x.get("ts")) or None,  # R120 provenance (shadow fills instantly:
                        "decision_ts": now, "fill_ts": now,           # decision == fill by construction)
                        "fee": round(_entry_fee(x.get("platform"), c, x["price"], share=1.0), 4)}) # R67h: MAKER entry fee, charged at settle
        traded.add(k)
    # R77 item 1: since-reset display (the Go reset stamps closed_base/wins_base with net_base;
    # absent bases = 0 = lifetime, unchanged). Lifetime keeps accruing in life[].
    n = max(0, int(life["closed"]) - int(life.get("closed_base", 0) or 0))
    w = max(0, int(life["wins"]) - int(life.get("wins_base", 0) or 0))
    net = round(float(life["net"]) - net_base, 2)  # R25: since rebase
    bank0 = 800.0  # R64: $800 shadow bank (operator: "set balance to $800 for shadow") — ML book stays $250
    equity = bank0 + net
    hist = pf.get("equity", [])
    hist.append({"t": int(time.time()), "eq": round(equity, 2), "closed": n})
    hist = hist[-2000:]
    # R92: same since-reset-epoch window as the ML book (see the comment there) — the shadow's
    # gross ledger must not blend pre-reset closes / reset force-closes into its ev means either.
    cut = 0
    for i, c in enumerate(closed):
        if c.get("reset_close"):
            cut = i + 1
    epoch_closed = [c for c in closed[cut:] if not c.get("reset_close")]
    ev_pred = [c["ev"] for c in epoch_closed if c.get("ev") is not None and c.get("contracts")]
    ev_real = [(c["pnl"] / c["contracts"]) for c in epoch_closed if c.get("pnl") is not None and c.get("contracts")]
    mp = round(sum(ev_pred) / len(ev_pred), 4) if ev_pred else None
    mr = round(sum(ev_real) / len(ev_real), 4) if ev_real else None
    stats = {"closed": n, "wins": w, "win_rate": round(w / n, 3) if n else 0, "net": round(net, 2),
             "open_n": len(openpos), "bank0": bank0, "equity": round(equity, 2),
             "roi": round(net / bank0, 3) if bank0 else 0, "ev_pred_mean": mp, "ev_real_mean": mr}
    _atomic_dump({"open": openpos, "closed": closed[-1000:], "lifetime": life, "stats": stats,
                  "equity": hist, "bank0": bank0, "updated": int(time.time())}, path)
    return stats


# BOUNDED RETRAIN GUARD. A full 104k-row fit for a few dozen new settlements adds almost no
# information while recreating the Python/BLAS transient-memory and CPU spike every four minutes.
# AFTER the first --min-train warmup (150 book-v1 rows by default), retrain after max(200 new
# resolved rows, 0.2% of the prior training N), or after 15 minutes so quiet regimes still refresh.
# Persisted-model scoring remains every 10 seconds below; only FITS are coalesced. A shrink/reset
# of the training universe retrains immediately.
ML_RETRAIN_MIN_NEW = 200
ML_RETRAIN_MIN_FRAC = 0.002
ML_RETRAIN_MAX_AGE_SECONDS = 15 * 60
ML_FAST_RESCORE_SECONDS = 10


def _fit_policy_summary(min_train):
    return ("book-v2 fit policy: first model waits for %d resolved book snapshots; later full "
            "refits wait for +max(%d rows, %.1f%%) or %.0f minutes; current books rescore every %ds"
            % (int(min_train), ML_RETRAIN_MIN_NEW, ML_RETRAIN_MIN_FRAC * 100,
               ML_RETRAIN_MAX_AGE_SECONDS / 60, ML_FAST_RESCORE_SECONDS))


def _retrain_policy(current_n, previous_n, fitted_at, now=None):
    """Return (due, reason, delta, threshold, age_s) for a prospective full model fit."""
    now = time.time() if now is None else float(now)
    previous_n = int(previous_n)
    current_n = int(current_n)
    threshold = max(ML_RETRAIN_MIN_NEW, int(math.ceil(max(0, previous_n) * ML_RETRAIN_MIN_FRAC)))
    delta = current_n - previous_n
    age_s = max(0.0, now - float(fitted_at or 0))
    if previous_n < 0 or not fitted_at:
        return True, "first-fit", delta, threshold, age_s
    if delta < 0:
        return True, "training-set-shrank", delta, threshold, age_s
    if delta >= threshold:
        return True, "new-resolved-threshold", delta, threshold, age_s
    if age_s >= ML_RETRAIN_MAX_AGE_SECONDS:
        return True, "max-age", delta, threshold, age_s
    return False, "coalesced", delta, threshold, age_s


# Everything the last completed FULL fit produced. model=None until the first full fit after boot,
# which is therefore unconditional; fit_at drives the 15-minute maximum-age backstop.
_LAST_FIT = {"n_res": -1, "model": None, "vocab": None, "base": 0.0, "auc": float("nan"),
             "brier": float("nan"), "log_loss": float("nan"),
             "brier_raw": float("nan"), "ece": float("nan"),
             "reliability": [], "cal_method": "sigmoid", "split_receipt": {}, "fit_at": 0.0,
             "metric_scope": "none", "paper_authority": False, "live_authority": False}


def cycle(db, top, min_train, minp, maxp, fast=False):
    global _NCYCLE, MAX_RESOLVE_HOURS, MAX_CRYPTO_RESOLVE_HOURS
    global ML_IMPLAUSIBILITY_GUARD, ML_MIN_P_WIN, ML_BORDERS, REALIZATION_RATIOS, REALIZATION_ENABLED
    _NCYCLE += 1
    # R79: refresh the implausibility-guard flag from config.json EVERY cycle (fast + full paths
    # both come through here before _score_and_write runs the book passes), so flipping the
    # Settings toggle applies on the next cycle — no sidecar restart needed.
    ML_IMPLAUSIBILITY_GUARD = load_implausibility_guard(db)
    ML_MIN_P_WIN = load_min_p_win(db)  # R84: p_win entry floor, hot-reloaded the same way
    MAX_RESOLVE_HOURS = load_max_hours(db)
    MAX_CRYPTO_RESOLVE_HOURS = load_crypto_max_hours(db)
    ML_BORDERS = load_ml_borders(db)  # R90: plausibility band + EV window, hot-reloaded
    REALIZATION_RATIOS, REALIZATION_ENABLED = load_realization(db)  # R90 edge 29 haircut inputs
    run_ab = (_NCYCLE % 6 == 1)  # CALIBEXP: heavy calibration A/B + residual fits only ~hourly, not every cycle (CPU)
    # FAST FIRST SCORE (R10): on boot, reuse the persisted model (<6h old) and skip straight to
    # scoring — predictions land in seconds; the normal full retrain follows on the next loop pass.
    bundle = _load_bundle(db) if fast else None
    if bundle is not None:
        raw_rows = load_incremental(db)
        native_rows = _book_native_rows(raw_rows)
        rows = dedup(native_rows)
    else:
        raw_rows = load(db)
        native_rows = _book_native_rows(raw_rows)
        _prime_inc(native_rows, max((r.get("id", 0) for r in raw_rows), default=0))
        rows = dedup(native_rows)
    # Book-v1 predicts outcomes from the signal-time executable book. A later fill remains
    # evaluation/ledger provenance and is never substituted into the feature row.
    res = [r for r in rows if r.get("resolved") == 1 and r.get("won") in (0, 1)]
    opn = [r for r in rows if r.get("resolved") == 0]
    if len(res) < min_train:
        _publish_book_warming(db, len(res), len(opn), min_train, len(raw_rows))
        print("  BOOK-V2 WARMING: %d resolved / %d required; %d open; zero actionable predictions"
              % (len(res), min_train, len(opn)))
        return
    res.sort(key=lambda r: r.get("ts", 0))
    if bundle is not None:
        # R10 fast path: score with the persisted model; all heavy fitting is skipped this cycle.
        model, vocab, base = bundle["model"], bundle["vocab"], bundle["base"]
        auc = bundle.get("auc", float("nan")); brier = bundle.get("brier", float("nan"))
        log_loss = bundle.get("log_loss", float("nan"))
        ece = bundle.get("ece", float("nan")); reliability = bundle.get("reliability", [])
        cal_method = bundle.get("cal_method", "sigmoid")
        # R90 bug 121: seed the in-memory carry from the persisted bundle so the FIRST full
        # retrain after a restart keeps the last A/B winner instead of resetting to sigmoid.
        _LAST_FIT["cal_method"] = str(cal_method or "sigmoid")
        res_brier = float("nan"); res_auc = float("nan")
        brier_raw = bundle.get("brier_raw", float("nan"))
        print("  FAST SCORE: persisted model reused (age %.0f min) — full retrain follows on schedule"
              % ((time.time() - os.path.getmtime(_bundle_path(db))) / 60.0))
        # R135: ALWAYS republish on the persisted-model fast path. A prior process's export can
        # belong to a different bundle, and _EXPORT_VER is process-local/empty after restart.
        # Vectors are withheld from Go unless this exact model earns a fresh version receipt.
        try:
            _export_flat_model(db, model, vocab, (res[0] if res else None), cal_method,
                               featurize(res[-1000:], vocab) if res else None)
        except Exception as e:
            print("  (R135 flat export (boot) failed — eval vectors remain unversioned: %s)" % e)
        return _score_and_write(db, top, minp, maxp, res, opn, model, vocab, base,
                                auc, brier, ece, reliability, cal_method, res_brier, res_auc,
                                min_train, brier_raw,
                                ece_gated=bundle.get("ece_gated", float("nan")),
                                ece_gated_n=bundle.get("ece_gated_n", 0),
                                split_receipt=bundle.get("split_receipt") or {},
                                metric_scope=bundle.get("metric_scope") or "none",
                                paper_authority=bool(bundle.get("paper_authority")),
                                live_authority=bool(bundle.get("live_authority")),
                                log_loss=log_loss)
    # BOUNDED RETRAIN: coalesce statistically negligible settlement deltas, but keep the exact
    # scoring/publish pass on this cycle's fresh rows and prices. The main loop's 10-second fast
    # rescore is independent of this decision and remains unchanged.
    fit_due, fit_reason, fit_delta, fit_threshold, fit_age = _retrain_policy(
        len(res), _LAST_FIT["n_res"], _LAST_FIT.get("fit_at", 0.0))
    if _LAST_FIT["model"] is not None and not fit_due:
        print("  skip fit: +%d resolved < %d threshold; model age %.1f/%.1f min — rescoring current prices"
              % (fit_delta, fit_threshold, fit_age / 60.0, ML_RETRAIN_MAX_AGE_SECONDS / 60.0))
        try:
            # Keep the R10 bundle eligible for the persisted-model rescore path. The bounded
            # policy, not a byte-identical training set, now defines when this model is current.
            os.utime(_bundle_path(db))
        except OSError:
            pass
        if not os.path.exists(_export_path(db)):  # R123: coalesced fit with a missing export → publish
            try:
                _export_flat_model(db, _LAST_FIT["model"], _LAST_FIT["vocab"],
                                   (res[0] if res else None), _LAST_FIT["cal_method"],
                                   featurize(res[-1000:], _LAST_FIT["vocab"]) if res else None)
            except Exception as e:
                print("  (R123 flat export (coalesced) failed: %s)" % e)
        return _score_and_write(db, top, minp, maxp, res, opn,
                                _LAST_FIT["model"], _LAST_FIT["vocab"], _LAST_FIT["base"],
                                _LAST_FIT["auc"], _LAST_FIT["brier"], _LAST_FIT["ece"],
                                _LAST_FIT["reliability"], _LAST_FIT["cal_method"],
                                float("nan"), float("nan"), min_train, _LAST_FIT["brier_raw"],
                                ece_gated=_LAST_FIT.get("ece_gated", float("nan")),
                                ece_gated_n=_LAST_FIT.get("ece_gated_n", 0),
                                split_receipt=_LAST_FIT.get("split_receipt") or {},
                                metric_scope=_LAST_FIT.get("metric_scope") or "none",
                                paper_authority=bool(_LAST_FIT.get("paper_authority")),
                                live_authority=bool(_LAST_FIT.get("live_authority")),
                                log_loss=_LAST_FIT.get("log_loss", float("nan")))
    if _LAST_FIT["model"] is not None:
        print("  retrain due: %s (+%d resolved; threshold %d; age %.1f min)"
              % (fit_reason, fit_delta, fit_threshold, fit_age / 60.0))
    # GPU RE-PROBE (audit #5): re-check CUDA before building any model this retrain — a GPU that
    # was busy at boot gets adopted once free, one that died falls back to CPU instead of erroring.
    _model_backend(refresh=True)
    # R138 NESTED MONEY-TRUTH FIT. Event/ticker groups and UTC-day blocks are indivisible; one-day
    # embargoes separate train -> validation -> rolling event-day final slice. The base model sees train only,
    # calibrator fitting/selection sees validation only, and final metrics consume test once.
    fit_kind = "strict"
    try:
        fit = _train_nested_model(res, db)
    except Exception as strict_err:
        # R143: the five-day holdout remains the LIVE prerequisite, but its warm-up must not make
        # PAPER evidence impossible.  A separate three-day chronological/event-disjoint fit may
        # sample Paper only and publishes its metrics under provisional_* fields.
        try:
            fit = _train_provisional_model(res, db)
            fit_kind = "provisional"
            print("  BOOK-V2 LIVE HOLDOUT WARMING: %s" % strict_err)
        except Exception as provisional_err:
            split = _nested_event_day_split(res, embargo_days=1)[3]
            provisional_split = _provisional_event_day_split(res)[3]
            split["provisional_attempt"] = provisional_split
            _publish_book_warming(
                db, len(res), len(opn), min_train, len(raw_rows), split_receipt=split,
                reason="strict holdout unavailable (%s); provisional Paper fit unavailable (%s)" %
                       (strict_err, provisional_err))
            print("  BOOK-V2 HOLDOUT WARMING: %s; provisional Paper unavailable: %s; zero authority"
                  % (strict_err, provisional_err))
            return
    model, vocab, base = fit["model"], fit["vocab"], fit["base"]
    auc, brier, log_loss, brier_raw = (fit["auc"], fit["brier"], fit["log_loss"],
                                       fit["brier_raw"])
    ece, ece_gated, ece_gated_n = fit["ece"], fit["ece_gated"], fit["ece_gated_n"]
    reliability, cal_method, split = fit["reliability"], fit["cal_method"], fit["split"]
    res_brier, res_auc = float("nan"), float("nan")
    _save_bundle(db, {"model": model, "vocab": vocab, "base": base,
                      "n_features": int(fit["parity_X"].shape[1]),
                      "feature_schema": BOOK_FEATURE_SCHEMA, "book_feature_version": BOOK_FEATURE_VERSION,
                      "n_num": len(NUM), "auc": auc, "brier": brier,
                      "log_loss": log_loss, "brier_raw": brier_raw,
                      "ece": ece, "ece_gated": ece_gated, "ece_gated_n": ece_gated_n,
                      "reliability": reliability, "cal_method": cal_method,
                      "split_receipt": split, "metric_scope": fit["metric_scope"],
                      "paper_authority": bool(fit["paper_authority"]),
                      "live_authority": bool(fit["live_authority"])})
    _LAST_FIT.update(n_res=len(res), model=model, vocab=vocab, base=base, auc=auc, brier=brier,
                     log_loss=log_loss,
                     brier_raw=brier_raw, ece=ece, ece_gated=ece_gated, ece_gated_n=ece_gated_n,
                      reliability=reliability, cal_method=cal_method, split_receipt=split,
                      metric_scope=fit["metric_scope"],
                      paper_authority=bool(fit["paper_authority"]),
                      live_authority=bool(fit["live_authority"]), fit_at=time.time())
    try:
        _export_flat_model(db, model, vocab, (fit["fit_rows"][0] if fit["fit_rows"] else None),
                           cal_method, fit["parity_X"])
    except Exception as e:
        print("  (R138 flat export failed; research file scoring continues: %s)" % e)
    print("  %s fit: train %d / validation %d / final slice %d; purged groups %d; %s selected"
          % (fit_kind, split["train_rows"], split["validation_rows"], split["test_rows"],
             split["purged_groups"], cal_method))
    print("  %s AUC %.3f | Brier %.4f | LogLoss %.4f | ECE %.4f | Paper %s | LIVE OFF"
          % (fit["metric_scope"], auc, brier, log_loss, ece,
             "ON" if fit["paper_authority"] else "OFF"))
    history_metrics = _holdout_history_metrics(
        fit["metric_scope"], auc, brier, brier_raw, ece, log_loss)
    history_row = {
        "ts": int(time.time()),
        "ece_gated": (round(ece_gated, 5) if ece_gated == ece_gated else None),
        "ece_gated_n": ece_gated_n, "cal_method": cal_method, "n_resolved": len(res),
        "n_features": int(fit["parity_X"].shape[1]), "backend": _model_backend()[0],
        "feature_schema": BOOK_FEATURE_SCHEMA, "book_feature_version": BOOK_FEATURE_VERSION,
        "decay_half_life_days": fit["decay_half_life"], "split_receipt": split,
        "metric_scope": fit["metric_scope"], "paper_authority": bool(fit["paper_authority"]),
        "live_authority": bool(fit["live_authority"]),
        "top_importances": _model_importances(model, vocab, fit["fit_rows"][0] if fit["fit_rows"] else None),
    }
    history_row.update(history_metrics)
    _append_model_history(db, history_row)
    return _score_and_write(db, top, minp, maxp, res, opn, model, vocab, base,
                            auc, brier, ece, reliability, cal_method, res_brier, res_auc,
                            min_train, brier_raw, ece_gated=ece_gated,
                            ece_gated_n=ece_gated_n, split_receipt=split,
                            metric_scope=fit["metric_scope"],
                            paper_authority=bool(fit["paper_authority"]),
                            live_authority=bool(fit["live_authority"]),
                            log_loss=log_loss)

    # Legacy pre-R138 fit path retained below as unreachable migration reference. It must not be
    # re-enabled: it selected calibration and reported metrics on the same tail.
    vocab = {c: sorted({str(r.get(c) or "") for r in res}) for c in CATS}
    # R103 (auditor 244, applied at the epoch-3 reset): market_type is an OPEN venue-string set —
    # every new raw value used to become a permanent one-hot column (nf crept 356→370 in a week,
    # +14 columns of one-off type strings). Cap its vocab to the TOP-15 by frequency among the
    # resolved rows this model trains on; rarer values one-hot as all-zeros — the same implicit
    # "other" bucket pre-R72 rows already get, so no schema change anywhere else. The bounded
    # sets (signal_type/platform/category/side/kind) are untouched.
    try:
        from collections import Counter
        _mt_freq = Counter(str(r.get("market_type") or "") for r in res)
        _mt_keep = {v for v, _ in _mt_freq.most_common(15)}
        vocab["market_type"] = sorted(v for v in vocab["market_type"] if v in _mt_keep)
    except Exception as e:
        print("  (244 market_type vocab cap skipped: %s)" % e)
    Xr = featurize(res, vocab)
    yr = np.array([r["won"] for r in res])
    # R114: exponential time-decay sample weights (see load_decay_half_life for the offline A/B).
    decay_hl = load_decay_half_life(db)
    sw = _decay_weights(res, decay_hl)
    def _fit_w(est, X, y, w):
        """fit with sample_weight, falling back to unweighted if the backend refuses."""
        if w is not None:
            try:
                est.fit(X, y, sample_weight=w)
                return est
            except TypeError:
                pass
        est.fit(X, y)
        return est
    print("  R114 decay weighting: half_life=%sd (%s)" % (decay_hl, "active" if sw is not None else "off/unavailable"))

    # honest walk-forward score + calibration reliability bins (predicted prob vs actual, out-of-sample)
    cut = int(0.7 * len(res))
    auc = float("nan")
    brier = float("nan")
    brier_raw = float("nan")        # audit #2: pre-calibration Brier, kept for reference alongside the calibrated headline
    ece = float("nan")
    ece_gated = float("nan")        # R122 (407): live-gates ECE sensor (nan until the OOS metrics block computes it)
    ece_gated_n = 0                  # R132: publish the sample size so a noise-floor cell cannot flap money policy
    reliability = []
    # R90 bug 121 (auditor DO-THIS 8): CARRY the persisted A/B winner instead of resetting to
    # sigmoid on every non-A/B retrain. The A/B runs ~hourly (every 6th cycle) but the winner was
    # discarded 5 out of 6 retrains — measured cost at r22: isotonic mean ece .0199 vs sigmoid
    # .0310 since boot. _LAST_FIT carries the winner within-process; the model bundle
    # (_save_bundle "cal_method") persists it across restarts via the boot fast path.
    cal_method = str(_LAST_FIT.get("cal_method") or "sigmoid")  # CALIBEXP G18: chosen calibration (sigmoid/isotonic), decided on the OOS split
    res_brier = float("nan")        # CALIBEXP G19: residual-target experiment OOS Brier
    res_auc = float("nan")          # CALIBEXP G19: residual-target experiment OOS AUC
    if cut > 30 and len(res) - cut > 20 and len(set(yr[cut:])) > 1:
        m = _mk_clf()  # R52: GPU (xgboost-cuda) when available, else the sklearn CPU model
        _fit_w(m, Xr[:cut], yr[:cut], sw[:cut] if sw is not None else None)  # R114 decay
        pp, yt = m.predict_proba(Xr[cut:])[:, 1], yr[cut:]
        auc = roc_auc_score(yt, pp)  # ranking metric — unchanged by the monotone calibrators below
        brier_raw = float(np.mean((pp - yt) ** 2))  # RAW (pre-calibration) Brier — reference only (audit #2)
        cal_eval = None  # audit #2: walk-forward model calibrated LIKE THE LIVE ONE — headline-metrics source
        if run_ab:  # CALIBEXP throttle: heavy calibration A/B + residual fits only ~hourly (every 6th cycle), not every cycle
            # CALIBEXP G18: A/B sigmoid vs isotonic calibration on the SAME split → adopt the lower-OOS-Brier one for the live model.
            try:
                cb, cfit = {}, {}
                for meth in ("sigmoid", "isotonic"):
                    cc = CalibratedClassifierCV(_mk_clf(), method=meth, cv=3)  # R52: GPU-backed when available
                    _fit_w(cc, Xr[:cut], yr[:cut], sw[:cut] if sw is not None else None)  # R114 decay
                    cb[meth] = float(np.mean((cc.predict_proba(Xr[cut:])[:, 1] - yt) ** 2))
                    cfit[meth] = cc
                cal_method = min(cb, key=cb.get)
                cal_eval = cfit[cal_method]  # audit #2: reuse the fitted winner — no extra fit on A/B cycles
                print("  CALIBEXP G18: calib OOS-Brier sigmoid=%.4f isotonic=%.4f -> using %s" % (cb["sigmoid"], cb["isotonic"], cal_method))
            except Exception as e:
                print("  CALIBEXP G18 calib A/B skipped: %s" % e)
            # CALIBEXP G19: residual-target experiment — model (won − price), then p_win = clip(price + resid). Report-only.
            try:
                pr = np.array([float(r.get("book_taker_price") or 0) for r in res], dtype=float)
                resid = yr.astype(float) - pr
                rg = _mk_reg()  # R52: GPU-backed when available
                rg.fit(Xr[:cut], resid[:cut])
                pwr = np.clip(pr[cut:] + rg.predict(Xr[cut:]), 0.0, 1.0)
                res_brier = float(np.mean((pwr - yt) ** 2))
                res_auc = float(roc_auc_score(yt, pwr)) if len(set(yt)) > 1 else float("nan")
                print("  CALIBEXP G19: residual-target OOS Brier=%.4f AUC=%.3f  vs  direct Brier=%.4f AUC=%.3f" % (res_brier, res_auc, brier_raw, auc))
            except Exception as e:
                print("  CALIBEXP G19 residual exp skipped: %s" % e)
        # METRICS AFTER CALIBRATION (audit #2): the published brier/ece/reliability used to come from
        # the RAW model above while live predictions trade the CALIBRATED one — the quality numbers
        # didn't describe what trades. Score the held-out tail through the SAME CalibratedClassifierCV
        # the live model uses (fit on the train split only, so it stays walk-forward-honest); the raw
        # Brier stays published as brier_raw. If the calibrated fit fails, fall back to the raw probs.
        if cal_eval is None:
            try:
                cal_eval = CalibratedClassifierCV(_mk_clf(), method=cal_method, cv=3)  # R52: GPU-backed when available
                _fit_w(cal_eval, Xr[:cut], yr[:cut], sw[:cut] if sw is not None else None)  # R114 decay
            except Exception as e:
                print("  (calibrated-metrics fit failed — publishing RAW-model metrics this cycle: %s)" % e)
        if cal_eval is not None:
            pp = cal_eval.predict_proba(Xr[cut:])[:, 1]
        brier = float(np.mean((pp - yt) ** 2))  # mean squared error of prob vs outcome (lower better) — the calibration-aware metric
        ece_num = 0.0
        for i in range(10):
            lo, hi = i / 10.0, (i + 1) / 10.0
            mask = (pp >= lo) & (pp <= hi if i == 9 else pp < hi)
            nb = int(mask.sum())
            if nb:
                pm, am = float(pp[mask].mean()), float(yt[mask].mean())
                reliability.append({"lo": round(lo, 2), "pred": round(pm, 3), "actual": round(am, 3), "n": nb})
                ece_num += abs(pm - am) * nb  # |predicted − actual| weighted by bin size
        ece = float(ece_num / len(yt))  # Expected Calibration Error: avg gap between confidence and reality
        # R122 (auditor 407, spec final r55): ece_gated — the SAME 10-bin ECE on the OOS subset
        # that would PASS the live gates (p_win>=.50 · net-EV 2-20c · <=4h · tradeable venue).
        # The plain self-ECE is structurally BLIND to deployed miscalibration (winner's curse on
        # gate-selected bins): r51 measured deployed-gates ECE 5.1% while self-ECE p95 was 3.8%.
        # Consumers: the calibration brake below reads max(ece, ece_gated); the Go router's ML
        # maker pause reads the published sensor. nan when the gated OOS cell is too thin (<30).
        ece_gated = float("nan")
        try:
            tail = res[cut:]
            gmask = np.zeros(len(tail), dtype=bool)
            for gi, gr in enumerate(tail):
                gpw = float(pp[gi])
                gpx = float(gr.get("book_taker_price") or 0)
                gplat = str(gr.get("platform") or "")
                grh = float(gr.get("resolve_hours") or 0)
                gev = (gpw - gpx) - float(gr.get("book_taker_fee_pc") or 0)
                gmask[gi] = (gpw >= 0.50 and 0.02 <= gev <= 0.20 and 0 < grh <= 4.0
                             and gplat != "polymarket")
            ece_gated_n = int(gmask.sum())
            if ece_gated_n >= 30:
                ppg, ytg = pp[gmask], yt[gmask]
                gnum = 0.0
                for gi in range(10):
                    glo, ghi = gi / 10.0, (gi + 1) / 10.0
                    gm2 = (ppg >= glo) & (ppg <= ghi if gi == 9 else ppg < ghi)
                    gnb = int(gm2.sum())
                    if gnb:
                        gnum += abs(float(ppg[gm2].mean()) - float(ytg[gm2].mean())) * gnb
                ece_gated = float(gnum / len(ytg))
                print("  407 ece_gated=%.4f on %d gated OOS rows (self-ece %.4f)" % (ece_gated, int(gmask.sum()), ece))
            else:
                print("  407 ece_gated: gated OOS cell too thin (%d < 30) — sensor nan this retrain" % int(gmask.sum()))
        except Exception as e:
            print("  407 ece_gated skipped: %s" % e)

    # retrain on ALL resolved WITH PROBABILITY CALIBRATION, then score the open markets.
    # Calibration matters a lot: a raw tree model will happily output "89% win" on a 5c longshot,
    # which is not real (it over-fits the cheap tail and gives a fake +1600% EV). Sigmoid/Platt
    # calibration pulls the probabilities back to reality so EV = p - price is trustworthy.
    base_clf = _mk_clf()  # R52: xgboost-cuda on the RTX when available, sklearn CPU otherwise — same hyperparams
    model = CalibratedClassifierCV(base_clf, method=cal_method, cv=3)  # CALIBEXP G18: OOS-chosen calibration
    _fit_w(model, Xr, yr, sw)  # R114 decay: full-fit weights (scale-invariant vs the split slice)
    base = float(yr.mean())
    _save_bundle(db, {"model": model, "vocab": vocab, "base": base, "n_features": Xr.shape[1],
                      "feature_schema": BOOK_FEATURE_SCHEMA, "book_feature_version": BOOK_FEATURE_VERSION,
                      "n_num": len(NUM),  # R65: raw-feature width — invalidates the bundle when NUM grows
                      "auc": auc, "brier": brier, "brier_raw": brier_raw, "ece": ece, "ece_gated": ece_gated,
                      "ece_gated_n": ece_gated_n,  # R132: gate may require a statistically useful cell
                      "reliability": reliability,
                      "cal_method": cal_method})  # R10: restart scores instantly with this model
    # BOUNDED RETRAIN GUARD: remember this fit's N, completion time and metrics. Small settlement
    # deltas reuse these exact model outputs until the row threshold or 15-minute backstop fires.
    _LAST_FIT.update(n_res=len(res), model=model, vocab=vocab, base=base, auc=auc, brier=brier,
                     brier_raw=brier_raw, ece=ece, ece_gated=ece_gated, ece_gated_n=ece_gated_n,
                     reliability=reliability, cal_method=cal_method, fit_at=time.time())
    # R123 (Part 1): export the freshly-trained model in the flat Go-evaluable format — after
    # EVERY full retrain, parity-gated (see _export_flat_model). Failure never breaks a retrain.
    try:
        _export_flat_model(db, model, vocab, (res[0] if res else None), cal_method, Xr)
    except Exception as e:
        print("  (R123 flat export failed: %s)" % e)
    print("  trained on %d resolved | base win-rate %.1f%% | walk-forward OOS-AUC %s | %d open to score"
          % (len(res), 100 * base, ("%.3f" % auc if auc == auc else "n/a"), len(opn)))
    print("  BOOK-V1: outcome labels train only on exact side-book snapshots; legacy signal price is input-only.")
    print("  ML remains paper/research decision support and has no live-order authority.")
    # R72-A #3: after EVERY retrain (this full path only — the fast path reuses a persisted model,
    # so appending there would duplicate yesterday's metrics), log the quality point.
    _append_model_history(db, {
        "ts": int(time.time()),
        "oos_auc": (round(auc, 4) if auc == auc else None),
        "brier": (round(brier, 5) if brier == brier else None),  # audit #2: CALIBRATED — describes what trades
        "brier_raw": (round(brier_raw, 5) if brier_raw == brier_raw else None),
        "ece": (round(ece, 5) if ece == ece else None),
        "ece_gated": (round(ece_gated, 5) if ece_gated == ece_gated else None),  # R122 (407): live-gates ECE sensor
        "ece_gated_n": ece_gated_n,
        "cal_method": cal_method,
        "n_resolved": len(res),
        "n_features": int(Xr.shape[1]),
        "backend": _model_backend()[0],
        "feature_schema": BOOK_FEATURE_SCHEMA,
        "book_feature_version": BOOK_FEATURE_VERSION,
        "decay_half_life_days": (decay_hl if sw is not None else 0),  # R114: recency weighting in force
        # R103 (auditor 252): which features carry the model — top-20 mean importances across the
        # calibration folds (None on backends without importances; absent = the row stays small).
        "top_importances": _model_importances(model, vocab, res[0] if res else None),
    })
    return _score_and_write(db, top, minp, maxp, res, opn, model, vocab, base,
                            auc, brier, ece, reliability, cal_method, res_brier, res_auc,
                            min_train, brier_raw,
                            ece_gated=ece_gated, ece_gated_n=ece_gated_n)


def _score_and_write(db, top, minp, maxp, res, opn, model, vocab, base,
                     auc, brier, ece, reliability, cal_method, res_brier, res_auc, min_train,
                     brier_raw=float("nan"),
                     ece_gated=float("nan"), ece_gated_n=0, split_receipt=None,
                     metric_scope="none", paper_authority=False, live_authority=False,
                     log_loss=float("nan")):
    """Scoring + output tail shared by the full cycle and the R10 fast path — ONE implementation,
    so the fast path can never drift from the real one."""

    preds = []        # band-filtered + display/paper-book picks
    all_scored = []   # EVERY open market scored — lets the portfolio show ML EV for ANY holding (PRICETAG)
    shadow_cands = [] # R67h: the SHADOW book's candidate universe = the FULL scored file (both sides,
                      # no price-band filter — its own live-price guards handle the tails), so "buys
                      # EVERY +EV candidate" is literal, not top-N-of-preds
    if opn:
        Xo = featurize(opn, vocab)
        p = model.predict_proba(Xo)[:, 1]
        # CALIBRATION BRAKE (audit §3): drift was measured (ECE persisted each cycle) but never ACTED
        # on. When the walk-forward ECE degrades past 5%, every edge is haircut by half — a model that
        # can't match its stated confidence doesn't get to spend its stated edge.
        # R122 (auditor 407): the brake now watches max(ece, ece_gated) — the plain self-ECE was
        # structurally blind to deployed miscalibration (it fired 27/912 while deployed-gates ECE
        # ran 5.1%); the gated sensor sees exactly the rows the live gates would trade.
        calib_brake = 1.0
        eff_ece = ece
        if ece_gated == ece_gated and (eff_ece != eff_ece or ece_gated > eff_ece):
            eff_ece = ece_gated
        if eff_ece == eff_ece and eff_ece > 0.05:
            calib_brake = 0.5
            print("  CALIB ALARM: OOS ECE %.3f (self %.3f / gated %s) > 0.05 — down-weighting all EVs by 50%% until calibration recovers"
                  % (eff_ece, ece, ("%.3f" % ece_gated if ece_gated == ece_gated else "n/a")))
        # EXPIRY AT THE SOURCE (root cause of the combo 400s): "open" in the DB lags market close by
        # minutes (close -> venue determination -> lifecycle push -> resolve). Every consumer of this
        # file (combos, live proposals, ML executor, parlay suggestions) treated presence as
        # tradeable-now and inherited that lag — a 15-min window died at :15 and was still being
        # composed into combos at :18. The signal row itself knows better: ts + resolve_hours =
        # expected close. Anything past (close − 90s) stays in all_scored (display/EV for HELD
        # positions) but NEVER enters the actionable preds list.
        now_utc = time.time()

        def _expired(r):
            rh = float(r.get("resolve_hours") or 0)
            if rh <= 0:
                return False  # unknown horizon — can't infer expiry, leave to live-market checks
            ts = str(r.get("ts") or "")
            try:
                from datetime import datetime as _dt
                t0 = _dt.fromisoformat(ts.replace("Z", "+00:00")).timestamp()
            except Exception:
                return False
            return (t0 + rh * 3600) < (now_utc + 90)

        for r, pw in zip(opn, p):
            price = float(r.get("book_taker_price") or 0)
            if price <= 0 or price >= 1:
                continue
            plat = r.get("platform")
            ev = (float(pw) - price) * calib_brake       # GROSS edge (kept for display/back-compat)
            ev_net = ev - float(r.get("book_taker_fee_pc") or 0)  # exact one-share taker schedule
            tk, sd = r.get("ticker"), (r.get("side") or "")
            remaining_hours = _remaining_hours(r, now_utc)
            # R137 proper-score research consumes all_scored, never `predictions`: the latter is
            # action/band selected and would create survivorship bias. Preserve the full forecast
            # persona and its historical book receipt here; Go re-stamps a CURRENT complete book
            # before any proper-position observation is admitted.
            all_scored.append({"ticker": tk, "side": sd, "platform": plat,
                               "title": (r.get("title") or "")[:140],
                               "signal_type": r.get("signal_type"), "category": r.get("category"),
                               "resolve_hours": r.get("resolve_hours"),
                               "remaining_hours": round(remaining_hours, 6), "ts": r.get("ts"),
                               "ev": round(ev, 4), "ev_net": round(ev_net, 4), "p_win": round(float(pw), 6),
                               "price": round(price, 4), "legacy_signal_price": round(float(r.get("entry_price") or 0), 4),
                               "model_cohort": BOOK_FEATURE_SCHEMA, "px_src": r.get("book_source"),
                               "book_bid": r.get("book_maker_price"), "book_ask": r.get("book_taker_price"),
                               "book_bid_depth": r.get("book_maker_depth"), "book_ask_depth": r.get("book_taker_depth"),
                               "book_quote_age_s": r.get("book_quote_age_s"),
                               "book_taker_fee_pc": r.get("book_taker_fee_pc")})
            # Book-v1 never fabricates an opposite ask from 1-taker-ask. The opposite taker ask is
            # 1-maker-bid and needs its own exact fee receipt; score its real signal row instead.
            if not _expired(r) and plat in TRADEABLE_PLATFORMS:
                _base = {"ticker": tk, "signal_type": r.get("signal_type"), "platform": plat,
                         "resolve_hours": r.get("resolve_hours"),
                         "remaining_hours": round(remaining_hours, 6),
                         "title": (r.get("title") or "")[:140],
                         "ts": r.get("ts"), "model_cohort": BOOK_FEATURE_SCHEMA,
                         "book_taker_fee_pc": float(r.get("book_taker_fee_pc") or 0)}
                shadow_cands.append(dict(_base, side=sd, price=round(price, 3), p_win=round(float(pw), 3),
                                         ev_per_contract=round(ev, 4)))
            t_sd, t_price, t_pw, t_ev, t_evnet = sd, price, float(pw), ev, ev_net
            if t_price < minp or t_price > maxp:  # band-filter only what we DISPLAY/TRADE (calibration weakest at the tails) — on the side we'd ACTUALLY trade
                continue
            if _expired(r):  # past expected close — display-only above, never actionable
                continue
            entry = {
                "ticker": r.get("ticker"), "side": t_sd,
                # R63 7c: title cap raised 70 -> 140 (operator spec) — the old [:70] produced cut-off
                # rows like "Will the lot sold price…"; 140 keeps the payload bounded while every
                # real market title fits. "side" is what the Go renderer suffixes as "— YES/NO".
                "signal_type": r.get("signal_type"), "platform": plat,
                "resolve_hours": r.get("resolve_hours"),
                "remaining_hours": round(remaining_hours, 6),
                "title": (r.get("title") or "")[:140],
                "ts": r.get("ts"), "price": round(t_price, 4),
                "legacy_signal_price": round(float(r.get("entry_price") or 0), 4), "p_win": round(t_pw, 3),
                "ev_per_contract": round(t_ev, 4), "ev_net": round(t_evnet, 4), "roi": round(t_evnet / t_price, 3),
                "book_taker_fee_pc": float(r.get("book_taker_fee_pc") or 0), "model_cohort": BOOK_FEATURE_SCHEMA,
            }
            preds.append(entry)

    # R98 LIVE-PRICED RANKING (operator: "@29→99 still shows +44¢ EV at the top — price at NOW,
    # not at signal"). The preds' ev_net above is computed at the SIGNAL-TIME price, which is the
    # right HISTORY record but the wrong RANKING key: a pick that already ran to 99¢ has ~zero
    # live edge left. So: fetch the one-per-cycle live price map FIRST (it used to be fetched
    # after the json write — the display literally could not know the current price), re-score
    # every pred's chosen side at its CURRENT venue price through the R97 two-sided scorer
    # (fee-net, calibration-brake symmetric), and rank by that. Signal-time fields stay on the
    # entry untouched for the @sig→now column. px_src says which price ranked the row; the book
    # manager keeps applying its own live-price gates downstream exactly as before.
    # R98 SHADOW RETIREMENT: a retired shadow book takes no new lots, so pricing its thousands of
    # candidates every cycle (15-20 chunked /api/livepx calls) is pure waste — preds-only then.
    _shadow_en = load_shadow_enabled(os.path.dirname(os.path.abspath(db)))
    _seen_px = set()
    _plist = []
    for p0 in list(preds) + (list(shadow_cands) if _shadow_en else []) + _held_price_requests(db):
        k0 = _pxkey(p0)
        if k0 in _seen_px:
            continue
        _seen_px.add(k0)
        _plist.append(p0)
    livemap = _live_px(db, _plist)
    if livemap:
        print("  R32 live repricing: %d/%d picks%s have a live venue price"
              % (len(livemap), len(_plist), "+shadow candidates" if _shadow_en else " (shadow retired — candidates unpriced)"))
    n_live, n_stale = 0, 0
    for x in preds:
        lp = livemap.get("%s|%s|%s" % (x.get("platform") or "", x.get("ticker") or "", x.get("side") or ""))
        try:
            lp = float(lp) if lp is not None else None
        except (TypeError, ValueError):
            lp = None
        if lp is not None and 0 < lp < 1:
            _bk = _LIVE_BOOK.get("%s|%s|%s" % (x.get("platform") or "", x.get("ticker") or "", x.get("side") or "")) or {}
            live_fee = float(_bk.get("taker_fee_pc") or 0)
            x["live_price"] = round(lp, 3)
            # rank/display EV = the ENTRY side's fee-net EV at the live price (the side the book
            # would actually trade); if the complement is now the better side, that shows up here
            # as a negative/zero entry-side EV (the pick has run) plus a live_flip marker.
            ent_ev = (float(x["p_win"]) - lp) * calib_brake
            x["ev_net_live"] = round(ent_ev - live_fee, 4)
            x["roi_live"] = round(x["ev_net_live"] / lp, 3) if lp > 0 else 0.0
            x["live_taker_fee_pc"] = live_fee
            x["px_src"] = _bk.get("source") or "book-v2"
            # R98 CRASH CLAMP: a live price far BELOW the signal entry is not a bargain — it is
            # the market declaring the pick lost/dying (finished-game corners, decided primaries)
            # while p_win, frozen on signal-time features, can't see it. Without this the ranking's
            # top filled with p_win .9 @ 2¢ "+88¢" fantasy rows (the very failure class the
            # auditor's realization study measured). Crashed = an absolute ≥25¢ drop OR a ≥60%
            # relative collapse (with an 8¢ floor so noise can't trip it) — the relative arm is
            # what catches 24¢→1¢ (a 96% collapse the flat-25¢ rule missed). Clamped rows can
            # never rank above flat; the book's 5–95¢ live band already refuses the worst buys.
            _sigp = float(x.get("price") or lp)
            _drop = _sigp - lp
            if x["ev_net_live"] > 0 and (_drop >= 0.25 or (_drop >= 0.08 and lp < 0.4 * _sigp)):
                x["ev_net_live"] = 0.0
                x["roi_live"] = 0.0
                x["px_src"] = "live-crashed"
            n_live += 1
        else:
            x["px_src"] = "sig-stale"  # no live quote this cycle — research-only, never actionable
            n_stale += 1
    # Book-v1 has no stale-price actionable lane. Signal-time scores remain in all_scored for
    # research, while predictions consumed by paper/combo surfaces require this cycle's exact book.
    preds = [x for x in preds if x.get("px_src") != "sig-stale"]
    preds.sort(key=lambda x: -(x["ev_net_live"] if x.get("ev_net_live") is not None else x.get("ev_net", x["ev_per_contract"])))
    if preds:
        print("  R98 live-priced ranking: %d/%d preds ranked at current venue price (%d no-quote fallback)"
              % (n_live, len(preds), n_stale))
    # R99 (auditor r24 §3c + operator): PLACE at most ONE side per market — collapse dual-side
    # rows AFTER the live-EV sort so the surviving side is the better fee-net side at TODAY's
    # price. Everything actionable (book, Go executor, live funnel, Top-Scored) sees one side per
    # market; pair-sum inconsistency is kept as a per-cycle calibration health metric.
    preds, n_dual_dropped, n_pair_viol = _collapse_dual_sides(preds)
    if n_dual_dropped or n_pair_viol:
        print("  R99 one-side rule: %d opposite-side pred rows collapsed | %d markets with |p_yes+p_no-1| > 0.10"
              % (n_dual_dropped, n_pair_viol))

    # R75 EVERY-MARKET SCORING: refresh the full-board pass at most every ~5 min (the board is
    # ~15-20k rows — trivial for the model, but no reason to re-pull it on 10s fast rescores);
    # between passes the last computed list republishes so the section never flickers empty.
    try:
        if time.time() - _BOARD["t"] > 300:
            _eff = ece
            if ece_gated == ece_gated and (_eff != _eff or ece_gated > _eff):
                _eff = ece_gated  # R122 (407): board scoring brakes on the same max(ece, ece_gated)
            _brake = 0.5 if (_eff == _eff and _eff > 0.05) else 1.0
            _BOARD["scores"] = _score_board(db, model, vocab, _brake)
            _BOARD["t"] = time.time()
            print("  R75 market_scores: %d +EV rows from the full-board scoring pass" % len(_BOARD["scores"]))
    except Exception as e:
        print("  (R75 market_scores skipped: %s)" % e)

    paper_enabled = bool(paper_authority and ML_PAPER_EXECUTION_AUTHORITY)
    provisional = metric_scope == "provisional_paper"
    split_receipt = split_receipt or {}
    forecast_model_version = _ensure_forecast_model_version({
        "model": model, "vocab": vocab, "base": base,
        "feature_schema": BOOK_FEATURE_SCHEMA,
        "book_feature_version": BOOK_FEATURE_VERSION,
        "metric_scope": metric_scope, "split_receipt": split_receipt,
        "cal_method": cal_method,
    })
    # A rolling final slice is a diagnostic, not an untouched promotion sample. LIVE can turn on
    # only when a separate frozen-before-test replication receipt is bound to this exact fitted
    # generation. No currently fitted path fabricates this object; absent/malformed proof is OFF.
    _live_rep = split_receipt.get("live_replication") or {}
    if not isinstance(_live_rep, dict):
        _live_rep = {}
    live_enabled = bool(
        live_authority and ML_LIVE_EXECUTION_AUTHORITY
        and _live_rep.get("authority") is True
        and _live_rep.get("frozen_before_test") is True
        and _live_rep.get("untouched") is True
        and str(_live_rep.get("state") or "").upper() == "SEALED_UNTOUCHED_PASS"
        and str(_live_rep.get("replication_id") or "").strip()
        and str(_live_rep.get("model_version") or "").strip() == forecast_model_version)
    # Carry generation provenance into both the published prediction and every later Paper lot.
    for _pred in preds:
        _pred["model_version"] = forecast_model_version
    out = {"generated_at": int(time.time()),
           "model_status": "PAPER_PROVISIONAL" if provisional else "ACTIVE_RESEARCH",
           "model_cohort": BOOK_FEATURE_SCHEMA,
           "model_lineage": MODEL_LINEAGE,
           "forecast_source": "ml-book-v2-calibrated", "model_version": forecast_model_version,
           # Provenance separation: this optional flat Go copy can be empty/refused while the
           # fitted forecast model above remains valid for Paper/research collection.
           "flat_export_version": str(_EXPORT_VER.get("v") or ""),
           "feature_schema": BOOK_FEATURE_SCHEMA, "book_feature_version": BOOK_FEATURE_VERSION,
           # Legacy generic authority is the strict LIVE bit. Paper callers must read the explicit
           # paper_authority field; this prevents an older unknown consumer from treating
           # provisional Paper permission as real-money permission.
           "authority": live_enabled,
           "paper_authority": paper_enabled, "live_authority": live_enabled,
           "execution_enabled": paper_enabled or live_enabled,
           "execution_role": "paper-exploration-only" if paper_enabled and not live_enabled else
                             ("paper-and-live" if live_enabled else "research-ranking-risk-only"),
           "actionable_predictions": len(preds) if (paper_enabled or live_enabled) else 0,
           "model_backend": _model_backend()[0],  # R52: "xgboost-cuda" (GPU) or "sklearn-cpu" — visible proof of what trained this cycle
           "metric_scope": metric_scope,
           "oos_auc": None if provisional else (round(auc, 3) if auc == auc else None),
           "oos_brier": None if provisional else (round(brier, 4) if brier == brier else None),
           "oos_log_loss": None if provisional else
                           (round(log_loss, 4) if log_loss == log_loss else None),
           "oos_brier_raw": None if provisional else (round(brier_raw, 4) if brier_raw == brier_raw else None),
           "oos_ece": None if provisional else (round(ece, 4) if ece == ece else None),
           "provisional_auc": (round(auc, 3) if provisional and auc == auc else None),
           "provisional_brier": (round(brier, 4) if provisional and brier == brier else None),
           "provisional_log_loss": (round(log_loss, 4)
                                    if provisional and log_loss == log_loss else None),
           "provisional_brier_raw": (round(brier_raw, 4) if provisional and brier_raw == brier_raw else None),
           "provisional_ece": (round(ece, 4) if provisional and ece == ece else None),
           # R122 (407): the live-gates calibration sensor — the Go router's ML maker pause reads
           # `ece_gated` (absent/None ⇒ pause fails SAFE); dashboards read oos_ece_gated.
           "ece_gated": (round(ece_gated, 4) if ece_gated == ece_gated else None),
           "ece_gated_n": int(ece_gated_n or 0),
           "oos_ece_gated": (round(ece_gated, 4) if ece_gated == ece_gated else None),
           "calib_method": cal_method,  # CALIBEXP G18: which calibration won the OOS A/B
           "resid_brier": (round(res_brier, 4) if res_brier == res_brier else None),  # CALIBEXP G19 experiment
           "resid_auc": (round(res_auc, 3) if res_auc == res_auc else None),
           "base_winrate": round(base, 3), "n_resolved": len(res),
           "book_v2_resolved": len(res), "book_v2_open": len(opn),
           "book_v1_resolved": len(res), "book_v1_open": len(opn),
           "min_train_required": min_train, "split_receipt": split_receipt,
           "live_validation": {
               "state": "SEALED_UNTOUCHED_PASS" if live_enabled else
                        ("READY_FOR_SEPARATE_REPLICATION" if split_receipt.get("days", 0) >= 5 else "WARMING"),
               "days_observed": int(split_receipt.get("days", 0) or 0),
               "days_required": 5, "authority": live_enabled,
               "model_version": forecast_model_version,
               "replication_id": str(_live_rep.get("replication_id") or "") if live_enabled else "",
               "frozen_before_test": bool(live_enabled), "untouched": bool(live_enabled),
           },
           "reliability": reliability,
           # R99 both-sides health: opposite-side rows collapsed by the one-side-per-market rule +
           # markets whose two sides' p_win disagree with p_yes+p_no≈1 by >0.10 (auditor r24 §3c
           # calibration-consistency metric — the raw material the dual-side EV mirage was made of).
           "dual_side_dropped": n_dual_dropped, "pair_sum_violations": n_pair_viol,
           "predictions": preds if (paper_enabled or live_enabled) else [],
           "research_ranked": preds, "all_scored": all_scored,
           # R75: EVERY open tradeable market scored each full cycle (market-context features,
           # signal_type="none"), +EV rows only — display/logging, NEVER a placement input.
           "market_scores": _BOARD["scores"]}
    try:
        _atomic_dump(out, os.path.join(os.path.dirname(os.path.abspath(db)), "ml_predictions.json"))
    except Exception as e:
        print("  (could not write ml_predictions.json: %s)" % e)

    # R123 (Part 1): publish eval vectors for the Go tick re-scorer (held lots + actionable
    # preds, signal-side rows; Go complements binary sides). Every publish also refreshes the
    # Go side's drift ground truth (row p_win = this cycle's sidecar score).
    if not live_enabled:
        try:
            _publish_no_execution_vectors(db)
        except Exception as e:
            print("  (R138 execution-revocation receipt skipped: %s)" % e)
    elif opn:
        try:
            _dump_eval_vectors(db, opn, Xo, p, preds)
        except Exception as e:
            print("  (R123 eval vectors skipped: %s)" % e)

    # the ML's OWN paper book — trade its +EV picks, settle on resolution, track P&L
    try:
        # SCALARPAY: map each resolved (ticker, side) to its EXACT payout fraction per contract (0..1).
        # When the settled YES value is recorded (settle_val>=0), the side gets sv (YES) or 1-sv (NO) — so
        # a scalar goalscorer settling 0.10 pays NO 0.90, not a binary 1.0. Legacy rows (settle_val=-1)
        # fall back to the binary won flag.
        # per-TICKER settled YES value (0..1) — NOT keyed by side, so a held position on the opposite side
        # from the logged signal still settles (the (ticker,side) map used to miss those, so they lingered).
        outcome = {}
        for r in res:
            tk = r.get("ticker"); sv = r.get("settle_val"); sd = (r.get("side") or "").upper()
            if sv is not None and sv >= 0:
                outcome[tk] = sv
            elif tk not in outcome and r.get("won") is not None:
                outcome[tk] = (1.0 if r.get("won") == 1 else 0.0) if sd in ("YES", "UP") else (0.0 if r.get("won") == 1 else 1.0)
        # R32: ONE live-price fetch per cycle, shared by both books — they BUY at the venue's
        # current price (suite /api/livepx), never the stale signal-log price.
        # R67h: the fetch prices the FULL shadow universe too (chunked, deduped).
        # R98: the fetch itself moved ABOVE the json write (live-priced ranking needs it); the
        # books reuse that same `livemap` here — still exactly one /api/livepx pass per cycle.
        # R143: provisional rows may sample PAPER through the exact executable-book route.  LIVE
        # remains blocked by its separate authority receipt.
        ps = manage_portfolio(db, preds if paper_enabled else [], outcome, livepx=livemap)
        print("  ML paper book: %d closed %.0f%% win | net $%+.2f | %d open"
              % (ps["closed"], 100 * ps["win_rate"], ps["net"], ps["open_n"]))
        try:
            sh = manage_shadow(db, [], outcome, livepx=livemap, allow_new=False)
            print("  ML SHADOW (%s): %d closed %.0f%% win | net $%+.2f | %d open"
                  % ("every gross-+EV candidate, MAKER fees" if _shadow_en else "RETIRED — settling out, no new lots (R98)",
                     sh["closed"], 100 * sh["win_rate"], sh["net"], sh["open_n"]))
        except Exception as e:
            print("  (ML shadow book skipped: %s)" % e)
    except Exception as e:
        print("  (ML paper book skipped: %s)" % e)

    pos = [x for x in preds if x["ev_per_contract"] > 0]
    print("  %d open markets scored +EV. Top %d:" % (len(pos), min(top, len(pos))))
    for x in pos[:top]:
        print("    %+.3f EV/ct (ROI %+.0f%%)  p=%.2f @ %.2f  %-12s %s"
              % (x["ev_per_contract"], 100 * x["roi"], x["p_win"], x["price"],
                 x["signal_type"], x["title"]))


def disable_quickedit():
    """Windows: clear console QuickEdit so a stray click/selection doesn't PAUSE the process (the classic
    'it only runs when I click the window' freeze). Also reduce output buffering. No-op off Windows."""
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="backslashreplace", line_buffering=True)
    except Exception:
        pass
    try:
        sys.stderr.reconfigure(encoding="utf-8", errors="backslashreplace", line_buffering=True)
    except Exception:
        pass
    if os.name != "nt":
        return
    try:
        import ctypes
        k = ctypes.windll.kernel32
        h = k.GetStdHandle(-10)  # STD_INPUT_HANDLE
        mode = ctypes.c_uint()
        if k.GetConsoleMode(h, ctypes.byref(mode)):
            # clear ENABLE_QUICK_EDIT_MODE (0x40); set ENABLE_EXTENDED_FLAGS (0x80) so the change takes
            k.SetConsoleMode(h, (mode.value & ~0x0040) | 0x0080)
    except Exception:
        pass


def _refresh_fast_settle_stats(pf, now=None):
    """Keep the sidecar file internally consistent after the cheap settlement path.

    The old fast path moved a lot from open to closed but left stats/open_n/equity one cycle behind,
    which is how the UI reported one open while the actual open array was empty.
    """
    now = int(now or time.time())
    life = pf.setdefault("lifetime", {"net": 0.0, "wins": 0, "closed": 0})
    openpos = pf.get("open") or []
    n = max(0, int(life.get("closed", 0) or 0) - int(life.get("closed_base", 0) or 0))
    w = max(0, int(life.get("wins", 0) or 0) - int(life.get("wins_base", 0) or 0))
    net = round(float(life.get("net", 0.0) or 0.0) -
                float(life.get("net_base", 0.0) or 0.0), 2)
    stats = pf.get("stats") if isinstance(pf.get("stats"), dict) else {}
    bank0 = float(pf.get("bank0") or stats.get("bank0") or 250.0)
    equity = round(bank0 + net, 2)
    stats.update({
        "closed": n, "wins": w, "win_rate": round(w / n, 3) if n else 0,
        "net": net, "contracts": sum(_ml_venue_epoch_metric(life, "contracts").values()),
        "open_n": len(openpos), "bank0": bank0, "equity": equity,
        "roi": round(net / bank0, 3) if bank0 else 0,
    })
    pf["stats"] = stats
    pf["eq_peak"] = round(_book_equity_peak(pf.get("eq_peak", equity), bank0, equity), 2)
    hist = pf.get("equity") if isinstance(pf.get("equity"), list) else []
    hist.append({"t": now, "eq": equity, "closed": n})
    pf["equity"] = hist[-2000:]
    pf["updated"] = now
    return pf


def settle_book(db):
    """FAST-SETTLE (matches the main portfolio's cadence): close any ML paper position whose market has
    resolved, WITHOUT the heavy retrain — so decided markets clear in ~45s instead of lingering to the next
    full cycle. Cheap: only queries resolved rows for the tickers currently open, then settles + saves.
    Runs under the cross-process book lock (audit #6); when the Go settler holds it, skip — it is doing
    this exact job."""
    path = os.path.join(os.path.dirname(os.path.abspath(db)), "ml_paper.json")
    if not os.path.exists(path):
        return
    with _book_lock(path) as got:
        if not got:
            return
        _settle_book_locked(db, path)


def _settle_book_locked(db, path):
    try:
        pf = _json_read(path)  # R80: utf-8 — cp1252-default open() misread the Go side's emoji titles as 'corrupt'
    except Exception as e:
        # CORRUPT-BOOK GUARD (audit): this path already aborted-without-rewriting on a parse
        # failure — keep that, but say so LOUDLY instead of dying silent.
        print("  CORRUPT BOOK: ml_paper.json failed to parse in fast-settle (%s) — skipped, file left untouched" % e)
        return
    repair_block = str(pf.get("settlement_repair_blocked") or "").strip()
    if repair_block:
        print("  SETTLEMENT REPAIR BLOCKED: ML book fast-settle skipped (%s)" % repair_block)
        return
    opens = pf.get("open", [])
    if not opens:
        return
    tickers = tuple(sorted({p.get("ticker") for p in opens if p.get("ticker")}))
    if not tickers:
        return
    try:
        con = sqlite3.connect("file:%s?mode=ro" % db, uri=True)  # READ-ONLY: never block/lock against the live WAL DB the Go app writes
        con.row_factory = sqlite3.Row
        qm = ",".join("?" * len(tickers))
        rows = con.execute("SELECT ticker,side,settle_val,won FROM signal_log WHERE resolved=1 AND ticker IN (%s)" % qm, tickers).fetchall()
        con.close()
    except Exception:
        return
    outcome = {}
    # Build a per-TICKER settled YES value (0..1). settle_val is the ticker's YES value (same on every
    # row); fall back to deriving it from a row's won flag. Keying by TICKER (not (ticker,side)) is the
    # fix: the ML book can hold the OPPOSITE side from the logged signal, so a (ticker,side) map missed it.
    for r in rows:
        tk = r["ticker"]; sv = r["settle_val"]; sd = (r["side"] or "").upper()
        if sv is not None and sv >= 0:
            outcome[tk] = sv
        elif tk not in outcome and r["won"] is not None:
            outcome[tk] = (1.0 if r["won"] == 1 else 0.0) if sd in ("YES", "UP") else (0.0 if r["won"] == 1 else 1.0)
    if not outcome:
        return
    closed = pf.get("closed", [])
    life = pf.get("lifetime", {"net": 0.0, "wins": 0, "closed": 0})
    _ensure_ml_venue_lifetime(life, closed)
    still, changed = [], 0
    for p in opens:
        v = outcome.get(p.get("ticker"))
        if v is None:
            still.append(p)
            continue
        side = (p.get("side") or "").upper()
        payout = v if side in ("YES", "UP") else (1.0 - v)  # side-adjust the per-ticker YES value
        e, c = p["price"], p["contracts"]
        fee = _entry_fee(p.get("platform"), c, e)  # venue-true fee (audit F11)
        if p.get("fee") is not None:
            fee = float(p.get("fee") or 0.0)  # R107: explicit maker-sim entry fee wins (parity with the main settle path)
        win = 1 if payout > e else 0
        p["won"] = win
        p["pnl"] = _money_round(c * (payout - e) - fee)
        p["closed_ts"] = int(time.time())  # R90 DO-THIS 14
        _stamp_terminal_provenance(p, payout=payout, reason="settlement")
        closed.append(p)
        life["net"] = _money_round(float(life.get("net", 0.0) or 0.0) + p["pnl"])
        _ml_venue_life_add(life, p.get("platform"), p["pnl"], p.get("contracts"))
        life["closed"] += 1
        if win == 1:
            life["wins"] += 1
        changed += 1
    if changed:
        pf["open"], pf["closed"], pf["lifetime"] = still, closed, life
        _refresh_fast_settle_stats(pf)
        try:
            _atomic_dump(pf, path)
            if _sync_funded_relation_outcomes(path, closed):
                _atomic_dump(pf, path)
            print("  ML fast-settle: closed %d decided position(s)" % changed)
        except Exception as e:
            print("  (fast-settle save failed: %s)" % e)


def main():
    disable_quickedit()
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=os.path.join(os.path.dirname(__file__), "..", "data", "kalshi.db"))
    ap.add_argument("--config", default="",
                    help="exact suite config path (passed by the Go parent; standalone default is adjacent to data dir)")
    ap.add_argument("--interval", type=float, default=10, help="minutes between cycles")
    ap.add_argument("--once", action="store_true", help="run a single cycle and exit")
    ap.add_argument("--top", type=int, default=15)
    ap.add_argument("--min-train", type=int, default=150,
                    help="book-v2 resolved rows required for the first model (later refits use a separate delta policy)")
    ap.add_argument("--minprice", type=float, default=0.10, help="ignore open markets cheaper than this")
    ap.add_argument("--maxprice", type=float, default=0.90, help="ignore open markets pricier than this")
    a = ap.parse_args()
    db = os.path.abspath(a.db)
    if not os.path.exists(db):
        sys.exit("DB not found: %s  (point --db at <dataDir>/kalshi.db)" % db)
    global MAX_RESOLVE_HOURS, MAX_CRYPTO_RESOLVE_HOURS
    global FEE_MAKER_SHARE, ML_IMPLAUSIBILITY_GUARD, ML_MIN_P_WIN, _CONFIG_PATH, _SUITE_BASE
    _CONFIG_PATH = os.path.abspath(a.config) if a.config else None
    _SUITE_BASE = None  # config may select a non-default server_addr
    MAX_RESOLVE_HOURS = load_max_hours(db)
    MAX_CRYPTO_RESOLVE_HOURS = load_crypto_max_hours(db)
    FEE_MAKER_SHARE = load_maker_share(db)  # audit F11: blend fees with the SAME maker share as the suite
    ML_IMPLAUSIBILITY_GUARD = load_implausibility_guard(db)  # R79: opt-in guard (default OFF per operator; re-read each cycle)
    ML_MIN_P_WIN = load_min_p_win(db)  # R84: p_win entry floor for the REAL book (default 0.50; re-read each cycle)
    print("live_ml: %s  (book horizon <= %.1fh regular / %.1fh crypto, maker share %.2f, implausibility guard %s, p_win floor %.2f)"
          % (db, MAX_RESOLVE_HOURS, MAX_CRYPTO_RESOLVE_HOURS, FEE_MAKER_SHARE,
             "ON" if ML_IMPLAUSIBILITY_GUARD else "OFF (R79 operator default)", ML_MIN_P_WIN))
    print("  " + _fit_policy_summary(a.min_train))
    print("  compute budget: CPU helper threads %d (hard cap %d); CUDA probed on each actual retrain"
          % (ML_CPU_HELPER_THREADS, ML_CPU_HELPER_THREAD_CAP))
    last_full = 0.0
    last_score = 0.0
    while True:
        now = time.time()
        # SETTLE FIRST, ALWAYS — decoupled from the heavy cycle. Previously a failing cycle() left
        # last_full=0 forever, so the loop only ever retried cycle() and settle_book NEVER ran (that's
        # why decided positions never cleared even though the settle logic was correct). settle_book is
        # cheap and in its own try, so settlement fires ~every 1s no matter what the model cycle does.
        try:
            settle_book(db)
        except Exception as e:
            print("  settle error: %s" % e)
        # Full retrain + re-score on the interval; advance last_full even on error so it retries on
        # schedule (not every iteration) and never starves the settle path above.
        if a.once or last_full == 0.0 or (now - last_full) >= a.interval * 60:
            first = last_full == 0.0
            last_full = now
            if first and not a.once:
                last_full = now - a.interval * 60 + 20  # R10: full retrain ~20s after the fast first score
            try:
                print("[%s] cycle (%s)" % (time.strftime("%H:%M:%S"), "FAST first score" if first else "full: retrain + score"))
                cycle(db, a.top, a.min_train, a.minprice, a.maxprice, fast=first)
                last_score = time.time()
            except Exception as e:
                import traceback
                print("  cycle error: %s" % e)
                traceback.print_exc()
        elif (now - last_score) >= ML_FAST_RESCORE_SECONDS:
            # FRESH PICKS (operator: "picks still coming in minutes old"): between full retrains,
            # RE-SCORE the open markets with the persisted model every ~10s. Prices move; the
            # model's opinion of them should too. The suite's 1s file-watcher pushes the result to
            # open dashboards instantly, so picks are never older than ~10s + scoring time.
            last_score = time.time()
            try:
                cycle(db, a.top, a.min_train, a.minprice, a.maxprice, fast=True)
                print("[%s] rescore (persisted model)" % time.strftime("%H:%M:%S"))
            except Exception as e:
                print("  rescore error: %s" % e)
        if a.once:
            break
        time.sleep(1)


if __name__ == "__main__":
    main()
