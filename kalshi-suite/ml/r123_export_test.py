# r123_export_test.py — R123 flat-model-export tests (run: python r123_export_test.py).
# Contract under test: _export_flat_model writes a flat JSON the Go evaluator can walk, and
# _flat_pwin (the Python twin of the Go arithmetic) matches CalibratedClassifierCV
# .predict_proba within 1e-6 — on FRESH vectors, not just the parity set. Also: the isotonic
# emulation matches IsotonicRegression.predict, the refusal gate has teeth, and the eval-vector
# dump carries what the Go tick re-scorer needs.
import json
import os
import sys
import tempfile

import numpy as np

try:  # r56 QA precedent: Windows consoles default cp1252 — unicode in test output must not crash
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import live_ml  # noqa: E402

FAIL = 0


def ok(cond, msg):
    global FAIL
    print(("PASS  " if cond else "FAIL  ") + msg)
    if not cond:
        FAIL = 1


def _mk_calibrated(method, nf=12, n=900, seed=7):
    from sklearn.calibration import CalibratedClassifierCV
    from sklearn.ensemble import HistGradientBoostingClassifier
    rng = np.random.RandomState(seed)
    X = rng.rand(n, nf)
    y = (X[:, 0] + 0.5 * X[:, 1] + 0.2 * rng.randn(n) > 0.75).astype(int)
    m = CalibratedClassifierCV(
        HistGradientBoostingClassifier(max_depth=3, max_iter=25, learning_rate=0.1),
        method=method, cv=3)
    m.fit(X, y)
    return m, X, rng


def _folds_of(model):
    return live_ml._flat_folds(model)


def test_parity(method):
    model, X, rng = _mk_calibrated(method)
    folds = _folds_of(model)
    fresh = rng.rand(300, X.shape[1])
    true_p = model.predict_proba(fresh)[:, 1]
    flat_p = np.array([live_ml._flat_pwin(folds, fresh[i]) for i in range(fresh.shape[0])])
    maxd = float(np.max(np.abs(flat_p - true_p)))
    ok(maxd <= 1e-6, "hgb+%s flat eval parity on fresh vectors: max diff %.2e <= 1e-6" % (method, maxd))


def test_xgb_contract():
    """The REAL contract, not an ungated fresh-vector bound: the export gate replays the
    parity pairs with the exact Go arithmetic and refuses >5e-7; whatever it PUBLISHES must
    replay within 1e-6. (An adversarially tiny isotonic — 300 calibration points, near-
    vertical segments — can amplify a 1-ulp float32 sigmoid divergence past 1e-6; the gate's
    job is to refuse exactly those. The live 29k-row model measured 2.9e-7 in the r123 probe.)"""
    try:
        from xgboost import XGBClassifier  # noqa: F401
    except Exception:
        print("SKIP  xgboost not installed — xgb fold parity covered by the sidecar's live self-gate")
        return
    from sklearn.calibration import CalibratedClassifierCV
    rng = np.random.RandomState(11)
    X = rng.rand(900, 12)
    y = (X[:, 0] + 0.5 * X[:, 1] + 0.2 * rng.randn(900) > 0.75).astype(int)
    try:
        m = CalibratedClassifierCV(
            XGBClassifier(max_depth=3, n_estimators=25, learning_rate=0.1,
                          tree_method="hist", eval_metric="logloss", verbosity=0),
            method="isotonic", cv=3)
        m.fit(X, y)
    except Exception as e:
        print("SKIP  xgb fit unavailable here (%s)" % str(e)[:80])
        return
    with tempfile.TemporaryDirectory() as td:
        db = os.path.join(td, "kalshi.db")
        open(db, "w").close()
        vocab = {c: [""] for c in live_ml.CATS}
        ver = live_ml._export_flat_model(db, m, vocab, None, "isotonic", X[:400])
        path = live_ml._export_path(db)
        if ver is None:
            ok(not os.path.exists(path),
               "xgb export refused by the self-gate on this adversarial tiny model — nothing published (correct)")
            return
        exp = json.load(open(path, encoding="utf-8"))
        maxd = max(abs(live_ml._flat_pwin(exp["folds"], np.asarray(v)) - pw)
                   for v, pw in zip(exp["parity"]["vectors"], exp["parity"]["pwin"]))
        ok(maxd <= 1e-6, "xgb published export replays its pairs within 1e-6 (%.2e)" % maxd)


def test_isotonic_emulation():
    from sklearn.isotonic import IsotonicRegression
    rng = np.random.RandomState(3)
    x = np.sort(rng.rand(200))
    y = np.clip(x + 0.1 * rng.randn(200), 0, 1)
    iso = IsotonicRegression(out_of_bounds="clip").fit(x, y)
    cal = live_ml._flat_calibrator(iso)
    g = np.linspace(-0.2, 1.2, 1000)
    emu = np.array([live_ml._flat_calibrate(cal, float(v)) for v in g])
    ref = iso.predict(np.clip(g, iso.X_min_, iso.X_max_))
    maxd = float(np.max(np.abs(emu - ref)))
    ok(maxd <= 5e-7, "isotonic np.interp emulation vs IsotonicRegression.predict: %.2e <= 5e-7" % maxd)


def test_export_file_and_gate():
    model, X, _ = _mk_calibrated("sigmoid")
    with tempfile.TemporaryDirectory() as td:
        db = os.path.join(td, "kalshi.db")
        open(db, "w").close()
        vocab = {c: [""] for c in live_ml.CATS}
        ver = live_ml._export_flat_model(db, model, vocab, None, "sigmoid", X[:200])
        ok(ver is not None, "export publishes on healthy self-parity (version %s)" % ver)
        path = live_ml._export_path(db)
        ok(os.path.exists(path), "export file written")
        if not os.path.exists(path):
            return  # refusal above already failed the test — don't crash the runner
        exp = json.load(open(path, encoding="utf-8"))
        ok(exp["parity"]["n"] == 200 and len(exp["parity"]["vectors"]) == 200
           and len(exp["parity"]["pwin"]) == 200, "parity block carries 200 vector→p_win pairs")
        ok(exp["version"] == ver and exp["n_features"] == X.shape[1], "version + n_features stamped")
        # the Go loader's check, emulated: recompute each pair through the flat pipeline
        folds = exp["folds"]
        maxd = max(abs(live_ml._flat_pwin(folds, np.asarray(v)) - pw)
                   for v, pw in zip(exp["parity"]["vectors"], exp["parity"]["pwin"]))
        ok(maxd <= 1e-6, "loader-side parity replay on the written file: %.2e <= 1e-6" % maxd)
        # teeth: perturb one leaf — the same replay must now FAIL the 1e-6 gate
        exp["folds"][0]["trees"][0]["t"] = [v + 0.05 for v in exp["folds"][0]["trees"][0]["t"]]
        maxd2 = max(abs(live_ml._flat_pwin(exp["folds"], np.asarray(v)) - pw)
                    for v, pw in zip(exp["parity"]["vectors"], exp["parity"]["pwin"]))
        ok(maxd2 > 1e-6, "corrupted tree fails the parity replay (%.2e > 1e-6) — the gate has teeth" % maxd2)


def test_eval_vectors_dump():
    with tempfile.TemporaryDirectory() as td:
        db = os.path.join(td, "kalshi.db")
        open(db, "w").close()
        json.dump({"open": [{"ticker": "T-HELD", "side": "no"}]},
                  open(os.path.join(td, "ml_paper.json"), "w"))
        opn = [
            {"ticker": "T-HELD", "side": "YES", "platform": "kalshi", "ts": "2026-07-09T12:00:00",
             "resolve_hours": 2.0, "secs_to_start": -600, "entry_price": 0.55},
            {"ticker": "T-PRED", "side": "YES", "platform": "kalshi", "ts": "2026-07-09T12:00:00",
             "resolve_hours": 1.0, "secs_to_start": 10000000, "entry_price": 0.40},
            {"ticker": "T-NOBODY", "side": "YES", "platform": "kalshi", "ts": "2026-07-09T12:00:00",
             "resolve_hours": 1.0, "secs_to_start": 0, "entry_price": 0.30},
        ]
        Xo = np.arange(9, dtype=float).reshape(3, 3)
        p = np.array([0.61234567, 0.5, 0.4])
        preds = [{"ticker": "T-PRED", "side": "NO"}]
        # Exercise the export mechanics behind an explicitly authorized switch. Production calls
        # this only after the exact-generation sealed untouched receipt passes; otherwise it
        # publishes an empty revocation receipt instead.
        old_authority = live_ml.ML_LIVE_EXECUTION_AUTHORITY
        try:
            live_ml.ML_LIVE_EXECUTION_AUTHORITY = True
            live_ml._dump_eval_vectors(db, opn, Xo, p, preds)
        finally:
            live_ml.ML_LIVE_EXECUTION_AUTHORITY = old_authority
        out = json.load(open(os.path.join(td, "ml_eval_vectors.json"), encoding="utf-8"))
        ok(out.get("authority") is True, "authorized vector fixture carries an explicit authority receipt")
        tks = {r["ticker"]: r for r in out["rows"]}
        ok(set(tks) == {"T-HELD", "T-PRED"}, "vectors = held ∪ preds only (got %s)" % sorted(tks))
        held_row = tks["T-HELD"]
        ok(held_row["held"] is True and abs(held_row["p_win"] - 0.61234567) < 1e-9,
           "held row flagged, p_win unrounded (drift ground truth)")
        ok(held_row["close_ts"] > 0 and held_row["start_ts"] > 0, "close_ts/start_ts derived")
        ok(tks["T-PRED"]["start_ts"] == 0, "secs_to_start sentinel (1e7) → start_ts 0")
        ok(held_row["x"] == [0.0, 1.0, 2.0], "vector rides the row")


if __name__ == "__main__":
    test_parity("sigmoid")
    test_parity("isotonic")
    test_xgb_contract()
    test_isotonic_emulation()
    test_export_file_and_gate()
    test_eval_vectors_dump()
    print("R123 export tests: " + ("FAIL" if FAIL else "ALL PASS"))
    sys.exit(1 if FAIL else 0)
