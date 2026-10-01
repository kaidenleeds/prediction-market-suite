"""Focused R133 book-native cohort and fail-closed warmup regressions."""

import importlib.util
import json
import math
import pathlib
import tempfile
import time

HERE = pathlib.Path(__file__).resolve().parent
SPEC = importlib.util.spec_from_file_location("live_ml_book_v1", HERE / "live_ml.py")
m = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(m)


def row(**overrides):
    r = {
        "id": 1, "ts": "2026-07-10T20:00:00Z", "platform": "kalshi", "ticker": "KX-BOOK",
        "title": "Book test", "side": "YES", "signal_type": "edge", "entry_price": .40,
        "book_feature_ver": 1, "book_maker_price": .39, "book_taker_price": .41,
        "book_maker_depth": 12, "book_taker_depth": 7, "book_quote_age_s": .25,
        "book_maker_tick": .001, "book_taker_tick": .01,
        "book_maker_fee_pc": .002, "book_taker_fee_pc": .008,
        "book_latency_ms": None, "book_source": "kalshi-ws-depth",
        "pricing_version": "book-native-v2", "label_version": "kalshi_start_clock_v2",
        "secs_to_start": 3600, "is_live": 0,
        "resolved": 0, "won": None, "fill_price": 0,
    }
    r.update(overrides)
    return r


legacy = row(book_feature_ver=0, book_maker_price=None, book_taker_price=None,
             book_maker_depth=None, book_taker_depth=None, book_source="")
assert not m._is_book_native(legacy)
assert m._is_book_native(row())
assert not m._is_book_native(row(book_taker_fee_pc=None))
assert not m._is_book_native(row(book_taker_price=.38))

# Operator-facing boot wording must keep the two thresholds distinct: 150 book-native resolved
# rows for the first model, then the independent 200-new-row/15-minute refit policy.
fit_policy = m._fit_policy_summary(150)
assert "first model waits for 150 resolved book snapshots" in fit_policy
assert "later full refits wait for +max(200 rows, 0.2%) or 15 minutes" in fit_policy
assert "current books rescore every 10s" in fit_policy

# Filter-before-dedup means an earlier legacy row cannot hide the first honest book observation.
picked = m.dedup(m._book_native_rows([
    legacy,
    row(id=2, ts="2026-07-10T20:10:00Z", entry_price=.77, fill_price=.91),
]))
assert len(picked) == 1 and picked[0]["entry_price"] == .77
assert picked[0]["fill_price"] == .91  # retained as provenance, never substituted into entry_price

vocab = {c: sorted({str(picked[0].get(c) or "")}) for c in m.CATS}
X = m.featurize(picked, vocab)
names = m._feature_names(vocab, picked[0])
ix = {name: i for i, name in enumerate(names)}
assert abs(X[0, ix["legacy_signal_price"]] - .77) < 1e-12
assert abs(X[0, ix["book_taker_price"]] - .41) < 1e-12
assert abs(X[0, ix["book_spread_cents"]] - 2.0) < 1e-12
assert X[0, ix["book_latency_missing"]] == 1
assert X[0, ix["book_is_tapered"]] == 1

with tempfile.TemporaryDirectory() as td:
    db = str(pathlib.Path(td) / "kalshi.db")
    out = m._publish_book_warming(db, resolved_n=7, open_n=13, min_train=150, raw_n=20)
    assert out["model_status"] == "WARMING"
    assert out["model_cohort"] == "book-native-v2"
    assert out["live_authority"] is False and out["actionable_predictions"] == 0
    assert out["warmup_remaining"] == 143 and out["predictions"] == []
    disk = json.loads((pathlib.Path(td) / "ml_predictions.json").read_text(encoding="utf-8"))
    assert disk["book_v1_resolved"] == 7 and disk["min_train_required"] == 150
    revoke = json.loads((pathlib.Path(td) / "ml_eval_vectors.json").read_text(encoding="utf-8"))
    assert revoke["authority"] is False and revoke["rows"] == []
    assert m._bundle_path(db).endswith("ml_model_book_v2.pkl")

    # Post-warmup publish regression: R133 originally referenced min_train in the ACTIVE_RESEARCH
    # receipt without threading it into _score_and_write. The sidecar stayed alive and retried, but
    # ml_predictions.json froze on its last WARMING receipt. Exercise the no-open-row publish tail
    # with external effects stubbed so a missing parameter/local fails here, not in a live soak.
    old_board = m._BOARD
    old_shadow = m.load_shadow_enabled
    old_live_px = m._live_px
    old_backend = m._model_backend
    old_manage = m.manage_portfolio
    old_manage_shadow = m.manage_shadow
    try:
        m._BOARD = {"t": time.time(), "scores": []}
        m.load_shadow_enabled = lambda _dir: False
        m._live_px = lambda _db, _rows: {}
        m._model_backend = lambda refresh=False: ("test-backend", None)
        m.manage_portfolio = lambda *_a, **_k: {
            "closed": 0, "win_rate": 0, "net": 0, "open_n": 0,
        }
        m.manage_shadow = m.manage_portfolio
        resolved = [{"ticker": "KX-DONE", "side": "YES", "settle_val": 1.0, "won": 1}] * 150
        m._score_and_write(
            db, 15, .10, .90, resolved, [], None, {}, .5,
            math.nan, math.nan, math.nan, [], "sigmoid", math.nan, math.nan, 150,
        )
        active = json.loads((pathlib.Path(td) / "ml_predictions.json").read_text(encoding="utf-8"))
        assert active["model_status"] == "ACTIVE_RESEARCH"
        assert active["book_v1_resolved"] == 150
        assert active["min_train_required"] == 150
        assert active["live_authority"] is False and active["paper_authority"] is False
        assert active["execution_enabled"] is False and active["predictions"] == []
    finally:
        m._BOARD = old_board
        m.load_shadow_enabled = old_shadow
        m._live_px = old_live_px
        m._model_backend = old_backend
        m.manage_portfolio = old_manage
        m.manage_shadow = old_manage_shadow

print("PASS r133_book_native_ml_test: exclusive cohort, book-first features, warmup authority gate")
