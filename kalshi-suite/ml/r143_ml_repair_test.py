"""R143: provisional Paper authority, honest warming metrics, and fast-settle consistency."""

import importlib.util
import json
import math
import pathlib
import tempfile
import time
from datetime import datetime, timedelta, timezone

HERE = pathlib.Path(__file__).resolve().parent
SPEC = importlib.util.spec_from_file_location("live_ml_r143", HERE / "live_ml.py")
m = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(m)


def native_row(i, day, won):
    dt = datetime(2026, 7, 10, 12, tzinfo=timezone.utc) + timedelta(days=day, minutes=i)
    event = "D%d-E%d" % (day, i)
    return {
        "id": day * 1000 + i,
        "ts": dt.isoformat().replace("+00:00", "Z"),
        "day": dt.date().isoformat(),
        "platform": "kalshi", "ticker": event + "-M", "title": event,
        "side": "YES", "signal_type": "edge", "entry_price": .40,
        "book_feature_ver": 1, "pricing_version": "book-native-v2",
        "label_version": "kalshi_start_clock_v2", "canonical_event_id": event,
        "catalog_event_key": event, "book_maker_price": .39, "book_taker_price": .41,
        "book_maker_depth": 12.0, "book_taker_depth": 9.0, "book_quote_age_s": .2,
        "book_maker_tick": .01, "book_taker_tick": .01,
        "book_maker_fee_pc": .002, "book_taker_fee_pc": .008,
        "book_latency_ms": None, "book_source": "kalshi-ws-depth",
        "secs_to_start": 3600.0, "is_live": 0, "resolved": 1,
        "resolved_at": (dt + timedelta(hours=2)).isoformat().replace("+00:00", "Z"),
        "won": won, "resolve_hours": 2.0,
    }


def test_provisional_split_and_fit_are_paper_only():
    rows = [native_row(i, day, (i + day) % 2) for day in range(3) for i in range(30)]
    train, val, test, receipt = m._provisional_event_day_split(rows)
    assert receipt["valid"] and receipt["provisional"]
    assert receipt["live_eligible"] is False and receipt["days"] == 3
    assert receipt["group_disjoint"] and receipt["ticker_disjoint"]
    assert len(train) == len(val) == len(test) == 30

    old_mk, old_decay = m._mk_clf, m.load_decay_half_life
    try:
        m._mk_clf = lambda: m.HistGradientBoostingClassifier(
            max_depth=2, max_iter=8, learning_rate=.1)
        m.load_decay_half_life = lambda _db: 0
        fit = m._train_provisional_model(rows, "unused.db")
    finally:
        m._mk_clf, m.load_decay_half_life = old_mk, old_decay
    assert fit["metric_scope"] == "provisional_paper"
    assert fit["paper_authority"] is True and fit["live_authority"] is False
    assert math.isfinite(fit["auc"]) and math.isfinite(fit["brier"])
    assert math.isfinite(fit["log_loss"]) and fit["log_loss"] >= 0


def test_log_loss_clips_endpoints_and_history_scopes_never_mix():
    loss = m._binary_log_loss([0.0, 1.0], [1, 0])
    assert math.isfinite(loss) and loss > 30.0

    provisional = m._holdout_history_metrics(
        "provisional_paper", .61, .22222, .23, .03111, .54321)
    assert provisional["oos_log_loss"] is None
    assert provisional["provisional_log_loss"] == .54321

    strict = m._holdout_history_metrics(
        "rolling_five_day_holdout", .62, .21111, .22, .02111, .43219)
    assert strict["oos_log_loss"] == .43219
    assert strict["provisional_log_loss"] is None

    with tempfile.TemporaryDirectory() as td:
        warming = m._publish_book_warming(
            str(pathlib.Path(td) / "kalshi.db"), 12, 7, 150, raw_n=20)
        assert warming["oos_log_loss"] is None
        assert warming["provisional_log_loss"] is None


def test_provisional_receipt_labels_metrics_without_live_authority():
    with tempfile.TemporaryDirectory() as td:
        db = str(pathlib.Path(td) / "kalshi.db")
        old_board = m._BOARD
        old_shadow = m.load_shadow_enabled
        old_live_px = m._live_px
        old_backend = m._model_backend
        old_manage = m.manage_portfolio
        old_manage_shadow = m.manage_shadow
        old_model_ver = dict(m._MODEL_VER)
        old_export_ver = dict(m._EXPORT_VER)
        try:
            m._BOARD = {"t": time.time(), "scores": []}
            m.load_shadow_enabled = lambda _dir: False
            m._live_px = lambda _db, _rows: {}
            m._model_backend = lambda refresh=False: ("test-backend", None)
            m.manage_portfolio = lambda *_a, **_k: {
                "closed": 0, "win_rate": 0, "net": 0, "open_n": 0,
            }
            m.manage_shadow = m.manage_portfolio
            m._MODEL_VER.update(v="", nonce="")
            m._EXPORT_VER["v"] = "flat-copy-fixture"
            resolved = [{"ticker": "KX-DONE", "side": "YES", "settle_val": 1.0,
                         "won": 1, "model_cohort": "book-native-v2"}] * 90
            m._score_and_write(
                db, 15, .10, .90, resolved, [], None, {}, .5,
                .61, .222, .031, [], "sigmoid", math.nan, math.nan, 150,
                brier_raw=.23, split_receipt={"days": 3},
                metric_scope="provisional_paper", paper_authority=True,
                live_authority=False, log_loss=.54321,
            )
            out = json.loads((pathlib.Path(td) / "ml_predictions.json").read_text("utf-8"))
            assert out["model_status"] == "PAPER_PROVISIONAL"
            assert out["execution_enabled"] and out["paper_authority"]
            assert out["live_authority"] is False
            assert out["oos_auc"] is None and out["oos_brier"] is None
            assert out["oos_log_loss"] is None
            assert out["provisional_auc"] == .61 and out["provisional_brier"] == .222
            assert out["provisional_log_loss"] == .5432
            assert out["live_validation"]["days_observed"] == 3
            assert out["live_validation"]["authority"] is False
            assert out["model_version"].startswith("book-v2-")
            assert out["model_version"] != out["flat_export_version"]
            assert out["flat_export_version"] == "flat-copy-fixture"
            provisional_version = out["model_version"]
            revoke = json.loads((pathlib.Path(td) / "ml_eval_vectors.json").read_text("utf-8"))
            assert revoke["authority"] is False

            m._score_and_write(
                db, 15, .10, .90, resolved, [], None, {}, .5,
                .62, .211, .021, [], "sigmoid", math.nan, math.nan, 150,
                brier_raw=.22, split_receipt={"days": 5},
                metric_scope="rolling_five_day_holdout", paper_authority=True,
                # A caller's point boolean is insufficient without a frozen untouched receipt.
                live_authority=True, log_loss=.43219,
            )
            strict = json.loads((pathlib.Path(td) / "ml_predictions.json").read_text("utf-8"))
            assert strict["oos_log_loss"] == .4322
            assert strict["provisional_log_loss"] is None
            assert strict["model_version"] == provisional_version
            assert strict["live_authority"] is False
            assert strict["live_validation"]["authority"] is False
            assert strict["live_validation"]["model_version"] == strict["model_version"]
        finally:
            m._BOARD = old_board
            m.load_shadow_enabled = old_shadow
            m._live_px = old_live_px
            m._model_backend = old_backend
            m.manage_portfolio = old_manage
            m.manage_shadow = old_manage_shadow
            m._MODEL_VER.clear()
            m._MODEL_VER.update(old_model_ver)
            m._EXPORT_VER.clear()
            m._EXPORT_VER.update(old_export_ver)


def test_fast_settle_refreshes_open_count_and_equity_immediately():
    pf = {
        "bank0": 600.0, "eq_peak": 610.0, "open": [],
        "lifetime": {"net": 4.0, "wins": 1, "closed": 1,
                     "net_base": 0.0, "wins_base": 0, "closed_base": 0},
        "stats": {"open_n": 1, "net": 0.0, "equity": 600.0},
        "equity": [],
    }
    m._refresh_fast_settle_stats(pf, now=1234)
    assert pf["stats"]["open_n"] == 0
    assert pf["stats"]["closed"] == 1 and pf["stats"]["wins"] == 1
    assert pf["stats"]["net"] == 4.0 and pf["stats"]["equity"] == 604.0
    assert pf["equity"][-1] == {"t": 1234, "eq": 604.0, "closed": 1}


def test_maker_fill_journal_cannot_resurrect_pre_reset_or_legacy_lots():
    with tempfile.TemporaryDirectory() as td:
        paper = pathlib.Path(td) / "ml_paper.json"
        journal = pathlib.Path(td) / "ml_maker_fills.jsonl"
        rows = [
            {"seq": 1, "row": {"ticker": "OLD-V2", "model_cohort": "book-native-v2",
                                "fill_ts": 100}},
            {"seq": 2, "row": {"ticker": "LEGACY", "model_cohort": "legacy-v1",
                                "fill_ts": 300}},
            {"seq": 3, "row": {"ticker": "CURRENT", "model_cohort": "book-native-v2",
                                "fill_ts": 300}},
        ]
        journal.write_text("\n".join(json.dumps(x) for x in rows) + "\n", encoding="utf-8")
        pf = {"epoch_id": "ml-v2-new", "reset_at": "1970-01-01T00:03:20Z"}
        opens = []
        assert m._ingest_maker_fills(str(paper), pf, opens) == 1
        assert [x["ticker"] for x in opens] == ["CURRENT"]
        assert opens[0]["epoch_id"] == "ml-v2-new"
        assert pf["maker_fill_wm"] == 3 and pf["maker_fill_seen"] == [1, 2, 3]


def test_portfolio_write_preserves_go_owned_epoch_contract():
    with tempfile.TemporaryDirectory() as td:
        paper = pathlib.Path(td) / "ml_paper.json"
        contract = {
            "epoch_id": "ml-v2-operator-reset",
            "reset_at": "2026-07-13T22:11:12.123456Z",
            "current_model_cohort": "book-native-v2",
        }
        payload = {
            **contract,
            "bank0": 600.0,
            "open": [],
            "closed": [],
            "equity": [],
            "lifetime": {
                "net": 0.0, "wins": 0, "closed": 0,
                "net_base": 0.0, "wins_base": 0, "closed_base": 0,
                "bank_ver": 3,
            },
        }
        paper.write_text(json.dumps(payload), encoding="utf-8")

        m._manage_portfolio_locked(str(paper), [], {}, 10.0, .015, 30, {})
        written = json.loads(paper.read_text(encoding="utf-8"))

        assert {key: written.get(key) for key in contract} == contract


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
    print("PASS r143_ml_repair_test")
