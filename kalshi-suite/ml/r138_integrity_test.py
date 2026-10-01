"""R138 ML money-truth: grouped holdout, provenance, honest horizon exits, bounded cache."""

import importlib.util
import pathlib
from datetime import datetime, timedelta, timezone

HERE = pathlib.Path(__file__).resolve().parent
SPEC = importlib.util.spec_from_file_location("live_ml_r138", HERE / "live_ml.py")
m = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(m)


def native_row(i=1, day=0, event="E0", ticker=None, won=0, resolved=1, **kw):
    dt = datetime(2026, 7, 1, 12, tzinfo=timezone.utc) + timedelta(days=day, minutes=i)
    r = {
        "id": i, "ts": dt.isoformat().replace("+00:00", "Z"), "day": dt.date().isoformat(),
        "platform": "kalshi", "ticker": ticker or (event + "-M"), "title": event,
        "side": "YES", "signal_type": "edge", "entry_price": .40,
        "book_feature_ver": 1, "pricing_version": "book-native-v2",
        "label_version": "kalshi_start_clock_v2", "canonical_event_id": event,
        "catalog_event_key": event, "book_maker_price": .39, "book_taker_price": .41,
        "book_maker_depth": 12.0, "book_taker_depth": 9.0, "book_quote_age_s": .2,
        "book_maker_tick": .01, "book_taker_tick": .01,
        "book_maker_fee_pc": .002, "book_taker_fee_pc": .008,
        "book_latency_ms": None, "book_source": "kalshi-ws-depth",
        "secs_to_start": 3600.0, "is_live": 0, "resolved": resolved,
        "resolved_at": (dt + timedelta(hours=2)).isoformat().replace("+00:00", "Z"),
        "won": won, "resolve_hours": 2.0,
    }
    r.update(kw)
    return r


def test_grouped_chronological_split_is_disjoint():
    rows = []
    rid = 1
    for day in range(10):
        for evn in range(5):
            ev = "D%d-E%d" % (day, evn)
            # Two systems/sides from one event must stay together.
            rows.append(native_row(rid, day, ev, ev + "-A", rid % 2)); rid += 1
            rows.append(native_row(rid, day, ev, ev + "-B", rid % 2,
                                   side="NO", signal_type="other")); rid += 1
    train, val, test, receipt = m._nested_event_day_split(rows)
    assert receipt["valid"] and receipt["group_disjoint"] and receipt["ticker_disjoint"]
    assert train and val and test and receipt["purged_groups"] >= 10
    gs = [{m._event_group(r) for r in part} for part in (train, val, test)]
    assert not (gs[0] & gs[1] or gs[0] & gs[2] or gs[1] & gs[2])
    assert max(m._row_utc_day(r) for r in train) < min(m._row_utc_day(r) for r in val)
    assert max(m._row_utc_day(r) for r in val) < min(m._row_utc_day(r) for r in test)


def test_calibration_selection_uses_validation_only():
    rows = [native_row(i, i // 4, "V%d" % i, won=i % 2) for i in range(24)]
    fit_idx, select_idx = m._validation_calibration_indices(rows)
    assert len(fit_idx) and len(select_idx)
    fit_groups = {m._event_group(rows[i]) for i in fit_idx}
    select_groups = {m._event_group(rows[i]) for i in select_idx}
    assert not fit_groups & select_groups
    # The nested contract explicitly reserves test for one final prediction pass.
    full = []
    for day in range(10):
        for j in range(12):
            i = day * 12 + j
            full.append(native_row(i + 1, day, "N%d-%d" % (day, j), won=(i + day) % 2))
    old_mk, old_decay = m._mk_clf, m.load_decay_half_life
    try:
        m._mk_clf = lambda: m.HistGradientBoostingClassifier(max_depth=2, max_iter=8, learning_rate=.1)
        m.load_decay_half_life = lambda _db: 0
        fit = m._train_nested_model(full, "unused.db")
    finally:
        m._mk_clf, m.load_decay_half_life = old_mk, old_decay
    sr = fit["split"]
    assert sr["test_role"] == "rolling-event-day-final-slice"
    assert sr["test_prediction_calls"] == 1
    assert sr["selected_calibration"] in ("sigmoid", "isotonic")


def test_contaminated_rows_fail_closed():
    assert m._is_book_native(native_row())
    assert not m._is_book_native(native_row(pricing_version=""))
    assert not m._is_book_native(native_row(label_version=""))
    assert not m._is_book_native(native_row(secs_to_start=3600, is_live=1))
    assert not m._is_book_native(native_row(secs_to_start=-10, is_live=0))
    assert m._is_book_native(native_row(secs_to_start=-10, is_live=-1))


def test_horizon_exit_retains_without_truth_then_closes_fee_net():
    lot = {"platform": "kalshi", "ticker": "AGED", "side": "YES", "price": .40,
           "contracts": 10, "fee": .10, "opened": 1}
    m._LIVE_BOOK.clear()
    assert not m._horizon_exit_lot(lot, 100000, {})
    assert lot["horizon_exit_pending"] and not lot.get("horizon_exit")
    key = m._pxkey(lot)
    m._LIVE_BOOK[key] = {"exit_price": .30, "exit_taker_fee_pc": .01,
                         "exit_fee_known": True, "exit_fee_source": "authoritative-test"}
    assert m._horizon_exit_lot(lot, 100001, {})
    assert lot["horizon_exit"] and lot["close_reason"] == "horizon_exit"
    assert lot["pnl"] == -1.20 and lot["total_fees"] == .20


def test_incremental_cache_is_bounded_without_losing_open_winners():
    rows = []
    for i in range(100):
        rows.append(native_row(i + 1, i // 20, "R%d" % i, resolved=1, won=i % 2))
    for i in range(7):
        rows.append(native_row(1000 + i, 9, "OPEN%d" % i, resolved=0, won=None))
    # A later duplicate cannot replace the earliest feature row.
    rows.append(dict(rows[0], id=9999, ts="2026-07-11T00:00:00Z"))
    m._prime_inc(rows)
    m._inc_compact(max_resolved=10)
    winners = m._INC["winners"]
    assert len(winners) == 17
    assert sum(1 for r in winners.values() if int(r.get("resolved") or 0) == 0) == 7
    assert m._INC["dropped_resolved"] == 90
    assert m._INC["max_id"] == 9999


def test_drawdown_peak_never_falls_below_epoch_bank():
    assert m._book_equity_peak(406.0, 1000.0, 755.0) == 1000.0
    assert m._book_equity_peak(1200.0, 1000.0, 755.0) == 1200.0
    assert m._book_equity_peak("bad", 1000.0, 1015.0) == 1015.0


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
    print("PASS r138_integrity_test")
