"""R144: one New-ML grant, separately sized Kalshi and PolyUS Paper sleeves."""

import importlib.util
import json
import pathlib
import tempfile


HERE = pathlib.Path(__file__).resolve().parent
SPEC = importlib.util.spec_from_file_location("live_ml_r144_sleeves", HERE / "live_ml.py")
m = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(m)


def test_handshake_split_never_double_counts_total_bank():
    with tempfile.TemporaryDirectory() as td:
        p = pathlib.Path(td) / "ml_alloc.json"
        p.write_text(json.dumps({
            "ml_bank_usd": 600,
            "ml_venue_banks": {"kalshi": 240, "polyus": 360},
        }), encoding="utf-8")
        assert m.load_ml_venue_banks(td, 600) == {"kalshi": 240.0, "polyus": 360.0}

        # Two full-size $600 sleeves are invalid. Fail back to one neutral 300+300 split.
        p.write_text(json.dumps({
            "ml_bank_usd": 600,
            "ml_venue_banks": {"kalshi": 600, "polyus": 600},
        }), encoding="utf-8")
        assert m.load_ml_venue_banks(td, 600) == {"kalshi": 300.0, "polyus": 300.0}

        # Cent rounding still conserves the exact parent grant.
        p.unlink()
        odd = m.load_ml_venue_banks(td, 600.01)
        assert round(odd["kalshi"] + odd["polyus"], 2) == 600.01


def test_sleeves_track_only_their_own_placed_lots_and_reservations():
    sleeves = m._ml_venue_sleeves(
        openpos=[
            {"platform": "kalshi", "price": .40, "contracts": 10, "fee": 1.25},
            {"platform": "polyus", "price": .20, "contracts": 20, "fee": .50},
            {"platform": "polymarket", "price": .01, "contracts": 9999},
        ],
        epoch_closed=[
            {"platform": "kalshi", "pnl": 12},
            {"platform": "polyus", "pnl": -20},
            {"platform": "kalshi", "pnl": 100, "reset_close": True},
        ],
        pending=[{"platform": "polyus", "post_px": .30, "contracts": 10, "fee": .25}],
        venue_banks={"kalshi": 300, "polyus": 300},
    )
    assert sleeves["kalshi"] == {
        "grant": 300.0, "net": 12.0, "equity": 312.0, "sizing_balance": 312.0,
        "deployed": 5.25, "reserved": 0.0, "available": 306.75, "open": 1, "closed": 1,
        "contracts": 0.0,
    }
    assert sleeves["polyus"] == {
        "grant": 300.0, "net": -20.0, "equity": 280.0, "sizing_balance": 280.0,
        "deployed": 4.5, "reserved": 3.25, "available": 272.25, "open": 1, "closed": 1,
        "contracts": 0.0,
    }


def test_order_size_uses_destination_sleeve_not_parent_bank():
    # 25%-Kelly would request $75 from a $300 sleeve, but the per-entry wall is 10% = $30.
    # The old shared-$600 sizing would incorrectly allow $60.
    assert m._ml_sleeve_order_dollars(300, 0, 600, .25, 1) == 30
    # Kalshi can be full while PolyUS remains available; no cross-venue capital borrowing.
    assert m._ml_sleeve_order_dollars(300, 300, 300, .25, 1) == 0
    assert m._ml_sleeve_order_dollars(300, 0, 300, .25, 1) == 30
    # Parent remaining balance is always the final cap.
    assert m._ml_sleeve_order_dollars(300, 0, 7, .25, 1) == 7
    # Own profit compounds only that venue: a $30 Kalshi gain raises its 10% cap to $33 while
    # untouched PolyUS stays at $30.
    assert m._ml_sleeve_order_dollars(330, 0, 600, .25, 1) == 33
    assert m._ml_sleeve_order_dollars(300, 0, 600, .25, 1) == 30


def test_durable_venue_net_survives_detail_ring_trimming():
    life = {
        "venue_net": {"kalshi": 40.0, "polyus": -25.0},
        "venue_net_base": {"kalshi": 10.0, "polyus": -5.0},
    }
    m._ensure_ml_venue_lifetime(life, [])
    assert m._ml_venue_epoch_net(life) == {"kalshi": 30.0, "polyus": -20.0}
    m._ml_venue_life_add(life, "polyus", 7.5)
    assert m._ml_venue_epoch_net(life)["polyus"] == -12.5
    # Even with no retained close detail, sizing uses the durable epoch counters.
    sleeves = m._ml_venue_sleeves([], [], [], {"kalshi": 300, "polyus": 300},
                                  m._ml_venue_epoch_net(life))
    assert sleeves["kalshi"]["net"] == 30.0
    assert sleeves["polyus"]["net"] == -12.5
    assert sleeves["kalshi"]["sizing_balance"] == 330.0
    assert sleeves["polyus"]["sizing_balance"] == 287.5


def test_complete_epoch_detail_repairs_stale_durable_venue_split():
    life = {
        "net": 96.61, "net_base": 0.0, "closed": 3, "closed_base": 0,
        "venue_net": {"kalshi": 4.74, "polyus": 20.36},
        "venue_net_base": {"kalshi": 0.0, "polyus": 0.0},
    }
    closed = [
        {"platform": "kalshi", "pnl": 4.74},
        {"platform": "polyus", "pnl": 20.36},
        {"platform": "polyus", "pnl": 71.51},
    ]
    m._ensure_ml_venue_lifetime(life, closed)
    assert m._ml_venue_epoch_net(life) == {"kalshi": 4.74, "polyus": 91.87}
    assert life["venue_accounting_version"] == 3


def test_partial_epoch_detail_cannot_rewrite_durable_venue_split():
    life = {
        "net": 96.61, "net_base": 0.0, "closed": 3, "closed_base": 0,
        "venue_net": {"kalshi": 4.74, "polyus": 91.87},
        "venue_net_base": {"kalshi": 0.0, "polyus": 0.0},
    }
    m._ensure_ml_venue_lifetime(life, [
        {"platform": "kalshi", "pnl": 4.74},
        {"platform": "polyus", "pnl": 20.36},
    ])
    assert life["venue_net"]["polyus"] == 91.87


def test_durable_counts_contracts_and_cent_rounding_survive_500_row_ring():
    life = {
        "net": 0.0, "net_base": 0.0, "closed": 0, "closed_base": 0,
        "venue_net": {"kalshi": 0.0, "polyus": 0.0},
        "venue_net_base": {"kalshi": 0.0, "polyus": 0.0},
        "venue_closed": {"kalshi": 0.0, "polyus": 0.0},
        "venue_closed_base": {"kalshi": 0.0, "polyus": 0.0},
        "venue_contracts": {"kalshi": 0.0, "polyus": 0.0},
        "venue_contracts_base": {"kalshi": 0.0, "polyus": 0.0},
    }
    retained = []
    for i in range(650):
        venue = "kalshi" if i % 2 == 0 else "polyus"
        # +0.005 books to +1 cent in both Python and Go (never Python banker's rounding).
        pnl = m._money_round(0.005)
        life["net"] = m._money_round(life["net"] + pnl)
        life["closed"] += 1
        m._ml_venue_life_add(life, venue, pnl, 2)
        retained.append({"platform": venue, "pnl": pnl, "contracts": 2})
    retained = retained[-500:]
    m._ensure_ml_venue_lifetime(life, retained)
    assert life["net"] == 6.50
    assert m._ml_venue_epoch_metric(life, "closed") == {"kalshi": 325.0, "polyus": 325.0}
    assert m._ml_venue_epoch_metric(life, "contracts") == {"kalshi": 650.0, "polyus": 650.0}
    sleeves = m._ml_venue_sleeves(
        [], retained, [], {"kalshi": 300, "polyus": 300},
        m._ml_venue_epoch_net(life), m._ml_venue_epoch_metric(life, "closed"),
        m._ml_venue_epoch_metric(life, "contracts"))
    assert sleeves["kalshi"]["closed"] == 325
    assert sleeves["polyus"]["closed"] == 325
    assert sleeves["kalshi"]["contracts"] == 650
    assert sleeves["polyus"]["contracts"] == 650


def test_paper_lot_keeps_exact_fitted_model_generation():
    with tempfile.TemporaryDirectory() as td:
        root = pathlib.Path(td)
        (root / "ml_alloc.json").write_text(json.dumps({
            "ml_bank_usd": 600,
            "ml_venue_banks": {"kalshi": 300, "polyus": 300},
        }), encoding="utf-8")
        paper = root / "ml_paper.json"
        pred = {
            "ticker": "KX-R144-VERSION", "side": "YES", "signal_type": "kalshi-flow",
            "platform": "kalshi", "title": "version proof", "price": .40,
            "p_win": .56, "ev_per_contract": .16, "ev_net": .14,
            "live_taker_fee_pc": .02, "resolve_hours": 2.0,
            "model_version": "book-v2-exact-generation",
        }
        old = m.ML_PAPER_NEW_ENTRIES_ENABLED
        m.ML_PAPER_NEW_ENTRIES_ENABLED = True
        try:
            m._manage_portfolio_locked(str(paper), [pred], {}, 10.0, .015, 30,
                                       {m._pxkey(pred): .40})
        finally:
            m.ML_PAPER_NEW_ENTRIES_ENABLED = old
        out = json.loads(paper.read_text(encoding="utf-8"))
        assert len(out["open"]) == 1
        assert out["open"][0]["model_cohort"] == "book-native-v2"
        assert out["open"][0]["model_version"] == "book-v2-exact-generation"


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
    print("PASS r144_ml_venue_sleeves_test")
