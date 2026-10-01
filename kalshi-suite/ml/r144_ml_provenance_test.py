"""R144: explicit book-native ML entry, result, per-share P&L, and honest CLV fields."""

import importlib.util
import pathlib


HERE = pathlib.Path(__file__).resolve().parent
SPEC = importlib.util.spec_from_file_location("live_ml_r144", HERE / "live_ml.py")
m = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(m)


def test_terminal_provenance_keeps_forecast_entry_result_and_real_market_clv_separate():
    lot = {
        "p_win": .70, "price": .40, "ev_net": .25, "contracts": 2,
        "pnl": .38, "won": 1,
        "marks": [[10, .40, "mark"], [20, .45, "mark"], [30, 1.0, "settle"]],
    }
    m._stamp_terminal_provenance(lot, payout=1.0)
    assert lot["model_probability"] == .70
    assert lot["market_price_at_decision"] == .40
    assert lot["predicted_fee_net_edge"] == .25
    assert lot["entry_price"] == .40
    assert lot["settlement_payout"] == 1.0 and lot["final_result"] == "won"
    assert lot["realized_cents_per_share"] == 19.0
    assert lot["close_market_price"] == .45 and lot["clv"] == .05


def test_missing_market_close_stays_null_instead_of_using_settlement_payout():
    lot = {"p_win": .55, "price": .50, "ev_net": .04, "contracts": 1,
           "pnl": -.50, "won": 0, "marks": [[30, 0.0, "settle"]]}
    m._stamp_terminal_provenance(lot, payout=0.0)
    assert lot["settlement_payout"] == 0.0 and lot["final_result"] == "lost"
    assert lot["close_market_price"] is None and lot["clv"] is None


def test_maker_fill_can_update_entry_without_rewriting_decision_price():
    lot = {"p_win": .60, "price": .42, "market_price_at_decision": .45, "ev_net": .12}
    m._stamp_entry_provenance(lot)
    assert lot["entry_price"] == .42
    assert lot["market_price_at_decision"] == .45


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
    print("PASS r144_ml_provenance_test")
