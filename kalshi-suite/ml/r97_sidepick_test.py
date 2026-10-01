"""R97 — tests for the both-sides picker (_pick_trade_side) + its complement math.

Run directly on the host (same interpreter as the sidecar):
    python r97_sidepick_test.py
Prints "OK — N R97 side-pick tests passed" on success; any assert failure exits non-zero.

Context: signals are largely YES-canonical since R90, and the actionable preds list used to carry
ONLY the signal's side — every market where the model favored the OPPOSITE outcome died at the
real book's p_win floor instead of flipping (315 invisible candidates measured in one live
prediction file, 2026-07-06). Decision rule (fees are symmetric r·p·(1−p), so this reduces to:
trade the side the model rates ABOVE its price): flip iff the complement's fee-net EV is higher.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import live_ml as m  # noqa: E402


def approx(a, b, eps=1e-9):
    return abs(a - b) < eps


def test_keeps_signal_side_when_model_agrees():
    sd, price, pw, ev, evn, flip = m._pick_trade_side("YES", 0.50, 0.70, "kalshi", 1.0)
    assert sd == "YES" and not flip
    assert approx(price, 0.50) and approx(pw, 0.70) and approx(ev, 0.20)


def test_flips_to_no_when_model_favors_complement():
    # signal YES @45c but model says p(YES)=0.30 → the real bet is NO @55c with p=0.70, ev +0.15
    sd, price, pw, ev, evn, flip = m._pick_trade_side("YES", 0.45, 0.30, "kalshi", 1.0)
    assert sd == "NO" and flip
    assert approx(price, 0.55) and approx(pw, 0.70) and approx(ev, 0.15)
    assert evn < ev  # fee came off the complement's own price


def test_flips_no_signal_to_yes():
    sd, price, pw, ev, evn, flip = m._pick_trade_side("NO", 0.60, 0.25, "polyus", 1.0)
    assert sd == "YES" and flip
    assert approx(price, 0.40) and approx(pw, 0.75) and approx(ev, 0.35)


def test_up_down_families_flip_too():
    sd, price, pw, ev, evn, flip = m._pick_trade_side("UP", 0.50, 0.35, "kalshi", 1.0)
    assert sd == "DOWN" and flip and approx(pw, 0.65)


def test_calibration_brake_applies_symmetrically():
    _, _, _, ev_nb, _, f1 = m._pick_trade_side("YES", 0.45, 0.30, "kalshi", 1.0)
    _, _, _, ev_b, _, f2 = m._pick_trade_side("YES", 0.45, 0.30, "kalshi", 0.5)
    assert f1 and f2 and approx(ev_b, ev_nb * 0.5)


def test_degenerate_side_label_never_flips():
    # the side-label poison class ("BILIBILI GAMING") has no defined complement — pass through
    sd, price, pw, ev, evn, flip = m._pick_trade_side("BILIBILI GAMING", 0.30, 0.20, "kalshi", 1.0)
    assert sd == "BILIBILI GAMING" and not flip and approx(price, 0.30)


def test_tie_keeps_signal_side():
    sd, _, _, _, _, flip = m._pick_trade_side("YES", 0.50, 0.50, "kalshi", 1.0)
    assert sd == "YES" and not flip


def test_opp_map_is_involutive():
    for a, b in m._OPP_SIDE.items():
        assert m._OPP_SIDE[b] == a


if __name__ == "__main__":
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    for f in fns:
        f()
    print("OK — %d R97 side-pick tests passed" % len(fns))
