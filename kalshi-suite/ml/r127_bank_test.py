# r127_bank_test.py — R127 pin: the ML book's $1,000 handoff (upward-only bank re-anchor).
#
# The four-book restructure hands the sidecar a FIXED ml_bank_usd (book_ml_usd = $1,000) via
# ml_alloc.json. bank0 used to re-anchor only at a Go reset (reset_on_start is false), so a raise
# sat unused — sizing stayed clamped at min(alloc, stale own equity). _reanchor_bank0 moves the
# anchor UP by exactly the raise and NEVER down; positions/history/net are untouched by design
# (the helper only ever returns a new bank0 + the delta).
#
# Run: python ml/r127_bank_test.py            (standalone, repo convention — r99 pattern)
#  or: python -m pytest ml/r127_bank_test.py  (the same pins as pytest test functions)
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import live_ml  # noqa: E402


def test_upward_reanchor_applies_delta():
    # stale $500 anchor, $1,000 grant → +500 to exactly the grant
    bank0, delta = live_ml._reanchor_bank0(500.0, 1000.0)
    assert bank0 == 1000.0 and delta == 500.0, (bank0, delta)


def test_upward_reanchor_is_idempotent():
    # once re-anchored, the same incoming grant is a no-op (no ratchet spam, one journal line)
    bank0, delta = live_ml._reanchor_bank0(1000.0, 1000.0)
    assert bank0 == 1000.0 and delta == 0.0, (bank0, delta)


def test_downward_keeps_conservative_behavior():
    # a LOWER incoming bank never shrinks the anchor (sizing min() already clamps downward)
    bank0, delta = live_ml._reanchor_bank0(1000.0, 400.0)
    assert bank0 == 1000.0 and delta == 0.0, (bank0, delta)


def test_churn_deadband_and_bad_inputs():
    # ≤$0.50 moves sit inside the Go writer's churn guard — no re-anchor
    bank0, delta = live_ml._reanchor_bank0(1000.0, 1000.4)
    assert bank0 == 1000.0 and delta == 0.0, (bank0, delta)
    # missing/zero/garbage incoming values and an unseeded bank0 are all no-ops
    assert live_ml._reanchor_bank0(1000.0, None) == (1000.0, 0.0)
    assert live_ml._reanchor_bank0(0.0, 1000.0) == (0.0, 0.0)
    assert live_ml._reanchor_bank0(500.0, "junk") == (500.0, 0.0)


def test_history_and_net_are_not_inputs():
    # the anchor math is a pure function of (bank0, incoming) — epoch net/positions can't leak in
    # (losses are never erased: equity = bank0 + net moves only via net after the re-anchor)
    bank0, delta = live_ml._reanchor_bank0(650.25, 1000.0)
    assert bank0 == 1000.0 and round(delta, 2) == 349.75, (bank0, delta)


if __name__ == "__main__":
    PASS = FAIL = 0
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            try:
                fn()
                PASS += 1
                print("  ok  %s" % name)
            except AssertionError as e:
                FAIL += 1
                print("  FAIL %s %s" % (name, e))
    print("\n%d passed, %d failed" % (PASS, FAIL))
    sys.exit(1 if FAIL else 0)
