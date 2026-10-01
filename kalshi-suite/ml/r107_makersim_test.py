# r107_makersim_test.py — R107 maker-sim sidecar plumbing (hermetic, no network):
#   1. _maker_sim_on: absent config.json = False (standalone/test), knob honored when present.
#   2. _ingest_maker_fills: seq-watermark ingest — appends rows once, advances wm, never re-ingests
#      (incl. across the .1 rotation), tolerates junk lines.
#   3. the ingested lot carries Go's fill facts (price/fee/fill_kind) so both settlers honor them.
import json
import os
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import live_ml  # noqa: E402

passed = failed = 0


def ok(cond, name):
    global passed, failed
    if cond:
        passed += 1
        print("  ok  " + name)
    else:
        failed += 1
        print("  FAIL " + name)


with tempfile.TemporaryDirectory() as td:
    data = os.path.join(td, "data")
    os.makedirs(data)
    book = os.path.join(data, "ml_paper.json")

    # 1) no config.json anywhere near => maker sim OFF (legacy local booking for hermetic runs)
    ok(live_ml._maker_sim_on(book) is False, "_maker_sim_on: absent config => False")
    with open(os.path.join(td, "config.json"), "w", encoding="utf-8") as f:
        json.dump({"auto": {"maker_sim_books": True}}, f)
    ok(live_ml._maker_sim_on(book) is True, "_maker_sim_on: config true => True")
    with open(os.path.join(td, "config.json"), "w", encoding="utf-8") as f:
        json.dump({"auto": {"maker_sim_books": False}}, f)
    ok(live_ml._maker_sim_on(book) is False, "_maker_sim_on: config false => False")
    with open(os.path.join(td, "config.json"), "w", encoding="utf-8") as f:
        json.dump({"auto": {}}, f)
    ok(live_ml._maker_sim_on(book) is True, "_maker_sim_on: config present, key absent => True (Go default)")

    # 2) ingest: two rows + junk; watermark advances; second run ingests nothing
    q = os.path.join(data, "ml_maker_fills.jsonl")
    rows = [
        {"seq": 5, "ts": 1, "row": {"ticker": "T1", "side": "yes", "platform": "kalshi",
                                    "contracts": 3, "price": 0.35, "fee": 0.02,
                                    "fee_known": True, "fee_source": "kalshi:fill-receipt",
                                    "fill_kind": "maker", "fill_rule": "touch-through", "opened": 123,
                                    "model_cohort": "book-native-v2"}},
        {"seq": 9, "ts": 2, "row": {"ticker": "T2", "side": "no", "platform": "polyus",
                                    "contracts": 2, "price": 0.40, "fee": -0.01,
                                    "fill_kind": "maker", "fill_rule": "touch-through", "opened": 124,
                                    "model_cohort": "book-native-v2"}},
    ]
    with open(q, "w", encoding="utf-8") as f:
        f.write(json.dumps(rows[0]) + "\n")
        f.write("this is junk, not json\n")
        f.write(json.dumps(rows[1]) + "\n")
    pf, openpos = {}, []
    n = live_ml._ingest_maker_fills(book, pf, openpos)
    ok(n == 2 and len(openpos) == 2, "ingest: 2 rows in, junk skipped")
    ok(pf.get("maker_fill_wm") == 9, "ingest: watermark = max seq (9)")
    ok(openpos[0]["price"] == 0.35 and openpos[0]["fee"] == 0.02 and openpos[0]["fill_kind"] == "maker"
       and openpos[0]["fee_known"] is True and openpos[0]["fee_source"] == "kalshi:fill-receipt",
       "ingest: lot carries Go's exact fill and fee-authority facts")
    n2 = live_ml._ingest_maker_fills(book, pf, openpos)
    ok(n2 == 0 and len(openpos) == 2, "ingest: re-run ingests nothing (watermark)")

    # 3) rotation: old file becomes .1, new rows land in the fresh file — one pass reads both
    os.replace(q, q + ".1")
    with open(q, "w", encoding="utf-8") as f:
        f.write(json.dumps({"seq": 12, "ts": 3, "row": {"ticker": "T3", "side": "yes",
                                                        "platform": "kalshi", "contracts": 1,
                                                        "price": 0.20, "fee": 0.0,
                                                        "fill_kind": "maker", "fill_rule": "touch-through",
                                                        "opened": 125, "model_cohort": "book-native-v2"}}) + "\n")
    n3 = live_ml._ingest_maker_fills(book, pf, openpos)
    ok(n3 == 1 and len(openpos) == 3 and pf["maker_fill_wm"] == 12,
       "ingest: rotation-safe (reads .1 + current, wm advances)")

print("%d passed, %d failed" % (passed, failed))
sys.exit(1 if failed else 0)
