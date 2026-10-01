import importlib.util
import json
import os
import tempfile
import unittest


HERE = os.path.dirname(os.path.abspath(__file__))
SPEC = importlib.util.spec_from_file_location("live_ml_r132", os.path.join(HERE, "live_ml.py"))
ML = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(ML)


class MakerFillCheckpointTests(unittest.TestCase):
    def _write(self, path, seqs):
        with open(path, "w", encoding="utf-8") as f:
            for seq in seqs:
                f.write(json.dumps({"seq": seq, "row": {"ticker": "T%d" % seq,
                                                          "model_cohort": "book-native-v2",
                                                          "fill_ts": 100 + seq}}) + "\n")

    def test_checkpoint_survives_restart_and_detects_large_rotation(self):
        with tempfile.TemporaryDirectory() as td:
            book = os.path.join(td, "ml_paper.json")
            journal = os.path.join(td, "ml_maker_fills.jsonl")
            self._write(journal, [10, 12, 11])
            pf, openpos = {}, []
            self.assertEqual(ML._ingest_maker_fills(book, pf, openpos), 3)
            cp = ML._maker_fill_checkpoint(pf)
            self.assertEqual(cp["maker_fill_wm"], 12)
            self.assertEqual(cp["maker_fill_head"], 10)
            self.assertEqual(cp["maker_fill_seen"], [10, 11, 12])

            # Restart with the saved cursor: nothing re-ingests.
            pf2, open2 = dict(cp), []
            self.assertEqual(ML._ingest_maker_fills(book, pf2, open2), 0)

            # Replacement is deliberately made larger than the old byte offset. The old size-only
            # detector would seek into its middle; the head-seq identity must force a full read.
            self._write(journal, [20, 21, 22, 23, 24, 25, 26, 27])
            self.assertGreater(os.path.getsize(journal), cp["maker_fill_off"])
            self.assertEqual(ML._ingest_maker_fills(book, pf2, open2), 8)
            self.assertEqual([r["ticker"] for r in open2], ["T%d" % n for n in range(20, 28)])
            self.assertEqual(pf2["maker_fill_head"], 20)


if __name__ == "__main__":
    unittest.main()
