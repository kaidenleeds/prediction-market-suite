#!/usr/bin/env python3
"""R135: Go eval vectors may never reuse a stale flat-export version after restart/failure."""

import importlib.util
import pickle
import tempfile
from pathlib import Path

import numpy as np


HERE = Path(__file__).resolve().parent
SPEC = importlib.util.spec_from_file_location("live_ml_r135_version", HERE / "live_ml.py")
ML = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(ML)


class FakeModel:
    def predict_proba(self, x):
        return np.column_stack((np.full(len(x), 0.4), np.full(len(x), 0.6)))


def bundle():
    return {
        "model": FakeModel(), "vocab": {}, "base": 0.5, "n_features": 2,
        "feature_schema": ML.BOOK_FEATURE_SCHEMA,
        "book_feature_version": ML.BOOK_FEATURE_VERSION,
        "n_num": len(ML.NUM), "cal_method": "sigmoid",
        "split_receipt": {"valid": True, "test_role": "rolling-event-day-final-slice",
                          "test_prediction_calls": 1, "group_disjoint": True,
                          "ticker_disjoint": True},
    }


def main():
    # The forecast model owns a stable identity persisted inside its bundle and restored verbatim
    # after a simulated process restart.
    with tempfile.TemporaryDirectory() as td:
        db = str(Path(td) / "kalshi.db")
        ML._MODEL_VER["v"] = ""
        ML._save_bundle(db, bundle())
        fitted_version = ML._MODEL_VER["v"]
        assert fitted_version.startswith("book-v2-")
        ML._MODEL_VER["v"] = ""
        loaded = ML._load_bundle(db)
        assert loaded is not None
        assert loaded["model_version"] == fitted_version
        assert ML._MODEL_VER["v"] == fitted_version

        # A fresh-looking v2 file may not bypass the nested rolling-final receipt on restart.
        invalid = bundle()
        invalid.pop("split_receipt")
        with open(ML._bundle_path(db), "wb") as f:
            pickle.dump(invalid, f, protocol=pickle.HIGHEST_PROTOCOL)
        ML._MODEL_VER["v"] = ""
        assert ML._load_bundle(db) is None

        # Pre-version bundles get an exact file-hash identity that is stable across restarts.
        legacy = bundle()
        with open(ML._bundle_path(db), "wb") as f:
            pickle.dump(legacy, f, protocol=pickle.HIGHEST_PROTOCOL)
        ML._MODEL_VER["v"] = ""
        legacy_a = ML._load_bundle(db)["model_version"]
        ML._MODEL_VER["v"] = ""
        legacy_b = ML._load_bundle(db)["model_version"]
        assert legacy_a.startswith("book-v2-legacy-") and legacy_a == legacy_b

        # A valid in-memory fit may still be unpickleable on a particular backend. Bundle
        # persistence may fail, but its provisional/active forecast manifest must never publish
        # an empty identity or borrow the optional flat-export version.
        broken = bundle()
        broken["model"] = lambda x: x
        ML._MODEL_VER.update(v="", nonce="")
        ML._EXPORT_VER["v"] = "flat-copy-not-forecast"
        ML._save_bundle(db, broken)
        runtime_version = ML._MODEL_VER["v"]
        assert runtime_version.startswith("book-v2-runtime-")
        assert runtime_version != ML._EXPORT_VER["v"]
        assert ML._ensure_forecast_model_version(broken) == runtime_version

    old = ML._flat_folds
    ML._EXPORT_VER["v"] = "stale-model-version"
    ML._MODEL_VER["v"] = "book-v2-current-forecast"
    try:
        def fail(_model):
            raise RuntimeError("synthetic export failure")

        ML._flat_folds = fail
        try:
            ML._export_flat_model("unused.db", object(), {}, None, "sigmoid", [[0.0]])
        except RuntimeError:
            pass
        assert ML._EXPORT_VER["v"] == "", "failed export retained a stale vector version"
        assert ML._MODEL_VER["v"] == "book-v2-current-forecast", \
            "optional flat-export refusal erased valid forecast provenance"
    finally:
        ML._flat_folds = old
    print("PASS r135_model_version_test: forecast and flat-export provenance stay independent")


if __name__ == "__main__":
    main()
