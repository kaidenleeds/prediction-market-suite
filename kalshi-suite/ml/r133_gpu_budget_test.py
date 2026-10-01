#!/usr/bin/env python3
"""Focused R133 checks for bounded retraining and GPU-sidecar CPU budgets."""

import importlib.util
from pathlib import Path


HERE = Path(__file__).resolve().parent
SPEC = importlib.util.spec_from_file_location("live_ml_r133", HERE / "live_ml.py")
ML = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(ML)


def check(condition, message):
    if not condition:
        raise AssertionError(message)


def test_thread_budget():
    env = {"KALSHI_ML_CPU_THREADS": "3", "OMP_NUM_THREADS": "12"}
    check(ML._configure_helper_threads(env) == 3, "explicit lower helper budget must win")
    check(all(env[k] == "3" for k in ML._ML_THREAD_ENV), "every native helper pool must share the cap")

    env = {"KALSHI_ML_CPU_THREADS": "99"}
    check(ML._configure_helper_threads(env) == 4, "operator request cannot exceed hard cap")

    env = {"KALSHI_ML_CPU_THREADS": "4", "MKL_NUM_THREADS": "2"}
    check(ML._configure_helper_threads(env) == 2, "stricter inherited cap must be preserved")

    env = {"KALSHI_ML_CPU_THREADS": "invalid"}
    check(ML._configure_helper_threads(env) == 4, "invalid override must fall back safely")

    class Capture:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

    old = ML._BACKEND
    try:
        ML._BACKEND = ("xgboost-cuda", Capture, Capture)
        check(ML._mk_clf().kwargs["n_jobs"] == ML.ML_CPU_HELPER_THREADS,
              "XGBoost classifier must receive bounded n_jobs")
        check(ML._mk_reg().kwargs["n_jobs"] == ML.ML_CPU_HELPER_THREADS,
              "XGBoost regressor must receive bounded n_jobs")
    finally:
        ML._BACKEND = old


def test_retrain_policy():
    due, reason, _, _, _ = ML._retrain_policy(1000, -1, 0, now=1000)
    check(due and reason == "first-fit", "first in-process fit must be unconditional")

    due, _, delta, threshold, _ = ML._retrain_policy(1199, 1000, 100, now=200)
    check(not due and delta == 199 and threshold == 200, "+199 rows must coalesce")
    due, reason, _, _, _ = ML._retrain_policy(1200, 1000, 100, now=200)
    check(due and reason == "new-resolved-threshold", "+200 rows must retrain")

    due, _, delta, threshold, _ = ML._retrain_policy(200399, 200000, 100, now=200)
    check(not due and delta == 399 and threshold == 400, "0.2% threshold must dominate at scale")
    due, reason, _, _, _ = ML._retrain_policy(200400, 200000, 100, now=200)
    check(due and reason == "new-resolved-threshold", "0.2% threshold must fire at scale")

    due, _, _, _, _ = ML._retrain_policy(1000, 1000, 100, now=999)
    check(not due, "model younger than 15 minutes must coalesce")
    due, reason, _, _, _ = ML._retrain_policy(1000, 1000, 100, now=1000)
    check(due and reason == "max-age", "15-minute backstop must force a fit")

    due, reason, _, _, _ = ML._retrain_policy(999, 1000, 100, now=101)
    check(due and reason == "training-set-shrank", "training-universe shrink must retrain immediately")
    check(ML.ML_FAST_RESCORE_SECONDS == 10, "persisted-model scoring cadence changed")


def test_dependency_pin():
    req = (HERE / "requirements.txt").read_text(encoding="utf-8")
    check("xgboost==3.3.0" in req, "tested CUDA-capable XGBoost version must be pinned")


if __name__ == "__main__":
    test_thread_budget()
    test_retrain_policy()
    test_dependency_pin()
    print("PASS r133_gpu_budget_test: helper cap, retrain thresholds, 15m backstop, 10s rescoring, dependency pin")
