# r123_probe_export.py — R123 pre-implementation probe (read-only on the live pkl).
# Question: can a flat tree-walk + calibrator emulation reproduce CalibratedClassifierCV
# .predict_proba within 1e-6? Measures: (a) where XGBoost JSON model stores leaf values,
# (b) base_score/bias handling, (c) float32 vs float64 accumulation error, (d) isotonic
# calibrator emulation error. Prints findings; writes nothing.
import json
import math
import os
import pickle
import sys

import numpy as np

HERE = os.path.dirname(os.path.abspath(__file__))
PKL = os.path.join(HERE, "..", "data", "ml_model.pkl")


def main():
    with open(PKL, "rb") as f:
        b = pickle.load(f)
    model = b["model"]
    nf = int(b["n_features"])
    print("n_features=%d cal_method=%s" % (nf, b.get("cal_method")))
    ccs = model.calibrated_classifiers_
    print("folds=%d" % len(ccs))
    cc0 = ccs[0]
    est = getattr(cc0, "estimator", None) or getattr(cc0, "base_estimator", None)
    print("estimator type:", type(est).__name__)
    cals = getattr(cc0, "calibrators", None)
    print("calibrators:", [type(c).__name__ for c in (cals or [])])

    rng = np.random.RandomState(42)
    X = rng.rand(1000, nf).astype(np.float64)
    # widen a few columns so tree thresholds on unbounded features get exercised
    X[:, :30] *= 100.0

    import xgboost
    print("xgboost", xgboost.__version__)

    def parse_fold(est):
        booster = est.get_booster()
        raw = json.loads(bytearray(booster.save_raw(raw_format="json")))
        learner = raw["learner"]
        bs_raw = str(learner["learner_model_param"]["base_score"])
        base_score = float(bs_raw.strip("[]").split(",")[0])  # xgb3.x: '[5.834963E-1]'
        trees = learner["gradient_booster"]["model"]["trees"]
        t0 = trees[0]
        print("  tree0 keys:", sorted(t0.keys()))
        print("  base_score=%r n_trees=%d" % (base_score, len(trees)))
        return booster, base_score, trees

    booster, base_score, trees = parse_fold(est)

    def flat_margin(trees, x, dtype):
        # candidate A: leaf value = split_conditions[i] at leaf nodes (left_children == -1)
        s = dtype(0.0)
        x32 = x.astype(np.float32)
        for t in trees:
            li = t["left_children"]
            ri = t["right_children"]
            si = t["split_indices"]
            sc = np.asarray(t["split_conditions"], dtype=np.float32)
            bw = np.asarray(t["base_weights"], dtype=np.float32)
            i = 0
            while li[i] != -1:
                i = li[i] if x32[si[i]] < sc[i] else ri[i]
            s = dtype(s + dtype(sc[i]))
        return s, None

    def flat_margin_bw(trees, x, dtype):
        # candidate B: leaf value = base_weights[i]
        s = dtype(0.0)
        x32 = x.astype(np.float32)
        for t in trees:
            li = t["left_children"]
            ri = t["right_children"]
            si = t["split_indices"]
            sc = np.asarray(t["split_conditions"], dtype=np.float32)
            bw = np.asarray(t["base_weights"], dtype=np.float32)
            i = 0
            while li[i] != -1:
                i = li[i] if x32[si[i]] < sc[i] else ri[i]
            s = dtype(s + dtype(bw[i]))
        return s, None

    import xgboost as xgb
    dm = xgb.DMatrix(X)
    true_margin = booster.predict(dm, output_margin=True)
    n_probe = 200
    for name, fn in (("split_conditions", flat_margin), ("base_weights", flat_margin_bw)):
        for dtn, dt in (("f32", np.float32), ("f64", np.float64)):
            errs = []
            for i in range(n_probe):
                m, _ = fn(trees, X[i], dt)
                errs.append(abs(float(m) - float(true_margin[i])))
            print("  leaf=%s acc=%s: max|margin diff| = %.3e (no bias)" % (name, dtn, max(errs)))
            # with bias candidates
            for bias_name, bias in (("logit(base)", math.log(base_score / (1 - base_score)) if 0 < base_score < 1 else 0.0),
                                    ("base_raw", base_score)):
                errs2 = []
                for i in range(n_probe):
                    m, _ = fn(trees, X[i], dt)
                    errs2.append(abs(float(m) + bias - float(true_margin[i])))
                print("    +bias %s: max = %.3e" % (bias_name, max(errs2)))

    # empirical bias: margin(x) - flat_sum(x) should be constant
    m0, _ = flat_margin(trees, X[0], np.float64)
    m1, _ = flat_margin(trees, X[1], np.float64)
    print("  empirical bias x0=%.9f x1=%.9f" % (float(true_margin[0]) - float(m0), float(true_margin[1]) - float(m1)))

    # full pipeline: per-fold sigmoid+calibrator, averaged
    def emul_pwin(x):
        tot = 0.0
        for cc in ccs:
            e = getattr(cc, "estimator", None) or getattr(cc, "base_estimator", None)
            bo = e.get_booster()
            raw = json.loads(bytearray(bo.save_raw(raw_format="json")))
            trs = raw["learner"]["gradient_booster"]["model"]["trees"]
            bs = float(str(raw["learner"]["learner_model_param"]["base_score"]).strip("[]").split(",")[0])
            s = np.float32(0.0)
            x32 = x.astype(np.float32)
            for t in trs:
                li, ri, si = t["left_children"], t["right_children"], t["split_indices"]
                sc = np.asarray(t["split_conditions"], dtype=np.float32)
                i = 0
                while li[i] != -1:
                    i = li[i] if x32[si[i]] < sc[i] else ri[i]
                s = np.float32(s + sc[i])
            margin = float(s) + math.log(bs / (1 - bs))
            p_raw = 1.0 / (1.0 + math.exp(-margin))
            cal = cc.calibrators[0]
            tn = type(cal).__name__
            if tn == "IsotonicRegression":
                xt, yt = cal.X_thresholds_, cal.y_thresholds_
                p = float(np.interp(p_raw, xt, yt))
            else:
                p = 1.0 / (1.0 + math.exp(cal.a_ * p_raw + cal.b_))
            tot += p
        return tot / len(ccs)

    true_p = model.predict_proba(X[:n_probe])[:, 1]
    perrs = [abs(emul_pwin(X[i]) - float(true_p[i])) for i in range(n_probe)]
    print("FULL PIPELINE max|p diff| over %d rows = %.3e  (target <= 1e-6)" % (n_probe, max(perrs)))

    # calibrator internals
    cal = ccs[0].calibrators[0]
    if type(cal).__name__ == "IsotonicRegression":
        print("isotonic: %d thresholds, X range [%.6f, %.6f], increasing=%s, out_of_bounds=%s" % (
            len(cal.X_thresholds_), cal.X_thresholds_[0], cal.X_thresholds_[-1],
            getattr(cal, "increasing_", "?"), getattr(cal, "out_of_bounds", "?")))
        # verify np.interp matches cal.predict on a grid incl. out-of-range
        g = np.linspace(-0.1, 1.1, 500)
        via_interp = np.interp(g, cal.X_thresholds_, cal.y_thresholds_)
        via_cal = cal.predict(g)
        print("isotonic emulation max diff on grid: %.3e" % float(np.max(np.abs(via_interp - via_cal))))
    else:
        print("sigmoid a_=%r b_=%r" % (cal.a_, cal.b_))


if __name__ == "__main__":
    sys.exit(main())
