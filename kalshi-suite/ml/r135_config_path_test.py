"""R135: alternate config/data paths stay identical across the Go/Python sidecar boundary."""
import json
import os
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import live_ml  # noqa: E402


with tempfile.TemporaryDirectory() as td:
    data = os.path.join(td, "nonstandard-data")
    elsewhere = os.path.join(td, "nonstandard-config")
    os.makedirs(data)
    os.makedirs(elsewhere)
    db = os.path.join(data, "kalshi.db")
    open(db, "ab").close()
    cfg = os.path.join(elsewhere, "suite-settings.json")
    with open(cfg, "w", encoding="utf-8") as f:
        json.dump({
            "server_addr": "127.0.0.1:9876",
            "auto": {
                "maker_sim_books": False,
                "fee_maker_share": 0.37,
                "paper_ml_max_hours_out": 2.5,
                "ml_implausibility_guard": True,
                "ml_min_p_win": 0.61,
            },
        }, f)

    live_ml._CONFIG_PATH = os.path.abspath(cfg)
    live_ml._SUITE_BASE = None
    assert live_ml._config_path(db) == os.path.abspath(cfg)
    assert live_ml._suite_base(db) == "http://127.0.0.1:9876"
    assert live_ml._maker_sim_on(db) is False
    assert live_ml.load_maker_share(db) == 0.37
    assert live_ml.load_max_hours(db) == 2.5
    assert live_ml.load_implausibility_guard(db) is True
    assert live_ml.load_min_p_win(db) == 0.61

print("R135 alternate sidecar paths: passed")
