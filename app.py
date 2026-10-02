"""
Multi-SSID Pocket Option streaming service.
Runs on Railway. Set VM_ROLE=A (demo) or VM_ROLE=B (live).
"""

import os
import time
import threading
from flask import Flask, jsonify, render_template
from flask_cors import CORS
from dotenv import load_dotenv

from multistream import MultiStreamManager, TIMEFRAME_SECONDS
from perf import PerfHistory

load_dotenv()

app = Flask(__name__, static_folder='static', static_url_path='')
CORS(app)

# ============================================================
# VM ROLE + SLOT CONFIG
# ============================================================
VM_ROLE = os.getenv("VM_ROLE", "A").upper()
STARTUP_DELAY_SECS = float(os.getenv("STARTUP_DELAY_SECS", "0"))

if VM_ROLE == "A":
    # DEMO
    SLOTS_CONFIG = [
        (os.getenv("SSID_VM1_SLOT1"), ["MARA_otc", "GME_otc", "PLTR_otc"]),
        (os.getenv("SSID_VM1_SLOT2"), ["EURRUB_otc", "LINK_otc", "SOL-USD_otc"]),
        (os.getenv("SSID_VM1_SLOT3"), ["MATIC_otc", "TON-USD_otc"]),
    ]
elif VM_ROLE == "B":
    # LIVE
    SLOTS_CONFIG = [
        (os.getenv("SSID_VM2_SLOT1"), ["UKBrent_otc", "USCrude_otc", "JPN225_otc"]),
        (os.getenv("SSID_VM2_SLOT2"), ["SP500_otc", "BITB_otc", "XAGUSD_otc"]),
        (os.getenv("SSID_VM2_SLOT3"), ["XAUUSD_otc", "XNGUSD_otc"]),
    ]
else:
    raise RuntimeError(f"Unknown VM_ROLE: {VM_ROLE}")

# ============================================================
# GLOBAL MANAGER + PERF HISTORY
# ============================================================
manager = MultiStreamManager(VM_ROLE, SLOTS_CONFIG)
perf_history = PerfHistory(maxlen=240)  # 240 * 30s = 2h

def _perf_sampler():
    """Every 30s, snapshot perf into the ring buffer."""
    while True:
        time.sleep(30)
        try:
            manager.refresh_rates()
            perf_history.append({
                "ts": int(time.time()),
                "data": manager.snapshot_stats(),
            })
        except Exception as e:
            print(f"[perf] sampler error: {e}")

def _startup():
    manager.start(startup_delay_secs=STARTUP_DELAY_SECS)
    threading.Thread(target=_perf_sampler, daemon=True, name="perf-sampler").start()

# ============================================================
# ROUTES
# ============================================================
@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/candles")
def get_candles():
    """Full snapshot: all slots, all assets, all indicators."""
    return jsonify({
        "vm_role": VM_ROLE,
        "timeframe": TIMEFRAME_SECONDS,
        "server_time": int(time.time()),
        "data": manager.snapshot_all(),
    })


@app.route("/api/perf")
def get_perf():
    """Live performance stats for all slots."""
    manager.refresh_rates()
    return jsonify({
        "vm_role": VM_ROLE,
        "uptime_seconds": int(time.time() - _START_TS),
        "data": manager.snapshot_stats(),
    })


@app.route("/api/perf/history")
def get_perf_history():
    """Ring buffer of the last ~2h of perf samples."""
    return jsonify({"vm_role": VM_ROLE, "samples": perf_history.snapshot()})


@app.route("/api/health")
def health():
    return jsonify({"status": "ok", "vm_role": VM_ROLE})


# ============================================================
# STARTUP
# ============================================================
_START_TS = time.time()
_startup()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, threaded=True)
