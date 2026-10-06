"""
Multi-SSID Pocket Option streaming service with mode testing.

Three modes are available for candle sourcing:
  - "ticks":            raw tick subscription, self-bucketed
  - "time_aligned":     subscribe_symbol_time_aligned (developer suggestion)
  - "historical_ticks": get_candles(asset, 1, N) polled (user's workaround)

Configure per-slot via SSID_VMx_SLOTN_MODE env vars.
"""

import os
import time
import ctypes
import gc
import threading
from flask import Flask, jsonify, render_template
from flask_cors import CORS
from dotenv import load_dotenv

from multistream import MultiStreamManager, TIMEFRAME_SECONDS
from perf import PerfHistory

load_dotenv()

app = Flask(__name__, static_folder='static', static_url_path='')
CORS(app)

VM_ROLE = os.getenv("VM_ROLE", "A").upper()
STARTUP_DELAY_SECS = float(os.getenv("STARTUP_DELAY_SECS", "0"))

def _mode(env_key, default="ticks"):
    v = os.getenv(env_key, default).strip().lower()
    if v not in ("ticks", "time_aligned", "historical_ticks"):
        v = default
    return v

if VM_ROLE == "A":
    # VM A: control — all three slots use raw tick mode (baseline)
    SLOTS_CONFIG = [
        (os.getenv("SSID_VM1_SLOT1"), ["MARA_otc", "GME_otc", "XAUUSD_otc"], _mode("SSID_VM1_SLOT1_MODE")),
        (os.getenv("SSID_VM1_SLOT2"), ["NGNUSD_otc", "AUS200_otc"], _mode("SSID_VM1_SLOT2_MODE")),
        (os.getenv("SSID_VM1_SLOT3"), ["MATIC_otc", "TON-USD_otc"], _mode("SSID_VM1_SLOT3_MODE")),
    ]
elif VM_ROLE == "B":
    # VM B: test each mode
    #   slot 1 → ticks (control)
    #   slot 2 → time_aligned (developer suggestion)
    #   slot 3 → historical_ticks (user workaround)
    SLOTS_CONFIG = [
        (os.getenv("SSID_VM2_SLOT1"), ["UKBrent_otc", "USCrude_otc"], _mode("SSID_VM2_SLOT1_MODE", "ticks")),
        (os.getenv("SSID_VM2_SLOT2"), ["SP500_otc", "BITB_otc"], _mode("SSID_VM2_SLOT2_MODE", "time_aligned")),
        (os.getenv("SSID_VM2_SLOT3"), ["XAUUSD_otc", "XNGUSD_otc"], _mode("SSID_VM2_SLOT3_MODE", "historical_ticks")),
    ]
else:
    raise RuntimeError(f"Unknown VM_ROLE: {VM_ROLE}")

manager = MultiStreamManager(VM_ROLE, SLOTS_CONFIG)
perf_history = PerfHistory(maxlen=20)

def _perf_sampler():
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

def _memory_maintenance():
    time.sleep(600)
    while True:
        try:
            gc.collect()
            try:
                libc = ctypes.CDLL("libc.so.6")
                libc.malloc_trim(0)
                print("[maint] gc + malloc_trim done")
            except Exception:
                pass
        except Exception as e:
            print(f"[maint] error: {e}")
        time.sleep(3600)

def _startup():
    manager.start(startup_delay_secs=STARTUP_DELAY_SECS)
    threading.Thread(target=_perf_sampler, daemon=True, name="perf-sampler").start()
    threading.Thread(target=_memory_maintenance, daemon=True, name="mem-maint").start()


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/candles")
def get_candles():
    return jsonify({
        "vm_role": VM_ROLE,
        "timeframe": TIMEFRAME_SECONDS,
        "server_time": int(time.time()),
        "data": manager.snapshot_all(),
    })


@app.route("/api/perf")
def get_perf():
    manager.refresh_rates()
    return jsonify({
        "vm_role": VM_ROLE,
        "uptime_seconds": int(time.time() - _START_TS),
        "data": manager.snapshot_stats(),
    })


@app.route("/api/perf/history")
def get_perf_history():
    return jsonify({"vm_role": VM_ROLE, "samples": perf_history.snapshot()})


@app.route("/api/health")
def health():
    return jsonify({"status": "ok", "vm_role": VM_ROLE})


_START_TS = time.time()
_startup()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, threaded=True)
