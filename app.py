"""
Multi-SSID Pocket Option streaming service.

Supports two profiles:
  - VM_PROFILE=light  → 10 slots (20 assets) per VM — safe starting point
  - VM_PROFILE=full   → 15 slots (30 assets) per VM — memory-tight
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
VM_PROFILE = os.getenv("VM_PROFILE", "light").strip().lower()


# =====================================================================
# VM A (demo) — stocks, crypto, forex
# =====================================================================
VM_A_FULL = [
    (os.getenv("SSID_A_01"), ["MARA_otc", "GME_otc"],            "ticks"),
    (os.getenv("SSID_A_02"), ["PLTR_otc", "BITB_otc"],           "ticks"),
    (os.getenv("SSID_A_03"), ["EURRUB_otc", "USDCNH_otc"],       "ticks"),
    (os.getenv("SSID_A_04"), ["LINK_otc", "DOTUSD_otc"],         "ticks"),
    (os.getenv("SSID_A_05"), ["SOL-USD_otc", "ADA-USD_otc"],     "ticks"),
    (os.getenv("SSID_A_06"), ["MATIC_otc", "TON-USD_otc"],       "ticks"),
    (os.getenv("SSID_A_07"), ["BTCUSD_otc", "ETHUSD_otc"],       "ticks"),
    (os.getenv("SSID_A_08"), ["BNB-USD_otc", "LTCUSD_otc"],      "ticks"),
    (os.getenv("SSID_A_09"), ["TRX-USD_otc", "DOGE_otc"],        "ticks"),
    (os.getenv("SSID_A_10"), ["EURUSD_otc", "GBPUSD_otc"],       "ticks"),
    (os.getenv("SSID_A_11"), ["USDJPY_otc", "USDCAD_otc"],       "ticks"),
    (os.getenv("SSID_A_12"), ["AUDUSD_otc", "NZDUSD_otc"],       "ticks"),
    (os.getenv("SSID_A_13"), ["USDCHF_otc", "USDINR_otc"],       "ticks"),
    (os.getenv("SSID_A_14"), ["USDRUB_otc", "USDVND_otc"],       "ticks"),
    (os.getenv("SSID_A_15"), ["USDPKR_otc", "USDBRL_otc"],       "ticks"),
]

# =====================================================================
# VM B (demo) — commodities, indices, more stocks
# =====================================================================
VM_B_FULL = [
    (os.getenv("SSID_B_01"), ["UKBrent_otc", "USCrude_otc"],     "ticks"),
    (os.getenv("SSID_B_02"), ["XNGUSD_otc", "XPTUSD_otc"],       "ticks"),
    (os.getenv("SSID_B_03"), ["XAUUSD_otc", "XAGUSD_otc"],       "ticks"),
    (os.getenv("SSID_B_04"), ["XPDUSD_otc", "USDCAD_otc"],       "ticks"),
    (os.getenv("SSID_B_05"), ["SP500_otc", "NASUSD_otc"],        "ticks"),
    (os.getenv("SSID_B_06"), ["DJI30_otc", "JPN225_otc"],        "ticks"),
    (os.getenv("SSID_B_07"), ["AUS200_otc", "D30EUR_otc"],       "ticks"),
    (os.getenv("SSID_B_08"), ["E35EUR_otc", "E50EUR_otc"],       "ticks"),
    (os.getenv("SSID_B_09"), ["F40EUR_otc", "CITI_otc"],         "ticks"),
    (os.getenv("SSID_B_10"), ["#AAPL_otc", "#MSFT_otc"],         "ticks"),
    (os.getenv("SSID_B_11"), ["#TSLA_otc", "#INTC_otc"],         "ticks"),
    (os.getenv("SSID_B_12"), ["AMZN_otc", "NFLX_otc"],           "ticks"),
    (os.getenv("SSID_B_13"), ["BABA_otc", "TWITTER_otc"],        "ticks"),
    (os.getenv("SSID_B_14"), ["#JNJ_otc", "#MCD_otc"],           "ticks"),
    (os.getenv("SSID_B_15"), ["#PFE_otc", "FDX_otc"],            "ticks"),
]


def _pick_profile(full_list):
    if VM_PROFILE == "full":
        return full_list
    # light: first 10 slots
    return full_list[:10]


if VM_ROLE == "A":
    SLOTS_CONFIG = _pick_profile(VM_A_FULL)
elif VM_ROLE == "B":
    SLOTS_CONFIG = _pick_profile(VM_B_FULL)
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
        time.sleep(1800)  # 30 min now — more aggressive for 15 slots


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
        "vm_profile": VM_PROFILE,
        "timeframe": TIMEFRAME_SECONDS,
        "server_time": int(time.time()),
        "data": manager.snapshot_all(),
    })


@app.route("/api/perf")
def get_perf():
    manager.refresh_rates()
    return jsonify({
        "vm_role": VM_ROLE,
        "vm_profile": VM_PROFILE,
        "uptime_seconds": int(time.time() - _START_TS),
        "data": manager.snapshot_stats(),
    })


@app.route("/api/perf/history")
def get_perf_history():
    return jsonify({"vm_role": VM_ROLE, "samples": perf_history.snapshot()})


@app.route("/api/health")
def health():
    return jsonify({
        "status": "ok",
        "vm_role": VM_ROLE,
        "vm_profile": VM_PROFILE,
        "slot_count": len(SLOTS_CONFIG),
    })


_START_TS = time.time()
_startup()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, threaded=True)
