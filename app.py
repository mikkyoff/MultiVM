"""
Multi-SSID Pocket Option streaming service with per-slot proxy support.

Profile layout for this run:
  VM A (Service A): 10 SSIDs, 20 assets, 5 proxies (2 SSIDs per proxy)
  VM B (Service B): 10 SSIDs, 20 assets, 5 proxies (2 SSIDs per proxy)

Timeframe: 1 minute (60s).
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


def _proxy_url(host: str, port: str, user: str, pw: str) -> str:
    """Build an HTTP proxy URL from parts. Returns '' if any part missing."""
    if not all([host, port, user, pw]):
        return ""
    return f"http://{user}:{pw}@{host}:{port}"


def _env_proxy(prefix: str) -> str:
    return _proxy_url(
        os.getenv(f"{prefix}_HOST", "").strip(),
        os.getenv(f"{prefix}_PORT", "").strip(),
        os.getenv(f"{prefix}_USER", "").strip(),
        os.getenv(f"{prefix}_PASS", "").strip(),
    )


# ---------------------------------------------------------------------
# Proxies (5 for VM A, 5 for VM B)
# ---------------------------------------------------------------------
PROXY_A_01 = _env_proxy("PROXY_A_01")
PROXY_A_02 = _env_proxy("PROXY_A_02")
PROXY_A_03 = _env_proxy("PROXY_A_03")
PROXY_A_04 = _env_proxy("PROXY_A_04")
PROXY_A_05 = _env_proxy("PROXY_A_05")

PROXY_B_01 = _env_proxy("PROXY_B_01")
PROXY_B_02 = _env_proxy("PROXY_B_02")
PROXY_B_03 = _env_proxy("PROXY_B_03")
PROXY_B_04 = _env_proxy("PROXY_B_04")
PROXY_B_05 = _env_proxy("PROXY_B_05")


# ---------------------------------------------------------------------
# VM A slot config: 10 SSIDs, 2 per proxy, 2 assets per SSID
# ---------------------------------------------------------------------
VM_A_SLOTS = [
    (os.getenv("SSID_A_01"), ["MARA_otc", "GME_otc"],           "ticks", PROXY_A_01),
    (os.getenv("SSID_A_02"), ["PLTR_otc", "BITB_otc"],          "ticks", PROXY_A_01),
    (os.getenv("SSID_A_03"), ["EURRUB_otc", "USDCNH_otc"],      "ticks", PROXY_A_02),
    (os.getenv("SSID_A_04"), ["LINK_otc", "DOTUSD_otc"],        "ticks", PROXY_A_02),
    (os.getenv("SSID_A_05"), ["SOL-USD_otc", "ADA-USD_otc"],    "ticks", PROXY_A_03),
    (os.getenv("SSID_A_06"), ["MATIC_otc", "TON-USD_otc"],      "ticks", PROXY_A_03),
    (os.getenv("SSID_A_07"), ["BTCUSD_otc", "ETHUSD_otc"],      "ticks", PROXY_A_04),
    (os.getenv("SSID_A_08"), ["BNB-USD_otc", "LTCUSD_otc"],     "ticks", PROXY_A_04),
    (os.getenv("SSID_A_09"), ["TRX-USD_otc", "DOGE_otc"],       "ticks", PROXY_A_05),
    (os.getenv("SSID_A_10"), ["EURUSD_otc", "GBPUSD_otc"],      "ticks", PROXY_A_05),
]


# ---------------------------------------------------------------------
# VM B slot config: 10 SSIDs, 2 per proxy, 2 assets per SSID
# ---------------------------------------------------------------------
VM_B_SLOTS = [
    (os.getenv("SSID_B_01"), ["UKBrent_otc", "USCrude_otc"],    "ticks", PROXY_B_01),
    (os.getenv("SSID_B_02"), ["XNGUSD_otc", "XPTUSD_otc"],      "ticks", PROXY_B_01),
    (os.getenv("SSID_B_03"), ["XAUUSD_otc", "XAGUSD_otc"],      "ticks", PROXY_B_02),
    (os.getenv("SSID_B_04"), ["XPDUSD_otc", "USDCAD_otc"],      "ticks", PROXY_B_02),
    (os.getenv("SSID_B_05"), ["SP500_otc", "NASUSD_otc"],       "ticks", PROXY_B_03),
    (os.getenv("SSID_B_06"), ["DJI30_otc", "JPN225_otc"],       "ticks", PROXY_B_03),
    (os.getenv("SSID_B_07"), ["AUS200_otc", "D30EUR_otc"],      "ticks", PROXY_B_04),
    (os.getenv("SSID_B_08"), ["E35EUR_otc", "E50EUR_otc"],      "ticks", PROXY_B_04),
    (os.getenv("SSID_B_09"), ["F40EUR_otc", "CITI_otc"],        "ticks", PROXY_B_05),
    (os.getenv("SSID_B_10"), ["#AAPL_otc", "#MSFT_otc"],        "ticks", PROXY_B_05),
]


if VM_ROLE == "A":
    SLOTS_CONFIG = VM_A_SLOTS
elif VM_ROLE == "B":
    SLOTS_CONFIG = VM_B_SLOTS
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
        time.sleep(1800)


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
    return jsonify({
        "status": "ok",
        "vm_role": VM_ROLE,
        "slot_count": len(SLOTS_CONFIG),
    })


_START_TS = time.time()
_startup()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, threaded=True)
