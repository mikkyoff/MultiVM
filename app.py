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
        (os.getenv("SSID_VM1_SLOT1"), ["MARA_otc", "GME_otc", "PLTR_otc"], _mode("SSID_VM1_SLOT1_MODE")),
        (os.getenv("SSID_VM1_SLOT2"), ["EURRUB_otc", "LINK_otc", "SOL-USD_otc"], _mode("SSID_VM1_SLOT2_MODE")),
        (os.getenv("SSID_VM1_SLOT3"), ["MATIC_otc", "#XOM_otc"], _mode("SSID_VM1_SLOT3_MODE")),
    ]
elif VM_ROLE == "B":
    # VM B: test each mode
    #   slot 1 → ticks (control)
    #   slot 2 → time_aligned (developer suggestion)
    #   slot 3 → historical_ticks (user workaround)
    SLOTS_CONFIG = [
        (os.getenv("SSID_VM2_SLOT1"), ["UKBrent_otc", "USCrude_otc", "JPN225_otc"], _mode("SSID_VM2_SLOT1_MODE", "ticks")),
        (os.getenv("SSID_VM2_SLOT2"), ["SP500_otc", "BITB_otc", "XAGUSD_otc"], _mode("SSID_VM2_SLOT2_MODE", "time_aligned")),
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
                libc
