"""
Performance tracking. Counter-based tick rates. Adds mode + freshness.
"""

import time
import hashlib
import threading


def fingerprint(ssid: str) -> str:
    if not ssid:
        return "none"
    return hashlib.sha256(ssid.encode()).hexdigest()[:8]


class AssetStats:
    __slots__ = (
        "ticks", "candles", "gaps", "last_close", "last_tick_ts",
        "tick_rate_60s", "_window_start", "_window_ticks",
    )

    def __init__(self):
        self.ticks = 0
        self.candles = 0
        self.gaps = 0
        self.last_close = None
        self.last_tick_ts = 0
        self.tick_rate_60s = 0.0
        self._window_start = time.time()
        self._window_ticks = 0

    def record_tick(self, ts: int, price: float):
        self.ticks += 1
        self._window_ticks += 1
        self.last_close = price
        self.last_tick_ts = ts

    def record_candle(self):
        self.candles += 1

    def record_gap(self):
        self.gaps += 1

    def refresh_rate(self, window=60.0):
        now = time.time()
        elapsed = now - self._window_start
        if elapsed >= window:
            self.tick_rate_60s = self._window_ticks / elapsed
            self._window_ticks = 0
            self._window_start = now

    def snapshot(self):
        return {
            "ticks": self.ticks,
            "candles": self.candles,
            "gaps": self.gaps,
            "tick_rate": round(self.tick_rate_60s, 3),
            "last_close": self.last_close,
            "last_tick_age_s": round(time.time() - self.last_tick_ts, 2) if self.last_tick_ts else None,
        }


class SlotStats:
    def __init__(self, slot_id: int, ssid: str, assets: list, mode: str = "ticks"):
        self.slot_id = slot_id
        self.ssid_fp = fingerprint(ssid)
        self.assets = list(assets)
        self.mode = mode
        self.lock = threading.Lock()

        self.connected = False
        self.connected_since = None
        self.total_connected_seconds = 0.0
        self.started_at = time.time()
        self.reconnects = 0
        self.auth_failures = 0
        self.other_errors = 0
        self.last_error = None

        self.asset_stats = {a: AssetStats() for a in assets}

        self.slot_tick_rate_60s = 0.0
        self._slot_window_start = time.time()
        self._slot_window_ticks = 0

    def mark_connected(self):
        with self.lock:
            if not self.connected:
                self.connected = True
                self.connected_since = time.time()

    def mark_disconnected(self, reason=None):
        with self.lock:
            if self.connected and self.connected_since:
                self.total_connected_seconds += time.time() - self.connected_since
            self.connected = False
            self.connected_since = None
            if reason:
                self.last_error = reason

    def mark_reconnect(self):
        with self.lock:
            self.reconnects += 1

    def mark_auth_failure(self, reason=None):
        with self.lock:
            self.auth_failures += 1
            self.last_error = reason or "auth failure"

    def mark_error(self, reason):
        with self.lock:
            self.other_errors += 1
            self.last_error = str(reason)[:200]

    def get_asset(self, asset: str) -> AssetStats:
        return self.asset_stats[asset]

    def refresh_rates(self):
        now = time.time()
        elapsed = now - self._slot_window_start
        if elapsed >= 60.0:
            self.slot_tick_rate_60s = self._slot_window_ticks / elapsed
            self._slot_window_ticks = 0
            self._slot_window_start = now
        for st in self.asset_stats.values():
            st.refresh_rate()

    def record_slot_tick(self):
        self._slot_window_ticks += 1

    def uptime_pct(self):
        elapsed = time.time() - self.started_at
        if elapsed <= 0:
            return 0.0
        live = self.total_connected_seconds
        if self.connected and self.connected_since:
            live += time.time() - self.connected_since
        return round(live / elapsed * 100.0, 2)

    def snapshot(self):
        with self.lock:
            return {
                "slot_id": self.slot_id,
                "ssid_fingerprint": self.ssid_fp,
                "mode": self.mode,
                "assets": self.assets,
                "connected": self.connected,
                "connected_since": self.connected_since,
                "uptime_pct": self.uptime_pct(),
                "reconnects": self.reconnects,
                "auth_failures": self.auth_failures,
                "other_errors": self.other_errors,
                "last_error": self.last_error,
                "slot_tick_rate_60s": round(self.slot_tick_rate_60s, 3),
                "per_asset": {a: st.snapshot() for a, st in self.asset_stats.items()},
            }


class PerfHistory:
    def __init__(self, maxlen=20):
        from collections import deque
        self.samples = deque(maxlen=maxlen)

    def append(self, sample: dict):
        self.samples.append(sample)

    def snapshot(self):
        return list(self.samples)
