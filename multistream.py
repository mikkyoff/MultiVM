"""
Multi-SSID streaming manager.

Each slot owns:
  - one PocketOptionAsync client
  - a background asyncio loop + thread
  - its own set of assets
  - its own per-asset candle builders + indicator cache

Candles are built 100% from live ticks. Historical fetch at startup
is used only to seed the indicator math — it does not go on the chart.
"""

import os
import asyncio
import threading
import time
import random
from collections import deque
from typing import Dict, List, Tuple

from BinaryOptionsToolsV2 import PocketOptionAsync
from BinaryOptionsToolsV2.config import Config

from indicators import recompute
from perf import SlotStats, AssetStats


TIMEFRAME_SECONDS = 60
HISTORY_CANDLES = 60
MAX_CANDLES = 100

RSI_PERIOD = 14
BB_PERIOD = 20
BB_STD = 2.0
EMA_PERIOD = 6

# Delay between starting slots, to avoid thundering-herd auth
SLOT_START_GAP_SECS = 5.0


class AssetState:
    """Candle + indicator state for one asset."""
    __slots__ = (
        "asset", "candles", "forming", "last_boundary",
        "rsi", "bb_upper", "bb_middle", "bb_lower",
        "bb_bandwidth", "bb_pct_to_upper", "bb_pct_to_lower",
        "ema", "ema_signal",
    )

    def __init__(self, asset: str):
        self.asset = asset
        self.candles = deque(maxlen=MAX_CANDLES)
        self.forming = None
        self.last_boundary = None

        self.rsi = None
        self.bb_upper = None
        self.bb_middle = None
        self.bb_lower = None
        self.bb_bandwidth = None
        self.bb_pct_to_upper = None
        self.bb_pct_to_lower = None
        self.ema = None
        self.ema_signal = None

    def snapshot(self):
        return {
            "asset": self.asset,
            "candles": list(self.candles),
            "forming": self.forming,
            "indicators": {
                "rsi": self.rsi,
                "rsi_period": RSI_PERIOD,
                "bb_upper": self.bb_upper,
                "bb_middle": self.bb_middle,
                "bb_lower": self.bb_lower,
                "bb_period": BB_PERIOD,
                "bb_std": BB_STD,
                "bb_bandwidth": self.bb_bandwidth,
                "bb_pct_to_upper": self.bb_pct_to_upper,
                "bb_pct_to_lower": self.bb_pct_to_lower,
                "ema": self.ema,
                "ema_period": EMA_PERIOD,
                "ema_signal": self.ema_signal,
            },
        }


class Slot:
    """
    One SSID = one Slot.
    Manages its own thread, event loop, client, and asset states.
    """

    def __init__(self, slot_id: int, ssid: str, assets: List[str]):
        self.slot_id = slot_id
        self.ssid = ssid
        self.assets = assets
        self.stats = SlotStats(slot_id, ssid, assets)
        self.asset_state: Dict[str, AssetState] = {a: AssetState(a) for a in assets}
        self.state_lock = threading.Lock()
        self.thread = None
        self._stop = threading.Event()

    def start(self):
        self.thread = threading.Thread(
            target=self._run_thread, daemon=True,
            name=f"slot-{self.slot_id}",
        )
        self.thread.start()

    def _run_thread(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(self._main())
        except Exception as e:
            self.stats.mark_error(f"slot fatal: {e}")
        finally:
            try:
                loop.close()
            except Exception:
                pass

    async def _main(self):
        # Stagger startup within a VM
        await asyncio.sleep(random.uniform(0, 3))

        if not self.ssid:
            self.stats.mark_error("SSID not set for this slot")
            return

        config = Config(timeout_secs=30, terminal_logging=False)
        try:
            client = PocketOptionAsync(self.ssid, config=config)
            await client.wait_for_assets(timeout=60.0)
            balance = await client.balance()
        except Exception as e:
            self.stats.mark_auth_failure(str(e))
            return

        print(
            f"[slot {self.slot_id}] connected fp={self.stats.ssid_fp} "
            f"balance={balance} demo={client.is_demo()} assets={self.assets}"
        )
        self.stats.mark_connected()

        # --- Seed history for each asset (best-effort) ---
        for asset in self.assets:
            try:
                await self._seed_history(client, asset)
            except Exception as e:
                print(f"[slot {self.slot_id}] seed {asset} failed: {e}")

        # --- Subscribe to each asset's tick stream ---
        tasks = []
        for asset in self.assets:
            tasks.append(asyncio.create_task(self._stream_asset(client, asset)))

        try:
            await asyncio.gather(*tasks, return_exceptions=True)
        finally:
            self.stats.mark_disconnected("stream loop ended")
            try:
                await client.shutdown()
            except Exception:
                pass

    async def _seed_history(self, client, asset: str):
        """Fetch ~150 closed candles once, just to seed indicator math."""
        try:
            gen = client.get_candles_live(
                asset=asset,
                period=TIMEFRAME_SECONDS,
                hours=3.0,
                max_rows=HISTORY_CANDLES,
            )
            closed, _forming = await gen.__anext__()
            await gen.aclose()
        except Exception as e:
            print(f"[slot {self.slot_id}] history seed failed for {asset}: {e}")
            return

        state = self.asset_state[asset]
        for c in closed[-HISTORY_CANDLES:]:
            state.candles.append({
                "time": int(c["time"]),
                "open": float(c["open"]),
                "high": float(c["high"]),
                "low": float(c["low"]),
                "close": float(c["close"]),
            })
        if state.candles:
            state.last_boundary = state.candles[-1]["time"]

        closes = [c["close"] for c in state.candles]
        if closes:
            ind = recompute(closes, RSI_PERIOD, BB_PERIOD, BB_STD, EMA_PERIOD)
            with self.state_lock:
                state.rsi = ind["rsi"]
                state.bb_upper = ind["bb_upper"]
                state.bb_middle = ind["bb_middle"]
                state.bb_lower = ind["bb_lower"]
                state.bb_bandwidth = ind["bb_bandwidth"]
                state.bb_pct_to_upper = ind["bb_pct_to_upper"]
                state.bb_pct_to_lower = ind["bb_pct_to_lower"]
                state.ema = ind["ema"]
                state.ema_signal = ind["ema_signal"]
        print(f"[slot {self.slot_id}] seeded {len(state.candles)} candles for {asset}")

    async def _stream_asset(self, client, asset: str):
        """Consume the tick stream for one asset, forever."""
        state = self.asset_state[asset]
        st = self.stats.get_asset(asset)

        try:
            stream = await client.subscribe_symbol(asset)
        except Exception as e:
            self.stats.mark_error(f"subscribe {asset}: {e}")
            return

        last_ts = 0
        stale_cutoff = TIMEFRAME_SECONDS * 2

        while not self._stop.is_set():
            try:
                tick = await stream.__anext__()
            except StopAsyncIteration:
                self.stats.mark_error(f"stream ended: {asset}")
                return
            except Exception as e:
                self.stats.mark_error(f"tick error {asset}: {e}")
                # small backoff then keep trying
                await asyncio.sleep(1.0)
                continue

            try:
                price = float(tick.get("close") or tick.get("price") or 0)
                ts = int(tick.get("timestamp") or tick.get("time") or 0)
                if price <= 0 or ts <= 0:
                    continue

                now = int(time.time())
                if ts < now - stale_cutoff or ts < last_ts:
                    continue
                last_ts = ts

                self._ingest_tick(state, st, price, ts)
            except Exception as e:
                self.stats.mark_error(f"ingest {asset}: {e}")

    def _ingest_tick(self, state: AssetState, st: AssetStats, price: float, ts: int):
        bucket = (ts // TIMEFRAME_SECONDS) * TIMEFRAME_SECONDS

        with self.state_lock:
            if state.forming is None:
                state.forming = {
                    "time": bucket,
                    "open": price, "high": price, "low": price, "close": price,
                }
                state.last_boundary = bucket
            elif bucket > state.last_boundary:
                # Detect gap: if the new bucket is more than 1 timeframe away,
                # we may have missed buckets
                gap_buckets = (bucket - state.last_boundary) // TIMEFRAME_SECONDS - 1
                if gap_buckets > 0:
                    st.gaps += gap_buckets

                state.candles.append(dict(state.forming))
                st.record_candle()
                state.last_boundary = bucket
                state.forming = {
                    "time": bucket,
                    "open": price, "high": price, "low": price, "close": price,
                }
            else:
                state.forming["high"] = max(state.forming["high"], price)
                state.forming["low"] = min(state.forming["low"], price)
                state.forming["close"] = price

            # Recompute indicators on every tick
            closes = [c["close"] for c in state.candles]
            closes.append(state.forming["close"])
            ind = recompute(closes, RSI_PERIOD, BB_PERIOD, BB_STD, EMA_PERIOD)

            state.rsi = ind["rsi"]
            state.bb_upper = ind["bb_upper"]
            state.bb_middle = ind["bb_middle"]
            state.bb_lower = ind["bb_lower"]
            state.bb_bandwidth = ind["bb_bandwidth"]
            state.bb_pct_to_upper = ind["bb_pct_to_upper"]
            state.bb_pct_to_lower = ind["bb_pct_to_lower"]
            state.ema = ind["ema"]
            state.ema_signal = ind["ema_signal"]

        # Stats outside the state lock to keep it short
        st.record_tick(ts, price)
        self.stats.record_slot_tick()

    def snapshot(self):
        with self.state_lock:
            assets_snap = {a: s.snapshot() for a, s in self.asset_state.items()}
        return {
            "slot_id": self.slot_id,
            "assets": assets_snap,
            "stats": self.stats.snapshot(),
        }


class MultiStreamManager:
    """
    Owns all slots for a VM. Starts them staggered.
    """

    def __init__(self, vm_role: str, slots_config: List[Tuple[str, List[str]]]):
        self.vm_role = vm_role
        self.slots: List[Slot] = []
        for i, (ssid, assets) in enumerate(slots_config, start=1):
            self.slots.append(Slot(slot_id=i, ssid=ssid or "", assets=assets))

    def start(self, startup_delay_secs: float = 0.0):
        def _run():
            if startup_delay_secs > 0:
                print(f"[manager] startup delay {startup_delay_secs}s")
                time.sleep(startup_delay_secs)
            for i, slot in enumerate(self.slots):
                slot.start()
                if i < len(self.slots) - 1:
                    time.sleep(SLOT_START_GAP_SECS)

        threading.Thread(target=_run, daemon=True, name="manager-start").start()

    def snapshot_all(self):
        return {
            "vm_role": self.vm_role,
            "timeframe": TIMEFRAME_SECONDS,
            "slots": [s.snapshot() for s in self.slots],
        }

    def snapshot_stats(self):
        return {
            "vm_role": self.vm_role,
            "slots": [s.stats.snapshot() for s in self.slots],
        }

    def refresh_rates(self):
        for s in self.slots:
            s.stats.refresh_rates()
