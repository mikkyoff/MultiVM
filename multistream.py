"""
Multi-SSID streaming manager.
Supports three stream modes for candle sourcing:
  - "ticks":            raw tick subscription, self-bucketed (original approach)
  - "time_aligned":     subscribe_symbol_time_aligned (developer's suggestion)
  - "historical_ticks": get_candles(asset, 1, N) polled on a timer (user's workaround)
"""

import os
import asyncio
import threading
import time
import random
from collections import deque
from datetime import timedelta
from typing import Dict, List, Tuple

from BinaryOptionsToolsV2 import PocketOptionAsync
from BinaryOptionsToolsV2.config import Config

from indicators import recompute
from perf import SlotStats, AssetStats


TIMEFRAME_SECONDS = 300
HISTORY_CANDLES = 150
MAX_CANDLES = 100        # reduced from 300 as requested

RSI_PERIOD = 14
BB_PERIOD = 20
BB_STD = 2.0
EMA_PERIOD = 6

SLOT_START_GAP_SECS = 5.0

# How often historical_ticks mode polls the API
HISTORICAL_POLL_SECS = 10


class AssetState:
    __slots__ = (
        "asset", "candles", "forming", "last_boundary",
        "rsi", "bb_upper", "bb_middle", "bb_lower",
        "bb_bandwidth", "bb_pct_to_upper", "bb_pct_to_lower",
        "ema", "ema_signal",
        "last_remote_candle_time",   # for freshness tracking
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

        self.last_remote_candle_time = None

    def snapshot(self):
        return {
            "asset": self.asset,
            "candles": [
                {"time": t, "open": o, "high": h, "low": l, "close": c}
                for (t, o, h, l, c) in self.candles
            ],
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
    def __init__(self, slot_id: int, ssid: str, assets: List[str], mode: str = "ticks"):
        self.slot_id = slot_id
        self.ssid = ssid
        self.assets = assets
        self.mode = mode
        self.stats = SlotStats(slot_id, ssid, assets, mode=mode)
        self.asset_state: Dict[str, AssetState] = {a: AssetState(a) for a in assets}
        self.state_lock = threading.Lock()
        self.thread = None
        self._stop = threading.Event()

    def start(self):
        self.thread = threading.Thread(
            target=self._run_thread, daemon=True,
            name=f"slot-{self.slot_id}-{self.mode}",
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
        await asyncio.sleep(random.uniform(0, 3))

        if not self.ssid:
            self.stats.mark_error("SSID not set for this slot")
            return

        print(f"[slot {self.slot_id}] mode={self.mode} ssid_len={len(self.ssid)}")

        config = Config(timeout_secs=30, terminal_logging=False)
        try:
            client = PocketOptionAsync(self.ssid, config=config)
            await client.wait_for_assets(timeout=60.0)
            balance = await client.balance()
        except Exception as e:
            self.stats.mark_auth_failure(str(e))
            print(f"[slot {self.slot_id}] connect failed: {e}")
            return

        print(
            f"[slot {self.slot_id}] connected fp={self.stats.ssid_fp} "
            f"balance={balance} demo={client.is_demo()} assets={self.assets}"
        )
        self.stats.mark_connected()

        # Seed history (all modes fetch initial history the same way)
        for asset in self.assets:
            try:
                await self._seed_history(client, asset)
            except Exception as e:
                print(f"[slot {self.slot_id}] seed {asset} failed: {e}")

        # Route to the appropriate streaming loop
        if self.mode == "ticks":
            tasks = [asyncio.create_task(self._stream_ticks(client, a)) for a in self.assets]
        elif self.mode == "time_aligned":
            tasks = [asyncio.create_task(self._stream_time_aligned(client, a)) for a in self.assets]
        elif self.mode == "historical_ticks":
            tasks = [asyncio.create_task(self._stream_historical_ticks(client, a)) for a in self.assets]
        else:
            self.stats.mark_error(f"unknown mode: {self.mode}")
            return

        try:
            await asyncio.gather(*tasks, return_exceptions=True)
        finally:
            self.stats.mark_disconnected("stream loop ended")
            try:
                await client.shutdown()
            except Exception:
                pass

    # ------------------------------------------------------------------
    # History seeding (shared by all modes)
    # ------------------------------------------------------------------
    async def _seed_history(self, client, asset: str):
        """
        Fetch initial closed candles.
        Prefer get_candles(asset, 1, N) then compile locally if the mode
        is historical_ticks; otherwise use get_candles_live as before.
        """
        state = self.asset_state[asset]

        try:
            if self.mode == "historical_ticks":
                # Use the user's approach: pull 1s ticks, compile locally
                # Get enough ticks to cover HISTORY_CANDLES * TIMEFRAME_SECONDS
                # (worst case: 1 tick per second -> HISTORY_CANDLES * TIMEFRAME_SECONDS ticks)
                needed_ticks = min(HISTORY_CANDLES * TIMEFRAME_SECONDS, 3000)
                raw = await client.get_candles(asset, 1, needed_ticks)
                # raw is a list of {time, price} or similar
                candles = self._compile_from_ticks(raw, TIMEFRAME_SECONDS)
            else:
                gen = client.get_candles_live(
                    asset=asset,
                    period=TIMEFRAME_SECONDS,
                    hours=3.0,
                    max_rows=HISTORY_CANDLES,
                )
                closed, _forming = await gen.__anext__()
                await gen.aclose()
                candles = [
                    (int(c["time"]), float(c["open"]), float(c["high"]),
                     float(c["low"]), float(c["close"]))
                    for c in closed[-HISTORY_CANDLES:]
                ]
        except Exception as e:
            print(f"[slot {self.slot_id}] history seed failed for {asset}: {e}")
            return

        for c in candles:
            state.candles.append(c)

        if state.candles:
            state.last_boundary = state.candles[-1][0]
            state.last_remote_candle_time = state.candles[-1][0]

        closes = [c[4] for c in state.candles]
        if closes:
            self._apply_indicators(state, closes)

        print(f"[slot {self.slot_id}] seeded {len(state.candles)} candles for {asset} (mode={self.mode})")

    def _compile_from_ticks(self, raw_ticks, period: int):
        """
        Build OHLC candles from raw ticks.
        raw_ticks: list of dicts (with 'time'/'timestamp' and 'price'/'close') or tuples.
        """
        if not raw_ticks:
            return []

        parsed = []
        for t in raw_ticks:
            if isinstance(t, dict):
                ts = int(t.get("time") or t.get("timestamp") or 0)
                price = float(t.get("price") or t.get("close") or 0)
            elif isinstance(t, (list, tuple)) and len(t) >= 2:
                ts = int(t[0])
                price = float(t[1])
            else:
                continue
            if ts > 0 and price > 0:
                parsed.append((ts, price))

        parsed.sort()
        if not parsed:
            return []

        buckets = {}
        for ts, price in parsed:
            b = (ts // period) * period
            if b not in buckets:
                buckets[b] = [price, price, price, price]
            else:
                buckets[b][1] = max(buckets[b][1], price)
                buckets[b][2] = min(buckets[b][2], price)
                buckets[b][3] = price

        return [(b, v[0], v[1], v[2], v[3]) for b, v in sorted(buckets.items())]

    # ------------------------------------------------------------------
    # Mode 1: raw tick subscription (original approach)
    # ------------------------------------------------------------------
    async def _stream_ticks(self, client, asset: str):
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

    # ------------------------------------------------------------------
    # Mode 2: subscribe_symbol_time_aligned (developer's suggestion)
    # ------------------------------------------------------------------
    async def _stream_time_aligned(self, client, asset: str):
        state = self.asset_state[asset]
        st = self.stats.get_asset(asset)

        try:
            stream = await client.subscribe_symbol_time_aligned(
                asset, timedelta(seconds=TIMEFRAME_SECONDS)
            )
        except Exception as e:
            self.stats.mark_error(f"subscribe_aligned {asset}: {e}")
            print(f"[slot {self.slot_id}] aligned subscribe failed for {asset}: {e}")
            return

        # The aligned stream emits dicts with {time, open, high, low, close}
        while not self._stop.is_set():
            try:
                candle = await stream.__anext__()
            except StopAsyncIteration:
                self.stats.mark_error(f"aligned stream ended: {asset}")
                return
            except Exception as e:
                self.stats.mark_error(f"aligned tick error {asset}: {e}")
                await asyncio.sleep(1.0)
                continue

            try:
                ts = int(candle.get("time") or candle.get("timestamp") or 0)
                o = float(candle.get("open") or 0)
                h = float(candle.get("high") or 0)
                l = float(candle.get("low") or 0)
                c = float(candle.get("close") or 0)
                if ts <= 0 or o <= 0 or c <= 0:
                    continue

                with self.state_lock:
                    # Append the completed candle
                    state.candles.append((ts, o, h, l, c))
                    st.record_candle()

                    state.forming = {
                        "time": ts + TIMEFRAME_SECONDS,
                        "open": c, "high": c, "low": c, "close": c,
                    }
                    state.last_boundary = ts
                    state.last_remote_candle_time = ts

                    closes = [cc[4] for cc in state.candles]
                    closes.append(state.forming["close"])
                    self._apply_indicators(state, closes)

                st.record_tick(ts, c)
                self.stats.record_slot_tick()
            except Exception as e:
                self.stats.mark_error(f"aligned ingest {asset}: {e}")

    # ------------------------------------------------------------------
    # Mode 3: historical_ticks (user's getdatatest.py approach)
    # ------------------------------------------------------------------
    async def _stream_historical_ticks(self, client, asset: str):
        state = self.asset_state[asset]
        st = self.stats.get_asset(asset)

        while not self._stop.is_set():
            try:
                # Fetch ~2x the tick count we need in a period
                # On a 10s period, assume up to 20 ticks
                raw = await client.get_candles(asset, 1, 50)
                new_candles = self._compile_from_ticks(raw, TIMEFRAME_SECONDS)

                if new_candles:
                    with self.state_lock:
                        # Merge: only append candles we don't already have
                        existing_times = {c[0] for c in state.candles}
                        added = 0
                        for c in new_candles:
                            if c[0] not in existing_times:
                                state.candles.append(c)
                                st.record_candle()
                                added += 1

                        # Update forming candle to the newest bucket
                        latest = new_candles[-1]
                        state.forming = {
                            "time": latest[0],
                            "open": latest[1], "high": latest[2],
                            "low": latest[3], "close": latest[4],
                        }
                        state.last_boundary = latest[0]
                        state.last_remote_candle_time = latest[0]

                        closes = [cc[4] for cc in state.candles]
                        closes.append(state.forming["close"])
                        self._apply_indicators(state, closes)

                    if added:
                        st.record_tick(int(time.time()), latest[4])
                        self.stats.record_slot_tick()

                await asyncio.sleep(HISTORICAL_POLL_SECS)
            except Exception as e:
                self.stats.mark_error(f"hist poll {asset}: {e}")
                await asyncio.sleep(2.0)

    # ------------------------------------------------------------------
    # Shared: tick ingestion, indicators
    # ------------------------------------------------------------------
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
                gap_buckets = (bucket - state.last_boundary) // TIMEFRAME_SECONDS - 1
                if gap_buckets > 0:
                    st.gaps += gap_buckets

                f = state.forming
                state.candles.append((f["time"], f["open"], f["high"], f["low"], f["close"]))
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

            closes = [c[4] for c in state.candles]
            closes.append(state.forming["close"])
            self._apply_indicators(state, closes)

        st.record_tick(ts, price)
        self.stats.record_slot_tick()

    def _apply_indicators(self, state: AssetState, closes: List[float]):
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

    def snapshot(self):
        with self.state_lock:
            assets_snap = {a: s.snapshot() for a, s in self.asset_state.items()}
        return {
            "slot_id": self.slot_id,
            "mode": self.mode,
            "assets": assets_snap,
            "stats": self.stats.snapshot(),
        }


class MultiStreamManager:
    def __init__(self, vm_role: str, slots_config: List[Tuple[str, List[str], str]]):
        """
        slots_config: list of (ssid, assets, mode)
        mode: "ticks" | "time_aligned" | "historical_ticks"
        """
        self.vm_role = vm_role
        self.slots: List[Slot] = []
        for i, (ssid, assets, mode) in enumerate(slots_config, start=1):
            self.slots.append(Slot(slot_id=i, ssid=ssid or "", assets=assets, mode=mode))

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
