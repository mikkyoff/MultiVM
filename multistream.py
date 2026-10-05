"""
Multi-SSID streaming manager.

Correct sequencing:
  1. Connect to PocketOption.
  2. Subscribe to raw ticks IMMEDIATELY (start buffering, don't ingest yet).
  3. Wait for the next clock boundary — with a safety margin so we don't
     cut it too close. If the next boundary is < MIN_MARGIN seconds away,
     skip it and wait for the one after.
  4. At the boundary, flip the alignment flag.
  5. The first tick at/after the boundary becomes the open of the new candle.
  6. Concurrently fetch historical closed candles. Because we waited for
     the boundary, the previous in-progress candle is now closed and will
     be included in the history response — no gap.
  7. Prepend the fetched history to our buffer.
  8. Recompute indicators on the full series.
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


TIMEFRAME_SECONDS = 180       # 3-minute candles
HISTORY_CANDLES = 120
MAX_CANDLES = 100

RSI_PERIOD = 14
BB_PERIOD = 20
BB_STD = 2.0
EMA_PERIOD = 6

SLOT_START_GAP_SECS = 5.0
HISTORICAL_POLL_SECS = 10

# Minimum seconds before a boundary that we'll accept.
# If we're too close, we skip the boundary and wait for the next one,
# giving the connection time to stabilize.
MIN_MARGIN_BEFORE_BOUNDARY = 5.0


class AssetState:
    __slots__ = (
        "asset", "candles", "forming", "last_boundary",
        "rsi", "bb_upper", "bb_middle", "bb_lower",
        "bb_bandwidth", "bb_pct_to_upper", "bb_pct_to_lower",
        "ema", "ema_signal",
        "last_remote_candle_time",
        "aligned_started",
        "history_loaded",
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
        self.aligned_started = False
        self.history_loaded = False

    def snapshot(self):
        return {
            "asset": self.asset,
            "aligned_started": self.aligned_started,
            "history_loaded": self.history_loaded,
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

        # ---- Subscribe FIRST, buffer ticks in the background ----
        # Ticks received before the boundary will be discarded, but they
        # keep the socket warm and let us catch the very first tick after
        # the boundary with no setup delay.
        pre_subscribe_tasks = []
        for asset in self.assets:
            pre_subscribe_tasks.append(
                asyncio.create_task(self._early_buffer(client, asset))
            )

        # ---- Compute the target boundary with a safety margin ----
        target_boundary = self._compute_target_boundary()
        now = time.time()
        wait_secs = target_boundary - now
        print(
            f"[slot {self.slot_id}] waiting {wait_secs:.1f}s for boundary at "
            f"{int(target_boundary)} (margin: {MIN_MARGIN_BEFORE_BOUNDARY}s)"
        )
        await asyncio.sleep(max(0.0, wait_secs))

        # ---- Cross the boundary ----
        print(f"[slot {self.slot_id}] boundary crossed at {int(time.time())}")

        with self.state_lock:
            for st in self.asset_state.values():
                st.aligned_started = True
                st.forming = None  # anything buffered pre-boundary is discarded

        # ---- Kick off historical fetch AFTER the boundary ----
        # The candle that was in-progress before this moment is now closed
        # and will be included in the fetched history.
        for asset in self.assets:
            asyncio.create_task(self._post_boundary_history(client, asset))

        # ---- Wait for early-buffer tasks to hand off ----
        await asyncio.gather(*pre_subscribe_tasks, return_exceptions=True)

        # ---- Now switch to the real stream loops ----
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

    def _compute_target_boundary(self) -> float:
        """
        Return the next boundary timestamp we should align to.
        If the next boundary is less than MIN_MARGIN_BEFORE_BOUNDARY seconds
        away, return the one after it.
        """
        now = time.time()
        next_boundary = (int(now) // TIMEFRAME_SECONDS + 1) * TIMEFRAME_SECONDS
        if next_boundary - now < MIN_MARGIN_BEFORE_BOUNDARY:
            next_boundary += TIMEFRAME_SECONDS
        return float(next_boundary)

    async def _early_buffer(self, client, asset: str):
        """
        Subscribe to ticks before the boundary. Buffer ticks in memory.
        Discard everything buffered when the boundary crosses. This task
        exits after the boundary is crossed, handing off to _stream_ticks.
        """
        state = self.asset_state[asset]
        st = self.stats.get_asset(asset)

        try:
            stream = await client.subscribe_symbol(asset)
        except Exception as e:
            self.stats.mark_error(f"subscribe {asset}: {e}")
            return

        # Keep ticks in a rolling buffer until aligned_started is True
        while not self._stop.is_set():
            with self.state_lock:
                if state.aligned_started:
                    # Boundary crossed. Ingest any buffered ticks that arrived
                    # just after the boundary, then exit — _stream_ticks takes over.
                    break

            try:
                tick = await asyncio.wait_for(stream.__anext__(), timeout=0.5)
            except asyncio.TimeoutError:
                continue
            except StopAsyncIteration:
                return
            except Exception:
                await asyncio.sleep(0.1)
                continue

            # Discard — we don't want pre-boundary ticks
            # (their timestamp is in the old bucket)

        # Hand off: continue reading from the same stream but now ingesting.
        # Note: the stream is owned by this task; we pass it to _stream_ticks
        # via a shared slot on the state object.
        await self._stream_ticks_from_stream(client, asset, stream)

    async def _stream_ticks_from_stream(self, client, asset: str, stream):
        """
        Once the boundary has been crossed, ingest ticks into the forming
        candle. This replaces _stream_ticks for the initial handoff.
        """
        state = self.asset_state[asset]
        st = self.stats.get_asset(asset)
        last_ts = 0

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
                if ts < last_ts:
                    continue
                last_ts = ts

                with self.state_lock:
                    if not state.aligned_started:
                        continue

                self._ingest_tick(state, st, price, ts)
            except Exception as e:
                self.stats.mark_error(f"ingest {asset}: {e}")

    async def _post_boundary_history(self, client, asset: str):
        """
        Fetch historical closed candles. Run AFTER the boundary so the
        candle that just closed is included.
        """
        state = self.asset_state[asset]
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
            print(f"[slot {self.slot_id}] history fetch failed for {asset}: {e}")
            return

        hist_candles = [
            (int(c["time"]), float(c["open"]), float(c["high"]),
             float(c["low"]), float(c["close"]))
            for c in closed[-HISTORY_CANDLES:]
        ]

        with self.state_lock:
            # Prepend history: build a fresh deque with history + current buffer
            current = list(state.candles)
            # Deduplicate: anything in history that's older than the oldest
            # current candle
            current_min = current[0][0] if current else None
            if current_min is not None:
                hist_candles = [c for c in hist_candles if c[0] < current_min]

            combined = hist_candles + current
            # Trim to MAX_CANDLES (keep the newest)
            combined = combined[-MAX_CANDLES:]
            state.candles = deque(combined, maxlen=MAX_CANDLES)
            state.history_loaded = True

            # Recompute indicators with the full series
            closes = [c[4] for c in state.candles]
            if state.forming:
                closes.append(state.forming["close"])
            if closes:
                self._apply_indicators(state, closes)

        print(f"[slot {self.slot_id}] history loaded for {asset}: "
              f"{len(hist_candles)} hist + buffer = {len(state.candles)} total")

    # ------------------------------------------------------------------
    # Mode: raw tick subscription (used after the initial handoff)
    # ------------------------------------------------------------------
    async def _stream_ticks(self, client, asset: str):
        # This path is only reached if _early_buffer failed to subscribe.
        # Normal flow: _early_buffer subscribes, then hands off.
        state = self.asset_state[asset]
        st = self.stats.get_asset(asset)

        try:
            stream = await client.subscribe_symbol(asset)
        except Exception as e:
            self.stats.mark_error(f"subscribe {asset}: {e}")
            return

        last_ts = 0
        while not self._stop.is_set():
            try:
                tick = await stream.__anext__()
            except StopAsyncIteration:
                return
            except Exception as e:
                self.stats.mark_error(f"tick error {asset}: {e}")
                await asyncio.sleep(1.0)
                continue

            try:
                price = float(tick.get("close") or tick.get("price") or 0)
                ts = int(tick.get("timestamp") or tick.get("time") or 0)
                if price <= 0 or ts <= 0 or ts < last_ts:
                    continue
                last_ts = ts
                with self.state_lock:
                    if not state.aligned_started:
                        continue
                self._ingest_tick(state, st, price, ts)
            except Exception as e:
                self.stats.mark_error(f"ingest {asset}: {e}")

    # ------------------------------------------------------------------
    # Mode: time_aligned
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
            return

        while not self._stop.is_set():
            try:
                candle = await stream.__anext__()
            except StopAsyncIteration:
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
                    if not state.aligned_started:
                        continue
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
    # Mode: historical_ticks
    # ------------------------------------------------------------------
    async def _stream_historical_ticks(self, client, asset: str):
        state = self.asset_state[asset]
        st = self.stats.get_asset(asset)

        while not self._stop.is_set():
            try:
                raw = await client.get_candles(asset, 1, 50)
                new_candles = self._compile_from_ticks(raw, TIMEFRAME_SECONDS)

                if new_candles:
                    with self.state_lock:
                        if not state.aligned_started:
                            await asyncio.sleep(HISTORICAL_POLL_SECS)
                            continue
                        existing = {c[0] for c in state.candles}
                        added = 0
                        for c in new_candles:
                            if c[0] not in existing:
                                state.candles.append(c)
                                st.record_candle()
                                added += 1
                        latest = new_candles[-1]
                        state.forming = {
                            "time": latest[0], "open": latest[1],
                            "high": latest[2], "low": latest[3], "close": latest[4],
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
    # Helpers
    # ------------------------------------------------------------------
    def _compile_from_ticks(self, raw_ticks, period: int):
        if not raw_ticks:
            return []
        parsed = []
        for t in raw_ticks:
            if isinstance(t, dict):
                ts = int(t.get("time") or t.get("timestamp") or 0)
                price = float(t.get("price") or t.get("close") or 0)
            elif isinstance(t, (list, tuple)) and len(t) >= 2:
                ts = int(t[0]); price = float(t[1])
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
