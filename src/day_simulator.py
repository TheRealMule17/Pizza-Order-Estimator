"""
Full-day simulation scheduler.

Runs a simulated store day from open to close, pacing order arrivals and
adjusting staffing according to traffic_blocks and staffing_blocks in
config.yaml.  The existing manual mode (Start / Stop / Rush) is unaffected.

Cross-midnight schedules (e.g. open 10:00, close 01:00) are supported: any
time value that falls before store_open is treated as next-day (+1440 min).
"""

from __future__ import annotations

import random
import threading
import time
from typing import Optional

from .config import cfg
from .simulation import Simulation


# ── Time helpers ──────────────────────────────────────────────────────────────

def _parse_hhmm(s: str) -> int:
    """Return minutes-from-midnight for a 'HH:MM' string."""
    h, m = s.split(":")
    return int(h) * 60 + int(m)


def _normalize(t: int, open_min: int) -> int:
    """Push times that fall before open into the next calendar day."""
    return t if t >= open_min else t + 1440


def _fmt_clock(day_min: float) -> str:
    """Format minutes-from-midnight (may exceed 1440) as a 12-hour clock."""
    h = int(day_min // 60) % 24
    m = int(day_min % 60)
    suffix = "AM" if h < 12 else "PM"
    h12 = h % 12 or 12
    return f"{h12}:{m:02d} {suffix}"


# ── DaySimulator ──────────────────────────────────────────────────────────────

class DaySimulator:
    """
    Controls order pacing and staffing for a full simulated store day.

    Runs in a daemon thread.  The underlying Simulation handles all kitchen
    logic; DaySimulator only controls *when* orders arrive and *how many*
    insiders/drivers are on shift.

    Interval values in traffic_blocks are in **simulated seconds** (same unit
    as normal_interval_min/max in config.yaml).
    """

    def __init__(self, sim: Simulation) -> None:
        self._sim       = sim
        self._thread:   Optional[threading.Thread] = None
        self._stop_flag = threading.Event()

    def start(self) -> None:
        """Begin a full-day run in a background thread."""
        if self._thread and self._thread.is_alive():
            return
        self._stop_flag.clear()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        """Cancel the running day (best-effort; may take up to one loop tick)."""
        self._stop_flag.set()

    def is_running(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    # ── Internal ──────────────────────────────────────────────────────────────

    def _run(self) -> None:
        schedule = cfg.day_schedule
        if not schedule:
            return

        open_min  = _parse_hhmm(schedule.get("store_open",  "10:00"))
        close_min = _parse_hhmm(schedule.get("store_close", "22:00"))
        traffic   = schedule.get("traffic_blocks",  [])
        staffing  = schedule.get("staffing_blocks", [])

        # Support cross-midnight close times (e.g. open=10:00, close=01:00 → 1500)
        close_min = _normalize(close_min, open_min)

        if not traffic:
            return

        # Skip the pre-open gap: find the first traffic block's start time
        skip_min = 0.0
        if traffic:
            first_start = min(
                _normalize(_parse_hhmm(b["start"]), open_min) for b in traffic
            )
            skip_min = max(0.0, float(first_start - open_min))

        # Determine opening staffing at the effective start (first block, not store_open)
        effective_open = open_min + skip_min
        opening_staff = self._get_staffing_block(staffing, effective_open, open_min)
        insiders = opening_staff["insiders"] if opening_staff else cfg.workers
        drivers  = opening_staff["drivers"]  if opening_staff else cfg.driver_count

        # Reset simulation, enter day mode, start the engine
        self._sim.reset()
        self._sim.enable_day_mode(insiders, drivers)

        # Tell analytics the effective open time so bottleneck events show correct clock times
        self._sim.analytics.day_start_min = effective_open

        self._sim.start()

        # -- Main pacing loop --------------------------------------------------
        current_block:       Optional[dict] = None
        current_staff_block: Optional[dict] = None
        next_order_real = time.monotonic()  # fire first order immediately

        while not self._stop_flag.is_set():
            now_real    = time.monotonic()
            sim_elapsed = self._sim.sim_now_minutes() + skip_min  # offset past pre-open
            day_min     = open_min + sim_elapsed                   # absolute time of day (min)

            # Store closed?
            if day_min >= close_min:
                break

            # Update traffic block
            block = self._get_traffic_block(traffic, day_min, open_min)
            if block is not current_block:
                current_block = block

            # Update staffing block
            staff = self._get_staffing_block(staffing, day_min, open_min)
            if staff is not None and staff is not current_staff_block:
                current_staff_block = staff
                self._sim.set_worker_count(staff["insiders"])
                self._sim.set_driver_count(staff["drivers"])

            # Refresh clock display every tick
            label = current_block.get("label", "") if current_block else "Pre-Open"
            self._sim.set_traffic_label(label, _fmt_clock(day_min))

            # Generate an order if it's time and there's an active traffic block
            if now_real >= next_order_real and current_block:
                with self._sim.lock:
                    self._sim._generate_order(self._sim.sim_now_minutes())

                iv_sim_sec = random.uniform(
                    current_block["interval_min"],
                    current_block["interval_max"],
                )
                # Convert sim-seconds → real-seconds
                iv_real = iv_sim_sec / self._sim.time_scale
                next_order_real = now_real + iv_real

            time.sleep(0.05)

        # -- Drain phase: wait for all active orders to complete ---------------
        if not self._stop_flag.is_set():
            self._sim.set_traffic_label("Closing — draining orders", _fmt_clock(close_min))
            drain_start   = time.monotonic()
            max_drain_sec = 180.0  # give up after 3 real minutes

            while (not self._stop_flag.is_set()
                   and time.monotonic() - drain_start < max_drain_sec):
                with self._sim.lock:
                    remaining = len(self._sim.queue)
                if remaining == 0:
                    break
                time.sleep(0.2)

        # -- Wrap up -----------------------------------------------------------
        self._sim.disable_day_mode()
        self._sim.stop()

    # ── Block lookup helpers ──────────────────────────────────────────────────

    def _get_traffic_block(
        self, blocks: list, day_min: float, open_min: int
    ) -> Optional[dict]:
        for b in blocks:
            s = _normalize(_parse_hhmm(b["start"]), open_min)
            e = _normalize(_parse_hhmm(b["end"]),   open_min)
            if s <= day_min < e:
                return b
        return None

    def _get_staffing_block(
        self, blocks: list, day_min: float, open_min: int
    ) -> Optional[dict]:
        for b in blocks:
            s = _normalize(_parse_hhmm(b["start"]), open_min)
            e = _normalize(_parse_hhmm(b["end"]),   open_min)
            if s <= day_min < e:
                return b
        return None
