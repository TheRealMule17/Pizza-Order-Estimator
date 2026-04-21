"""
RawEmitter — translates clean simulation events into four messy raw data streams
that simulate the output of real store systems.

Each system has its own format, timestamp style, and realistic failure modes.
Controlled by config.yaml [pipeline] section.

Sources written:
  data/raw/pos/       — POS JSONL (American timestamps, null fields, duplicates)
  data/raw/kitchen/   — Kitchen Display CSV (Unix epoch, mangled order refs)
  data/raw/oven/      — Oven Sensor log (ISO 8601, abbreviations, corruption)
  data/raw/dispatch/  — Driver Dispatch JSON (ISO 8601+ms, int/string IDs)
  data/raw/staffing/  — Staffing CSV (clean, generated from set_*_count events)
"""

from __future__ import annotations

import csv
import json
import random
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

from .config import cfg


# ── Constants ─────────────────────────────────────────────────────────────────

_NOISE_FACTORS: dict[str, float] = {"none": 0.0, "normal": 1.0, "heavy": 2.5}

_PIZZA_ABBREVS: dict[str, str] = {
    "Pepperoni":   "Pep",
    "Sausage":     "Ssg",
    "Supreme":     "Supr",
    "Veggie":      "Veg",
    "Meat Lovers": "ML",
    "Cheese":      "Chz",
    "Custom":      "Cust",
}

_JUNK = "▒▓░█"


def _junk(n: int = 4) -> str:
    return "".join(random.choice(_JUNK) for _ in range(n))


# ── RawEmitter ────────────────────────────────────────────────────────────────

class RawEmitter:
    """
    Observes simulation events and writes them to raw source directories in
    the format (and with the quirks) of real store systems.

    All on_* methods are called with the simulation lock already held,
    so they must not block.  File I/O is guarded by a separate internal lock
    so the emitter is safe if ever called from multiple threads.
    """

    def __init__(self, sim_start_real: float, time_scale: float) -> None:
        self._sim_start_real = sim_start_real
        self._time_scale     = time_scale
        self._lock           = threading.Lock()

        # Build a simulated store-clock reference date so _wall() returns
        # meaningful datetimes (sim_min=0 → effective store open on today's date).
        _day = cfg.day_schedule
        _store_open = _day.get("store_open", "10:00")
        _traffic    = _day.get("traffic_blocks", [])
        _eff_start  = _traffic[0].get("start", _store_open) if _traffic else _store_open
        _h, _m      = (int(x) for x in _eff_start.split(":"))
        _today      = datetime.now(tz=timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
        self._store_open_dt = _today.replace(hour=_h, minute=_m)

        pl = cfg.pipeline
        self._enabled: bool        = bool(pl.get("emit_raw_data", True))
        noise_str: str             = str(pl.get("noise_level", "normal"))
        self._nf: float            = _NOISE_FACTORS.get(noise_str, 1.0)

        if not self._enabled:
            return

        base = Path(pl.get("raw_data_dir", "data/raw"))
        self._pos_dir      = base / "pos"
        self._kitchen_dir  = base / "kitchen"
        self._oven_dir     = base / "oven"
        self._dispatch_dir = base / "dispatch"
        self._staffing_dir = base / "staffing"

        for d in [self._pos_dir, self._kitchen_dir, self._oven_dir,
                  self._dispatch_dir, self._staffing_dir]:
            d.mkdir(parents=True, exist_ok=True)

        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self._stamp = stamp

        self._pos_fh = open(self._pos_dir / f"pos_{stamp}.jsonl", "w", encoding="utf-8")

        self._kitchen_fh = open(
            self._kitchen_dir / f"kitchen_{stamp}.csv", "w", newline="", encoding="utf-8"
        )
        self._kitchen_csv = csv.writer(self._kitchen_fh)
        self._kitchen_csv.writerow(
            ["timestamp", "order_ref", "item_name", "station_id", "event_type"]
        )
        self._kitchen_fh.flush()

        self._oven_fh = open(self._oven_dir / f"oven_{stamp}.log", "w", encoding="utf-8")

        # Dispatch events buffered — written as JSON array on flush
        self._dispatch_events: list[dict] = []
        self._dispatch_path = self._dispatch_dir / f"dispatch_{stamp}.json"

        # Staffing events buffered — written as CSV on flush
        self._staffing_events: list[dict] = []
        self._staffing_path = self._staffing_dir / f"staffing_{stamp}.csv"

        # Per-item tracking for kitchen quirks
        self._skip_make_complete: set[str] = set()   # item_ids whose completion is dropped
        self._flushed = False

    # ── Noise helper ──────────────────────────────────────────────────────────

    def _noise(self, p: float) -> bool:
        """Return True with probability p * noise_factor."""
        return self._nf > 0 and random.random() < p * self._nf

    # ── Time helpers ──────────────────────────────────────────────────────────

    def _wall(self, sim_min: float) -> datetime:
        # sim_min=0 anchors to the effective store open; produces proper store-clock datetimes.
        return self._store_open_dt + timedelta(minutes=sim_min)

    def _iso(self, dt: datetime, with_z: bool) -> str:
        if with_z:
            return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        return dt.strftime("%Y-%m-%dT%H:%M:%S")

    def _dispatch_ts(self, dt: datetime) -> str:
        ms = dt.microsecond // 1000
        return dt.strftime(f"%Y-%m-%dT%H:%M:%S.{ms:03d}Z")

    # ── Name / ID helpers ─────────────────────────────────────────────────────

    def _mangle_name(self, name: str) -> str:
        """Randomly mangle pizza name casing (POS quirk)."""
        r = random.random()
        if r < 0.12:
            return name.upper()
        if r < 0.22:
            return name.lower()
        if r < 0.30:
            words = name.split()
            return " ".join(w.upper() if i % 2 == 0 else w.lower()
                            for i, w in enumerate(words))
        if r < 0.36:
            return name + " "   # trailing space
        return name              # correct Title Case

    def _kitchen_ref(self, order_id: str) -> str:
        """Return a mangled order reference for the kitchen system."""
        r = random.random()
        if r < 0.35:
            return order_id.lower()
        if r < 0.55:
            return f"ord_{order_id.lower()}"
        return order_id

    def _driver_fmt(self, driver_id: str) -> object:
        """50% chance return driver ID as integer instead of string."""
        num = driver_id[1:]   # strip "D" prefix
        if self._noise(0.50) and num.isdigit():
            return int(num)
        return f"driver_{num}"

    def _pizza_price(self, pizza) -> float:
        if pizza.pizza_type in cfg.pricing_tier_normal:
            return cfg.normal_pizza_price
        if pizza.pizza_type in cfg.pricing_tier_specialty:
            return cfg.specialty_pizza_price
        return cfg.custom_base_price + len(pizza.toppings) * cfg.custom_per_topping_price

    # ── POS events ────────────────────────────────────────────────────────────

    def on_order_placed(self, order, now_min: float) -> None:
        if not self._enabled:
            return

        dt = self._wall(now_min)
        ts = dt.strftime("%m/%d/%Y %I:%M:%S %p")          # American POS style

        order_type = None if self._noise(0.05) else order.order_type.value

        items = []
        for pizza in order.pizzas:
            raw_name = (pizza.pizza_type if pizza.pizza_type != "Custom"
                        else f"Custom ({', '.join(pizza.toppings)})")
            name  = self._mangle_name(raw_name)
            price = self._pizza_price(pizza)
            items.append({"name": name, "qty": 1, "price": f"${price:.2f}"})

        total = sum(self._pizza_price(p) for p in order.pizzas)

        record = {
            "order_id":          f"ORD-{order.order_id}",
            "timestamp":         ts,
            "order_type":        order_type,
            "items":             items,
            "total":             f"${total:.2f}",
            "store_id":          "101",
            # Analytical fields embedded in POS (computed at placement time)
            "_naive_est":        order.naive_estimate,
            "_dynamic_est":      order.dynamic_estimate,
            "_total_prep_weight": round(sum(p.make_duration for p in order.pizzas), 3),
        }

        line = json.dumps(record) + "\n"
        with self._lock:
            self._pos_fh.write(line)
            if self._noise(0.02):          # 2% duplicate
                self._pos_fh.write(line)
            self._pos_fh.flush()

    # ── Kitchen events ────────────────────────────────────────────────────────

    def on_make_start(self, pizza, now_min: float, num_workers: int) -> None:
        if not self._enabled:
            return

        dt      = self._wall(now_min)
        epoch   = int(dt.timestamp())
        ref     = self._kitchen_ref(pizza.order_id)
        name    = pizza.pizza_type if pizza.pizza_type != "Custom" else "Custom Pizza"
        if self._noise(0.12):
            name = name.lower()
        station = f"STN-{random.randint(1, max(1, num_workers))}"

        # Flag 3% of items to drop their make_complete row
        if self._noise(0.03):
            self._skip_make_complete.add(pizza.item_id)

        with self._lock:
            self._kitchen_csv.writerow([epoch, ref, name, station, "make_start"])
            self._kitchen_fh.flush()

    def on_make_complete(self, pizza, now_min: float) -> None:
        if not self._enabled:
            return
        if pizza.item_id in self._skip_make_complete:
            return

        dt    = self._wall(now_min)
        epoch = int(dt.timestamp())
        ref   = self._kitchen_ref(pizza.order_id)
        name  = pizza.pizza_type if pizza.pizza_type != "Custom" else "Custom Pizza"

        with self._lock:
            self._kitchen_csv.writerow([epoch, ref, name, "STN-?", "make_complete"])
            # 0.5% chance: inject phantom row for a non-existent item
            if self._noise(0.005):
                ghost_ref = f"ghost_{random.randint(1000, 9999)}"
                self._kitchen_csv.writerow([epoch, ghost_ref, "Ghost Pizza",
                                            "STN-0", "make_complete"])
            self._kitchen_fh.flush()

    # ── Oven events ───────────────────────────────────────────────────────────

    def _oven_line(self, event: str, pizza, now_min: float, oven_capacity: int) -> None:
        if not self._enabled:
            return

        dt     = self._wall(now_min)
        has_tz = not self._noise(0.40)     # 40% chance: no timezone suffix
        ts     = self._iso(dt, has_tz)

        if self._noise(0.01):              # 1% corrupted line
            with self._lock:
                self._oven_fh.write(f"[{ts}] {event} {_junk()}ERROR{_junk()}\n")
                self._oven_fh.flush()
            return

        slot = random.randint(1, oven_capacity)
        if self._noise(0.01):              # 1% impossible slot
            slot = oven_capacity + random.randint(1, 3)

        name = pizza.pizza_type if pizza.pizza_type != "Custom" else "Custom Pizza"
        if self._noise(0.20):             # 20% abbreviation
            name = _PIZZA_ABBREVS.get(pizza.pizza_type, name)

        with self._lock:
            self._oven_fh.write(
                f'[{ts}] {event} slot={slot} item="{name}" order=ORD-{pizza.order_id}\n'
            )
            self._oven_fh.flush()

    def on_oven_in(self, pizza, now_min: float, oven_capacity: int) -> None:
        self._oven_line("OVEN_IN", pizza, now_min, oven_capacity)

    def on_oven_out(self, pizza, now_min: float, oven_capacity: int) -> None:
        self._oven_line("OVEN_OUT", pizza, now_min, oven_capacity)

    # ── Dispatch events ───────────────────────────────────────────────────────

    def _add_dispatch(self, event: str, order_id: Optional[str],
                      driver_id: str, now_min: float) -> None:
        if not self._enabled:
            return

        dt  = self._wall(now_min)
        ts  = self._dispatch_ts(dt)
        oid = f"ORD-{order_id}" if order_id else None

        if oid and self._noise(0.02):      # 2% leading space in order_id
            oid = " " + oid

        record: dict = {
            "event":     event,
            "order_id":  oid,
            "driver_id": self._driver_fmt(driver_id),
            "timestamp": ts,
        }

        with self._lock:
            # 2% chance: insert before a random previous position (out-of-order)
            if self._noise(0.02) and self._dispatch_events:
                pos = random.randint(0, len(self._dispatch_events) - 1)
                self._dispatch_events.insert(pos, record)
            else:
                self._dispatch_events.append(record)

    def on_driver_dispatched(self, order_id: str, driver_id: str, now_min: float) -> None:
        self._add_dispatch("assigned", order_id, driver_id, now_min)

    def on_delivery_complete(self, order_id: str, driver_id: str, now_min: float) -> None:
        self._add_dispatch("delivered", order_id, driver_id, now_min)

    def on_driver_returned(self, driver_id: str, now_min: float) -> None:
        if self._noise(0.05):              # 5% skip return event
            return
        self._add_dispatch("returned", None, driver_id, now_min)

    # ── Staffing events ───────────────────────────────────────────────────────

    def on_staffing_change(self, insiders: int, drivers: int, now_min: float) -> None:
        if not self._enabled:
            return
        dt = self._wall(now_min)
        with self._lock:
            self._staffing_events.append({
                "wall_timestamp": dt.isoformat(),
                "sim_min":        round(now_min, 3),
                "insiders":       insiders,
                "drivers":        drivers,
            })

    # ── Flush ─────────────────────────────────────────────────────────────────

    def flush(self) -> None:
        if not self._enabled:
            return
        with self._lock:
            if self._flushed:
                return
            self._flushed = True

            for fh in [self._pos_fh, self._kitchen_fh, self._oven_fh]:
                try:
                    fh.close()
                except Exception:
                    pass

            with open(self._dispatch_path, "w", encoding="utf-8") as fh:
                json.dump(self._dispatch_events, fh, indent=2)

            with open(self._staffing_path, "w", newline="", encoding="utf-8") as fh:
                writer = csv.DictWriter(
                    fh,
                    fieldnames=["wall_timestamp", "sim_min", "insiders", "drivers"],
                )
                writer.writeheader()
                writer.writerows(self._staffing_events)
