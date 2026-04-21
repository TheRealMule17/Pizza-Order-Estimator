"""
Order lifecycle logger.

Writes one CSV row per completed order to a timestamped file under logs/.
Buffers writes and flushes every N orders or every T real seconds to avoid
I/O bottleneck during heavy traffic.  On stop/reset the caller should call
flush() followed by export_parquet() to finalise the run.
"""

from __future__ import annotations

import csv
import json
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

from .config import cfg
from .models import Order, OrderType

# ── Constants ──────────────────────────────────────────────────────────────────

_LOGS_DIR       = Path(__file__).parent.parent / "logs"
_FLUSH_EVERY_N  = 10      # flush after this many buffered rows
_FLUSH_EVERY_S  = 30.0    # flush after this many real seconds

_COLUMNS = [
    "order_id",
    "order_type",
    "num_items",
    "total_prep_weight",
    "item_details",
    "placed_at",
    "completed_at",
    "actual_time_min",
    "naive_estimate_min",
    "dynamic_estimate_min",
    "naive_error_min",
    "dynamic_error_min",
    "queue_depth_at_placement",
    "active_workers_at_placement",
    "available_drivers_at_placement",
    "rush_active_at_placement",
    "bottleneck_at_placement",
    "driver_wait_min",
    "driver_id",
    "order_revenue",
]


# ── Pricing helper ─────────────────────────────────────────────────────────────

def _pizza_price(pizza_type: str, topping_count: int) -> float:
    """Return the price for a single pizza based on its type."""
    if pizza_type in cfg.pricing_tier_normal:
        return cfg.normal_pizza_price
    if pizza_type in cfg.pricing_tier_specialty:
        return cfg.specialty_pizza_price
    # Custom (or any future type not in the tiers)
    return cfg.custom_base_price + cfg.custom_per_topping_price * topping_count


# ── Logger ─────────────────────────────────────────────────────────────────────

class OrderLogger:
    """
    Buffers completed-order records and appends them to a timestamped CSV.
    Thread-safe — all public methods acquire self._lock.
    """

    def __init__(self) -> None:
        _LOGS_DIR.mkdir(parents=True, exist_ok=True)
        self.stamp     = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        self._csv_path = _LOGS_DIR / f"order_log_{self.stamp}.csv"

        self._lock             = threading.Lock()
        self._buffer:          list[dict] = []
        self._rows_since_flush = 0
        self._last_flush_t     = time.monotonic()

        # Placement context: order_id → dict captured when order was generated
        self._placement:     dict[str, dict] = {}
        # Driver assignment:  order_id → {driver_id, assigned_at (sim-min)}
        self._driver_assign: dict[str, dict] = {}

        # Write header immediately so the file exists even on an empty run
        with open(self._csv_path, "w", newline="") as fh:
            csv.DictWriter(fh, fieldnames=_COLUMNS).writeheader()

    # ── Hooks called by simulation ────────────────────────────────────────────

    def capture_placement(
        self,
        order: Order,
        queue_depth: int,
        active_workers: int,
        available_drivers: int,
        rush_active: bool,
        bottleneck_active: bool = False,
    ) -> None:
        """Record kitchen state at the moment an order is placed."""
        with self._lock:
            self._placement[order.order_id] = {
                "queue_depth":        queue_depth,
                "active_workers":     active_workers,
                "available_drivers":  available_drivers,
                "rush_active":        rush_active,
                "bottleneck_active":  bottleneck_active,
            }

    def record_driver_assignment(
        self, order_id: str, driver_id: str, now_min: float
    ) -> None:
        """Record which driver picked up a delivery order and when."""
        with self._lock:
            self._driver_assign[order_id] = {
                "driver_id":   driver_id,
                "assigned_at": now_min,
            }

    def record_completion(self, order: Order) -> float:
        """
        Build a log row for a completed order, add it to the write buffer,
        and return the computed order revenue so the caller can pass it to
        the analytics tracker without duplicating the pricing logic.
        """
        ctx = {}
        drv = {}
        with self._lock:
            ctx = self._placement.pop(order.order_id, {})
            drv = self._driver_assign.pop(order.order_id, {})

        # Build item breakdown and compute revenue
        items       = []
        total_prep  = 0.0
        revenue     = 0.0
        for p in order.pizzas:
            prep  = round(p.make_duration, 3)
            price = _pizza_price(p.pizza_type, len(p.toppings))
            total_prep += prep
            revenue    += price
            name = (p.pizza_type if p.pizza_type != "Custom"
                    else f"Custom ({len(p.toppings)} toppings)")
            items.append({"name": name, "prep_weight": prep, "price": round(price, 2)})

        # driver_wait_min: gap between oven exit (ready_at) and driver pickup
        driver_wait = 0.0
        if order.order_type == OrderType.DELIVERY and order.ready_at is not None:
            assigned_at = drv.get("assigned_at")
            if assigned_at is not None:
                driver_wait = round(max(0.0, assigned_at - order.ready_at), 3)

        row = {
            "order_id":                       order.order_id,
            "order_type":                     order.order_type.value,
            "num_items":                      order.num_pizzas,
            "total_prep_weight":              round(total_prep, 3),
            "item_details":                   json.dumps(items),
            "placed_at":                      _fmt(order.placed_at),
            "completed_at":                   _fmt(order.completed_at),
            "actual_time_min":                _fmt(order.actual_duration),
            "naive_estimate_min":             _fmt(order.naive_estimate),
            "dynamic_estimate_min":           _fmt(order.dynamic_estimate),
            "naive_error_min":                _fmt(order.naive_error),
            "dynamic_error_min":              _fmt(order.dynamic_error),
            "queue_depth_at_placement":       ctx.get("queue_depth"),
            "active_workers_at_placement":    ctx.get("active_workers"),
            "available_drivers_at_placement": ctx.get("available_drivers"),
            "rush_active_at_placement":       ctx.get("rush_active"),
            "bottleneck_at_placement":        ctx.get("bottleneck_active"),
            "driver_wait_min":                driver_wait,
            "driver_id":                      drv.get("driver_id"),
            "order_revenue":                  round(revenue, 2),
        }

        with self._lock:
            self._buffer.append(row)
            self._rows_since_flush += 1
            elapsed = time.monotonic() - self._last_flush_t
            if self._rows_since_flush >= _FLUSH_EVERY_N or elapsed >= _FLUSH_EVERY_S:
                self._flush_locked()

        return revenue

    # ── I/O ──────────────────────────────────────────────────────────────────

    def _flush_locked(self) -> None:
        """Write buffered rows to disk. Must be called while holding self._lock."""
        rows = self._buffer[:]
        self._buffer.clear()
        self._rows_since_flush = 0
        self._last_flush_t     = time.monotonic()
        # Release lock before I/O to avoid blocking simulation
        self._lock.release()
        try:
            if rows:
                with open(self._csv_path, "a", newline="") as fh:
                    csv.DictWriter(fh, fieldnames=_COLUMNS).writerows(rows)
        finally:
            self._lock.acquire()

    def flush(self) -> None:
        """Public flush — call on stop/reset to ensure no rows are lost."""
        with self._lock:
            self._flush_locked()

    @property
    def csv_path(self) -> Path:
        return self._csv_path

    def export_parquet(self) -> Optional[Path]:
        """Convert the CSV log to Parquet. Returns path on success, None on failure."""
        try:
            import pandas as pd  # type: ignore
            df = pd.read_csv(self._csv_path)
            parquet_path = self._csv_path.with_suffix(".parquet")
            df.to_parquet(parquet_path, index=False)
            return parquet_path
        except Exception as exc:
            print(f"[logger] Parquet export skipped: {exc}")
            return None


# ── Stand-alone utility ───────────────────────────────────────────────────────

def export_to_parquet(csv_path: str) -> Optional[str]:
    """Convert any existing CSV order log to Parquet format."""
    try:
        import pandas as pd  # type: ignore
        src = Path(csv_path)
        out = src.with_suffix(".parquet")
        pd.read_csv(src).to_parquet(out, index=False)
        return str(out)
    except Exception as exc:
        print(f"[logger] export_to_parquet failed: {exc}")
        return None


# ── Internal helper ───────────────────────────────────────────────────────────

def _fmt(val: Optional[float], decimals: int = 3) -> Optional[float]:
    """Round a float for CSV output; pass through None."""
    if val is None:
        return None
    return round(val, decimals)
