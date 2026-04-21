"""
Real-time running analytics tracker.

Updated on every order completion (called under the simulation lock).
Exposes a snapshot() method that returns a JSON-serialisable dict for
the /api/state endpoint.
"""

from __future__ import annotations

from typing import Optional

from .config import cfg
from .models import Order, OrderType


class AnalyticsTracker:
    """All state is mutated inside the simulation lock — no internal locking needed."""

    def __init__(self) -> None:
        self._reset_state()

    def _reset_state(self) -> None:
        # Wait-time extremes
        self.orders_over_45: int  = 0
        self.orders_over_30: int  = 0
        self.orders_over_20: int  = 0
        self.longest_wait:   float = 0.0
        self.longest_wait_order_id: Optional[str] = None

        # Running sums for per-type averages
        self._carryout_total: float = 0.0
        self._carryout_count: int   = 0
        self._delivery_total: float = 0.0
        self._delivery_count: int   = 0

        # Revenue
        self.total_revenue: float = 0.0

        # Signed estimation errors (same order as completed orders)
        self._naive_errors:   list[float] = []
        self._dynamic_errors: list[float] = []

        # Rush impact
        self.orders_during_rush:  int   = 0
        self._rush_wait_total:    float = 0.0
        self._normal_wait_total:  float = 0.0
        self._normal_wait_count:  int   = 0

        # Bottleneck tracking
        self.bottleneck_events:       list[dict]        = []
        self._bottleneck_counts:      dict[str, int]    = {}
        self._last_bottleneck_min:    dict[str, float]  = {}

        # Day simulation — set by DaySimulator before start
        self.day_start_min: Optional[float] = None   # minutes-from-midnight at sim open

    def reset(self) -> None:
        self._reset_state()

    # ── Update (called on each completion) ───────────────────────────────────

    def update(self, order: Order, revenue: float, rush_at_placement: bool) -> None:
        """Record a completed order's metrics. Called under the simulation lock."""
        dur = order.actual_duration
        if dur is None:
            return

        # Wait-time thresholds
        if dur >= 45: self.orders_over_45 += 1
        if dur >= 30: self.orders_over_30 += 1
        if dur >= 20: self.orders_over_20 += 1

        if dur > self.longest_wait:
            self.longest_wait          = dur
            self.longest_wait_order_id = order.order_id

        # Per-type averages
        if order.order_type == OrderType.CARRYOUT:
            self._carryout_total += dur
            self._carryout_count += 1
        else:
            self._delivery_total += dur
            self._delivery_count += 1

        # Revenue
        self.total_revenue += revenue

        # Estimation accuracy
        if order.naive_error is not None:
            self._naive_errors.append(order.naive_error)
        if order.dynamic_error is not None:
            self._dynamic_errors.append(order.dynamic_error)

        # Rush impact
        if rush_at_placement:
            self.orders_during_rush += 1
            self._rush_wait_total   += dur
        else:
            self._normal_wait_count += 1
            self._normal_wait_total += dur

    # ── Computed metrics ──────────────────────────────────────────────────────

    @property
    def total_completed(self) -> int:
        return self._carryout_count + self._delivery_count

    @property
    def avg_wait_carryout(self) -> Optional[float]:
        return self._carryout_total / self._carryout_count if self._carryout_count else None

    @property
    def avg_wait_delivery(self) -> Optional[float]:
        return self._delivery_total / self._delivery_count if self._delivery_count else None

    @property
    def avg_wait_during_rush(self) -> Optional[float]:
        return (self._rush_wait_total / self.orders_during_rush
                if self.orders_during_rush else None)

    @property
    def avg_wait_outside_rush(self) -> Optional[float]:
        return (self._normal_wait_total / self._normal_wait_count
                if self._normal_wait_count else None)

    def _mae(self, errors: list[float]) -> Optional[float]:
        return sum(abs(e) for e in errors) / len(errors) if errors else None

    def _avg(self, errors: list[float]) -> Optional[float]:
        return sum(errors) / len(errors) if errors else None

    # ── Bottleneck tracking ───────────────────────────────────────────────────

    def _format_time(self, sim_min: float) -> str:
        """Format a simulated-minutes value as a 12-hour clock string."""
        if self.day_start_min is not None:
            actual_min = self.day_start_min + sim_min
        else:
            actual_min = sim_min
        h = int(actual_min // 60) % 24
        m = int(actual_min % 60)
        suffix = "AM" if h < 12 else "PM"
        h12 = h % 12 or 12
        return f"{h12}:{m:02d} {suffix}"

    def record_bottleneck(
        self,
        event_type:    str,
        now_min:       float,
        *,
        traffic_label: str = "",
        insiders:      int = 0,
        drivers:       int = 0,
        queue_depth:   int = 0,
        details:       str = "",
    ) -> None:
        """Record a bottleneck event unconditionally."""
        self.bottleneck_events.append({
            "event_type":          event_type,
            "simulated_time":      self._format_time(now_min),
            "sim_time_min":        round(now_min, 2),
            "traffic_block_label": traffic_label,
            "current_insiders":    insiders,
            "current_drivers":     drivers,
            "queue_depth":         queue_depth,
            "details":             details,
        })
        self._bottleneck_counts[event_type] = self._bottleneck_counts.get(event_type, 0) + 1

    def record_bottleneck_throttled(
        self,
        event_type:   str,
        now_min:      float,
        cooldown_min: float = 1.0,
        **kwargs,
    ) -> None:
        """Record a bottleneck event at most once per cooldown_min sim-minutes."""
        if now_min - self._last_bottleneck_min.get(event_type, -999.0) < cooldown_min:
            return
        self._last_bottleneck_min[event_type] = now_min
        self.record_bottleneck(event_type, now_min, **kwargs)

    def bottleneck_summary_lines(self) -> list[str]:
        """Return human-readable summary lines for the run report."""
        if not self._bottleneck_counts:
            return ["  (none detected)"]
        return [
            f"  {k}: {v} event(s)"
            for k, v in sorted(self._bottleneck_counts.items(), key=lambda x: -x[1])
        ]

    def _labor_metrics(
        self, sim_hours_elapsed: float, num_workers: int, num_drivers: int
    ) -> dict:
        """
        Compute labor cost and SPLH.
        All workers and all drivers are on the clock for the full sim duration.
        """
        hourly_cost = (num_workers * cfg.insider_hourly_wage
                       + num_drivers * cfg.driver_hourly_wage)
        total_labor_cost = hourly_cost * sim_hours_elapsed
        total_labor_hours = (num_workers + num_drivers) * sim_hours_elapsed

        splh       = (self.total_revenue / total_labor_hours
                      if total_labor_hours > 0 else None)
        labor_pct  = (total_labor_cost / self.total_revenue * 100
                      if self.total_revenue > 0 else None)
        return {
            "total_labor_cost":      round(total_labor_cost, 2),
            "splh":                  round(splh, 2) if splh is not None else None,
            "labor_cost_percentage": round(labor_pct, 1) if labor_pct is not None else None,
        }

    def snapshot(
        self, sim_hours_elapsed: float, num_workers: int, num_drivers: int
    ) -> dict:
        """Return all analytics as a JSON-serialisable dict."""
        ne = self._naive_errors
        de = self._dynamic_errors
        n_wins = sum(1 for a, b in zip(ne, de) if abs(a) < abs(b))
        d_wins = sum(1 for a, b in zip(ne, de) if abs(b) < abs(a))
        lab    = self._labor_metrics(sim_hours_elapsed, num_workers, num_drivers)

        def _r(v: Optional[float], d: int = 1) -> Optional[float]:
            return round(v, d) if v is not None else None

        return {
            # Wait-time extremes
            "orders_over_45_min":       self.orders_over_45,
            "orders_over_30_min":       self.orders_over_30,
            "orders_over_20_min":       self.orders_over_20,
            "longest_wait_min":         _r(self.longest_wait),
            "longest_wait_order_id":    self.longest_wait_order_id,
            "avg_wait_carryout_min":    _r(self.avg_wait_carryout),
            "avg_wait_delivery_min":    _r(self.avg_wait_delivery),
            # Financials
            "total_revenue":            round(self.total_revenue, 2),
            "total_labor_cost":         lab["total_labor_cost"],
            "splh":                     lab["splh"],
            "labor_cost_percentage":    lab["labor_cost_percentage"],
            # Estimation accuracy
            "naive_mae":                _r(self._mae(ne), 2),
            "dynamic_mae":              _r(self._mae(de), 2),
            "naive_wins":               n_wins,
            "dynamic_wins":             d_wins,
            "naive_avg_error":          _r(self._avg(ne), 2),
            "dynamic_avg_error":        _r(self._avg(de), 2),
            # Rush impact
            "orders_placed_during_rush": self.orders_during_rush,
            "avg_wait_during_rush":      _r(self.avg_wait_during_rush),
            "avg_wait_outside_rush":     _r(self.avg_wait_outside_rush),
            # Bottleneck tracking
            "bottleneck_counts":         dict(self._bottleneck_counts),
            "bottleneck_events":         self.bottleneck_events[-20:],
        }
