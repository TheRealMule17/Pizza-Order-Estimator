"""
Simulation engine: order generation and kitchen pipeline.

Runs in its own thread.  All shared state is protected by a single Lock so
the dashboard thread can read a consistent snapshot at any time.
"""

from __future__ import annotations

import random
import threading
import time
import uuid
from datetime import datetime
from pathlib import Path
from typing import Optional

from .analytics import AnalyticsTracker
from .config import cfg
from .logger import OrderLogger
from .raw_emitter import RawEmitter
from .estimators import (
    OVEN_DURATION,
    DynamicEstimator,
    NaiveEstimator,
)
from .models import (
    Driver,
    DriverStatus,
    KitchenState,
    Order,
    OrderStatus,
    OrderType,
    Pizza,
    PizzaStatus,
)


# ──────────────────────────────────────────────────────────────────────────────
# Order generation helpers
# ──────────────────────────────────────────────────────────────────────────────

def _random_item_count() -> int:
    return random.randint(cfg.items_per_order_min, cfg.items_per_order_max)


def _make_pizza(order_id: str, index: int) -> Pizza:
    """
    Build one pizza for an order.

    With probability cfg.custom_order_ratio the pizza is a custom creation
    with a random topping selection; otherwise it is a named preset.
    Make duration is derived entirely from topping count:
        duration = base_prep_time + len(toppings) * time_per_topping
    """
    item_id = f"{order_id}-{index}"

    if random.random() < cfg.custom_order_ratio:
        n = random.randint(cfg.custom_topping_min, cfg.custom_topping_max)
        toppings = random.sample(cfg.toppings, min(n, len(cfg.toppings)))
        pizza_type = "Custom"
    else:
        presets    = cfg.presets
        pizza_type = random.choice(list(presets.keys()))
        toppings   = list(presets[pizza_type])

    make_dur = round(cfg.base_prep_time + len(toppings) * cfg.time_per_topping, 3)
    return Pizza(
        order_id=order_id,
        make_duration=make_dur,
        pizza_type=pizza_type,
        toppings=toppings,
        item_id=item_id,
    )


# ──────────────────────────────────────────────────────────────────────────────
# Order serialisation helpers
# ──────────────────────────────────────────────────────────────────────────────

def _order_display_status(order: Order) -> str:
    if order.status == OrderStatus.QUEUED:
        return "Queued"
    if order.status == OrderStatus.IN_PROGRESS:
        statuses = {p.status for p in order.pizzas}
        if PizzaStatus.MAKING in statuses or PizzaStatus.QUEUED in statuses:
            return "Making"
        return "In Oven"
    if order.status == OrderStatus.READY_FOR_PICKUP:
        return "Ready for Pickup"
    if order.status == OrderStatus.WAITING_FOR_DRIVER:
        return "Waiting for Driver"
    if order.status == OrderStatus.OUT_FOR_DELIVERY:
        return "Out for Delivery"
    if order.status == OrderStatus.COMPLETE:
        return "Delivered"
    return str(order.status)


def _serialize_order(order: Order, now_min: float = 0.0) -> dict:
    items = []
    for p in order.pizzas:
        if p.pizza_type == "Custom":
            display = f"Custom ({', '.join(p.toppings)})"
        else:
            display = p.pizza_type
        items.append({
            "item_id":       p.item_id,
            "name":          display,
            "type":          p.pizza_type,
            "toppings":      p.toppings,
            "topping_count": len(p.toppings),
            "pizza_status":  p.status.name,  # QUEUED, MAKING, WAITING_OVEN, BAKING, DONE
            "oven_start":    p.oven_start,
            "oven_end":      p.oven_end,
        })
    oven_ends = [p.oven_end for p in order.pizzas if p.oven_end is not None]
    return {
        "order_id":         order.order_id,
        "order_type":       order.order_type.value,
        "pizza_count":      order.num_pizzas,
        "status":           _order_display_status(order),
        "naive_estimate":   order.naive_estimate,
        "dynamic_estimate": order.dynamic_estimate,
        "actual_duration":  order.actual_duration,
        "naive_error":      order.naive_error,
        "dynamic_error":    order.dynamic_error,
        "placed_at":        order.placed_at,
        "age_minutes":      now_min - order.placed_at,
        "ready_at":         order.ready_at,
        "oven_end_max":     max(oven_ends) if oven_ends else None,
        "items":            items,
        "total_toppings":   sum(len(p.toppings) for p in order.pizzas),
        "driver_id":        order.driver_id if hasattr(order, "driver_id") else None,
    }


# ──────────────────────────────────────────────────────────────────────────────
# Make-line (pipeline model)
# ──────────────────────────────────────────────────────────────────────────────

class MakeLine:
    """
    One shared make-line staffed by N workers arranged as a pipeline.

    * First pizza on an idle line:  takes full ``make_duration``.
    * Subsequent pizzas while line is flowing: exit one cycle_time later:
          cycle_time = make_duration / num_workers
    * At most ``num_workers`` pizzas in-flight simultaneously.
    """

    def __init__(self, num_workers: int) -> None:
        self.num_workers    = num_workers
        self._active: list[tuple[Pizza, float]] = []
        self._last_exit_time: float = 0.0

    @property
    def free_count(self) -> int:
        return max(0, self.num_workers - len(self._active))

    @property
    def busy_finish_times(self) -> list[float]:
        return [exit_t for (_, exit_t) in self._active]

    @property
    def station_states(self) -> list[dict]:
        states = [{"busy": True, "finish_at": exit_t} for (_, exit_t) in self._active]
        while len(states) < self.num_workers:
            states.append({"busy": False})
        return states

    def assign(self, pizza: Pizza, now: float) -> bool:
        if len(self._active) >= self.num_workers:
            return False
        if now >= self._last_exit_time:
            exit_time = now + pizza.make_duration
        else:
            cycle_time = pizza.make_duration / self.num_workers
            exit_time  = self._last_exit_time + cycle_time
        pizza.status     = PizzaStatus.MAKING
        pizza.make_start = now
        pizza.make_end   = exit_time
        self._last_exit_time = exit_time
        self._active.append((pizza, exit_time))
        return True

    def tick(self, now: float) -> list[Pizza]:
        finished:  list[Pizza] = []
        remaining: list[tuple[Pizza, float]] = []
        for pizza, exit_t in self._active:
            if now >= exit_t:
                pizza.status = PizzaStatus.WAITING_OVEN
                finished.append(pizza)
            else:
                remaining.append((pizza, exit_t))
        self._active = remaining
        return finished


# ──────────────────────────────────────────────────────────────────────────────
# Oven
# ──────────────────────────────────────────────────────────────────────────────

class Oven:
    """Fixed-capacity oven."""

    def __init__(self, capacity: int) -> None:
        self.capacity   = capacity
        self._slots:   list[tuple[Pizza, float]] = []
        self._waiting: list[Pizza] = []

    @property
    def active_count(self) -> int:
        return len(self._slots)

    @property
    def slot_exit_times(self) -> list[float]:
        return [t for (_, t) in self._slots]

    @property
    def waiting_count(self) -> int:
        return len(self._waiting)

    def enqueue(self, pizza: Pizza) -> None:
        pizza.status = PizzaStatus.WAITING_OVEN
        self._waiting.append(pizza)

    def _try_load(self, now: float) -> None:
        while self._waiting and len(self._slots) < self.capacity:
            pizza            = self._waiting.pop(0)
            pizza.status     = PizzaStatus.BAKING
            pizza.oven_start = now
            pizza.oven_end   = now + OVEN_DURATION
            self._slots.append((pizza, pizza.oven_end))

    def tick(self, now: float) -> list[Pizza]:
        done: list[Pizza] = []
        remaining: list[tuple[Pizza, float]] = []
        for pizza, exit_t in self._slots:
            if now >= exit_t:
                pizza.status = PizzaStatus.DONE
                done.append(pizza)
            else:
                remaining.append((pizza, exit_t))
        self._slots = remaining
        self._try_load(now)
        return done


# ──────────────────────────────────────────────────────────────────────────────
# Accuracy tracker
# ──────────────────────────────────────────────────────────────────────────────

class AccuracyTracker:
    def __init__(self) -> None:
        self.naive_errors:   list[float] = []
        self.dynamic_errors: list[float] = []

    def record(self, order: Order) -> None:
        if order.naive_error is not None:
            self.naive_errors.append(order.naive_error)
        if order.dynamic_error is not None:
            self.dynamic_errors.append(order.dynamic_error)

    def _mae(self, errors: list[float]) -> Optional[float]:
        return sum(abs(e) for e in errors) / len(errors) if errors else None

    def _avg(self, errors: list[float]) -> Optional[float]:
        return sum(errors) / len(errors) if errors else None

    @property
    def naive_mae(self) -> Optional[float]:
        return self._mae(self.naive_errors)

    @property
    def dynamic_mae(self) -> Optional[float]:
        return self._mae(self.dynamic_errors)

    @property
    def naive_avg_error(self) -> Optional[float]:
        return self._avg(self.naive_errors)

    @property
    def dynamic_avg_error(self) -> Optional[float]:
        return self._avg(self.dynamic_errors)

    @property
    def naive_wins(self) -> int:
        return sum(1 for ne, de in zip(self.naive_errors, self.dynamic_errors)
                   if abs(ne) < abs(de))

    @property
    def dynamic_wins(self) -> int:
        return sum(1 for ne, de in zip(self.naive_errors, self.dynamic_errors)
                   if abs(de) < abs(ne))

    @property
    def sample_count(self) -> int:
        return len(self.naive_errors)


# ──────────────────────────────────────────────────────────────────────────────
# Simulation
# ──────────────────────────────────────────────────────────────────────────────

class Simulation:
    """
    Central simulation engine.

    All time is in *simulated minutes*.  Wall-clock time is scaled by
    time_scale (default 60: 1 real second = 1 sim-minute).
    """

    def __init__(
        self,
        time_scale:    float | None = None,
        num_workers:   int   | None = None,
        oven_capacity: int   | None = None,
        naive_window:  int   | None = None,
    ) -> None:
        time_scale    = time_scale    if time_scale    is not None else cfg.time_scale
        num_workers   = num_workers   if num_workers   is not None else cfg.workers
        oven_capacity = oven_capacity if oven_capacity is not None else cfg.oven_capacity
        naive_window  = naive_window  if naive_window  is not None else cfg.naive_window

        self.time_scale  = time_scale
        self.make_line   = MakeLine(num_workers)
        self.oven        = Oven(oven_capacity)
        self.naive_est   = NaiveEstimator(naive_window)
        self.dynamic_est = DynamicEstimator()
        self.accuracy    = AccuracyTracker()

        self.lock        = threading.Lock()
        self.orders:     list[Order] = []
        self.queue:      list[Order] = []
        self.make_queue: list[Pizza] = []

        # Driver pool — initialised here and reset in reset()
        self.drivers: list[Driver] = [
            Driver(driver_id=f"D{i+1}") for i in range(cfg.driver_count)
        ]

        # Logging and analytics
        self.logger    = OrderLogger()
        self.analytics = AnalyticsTracker()
        # rush flag at time of placement — popped when order completes
        self._rush_at_placement: dict[str, bool] = {}

        self._rush_active    = False
        self._rush_end_sim   = 0.0
        self._sim_start_real = time.monotonic()
        self.emitter         = RawEmitter(self._sim_start_real, self.time_scale)
        self._running        = False
        self._thread: Optional[threading.Thread] = None
        self._completed_times: list[float] = []
        self._next_order_at  = 0.0

        # Day simulation state
        self._day_mode:             bool  = False
        self._day_complete:         bool  = False
        self._day_traffic_label:    str   = ""
        self._day_clock_display:    str   = ""
        self._current_day_insiders: int   = cfg.workers
        self._current_day_drivers:  int   = cfg.driver_count
        self._drivers_pending_removal: int = 0
        self._bottleneck_recent_until: float = 0.0  # sim-min until which flag is live

    # ── public controls ──────────────────────────────────────────────────────

    def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._thread  = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        if not self._running:
            return
        self._running = False
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=0.3)
        # Flush log and write summary before state is cleared
        self.logger.flush()
        self.logger.export_parquet()
        self._write_summary()
        # Flush raw emitter and kick off the ETL pipeline asynchronously
        self.emitter.flush()
        self._run_pipeline_async()

    def reset(self) -> None:
        self.stop()  # flushes logger + writes summary
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=0.5)
        with self.lock:
            self.orders.clear()
            self.queue.clear()
            self.make_queue.clear()
            self._completed_times.clear()
            self.make_line   = MakeLine(self.make_line.num_workers)
            self.oven        = Oven(self.oven.capacity)
            self.naive_est   = NaiveEstimator(self.naive_est._window)
            self.dynamic_est = DynamicEstimator()
            self.accuracy    = AccuracyTracker()
            self.drivers     = [Driver(driver_id=f"D{i+1}") for i in range(cfg.driver_count)]
            self.logger      = OrderLogger()
            self.analytics   = AnalyticsTracker()
            self.emitter     = RawEmitter(time.monotonic(), self.time_scale)
            self._rush_at_placement.clear()
            self._rush_active    = False
            self._rush_end_sim   = 0.0
            self._sim_start_real = time.monotonic()
            self._next_order_at  = 0.0
            self._thread         = None
            self._day_mode             = False
            self._day_complete         = False
            self._day_traffic_label    = ""
            self._day_clock_display    = ""
            self._current_day_insiders = cfg.workers
            self._current_day_drivers  = cfg.driver_count
            self._drivers_pending_removal = 0
            self._bottleneck_recent_until = 0.0

    def trigger_rush(self) -> None:
        with self.lock:
            now = self._sim_now()
            self._rush_active  = True
            self._rush_end_sim = now + cfg.rush_duration
            self._next_order_at = now

    # ── day mode controls ─────────────────────────────────────────────────────

    def enable_day_mode(self, insiders: int, drivers: int) -> None:
        """Switch the simulation into day mode (called by DaySimulator before start)."""
        with self.lock:
            self._day_mode    = True
            self._day_complete = False
            self._rush_active  = False
            self._current_day_insiders = insiders
            self._current_day_drivers  = drivers
            self.make_line = MakeLine(insiders)
            self.drivers   = [Driver(driver_id=f"D{i+1}") for i in range(drivers)]

    def disable_day_mode(self) -> None:
        """Mark day as complete (called by DaySimulator after drain)."""
        with self.lock:
            self._day_mode    = False
            self._day_complete = True

    def set_worker_count(self, n: int) -> None:
        """Smoothly change make-line worker count, preserving in-progress pizzas."""
        with self.lock:
            self._current_day_insiders = n
            old_active    = self.make_line._active
            old_last_exit = self.make_line._last_exit_time
            self.make_line = MakeLine(n)
            self.make_line._active         = old_active
            self.make_line._last_exit_time = old_last_exit
            self.emitter.on_staffing_change(n, len(self.drivers), self._sim_now() / 60.0)

    def set_driver_count(self, n: int) -> None:
        """Add drivers immediately; reduce by retiring them when they next go idle."""
        with self.lock:
            current = len(self.drivers)
            if n > current:
                for i in range(current, n):
                    self.drivers.append(Driver(driver_id=f"D{i+1}"))
            elif n < current:
                to_remove = current - n
                # Remove any available drivers immediately
                avail = [d for d in self.drivers if d.status == DriverStatus.AVAILABLE]
                for d in avail[:to_remove]:
                    self.drivers.remove(d)
                    to_remove -= 1
                # Rest retire when they return from a delivery
                self._drivers_pending_removal += to_remove
            self._current_day_drivers = n
            self.emitter.on_staffing_change(
                self._current_day_insiders, n, self._sim_now() / 60.0
            )

    def set_traffic_label(self, label: str, clock_display: str) -> None:
        """Update the traffic block label and clock display shown in the UI."""
        with self.lock:
            self._day_traffic_label = label
            self._day_clock_display = clock_display

    # ── time helpers ─────────────────────────────────────────────────────────

    def _sim_now(self) -> float:
        return (time.monotonic() - self._sim_start_real) * self.time_scale

    def sim_now_minutes(self) -> float:
        return self._sim_now() / 60.0

    @property
    def rush_active(self) -> bool:
        with self.lock:
            return self._rush_active

    @property
    def orders_in_queue(self) -> int:
        with self.lock:
            return len(self.queue)

    # ── snapshot for TUI ─────────────────────────────────────────────────────

    def snapshot(self) -> dict:
        with self.lock:
            now_min = self.sim_now_minutes()
            recent  = self.orders[-20:]

            cutoff  = now_min - 5.0
            tp      = len([t for t in self._completed_times if t > cutoff]) / 5.0

            return {
                "now_min":           now_min,
                "rush_active":       self._rush_active,
                "queue_waiting":     sum(1 for o in self.queue if o.status == OrderStatus.QUEUED),
                "queue_in_progress": sum(1 for o in self.queue if o.status == OrderStatus.IN_PROGRESS),
                "queue_delivery":    sum(1 for o in self.queue if o.status == OrderStatus.OUT_FOR_DELIVERY),
                "total_orders":      len(self.orders),
                "completed_orders":  len(self.orders) - len(self.queue),
                "make_line_busy":    self.make_line.num_workers - self.make_line.free_count,
                "make_line_total":   self.make_line.num_workers,
                "make_queue_depth":  len(self.make_queue),
                "oven_active":       self.oven.active_count,
                "oven_capacity":     self.oven.capacity,
                "oven_waiting":      self.oven.waiting_count,
                "throughput":        tp,
                "orders_in_queue":   len(self.queue),
                "recent_orders":     recent,
                "accuracy":          self.accuracy,
            }

    # ── snapshot for web API ─────────────────────────────────────────────────

    def web_snapshot(self) -> dict:
        with self.lock:
            now_min = self.sim_now_minutes()
            cutoff  = now_min - 5.0
            tp      = len([t for t in self._completed_times if t > cutoff]) / 5.0

            active_orders = [_serialize_order(o, now_min) for o in self.queue]

            queue_ids = {o.order_id for o in self.queue}
            recent_completed: list[dict] = []
            for order in reversed(self.orders):
                if order.order_id not in queue_ids:
                    recent_completed.append(_serialize_order(order, now_min))
                    if len(recent_completed) >= 10:
                        break

            latest_comparison = None
            for order in reversed(self.orders):
                if order.status == OrderStatus.COMPLETE and order.actual_duration is not None:
                    latest_comparison = {
                        "order_id":         order.order_id,
                        "order_type":       order.order_type.value,
                        "pizza_count":      order.num_pizzas,
                        "naive_estimate":   order.naive_estimate,
                        "dynamic_estimate": order.dynamic_estimate,
                        "actual_duration":  order.actual_duration,
                        "naive_error":      order.naive_error,
                        "dynamic_error":    order.dynamic_error,
                    }
                    break

            avg_toppings = (cfg.custom_topping_min + cfg.custom_topping_max) / 2
            avg_make     = cfg.base_prep_time + avg_toppings * cfg.time_per_topping
            ks           = self._build_kitchen_state(now_min)
            current_estimates = {
                "naive_carryout":   self.naive_est.estimate(OrderType.CARRYOUT),
                "naive_delivery":   self.naive_est.estimate(OrderType.DELIVERY),
                "dynamic_carryout": self.dynamic_est.estimate(1, [avg_make], OrderType.CARRYOUT, ks),
                "dynamic_delivery": self.dynamic_est.estimate(1, [avg_make], OrderType.DELIVERY, ks),
            }

            # Driver status for the UI
            driver_data = []
            for d in self.drivers:
                driver_data.append({
                    "driver_id":        d.driver_id,
                    "status":           d.status.name.lower(),
                    "current_order_id": d.current_order_id,
                    "available_at":     d.available_at,
                    "dropoff_at":       d.dropoff_at,
                })

            acc = self.accuracy
            return {
                "running":       self._running,
                "rush_active":   self._rush_active,
                "throughput":    tp,
                "now_min":       now_min,
                "day_mode":      self._day_mode,
                "day_complete":  self._day_complete,
                "day_clock":     self._day_clock_display,
                "traffic_label": self._day_traffic_label,
                "day_insiders":  self._current_day_insiders,
                "day_drivers":   self._current_day_drivers,
                "oven_time":  cfg.oven_time,
                "kitchen": {
                    "stations":         self.make_line.station_states,
                    "make_queue_depth": len(self.make_queue),
                    "oven_active":      self.oven.active_count,
                    "oven_capacity":    self.oven.capacity,
                    "oven_waiting":     self.oven.waiting_count,
                },
                "drivers": driver_data,
                "accuracy": {
                    "naive_mae":         acc.naive_mae,
                    "dynamic_mae":       acc.dynamic_mae,
                    "naive_wins":        acc.naive_wins,
                    "dynamic_wins":      acc.dynamic_wins,
                    "naive_avg_error":   acc.naive_avg_error,
                    "dynamic_avg_error": acc.dynamic_avg_error,
                    "sample_count":      acc.sample_count,
                },
                "current_estimates": current_estimates,
                "active_orders":     active_orders,
                "recent_completed":  recent_completed,
                "latest_comparison": latest_comparison,
                "analytics": self.analytics.snapshot(
                    sim_hours_elapsed=self._sim_now() / 3600.0,
                    num_workers=self.make_line.num_workers,
                    num_drivers=len(self.drivers),
                ),
            }

    # ── bottleneck helper ────────────────────────────────────────────────────

    def _check_bottleneck(self, event_type: str, now_min: float, details: str = "") -> None:
        """Record a throttled bottleneck event and keep the recent flag live."""
        self._bottleneck_recent_until = now_min + 5.0
        self.analytics.record_bottleneck_throttled(
            event_type, now_min,
            traffic_label=self._day_traffic_label,
            insiders=self._current_day_insiders,
            drivers=self._current_day_drivers,
            queue_depth=len(self.queue),
            details=details,
        )

    # ── simulation loop ──────────────────────────────────────────────────────

    def _loop(self) -> None:
        while self._running:
            with self.lock:
                now_sec = self._sim_now()
                now_min = now_sec / 60.0

                if self._rush_active and now_sec >= self._rush_end_sim:
                    self._rush_active = False

                # In day mode, order generation is controlled by DaySimulator
                if not self._day_mode and now_sec >= self._next_order_at:
                    self._generate_order(now_min)
                    if self._rush_active:
                        iv = random.uniform(cfg.rush_interval_min, cfg.rush_interval_max)
                    else:
                        iv = random.uniform(cfg.normal_interval_min, cfg.normal_interval_max)
                    self._next_order_at = now_sec + iv

                self._tick_make_line(now_min)
                self._tick_oven(now_min)
                self._tick_drivers(now_min)
                self._fill_stations(now_min)

            time.sleep(0.05)

    def _build_kitchen_state(self, now_min: float) -> KitchenState:
        # Count orders waiting for driver or already out for delivery
        pending_deliveries = sum(
            1 for o in self.queue
            if o.status in (OrderStatus.WAITING_FOR_DRIVER, OrderStatus.OUT_FOR_DELIVERY)
        )
        # All driver available-at times (now for available drivers, future for busy ones)
        driver_available_times = []
        for d in self.drivers:
            if d.status == DriverStatus.AVAILABLE:
                driver_available_times.append(now_min)
            elif d.available_at is not None:
                driver_available_times.append(d.available_at)
            else:
                driver_available_times.append(now_min)

        return KitchenState(
            num_stations=self.make_line.num_workers,
            free_stations=self.make_line.free_count,
            station_free_at=self.make_line.busy_finish_times,
            oven_capacity=self.oven.capacity,
            oven_slots_free=self.oven.capacity - self.oven.active_count,
            oven_slot_free_at=self.oven.slot_exit_times,
            make_queue_durations=[p.make_duration for p in self.make_queue],
            queued_order_pizza_counts=[
                sum(1 for p in o.pizzas if p.status != PizzaStatus.DONE)
                for o in self.queue
            ],
            drivers_available=sum(1 for d in self.drivers if d.status == DriverStatus.AVAILABLE),
            driver_available_times=driver_available_times,
            pending_deliveries=pending_deliveries,
            now=now_min,
        )

    def _generate_order(self, now_min: float) -> None:
        order_id   = str(uuid.uuid4())[:8].upper()
        order_type = (
            OrderType.DELIVERY if random.random() < cfg.delivery_ratio
            else OrderType.CARRYOUT
        )
        pizzas    = [_make_pizza(order_id, i + 1) for i in range(_random_item_count())]
        make_durs = [p.make_duration for p in pizzas]

        order = Order(order_id=order_id, order_type=order_type, pizzas=pizzas, placed_at=now_min)
        order.naive_estimate   = self.naive_est.estimate(order_type)
        order.dynamic_estimate = self.dynamic_est.estimate(
            len(pizzas), make_durs, order_type, self._build_kitchen_state(now_min)
        )

        # Capture placement context before the order joins the queue
        self._rush_at_placement[order.order_id] = self._rush_active
        bottleneck_now = now_min < self._bottleneck_recent_until
        self.logger.capture_placement(
            order,
            queue_depth=len(self.queue),
            active_workers=self.make_line.num_workers - self.make_line.free_count,
            available_drivers=sum(1 for d in self.drivers if d.status == DriverStatus.AVAILABLE),
            rush_active=self._rush_active,
            bottleneck_active=bottleneck_now,
        )

        self.orders.append(order)
        self.queue.append(order)
        self.make_queue.extend(pizzas)
        self.emitter.on_order_placed(order, now_min)

    def _fill_stations(self, now_min: float) -> None:
        while self.make_queue and self.make_line.free_count > 0:
            pizza = self.make_queue.pop(0)
            self.make_line.assign(pizza, now_min)
            self.emitter.on_make_start(pizza, now_min, self.make_line.num_workers)
            for order in self.queue:
                if order.order_id == pizza.order_id:
                    order.status = OrderStatus.IN_PROGRESS
                    break
        # Bottleneck: make queue backed up with no free stations
        if self.make_queue and self.make_line.free_count == 0:
            self._check_bottleneck(
                "make_line_full", now_min,
                f"queued pizzas: {len(self.make_queue)}"
            )

    def _tick_make_line(self, now_min: float) -> None:
        for pizza in self.make_line.tick(now_min):
            self.emitter.on_make_complete(pizza, now_min)
            self.oven.enqueue(pizza)
            self.emitter.on_oven_in(pizza, now_min, self.oven.capacity)

    def _tick_oven(self, now_min: float) -> None:
        """
        When all pizzas in an order exit the oven:
        - Carryout: mark COMPLETE immediately.
        - Delivery: mark WAITING_FOR_DRIVER; a driver is assigned in _tick_drivers.
        """
        for pizza in self.oven.tick(now_min):
            self.emitter.on_oven_out(pizza, now_min, self.oven.capacity)
            for order in self.queue:
                if order.order_id == pizza.order_id:
                    if all(p.status == PizzaStatus.DONE for p in order.pizzas) and order.ready_at is None:
                        order.ready_at = now_min
                        if order.order_type == OrderType.CARRYOUT:
                            order.status       = OrderStatus.READY_FOR_PICKUP
                            order.completed_at = now_min
                        else:
                            order.status = OrderStatus.WAITING_FOR_DRIVER
                    break
        # Bottleneck: pizzas queued for the oven (oven is full)
        if self.oven.waiting_count > 0:
            self._check_bottleneck(
                "oven_full", now_min,
                f"waiting for slot: {self.oven.waiting_count}"
            )

    def _tick_drivers(self, now_min: float) -> None:
        """
        Three sub-tasks each tick:
        1. Delivering → when dropoff_at is reached, mark order COMPLETE.
        2. Returning  → when available_at is reached, driver becomes AVAILABLE.
        3. Dispatch   → assign free drivers to orders WAITING_FOR_DRIVER.
        """
        # 1. Delivering → dropoff
        for driver in self.drivers:
            if driver.status == DriverStatus.DELIVERING and driver.dropoff_at is not None:
                if now_min >= driver.dropoff_at:
                    # Order is considered delivered at the halfway point
                    for order in self.queue:
                        if order.order_id == driver.current_order_id:
                            order.status       = OrderStatus.COMPLETE
                            order.completed_at = now_min
                            self.emitter.on_delivery_complete(
                                order.order_id, driver.driver_id, now_min
                            )
                            break
                    driver.status = DriverStatus.RETURNING

        # 2. Returning → available (with pending-removal support)
        to_retire: list[Driver] = []
        for driver in self.drivers:
            if driver.status == DriverStatus.RETURNING and driver.available_at is not None:
                if now_min >= driver.available_at:
                    if self._drivers_pending_removal > 0:
                        self._drivers_pending_removal -= 1
                        to_retire.append(driver)
                    else:
                        driver.status           = DriverStatus.AVAILABLE
                        driver.current_order_id = None
                        driver.dropoff_at       = None
                        driver.available_at     = None
                        self.emitter.on_driver_returned(driver.driver_id, now_min)
        for d in to_retire:
            self.drivers.remove(d)

        # 3. Dispatch free drivers to waiting orders
        waiting_for_driver = [o for o in self.queue if o.status == OrderStatus.WAITING_FOR_DRIVER]
        if waiting_for_driver:
            avail_drivers = sum(1 for d in self.drivers if d.status == DriverStatus.AVAILABLE)
            if avail_drivers == 0:
                self._check_bottleneck(
                    "no_drivers", now_min,
                    f"orders waiting: {len(waiting_for_driver)}"
                )
            elif len(waiting_for_driver) > avail_drivers + 2:
                self._check_bottleneck(
                    "driver_queue_backup", now_min,
                    f"waiting: {len(waiting_for_driver)}, available: {avail_drivers}"
                )

        for order in self.queue:
            if order.status != OrderStatus.WAITING_FOR_DRIVER:
                continue
            # Find a free driver
            free_driver = next(
                (d for d in self.drivers if d.status == DriverStatus.AVAILABLE), None
            )
            if free_driver is None:
                break  # no drivers available right now

            round_trip = random.uniform(cfg.driver_delivery_min, cfg.driver_delivery_max)
            dropoff    = now_min + round_trip / 2.0  # halfway point = delivered
            returns_at = now_min + round_trip

            free_driver.status           = DriverStatus.DELIVERING
            free_driver.current_order_id = order.order_id
            free_driver.dropoff_at       = dropoff
            free_driver.available_at     = returns_at

            order.status = OrderStatus.OUT_FOR_DELIVERY
            self.logger.record_driver_assignment(
                order.order_id, free_driver.driver_id, now_min
            )
            self.emitter.on_driver_dispatched(order.order_id, free_driver.driver_id, now_min)

        # 4. Remove completed orders from the active queue and record accuracy.
        #    Carryout orders linger as READY_FOR_PICKUP for 2 sim-minutes so
        #    the UI can show them; after that they are swept out as COMPLETE.
        completed_this_tick: list[Order] = []
        for order in self.queue:
            if order.status == OrderStatus.COMPLETE and order.completed_at is not None:
                completed_this_tick.append(order)
            elif (order.status == OrderStatus.READY_FOR_PICKUP
                  and order.completed_at is not None
                  and now_min - order.completed_at >= 2.0):
                order.status = OrderStatus.COMPLETE
                completed_this_tick.append(order)

        seen: set[str] = set()
        for order in completed_this_tick:
            if order.order_id in seen:
                continue
            seen.add(order.order_id)
            if order.actual_duration is not None:
                self.naive_est.record_completion(order.actual_duration)
                self.accuracy.record(order)
                self._completed_times.append(order.completed_at)  # type: ignore[arg-type]
                if order.actual_duration >= 45:
                    self._check_bottleneck(
                        "extreme_wait", now_min,
                        f"order {order.order_id}: {order.actual_duration:.1f} min"
                    )
            rush_flag = self._rush_at_placement.pop(order.order_id, False)
            revenue   = self.logger.record_completion(order)
            self.analytics.update(order, revenue, rush_flag)
            self.queue.remove(order)

    # ── Pipeline runner ──────────────────────────────────────────────────────

    def _run_pipeline_async(self) -> None:
        """Launch Bronze→Silver→Gold ETL in a daemon thread after simulation stop."""
        if not cfg.pipeline.get("emit_raw_data", True):
            return

        def _run() -> None:
            try:
                from .pipeline.bronze_to_silver import run_bronze_to_silver
                from .pipeline.silver_to_gold import run_silver_to_gold
                report = run_bronze_to_silver()
                print(f"[pipeline] {report}")
                run_silver_to_gold()
                print("[pipeline] Gold layer complete — see data/analytics/")
            except Exception as exc:  # pragma: no cover
                print(f"[pipeline] Error: {exc}")

        t = threading.Thread(target=_run, daemon=True, name="etl-pipeline")
        t.start()

    # ── Summary report ───────────────────────────────────────────────────────

    def _write_summary(self) -> None:
        """Write a plain-text run summary to logs/summary_<timestamp>.txt."""
        logs_dir = Path(__file__).parent.parent / "logs"
        logs_dir.mkdir(parents=True, exist_ok=True)

        sim_hours    = self._sim_now() / 3600.0
        num_workers  = self.make_line.num_workers
        num_drivers  = len(self.drivers)
        an           = self.analytics
        snap         = an.snapshot(sim_hours, num_workers, num_drivers)
        total        = an.total_completed

        def _pct(n: int) -> str:
            return f"({n / total * 100:.1f}%)" if total > 0 else ""

        lines = [
            "=== Pizza Order Estimator — Run Summary ===",
            f"Run Duration:           {sim_hours:.2f} simulated hours",
            f"Total Orders Completed: {total}",
            "",
            "--- Wait Times ---",
            f"Average Wait (Carryout): {snap['avg_wait_carryout_min'] or '—'} min",
            f"Average Wait (Delivery): {snap['avg_wait_delivery_min'] or '—'} min",
            (f"Orders Over 20 min:      {snap['orders_over_20_min']} "
             f"{_pct(snap['orders_over_20_min'])}"),
            (f"Orders Over 30 min:      {snap['orders_over_30_min']} "
             f"{_pct(snap['orders_over_30_min'])}"),
            (f"Orders Over 45 min:      {snap['orders_over_45_min']} "
             f"{_pct(snap['orders_over_45_min'])}"),
            (f"Longest Wait:            {snap['longest_wait_min']} min"
             + (f" (Order #{snap['longest_wait_order_id']})"
                if snap['longest_wait_order_id'] else "")),
            "",
            "--- Estimation Accuracy ---",
            f"Dynamic Model MAE:  {snap['dynamic_mae'] or '—'} min",
            f"Naive Model MAE:    {snap['naive_mae'] or '—'} min",
            f"Dynamic Wins: {snap['dynamic_wins']} | Naive Wins: {snap['naive_wins']}",
            "",
            "--- Rush Impact ---",
            f"Orders During Rush:          {snap['orders_placed_during_rush']}",
            f"Avg Wait (Rush Orders):      {snap['avg_wait_during_rush'] or '—'} min",
            f"Avg Wait (Non-Rush Orders):  {snap['avg_wait_outside_rush'] or '—'} min",
            "",
            "--- Financials ---",
            f"Total Revenue:    ${snap['total_revenue']:.2f}",
            f"Total Labor Cost: ${snap['total_labor_cost']:.2f}",
            f"SPLH:             ${snap['splh'] or 0:.2f}",
            f"Labor Cost %:     {snap['labor_cost_percentage'] or 0:.1f}%",
            "",
            "--- Config Snapshot ---",
            (f"Workers: {num_workers} | Drivers: {num_drivers} | "
             f"Oven Capacity: {self.oven.capacity} | "
             f"Time Scale: {self.time_scale}x"),
            "",
            "--- Bottleneck Events ---",
        ] + an.bottleneck_summary_lines()

        stamp    = self.logger.stamp
        out_path = logs_dir / f"summary_{stamp}.txt"
        try:
            with open(out_path, "w") as fh:
                fh.write("\n".join(lines) + "\n")
        except Exception as exc:
            print(f"[sim] Summary write failed: {exc}")
