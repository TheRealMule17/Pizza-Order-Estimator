"""
Estimation models for order completion time.

Model A — NaiveEstimator:
    Rolling average of the last N completed orders.  Ignores queue depth,
    order size, and kitchen load entirely.

Model B — DynamicEstimator:
    Projects the actual pipeline by simulating when each pizza in the
    incoming order will clear the make line and oven, accounting for all
    orders already ahead of it in the queue.

    Worker collaboration model
    --------------------------
    The make-line has N workers.  When a pizza starts on the line, the
    number of workers who are free at that moment all help, reducing the
    effective make time:

        effective_dur = make_dur / (idle_workers ** efficiency_exponent)

    This matches the MakeLine.assign() behaviour in simulation.py and means
    the DynamicEstimator correctly anticipates speedups from idle capacity.
"""

from __future__ import annotations

from collections import deque
from typing import Optional

from .config import cfg
from .models import KitchenState, OrderType

# ──────────────────────────────────────────────────────────────────────────────
# Constants (sourced from config.yaml at startup)
# ──────────────────────────────────────────────────────────────────────────────

OVEN_DURATION     = cfg.oven_time      # sim-minutes every pizza spends in the oven
DELIVERY_DURATION = cfg.delivery_time  # sim-minutes added for delivery orders

# Cold-start fallback: average make time across the menu's topping range
DEFAULT_MAKE_DURATION = (
    cfg.base_prep_time
    + ((cfg.custom_topping_min + cfg.custom_topping_max) / 2.0) * cfg.time_per_topping
)


# ──────────────────────────────────────────────────────────────────────────────
# Model A — Naive / Legacy
# ──────────────────────────────────────────────────────────────────────────────

class NaiveEstimator:
    """
    Rolling window of recent completion times.  Returns the average for
    every new order regardless of queue depth, order size, or kitchen load.
    """

    def __init__(self, window: int = 10) -> None:
        self._window  = window
        self._history: deque[float] = deque(maxlen=window)

    def record_completion(self, actual_duration: float) -> None:
        self._history.append(actual_duration)

    def estimate(self, order_type: OrderType) -> float:
        delivery_add = DELIVERY_DURATION if order_type == OrderType.DELIVERY else 0.0
        if not self._history:
            return DEFAULT_MAKE_DURATION + OVEN_DURATION + delivery_add
        return sum(self._history) / len(self._history)

    @property
    def sample_count(self) -> int:
        return len(self._history)


# ──────────────────────────────────────────────────────────────────────────────
# Model B — Dynamic
# ──────────────────────────────────────────────────────────────────────────────

class DynamicEstimator:
    """
    Projects when the incoming order will complete by simulating the
    make-line and oven pipeline, including the worker collaboration speedup.

    Algorithm
    ---------
    1. Initialise station free-at list (N entries: busy stations hold their
       finish time, idle stations hold now).
    2. Burn through the make queue ahead of this order, assigning each pizza
       to the earliest-free worker and applying the collaboration speedup.
    3. Assign THIS order's pizzas the same way.
    4. Initialise oven slot free-at list and burn through queued oven work.
    5. Assign this order's pizzas to the oven.
    6. Add delivery surcharge if applicable.
    """

    def estimate(
        self,
        num_pizzas: int,
        pizza_make_durations: list[float],
        order_type: OrderType,
        kitchen: KitchenState,
    ) -> float:
        """
        Return an estimate in simulated minutes from *now*.

        Pipeline make-line model
        ------------------------
        The make-line is a pipeline with ``num_stations`` stages (workers).
        ``station_free_at`` holds the scheduled exit times of in-flight
        pizzas.  The last of these is the pipeline's current "last exit" —
        the point from which the next pizza can start its final stage.

        * If the pipeline is idle (all exit times ≤ now), the next pizza
          takes its full ``make_duration`` (priming the line).
        * If the pipeline is flowing, each pizza exits one *cycle_time*
          after the previous one:
              cycle_time = make_duration / num_workers
        """
        now      = kitchen.now
        N        = kitchen.num_stations  # num_workers

        # ── Step 1: pipeline "last exit" ─────────────────────────────────────
        # If there are in-flight pizzas their exit times tell us when the
        # pipeline is scheduled to produce its next output.
        last_exit = max(kitchen.station_free_at) if kitchen.station_free_at else now
        if last_exit < now:
            last_exit = now  # line has gone idle

        # ── Step 2: advance through make_queue ahead of this order ───────────
        for make_dur in kitchen.make_queue_durations:
            if last_exit <= now:
                # Line idle — this pizza primes it
                last_exit = now + make_dur
            else:
                # Pipeline flowing — exits one cycle after the previous pizza
                last_exit = last_exit + make_dur / N

        # ── Step 3: project this order's pizzas through the pipeline ─────────
        # Use efficiency_exponent as a slight pessimism factor so the
        # estimate stays conservatively above the true (ideal) pipeline time.
        # exp=1 → pure linear pipeline; exp<1 → mild overhead assumed.
        exp = cfg.efficiency_exponent
        make_end_times: list[float] = []
        for make_dur in pizza_make_durations:
            if last_exit <= now:
                exit_time = now + make_dur
            else:
                cycle_time = make_dur / (N ** exp)
                exit_time  = last_exit + cycle_time
            last_exit = exit_time
            make_end_times.append(exit_time)

        # ── Step 4: oven ─────────────────────────────────────────────────────
        oven_free_at = sorted(kitchen.oven_slot_free_at)
        while len(oven_free_at) < kitchen.oven_capacity:
            oven_free_at.append(now)
        oven_free_at = sorted(oven_free_at)

        for _ in range(sum(kitchen.queued_order_pizza_counts)):
            earliest_oven   = oven_free_at[0]
            oven_free_at[0] = earliest_oven + OVEN_DURATION
            oven_free_at.sort()

        # ── Step 5: assign this order's pizzas to the oven ───────────────────
        oven_exit_times: list[float] = []
        for make_finish in sorted(make_end_times):
            earliest_oven = oven_free_at[0]
            oven_enter    = max(make_finish, earliest_oven)
            oven_exit     = oven_enter + OVEN_DURATION
            oven_free_at[0] = oven_exit
            oven_free_at.sort()
            oven_exit_times.append(oven_exit)

        ready_at = max(oven_exit_times)

        # ── Step 6: delivery ──────────────────────────────────────────────────
        if order_type != OrderType.DELIVERY:
            return max(ready_at - now, 0.0)

        # Project when a driver will be free for this order.
        # Sort all driver available-at times; skip the first N that are already
        # claimed by orders ahead of this one in the delivery queue.
        avail_times = sorted(kitchen.driver_available_times)
        for _ in range(kitchen.pending_deliveries):
            if avail_times:
                avail_times.pop(0)

        if avail_times:
            pickup_at = max(ready_at, avail_times[0])
        else:
            # No driver available at all — assume one returns at ready_at
            pickup_at = ready_at

        avg_trip     = (cfg.driver_delivery_min + cfg.driver_delivery_max) / 2.0
        completed_at = pickup_at + avg_trip / 2.0  # halfway = delivered
        return max(completed_at - now, 0.0)
