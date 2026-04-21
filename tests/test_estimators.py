"""
Unit tests for both estimation models.

Run with:  pytest tests/
"""

import pytest

from src.estimators import (
    DELIVERY_DURATION,
    OVEN_DURATION,
    DynamicEstimator,
    NaiveEstimator,
)
from src.models import KitchenState, OrderType


# ──────────────────────────────────────────────────────────────────────────────
# NaiveEstimator
# ──────────────────────────────────────────────────────────────────────────────

class TestNaiveEstimator:

    def test_cold_start_carryout(self):
        """With no history, falls back to make+oven baseline."""
        est = NaiveEstimator()
        result = est.estimate(OrderType.CARRYOUT)
        # DEFAULT_MAKE_DURATION + OVEN_DURATION
        assert result == pytest.approx(2.0 + 8.0)

    def test_cold_start_delivery(self):
        est = NaiveEstimator()
        result = est.estimate(OrderType.DELIVERY)
        assert result == pytest.approx(2.0 + 8.0 + 10.0)

    def test_rolling_average(self):
        est = NaiveEstimator(window=3)
        est.record_completion(10.0)
        est.record_completion(20.0)
        est.record_completion(30.0)
        # Window is full; average = 20
        assert est.estimate(OrderType.CARRYOUT) == pytest.approx(20.0)

    def test_window_eviction(self):
        """Oldest entry is evicted once the window is full."""
        est = NaiveEstimator(window=2)
        est.record_completion(100.0)  # will be evicted
        est.record_completion(10.0)
        est.record_completion(20.0)
        # Average of last 2: (10+20)/2 = 15
        assert est.estimate(OrderType.CARRYOUT) == pytest.approx(15.0)

    def test_sample_count(self):
        est = NaiveEstimator(window=5)
        assert est.sample_count == 0
        est.record_completion(10.0)
        est.record_completion(10.0)
        assert est.sample_count == 2


# ──────────────────────────────────────────────────────────────────────────────
# DynamicEstimator — idle kitchen
# ──────────────────────────────────────────────────────────────────────────────

class TestDynamicEstimatorIdleKitchen:
    """Tests where the kitchen is completely idle (no queue, all stations free)."""

    def _idle_kitchen(self, num_stations=3, oven_capacity=8) -> KitchenState:
        return KitchenState(
            num_stations=num_stations,
            free_stations=num_stations,
            station_free_at=[],
            oven_capacity=oven_capacity,
            oven_slots_free=oven_capacity,
            oven_slot_free_at=[],
            make_queue_durations=[],
            queued_order_pizza_counts=[],
            now=0.0,
        )

    def test_single_pizza_carryout(self):
        """1 pizza, idle kitchen: estimate ≈ make_time + oven_time."""
        est = DynamicEstimator()
        make_dur = 2.0
        result = est.estimate(1, [make_dur], OrderType.CARRYOUT, self._idle_kitchen())
        assert result == pytest.approx(make_dur + OVEN_DURATION)

    def test_single_pizza_delivery(self):
        est = DynamicEstimator()
        make_dur = 2.0
        result = est.estimate(1, [make_dur], OrderType.DELIVERY, self._idle_kitchen())
        assert result == pytest.approx(make_dur + OVEN_DURATION + DELIVERY_DURATION)

    def test_multiple_pizzas_parallel_make(self):
        """
        With 3 stations and 3 pizzas, all pizzas start on the make line at
        the same time — the order is ready after max(make_times) + oven.
        """
        est = DynamicEstimator()
        make_durs = [1.5, 2.0, 3.0]  # 3 pizzas, 3 free stations
        result = est.estimate(3, make_durs, OrderType.CARRYOUT, self._idle_kitchen(num_stations=3))
        # Longest pizza takes 3.0 min to make, then 8 min in oven
        expected = 3.0 + OVEN_DURATION
        assert result == pytest.approx(expected)

    def test_more_pizzas_than_stations(self):
        """
        4 pizzas, 2 stations: station serialisation should be reflected.
        With stations [A, B] and pizzas [2, 2, 2, 2]:
          - Pizza 1 starts at 0, ends at 2  → station A free at 2
          - Pizza 2 starts at 0, ends at 2  → station B free at 2
          - Pizza 3 starts at 2, ends at 4
          - Pizza 4 starts at 2, ends at 4
        All exit oven at max(make_end) + 8 = 4 + 8 = 12
        """
        est = DynamicEstimator()
        make_durs = [2.0, 2.0, 2.0, 2.0]
        kitchen = self._idle_kitchen(num_stations=2)
        result = est.estimate(4, make_durs, OrderType.CARRYOUT, kitchen)
        assert result == pytest.approx(4.0 + OVEN_DURATION)

    def test_result_is_non_negative(self):
        est = DynamicEstimator()
        result = est.estimate(1, [0.0], OrderType.CARRYOUT, self._idle_kitchen())
        assert result >= 0.0


# ──────────────────────────────────────────────────────────────────────────────
# DynamicEstimator — busy kitchen
# ──────────────────────────────────────────────────────────────────────────────

class TestDynamicEstimatorBusyKitchen:

    def test_all_stations_busy_causes_wait(self):
        """
        3 stations all finishing at t=5.  A 1-pizza order must wait until
        the earliest station is free (t=5), making ends at t=5+2=7.
        """
        est = DynamicEstimator()
        kitchen = KitchenState(
            num_stations=3,
            free_stations=0,
            station_free_at=[5.0, 7.0, 9.0],
            oven_capacity=8,
            oven_slots_free=8,
            oven_slot_free_at=[],
            make_queue_durations=[],
            queued_order_pizza_counts=[],
            now=0.0,
        )
        result = est.estimate(1, [2.0], OrderType.CARRYOUT, kitchen)
        # Station free at 5, make ends at 7, oven exits at 15
        assert result == pytest.approx(5.0 + 2.0 + OVEN_DURATION)

    def test_full_oven_delays_baking(self):
        """
        Oven is completely full (8/8).  The earliest slot frees at t=3.
        A pizza that finishes making at t=1 must wait until t=3 to enter.
        """
        est = DynamicEstimator()
        oven_exits = [3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]
        kitchen = KitchenState(
            num_stations=3,
            free_stations=3,
            station_free_at=[],
            oven_capacity=8,
            oven_slots_free=0,
            oven_slot_free_at=oven_exits,
            make_queue_durations=[],
            queued_order_pizza_counts=[],
            now=0.0,
        )
        result = est.estimate(1, [1.0], OrderType.CARRYOUT, kitchen)
        # Make done at 1, but earliest oven slot is 3 → oven exit at 3+8=11
        assert result == pytest.approx(3.0 + OVEN_DURATION)

    def test_queue_depth_increases_estimate(self):
        """
        An idle kitchen with 5 orders of 2 pizzas each ahead in queue
        should produce a higher estimate than an empty queue.
        """
        est = DynamicEstimator()

        def make_kitchen(queued_pizza_counts):
            return KitchenState(
                num_stations=3,
                free_stations=3,
                station_free_at=[],
                oven_capacity=8,
                oven_slots_free=8,
                oven_slot_free_at=[],
                make_queue_durations=[],
                queued_order_pizza_counts=queued_pizza_counts,
                now=0.0,
            )

        empty_est = est.estimate(1, [2.0], OrderType.CARRYOUT, make_kitchen([]))
        busy_est  = est.estimate(1, [2.0], OrderType.CARRYOUT, make_kitchen([2, 2, 2, 2, 2]))

        assert busy_est > empty_est

    def test_make_queue_durations_increase_estimate(self):
        """Pizzas ahead of us in the make queue push our start time back."""
        est = DynamicEstimator()

        def make_kitchen(make_queue):
            return KitchenState(
                num_stations=1,  # single station makes the queuing effect clearest
                free_stations=1,
                station_free_at=[],
                oven_capacity=8,
                oven_slots_free=8,
                oven_slot_free_at=[],
                make_queue_durations=make_queue,
                queued_order_pizza_counts=[],
                now=0.0,
            )

        no_queue  = est.estimate(1, [2.0], OrderType.CARRYOUT, make_kitchen([]))
        with_queue = est.estimate(1, [2.0], OrderType.CARRYOUT, make_kitchen([3.0, 3.0]))

        # Two 3-min pizzas ahead = our pizza starts at 6, makes until 8
        assert with_queue == pytest.approx(no_queue + 6.0)
