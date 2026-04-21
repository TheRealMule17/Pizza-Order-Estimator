"""
Data classes for the Pizza Order Estimator simulation.

Defines the core domain objects: Pizza, Order, and the snapshot of
kitchen state used by both estimation models.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Optional


class OrderType(Enum):
    CARRYOUT = "carryout"
    DELIVERY = "delivery"


class PizzaStatus(Enum):
    QUEUED = auto()       # waiting for a make-line station
    MAKING = auto()       # on a make-line station
    WAITING_OVEN = auto() # made but waiting for oven space
    BAKING = auto()       # in the oven
    DONE = auto()         # out of the oven


class OrderStatus(Enum):
    QUEUED = auto()              # no pizzas have started yet
    IN_PROGRESS = auto()         # at least one pizza is being made or baked
    READY_FOR_PICKUP = auto()    # carryout order out of oven, waiting for customer
    WAITING_FOR_DRIVER = auto()  # delivery order out of oven, awaiting a free driver
    OUT_FOR_DELIVERY = auto()    # driver has picked up the order
    COMPLETE = auto()


class DriverStatus(Enum):
    AVAILABLE = auto()
    DELIVERING = auto()
    RETURNING = auto()


@dataclass
class Driver:
    """A delivery driver in the driver pool."""
    driver_id: str
    status: DriverStatus = DriverStatus.AVAILABLE
    current_order_id: Optional[str] = None
    dropoff_at: Optional[float] = None    # sim-time when order is considered delivered
    available_at: Optional[float] = None  # sim-time when driver returns and is free again


@dataclass
class Pizza:
    """A single pizza belonging to an order."""
    order_id: str
    make_duration: float        # simulated minutes to assemble this pizza
    pizza_type: str = ""        # preset name ("Supreme") or "Custom"
    toppings: list = field(default_factory=list)  # ordered list of topping names
    item_id: str = ""           # unique ID for this pizza within the order
    status: PizzaStatus = PizzaStatus.QUEUED

    # wall-clock times (in sim-minutes from epoch) set as work progresses
    make_start: Optional[float] = None
    make_end: Optional[float] = None
    oven_start: Optional[float] = None
    oven_end: Optional[float] = None


@dataclass
class Order:
    """A customer order containing one or more pizzas."""
    order_id: str
    order_type: OrderType
    pizzas: list[Pizza]
    placed_at: float       # sim-time when order was placed (minutes)

    # estimates set at order creation time
    naive_estimate: Optional[float] = None    # minutes from placed_at
    dynamic_estimate: Optional[float] = None  # minutes from placed_at

    # completion times filled in by the simulation
    ready_at: Optional[float] = None          # sim-time all pizzas exit oven
    completed_at: Optional[float] = None      # ready_at + delivery time if applicable

    status: OrderStatus = OrderStatus.QUEUED

    @property
    def num_pizzas(self) -> int:
        return len(self.pizzas)

    @property
    def actual_duration(self) -> Optional[float]:
        """Total elapsed sim-minutes from placement to completion."""
        if self.completed_at is None:
            return None
        return self.completed_at - self.placed_at

    @property
    def naive_error(self) -> Optional[float]:
        """Signed error: positive = overestimate, negative = underestimate."""
        if self.actual_duration is None or self.naive_estimate is None:
            return None
        return self.naive_estimate - self.actual_duration

    @property
    def dynamic_error(self) -> Optional[float]:
        if self.actual_duration is None or self.dynamic_estimate is None:
            return None
        return self.dynamic_estimate - self.actual_duration


@dataclass
class KitchenState:
    """
    A snapshot of kitchen capacity and queue state, passed to the
    dynamic estimator so it can project when this order will complete.
    """
    # How many make-line stations exist and how many are free right now
    num_stations: int
    free_stations: int

    # Sim-time at which each busy station finishes its current pizza.
    # Length == (num_stations - free_stations).
    station_free_at: list[float] = field(default_factory=list)

    # Sim-time at which each oven slot opens up.
    # Slots still occupied hold the sim-time when that pizza exits the oven.
    oven_capacity: int = 8
    oven_slots_free: int = 8
    oven_slot_free_at: list[float] = field(default_factory=list)

    # Pizzas already queued for make-line (not yet assigned a station),
    # in arrival order.  Each entry is the estimated make-duration.
    make_queue_durations: list[float] = field(default_factory=list)

    # Orders currently in the queue ahead of the incoming order (not yet complete).
    # Each entry is the number of pizzas remaining to be made.
    queued_order_pizza_counts: list[int] = field(default_factory=list)

    # Driver pool state for delivery estimation
    drivers_available: int = 0
    driver_available_times: list[float] = field(default_factory=list)  # all drivers' available_at times
    pending_deliveries: int = 0  # orders already waiting for or assigned a driver

    # Current sim-time
    now: float = 0.0
