"""
Load and validate simulation configuration from config.yaml.

Provides a module-level `cfg` object whose properties expose every
tunable parameter.  Missing keys fall back to sensible defaults so the
simulation runs even if config.yaml is absent or partially written.
"""

from __future__ import annotations

import copy
import os
from typing import Any

# ── Defaults (mirrors the shipped config.yaml) ────────────────────────────────

_DEFAULTS: dict[str, Any] = {
    "simulation": {
        "time_scale": 60,
    },
    "kitchen": {
        "workers": 3,
        "efficiency_exponent": 0.75,
        "oven_capacity": 8,
        "oven_time": 8.0,
    },
    "delivery": {
        "delivery_time": 10.0,
        "delivery_ratio": 0.35,
    },
    "orders": {
        "normal_interval_min": 30,
        "normal_interval_max": 90,
        "rush_interval_min": 5,
        "rush_interval_max": 15,
        "rush_duration": 180,
        "items_per_order_min": 1,
        "items_per_order_max": 6,
    },
    "menu": {
        "base_prep_time": 1.0,
        "time_per_topping": 0.2,
        "custom_order_ratio": 0.3,
        "custom_topping_min": 1,
        "custom_topping_max": 8,
        "toppings": [
            "cheese", "pepperoni", "sausage", "beef", "chicken", "bacon",
            "onions", "green peppers", "olives", "mushrooms",
            "pineapple", "tomatoes", "banana peppers",
        ],
        "presets": {
            "Cheese":      ["cheese"],
            "Pepperoni":   ["cheese", "pepperoni"],
            "Sausage":     ["cheese", "sausage"],
            "Meat Lovers": ["cheese", "pepperoni", "sausage", "beef", "bacon"],
            "Veggie":      ["cheese", "mushrooms", "onions", "green peppers", "olives", "tomatoes"],
            "Supreme":     ["cheese", "pepperoni", "sausage", "beef", "onions", "green peppers", "olives", "mushrooms"],
        },
    },
    "estimator": {
        "naive_window": 10,
    },
    "drivers": {
        "count": 4,
        "delivery_time_min": 15,
        "delivery_time_max": 25,
    },
    "pricing": {
        "normal_pizza_price":    10.00,
        "specialty_pizza_price": 15.00,
        "custom_base_price":     10.00,
        "custom_per_topping":     1.00,
    },
    "pricing_tiers": {
        "normal":    ["Cheese", "Pepperoni", "Sausage"],
        "specialty": ["Veggie", "Supreme"],
    },
    "labor": {
        "insider_hourly_wage": 12.00,
        "driver_hourly_wage":   9.00,
    },
    "day_schedule": {},
}


def _deep_merge(base: dict, override: dict) -> dict:
    for k, v in override.items():
        if k in base and isinstance(base[k], dict) and isinstance(v, dict):
            _deep_merge(base[k], v)
        else:
            base[k] = v
    return base


def _load() -> dict:
    data = copy.deepcopy(_DEFAULTS)
    config_path = os.path.normpath(
        os.path.join(os.path.dirname(__file__), "..", "config.yaml")
    )
    if os.path.exists(config_path):
        try:
            import yaml  # type: ignore

            with open(config_path, "r") as f:
                user = yaml.safe_load(f) or {}
            _deep_merge(data, user)
        except Exception as exc:
            print(f"[config] Warning: failed to load config.yaml ({exc}), using defaults.")
    return data


# ── Typed config accessor ─────────────────────────────────────────────────────

class _Config:
    def __init__(self, data: dict) -> None:
        self._data = data

    # simulation
    @property
    def time_scale(self) -> float:
        return float(self._data["simulation"]["time_scale"])

    # kitchen
    @property
    def workers(self) -> int:
        return int(self._data["kitchen"]["workers"])

    @property
    def efficiency_exponent(self) -> float:
        return float(self._data["kitchen"]["efficiency_exponent"])

    @property
    def oven_capacity(self) -> int:
        return int(self._data["kitchen"]["oven_capacity"])

    @property
    def oven_time(self) -> float:
        return float(self._data["kitchen"]["oven_time"])

    # delivery
    @property
    def delivery_time(self) -> float:
        return float(self._data["delivery"]["delivery_time"])

    @property
    def delivery_ratio(self) -> float:
        return float(self._data["delivery"]["delivery_ratio"])

    # orders
    @property
    def normal_interval_min(self) -> int:
        return int(self._data["orders"]["normal_interval_min"])

    @property
    def normal_interval_max(self) -> int:
        return int(self._data["orders"]["normal_interval_max"])

    @property
    def rush_interval_min(self) -> int:
        return int(self._data["orders"]["rush_interval_min"])

    @property
    def rush_interval_max(self) -> int:
        return int(self._data["orders"]["rush_interval_max"])

    @property
    def rush_duration(self) -> int:
        return int(self._data["orders"]["rush_duration"])

    @property
    def items_per_order_min(self) -> int:
        return int(self._data["orders"]["items_per_order_min"])

    @property
    def items_per_order_max(self) -> int:
        return int(self._data["orders"]["items_per_order_max"])

    # menu
    @property
    def base_prep_time(self) -> float:
        return float(self._data["menu"]["base_prep_time"])

    @property
    def time_per_topping(self) -> float:
        return float(self._data["menu"]["time_per_topping"])

    @property
    def custom_order_ratio(self) -> float:
        return float(self._data["menu"]["custom_order_ratio"])

    @property
    def custom_topping_min(self) -> int:
        return int(self._data["menu"]["custom_topping_min"])

    @property
    def custom_topping_max(self) -> int:
        return int(self._data["menu"]["custom_topping_max"])

    @property
    def toppings(self) -> list[str]:
        return list(self._data["menu"]["toppings"])

    @property
    def presets(self) -> dict[str, list[str]]:
        return {k: list(v) for k, v in self._data["menu"]["presets"].items()}

    # estimator
    @property
    def naive_window(self) -> int:
        return int(self._data["estimator"]["naive_window"])

    # drivers
    @property
    def driver_count(self) -> int:
        return int(self._data["drivers"]["count"])

    @property
    def driver_delivery_min(self) -> float:
        return float(self._data["drivers"]["delivery_time_min"])

    @property
    def driver_delivery_max(self) -> float:
        return float(self._data["drivers"]["delivery_time_max"])

    # pricing
    @property
    def normal_pizza_price(self) -> float:
        return float(self._data["pricing"]["normal_pizza_price"])

    @property
    def specialty_pizza_price(self) -> float:
        return float(self._data["pricing"]["specialty_pizza_price"])

    @property
    def custom_base_price(self) -> float:
        return float(self._data["pricing"]["custom_base_price"])

    @property
    def custom_per_topping_price(self) -> float:
        return float(self._data["pricing"]["custom_per_topping"])

    @property
    def pricing_tier_normal(self) -> list[str]:
        return list(self._data["pricing_tiers"]["normal"])

    @property
    def pricing_tier_specialty(self) -> list[str]:
        return list(self._data["pricing_tiers"]["specialty"])

    # labor
    @property
    def insider_hourly_wage(self) -> float:
        return float(self._data["labor"]["insider_hourly_wage"])

    @property
    def driver_hourly_wage(self) -> float:
        return float(self._data["labor"]["driver_hourly_wage"])

    # day schedule
    @property
    def day_schedule(self) -> dict:
        return self._data.get("day_schedule", {})

    # pipeline
    @property
    def pipeline(self) -> dict:
        return self._data.get("pipeline", {})


cfg = _Config(_load())
