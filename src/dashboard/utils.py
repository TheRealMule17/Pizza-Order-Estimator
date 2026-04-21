import streamlit as st
import pandas as pd
from pathlib import Path
from typing import Optional

COLORS = {
    "dynamic": "#006491",
    "naive": "#E31837",
    "carryout": "#22c55e",
    "delivery": "#3b82f6",
    "revenue": "#22c55e",
    "labor": "#E31837",
    "bottleneck_severe": "#ef4444",
    "bottleneck_moderate": "#f97316",
    "bottleneck_mild": "#eab308",
    "neutral": "#6b7280",
}

BOTTLENECK_COLORS = {
    "oven_full": "#ef4444",
    "no_drivers": "#f97316",
    "make_line_full": "#eab308",
    "driver_queue_backup": "#f97316",
    "extreme_wait": "#ef4444",
}

CLEANED_DIR = Path("data/cleaned")
ANALYTICS_DIR = Path("data/analytics")

SILVER_TABLES = {
    "orders": CLEANED_DIR / "orders.parquet",
    "order_items": CLEANED_DIR / "order_items.parquet",
    "dispatch_events": CLEANED_DIR / "dispatch_events.parquet",
    "staff_changes": CLEANED_DIR / "staff_changes.parquet",
}

GOLD_TABLES = {
    "hourly_summary": ANALYTICS_DIR / "hourly_summary.parquet",
    "estimation_accuracy": ANALYTICS_DIR / "estimation_accuracy.parquet",
    "bottleneck_log": ANALYTICS_DIR / "bottleneck_log.parquet",
    "daily_kpis": ANALYTICS_DIR / "daily_kpis.parquet",
}

REJECTED_CSV = CLEANED_DIR / "_rejected.csv"

TRAFFIC_BLOCKS = {
    "Morning Prep": (10, 11),
    "Lunch Rush": (11, 14),
    "Afternoon Lull": (14, 16),
    "Pre-Dinner": (16, 18),
    "Dinner Rush": (18, 21),
    "Late Night": (21, 26),  # 24=midnight, 25=1 AM (cross-midnight normalized)
}


@st.cache_data
def load_silver(table: str) -> Optional[pd.DataFrame]:
    path = SILVER_TABLES.get(table)
    if path is None or not path.exists():
        return None
    try:
        return pd.read_parquet(path)
    except Exception:
        return None


@st.cache_data
def load_gold(table: str) -> Optional[pd.DataFrame]:
    path = GOLD_TABLES.get(table)
    if path is None or not path.exists():
        return None
    try:
        return pd.read_parquet(path)
    except Exception:
        return None


@st.cache_data
def load_rejected() -> Optional[pd.DataFrame]:
    if not REJECTED_CSV.exists():
        return None
    try:
        return pd.read_csv(REJECTED_CSV)
    except Exception:
        return None


def has_any_data() -> bool:
    return any(p.exists() for p in list(GOLD_TABLES.values()) + list(SILVER_TABLES.values()))


def no_data_banner():
    st.warning(
        "No simulation data found. Run a simulation first, then come back here to explore the results.",
        icon="⚠️",
    )


def fmt_currency(val) -> str:
    try:
        return f"${float(val):,.2f}"
    except (TypeError, ValueError):
        return "—"


def fmt_minutes(val) -> str:
    try:
        return f"{float(val):.1f} min"
    except (TypeError, ValueError):
        return "—"


def fmt_pct(val) -> str:
    try:
        return f"{float(val):.1f}%"
    except (TypeError, ValueError):
        return "—"


def safe_float(val, default=None):
    try:
        f = float(val)
        return None if pd.isna(f) else f
    except (TypeError, ValueError):
        return default


def format_hour(dt) -> str:
    """Cross-platform hour formatting (avoids %-I which fails on Windows)."""
    h = dt.hour
    if h == 0:
        return "12 AM"
    elif h < 12:
        return f"{h} AM"
    elif h == 12:
        return "12 PM"
    else:
        return f"{h - 12} PM"


def format_time(dt) -> str:
    """Cross-platform HH:MM AM/PM formatting."""
    h, m = dt.hour, dt.minute
    suffix = "AM" if h < 12 else "PM"
    disp_h = h if h <= 12 else h - 12
    if disp_h == 0:
        disp_h = 12
    return f"{disp_h}:{m:02d} {suffix}"


def assign_traffic_block(hour_int: int) -> str:
    # normalize cross-midnight hours (0, 1, 2 AM) to 24, 25, 26 for range comparison
    h = hour_int if hour_int >= 3 else hour_int + 24
    for label, (start, end) in TRAFFIC_BLOCKS.items():
        if start <= h < end:
            return label
    return "Other"


def mins_to_time_str(sim_min: float) -> str:
    """Convert simulated minutes-from-10AM to a readable time string."""
    total = int(sim_min)
    h = 10 + total // 60
    m = total % 60
    suffix = "AM" if h < 12 else "PM"
    disp_h = h if h <= 12 else h - 12
    if disp_h == 0:
        disp_h = 12
    return f"{disp_h}:{m:02d} {suffix}"
