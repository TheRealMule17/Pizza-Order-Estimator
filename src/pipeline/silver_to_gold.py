"""
Silver → Gold aggregation

Reads cleaned Parquet files from data/cleaned/ and produces business-ready
analytics tables in data/analytics/.

Run standalone:
    python -m src.pipeline.silver_to_gold
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Optional

import pandas as pd

# ── Path helpers ──────────────────────────────────────────────────────────────

_ROOT = Path(__file__).parent.parent.parent


def _cfg() -> dict:
    try:
        import yaml  # type: ignore
        with open(_ROOT / "config.yaml") as fh:
            data = yaml.safe_load(fh) or {}
        return data.get("pipeline", {})
    except Exception:
        return {}


def _cleaned_dir() -> Path:
    return _ROOT / _cfg().get("cleaned_data_dir", "data/cleaned")


def _analytics_dir() -> Path:
    p = _ROOT / _cfg().get("analytics_data_dir", "data/analytics")
    p.mkdir(parents=True, exist_ok=True)
    return p


def _read(name: str) -> Optional[pd.DataFrame]:
    path = _cleaned_dir() / name
    if not path.exists():
        return None
    try:
        return pd.read_parquet(path)
    except Exception:
        return None


def _write_gold(df: pd.DataFrame, name: str) -> None:
    if df is None or df.empty:
        return
    out = _analytics_dir() / name
    df.to_parquet(out, index=False)


# ── Hourly summary ────────────────────────────────────────────────────────────

def _hourly_summary(
    orders: pd.DataFrame,
    staff:  Optional[pd.DataFrame],
) -> pd.DataFrame:
    """
    One row per simulated hour with order counts, wait times, revenue,
    labor cost, SPLH, staffing levels, and model accuracy.
    """
    if orders.empty:
        return pd.DataFrame()

    orders = orders.copy()
    orders["placed_at"] = pd.to_datetime(orders["placed_at"], utc=True)
    orders["hour"] = orders["placed_at"].dt.floor("h")

    labor_cfg = _labor_config()

    rows = []
    for hour, grp in orders.groupby("hour"):
        carryout = grp[grp["order_type"] == "carryout"]
        delivery = grp[grp["order_type"] == "delivery"]

        def _mean(series: pd.Series) -> Optional[float]:
            vals = series.dropna()
            return round(float(vals.mean()), 3) if len(vals) > 0 else None

        rev  = grp["total_price"].sum() if "total_price" in grp.columns else 0.0
        rev  = round(float(rev), 2)

        # Staffing at this hour
        ins, drv = _staffing_at(staff, hour)

        labor_cost = round(
            ins * labor_cfg["insider_hourly_wage"]
            + drv * labor_cfg["driver_hourly_wage"],
            2,
        )
        splh = round(rev / max(ins, 1), 2) if ins else None

        # Accuracy for orders completed this hour
        comp = grp.dropna(subset=["actual_wait_min"])
        naive_errors   = (comp["naive_estimate"]   - comp["actual_wait_min"]).abs() if "naive_estimate"   in comp else pd.Series(dtype=float)
        dynamic_errors = (comp["dynamic_estimate"] - comp["actual_wait_min"]).abs() if "dynamic_estimate" in comp else pd.Series(dtype=float)

        rows.append({
            "hour":               hour,
            "order_count":        len(grp),
            "avg_wait_carryout":  _mean(carryout["actual_wait_min"]) if "actual_wait_min" in carryout else None,
            "avg_wait_delivery":  _mean(delivery["actual_wait_min"]) if "actual_wait_min" in delivery else None,
            "revenue":            rev,
            "labor_cost":         labor_cost,
            "splh":               splh,
            "insiders_on_clock":  ins,
            "drivers_on_clock":   drv,
            "naive_mae":          round(float(naive_errors.mean()),   3) if len(naive_errors)   > 0 else None,
            "dynamic_mae":        round(float(dynamic_errors.mean()), 3) if len(dynamic_errors) > 0 else None,
        })

    return pd.DataFrame(rows)


def _staffing_at(
    staff: Optional[pd.DataFrame], hour: pd.Timestamp
) -> tuple[int, int]:
    """Return (insiders, drivers) on the clock at the given hour."""
    if staff is None or staff.empty:
        return 0, 0
    staff = staff.copy()
    staff["timestamp"] = pd.to_datetime(staff["timestamp"], utc=True)
    before = staff[staff["timestamp"] <= hour]
    if before.empty:
        return 0, 0
    latest = before.iloc[-1]
    return int(latest.get("insiders", 0)), int(latest.get("drivers", 0))


def _labor_config() -> dict:
    try:
        import yaml  # type: ignore
        with open(_ROOT / "config.yaml") as fh:
            data = yaml.safe_load(fh) or {}
        return data.get("labor", {"insider_hourly_wage": 12.0, "driver_hourly_wage": 9.0})
    except Exception:
        return {"insider_hourly_wage": 12.0, "driver_hourly_wage": 9.0}


# ── Estimation accuracy ───────────────────────────────────────────────────────

def _estimation_accuracy(orders: pd.DataFrame) -> pd.DataFrame:
    """One row per completed order with errors and winner column."""
    if orders.empty:
        return pd.DataFrame()

    comp = orders.dropna(subset=["actual_wait_min"]).copy()
    if comp.empty:
        return pd.DataFrame()

    need = ["naive_estimate", "dynamic_estimate", "actual_wait_min"]
    for col in need:
        if col not in comp.columns:
            comp[col] = None
    comp = comp.dropna(subset=need)
    if comp.empty:
        return pd.DataFrame()

    comp["naive_error"]   = comp["naive_estimate"]   - comp["actual_wait_min"]
    comp["dynamic_error"] = comp["dynamic_estimate"] - comp["actual_wait_min"]
    comp["naive_abs"]     = comp["naive_error"].abs()
    comp["dynamic_abs"]   = comp["dynamic_error"].abs()

    def _winner(row):
        if pd.isna(row["naive_abs"]) or pd.isna(row["dynamic_abs"]):
            return "tie"
        if row["naive_abs"] < row["dynamic_abs"]:
            return "naive"
        if row["dynamic_abs"] < row["naive_abs"]:
            return "dynamic"
        return "tie"

    comp["winner"] = comp.apply(_winner, axis=1)

    cols = [
        "order_id", "order_type", "actual_wait_min",
        "naive_estimate", "dynamic_estimate",
        "naive_error", "dynamic_error", "winner",
    ]
    out_cols = [c for c in cols if c in comp.columns]
    return comp[out_cols].reset_index(drop=True)


# ── Bottleneck log ────────────────────────────────────────────────────────────

def _bottleneck_log() -> pd.DataFrame:
    """
    Read the bottleneck event log that the simulation's analytics tracker
    writes into the existing logs/summary_*.txt (or live analytics snapshot).
    For the Gold layer, we read from the analytics.json sidecar if present,
    otherwise return an empty frame.
    """
    # The raw emitter does not capture bottleneck events; they live in the
    # simulation's AnalyticsTracker.  We expose them via a JSON sidecar written
    # by simulation.stop() when the analytics snapshot is available.
    sidecar = _ROOT / "logs" / "bottleneck_events.json"
    if not sidecar.exists():
        return pd.DataFrame(
            columns=["event_type", "simulated_time", "sim_time_min",
                     "traffic_block_label", "current_insiders",
                     "current_drivers", "queue_depth", "details"]
        )
    try:
        import json
        events = json.loads(sidecar.read_text(encoding="utf-8"))
        return pd.DataFrame(events)
    except Exception:
        return pd.DataFrame()


# ── Daily KPIs ────────────────────────────────────────────────────────────────

def _daily_kpis(
    orders:  pd.DataFrame,
    hourly:  pd.DataFrame,
    items:   Optional[pd.DataFrame],
) -> pd.DataFrame:
    """Single-row headline KPI table."""
    if orders.empty:
        return pd.DataFrame()

    comp = orders.dropna(subset=["actual_wait_min"])
    total_orders  = len(orders)
    total_revenue = round(float(orders["total_price"].sum()), 2) if "total_price" in orders else 0.0

    labor_cfg = _labor_config()
    total_labor: float = 0.0
    if not hourly.empty:
        total_labor = round(float(hourly["labor_cost"].sum()), 2)

    splh: Optional[float] = None
    if total_labor > 0 and not hourly.empty:
        total_ins_hours = hourly["insiders_on_clock"].sum()
        splh = round(total_revenue / total_ins_hours, 2) if total_ins_hours > 0 else None

    labor_pct: Optional[float] = None
    if total_revenue > 0:
        labor_pct = round(total_labor / total_revenue * 100, 1)

    overall_naive_mae:   Optional[float] = None
    overall_dynamic_mae: Optional[float] = None
    if len(comp) > 0 and "naive_estimate" in comp.columns:
        errs_n = (comp["naive_estimate"]   - comp["actual_wait_min"]).abs().dropna()
        errs_d = (comp["dynamic_estimate"] - comp["actual_wait_min"]).abs().dropna()
        if len(errs_n) > 0:
            overall_naive_mae   = round(float(errs_n.mean()), 3)
        if len(errs_d) > 0:
            overall_dynamic_mae = round(float(errs_d.mean()), 3)

    # Peak hour
    peak_hour = None
    if not hourly.empty and "order_count" in hourly.columns:
        idx = hourly["order_count"].idxmax()
        peak_hour = str(hourly.loc[idx, "hour"])

    # Worst bottleneck period
    worst_bottleneck = None
    if not hourly.empty and "revenue" in hourly.columns:
        # Proxy: hour with highest avg_wait_delivery
        if "avg_wait_delivery" in hourly.columns:
            idx2 = hourly["avg_wait_delivery"].dropna().idxmax() if hourly["avg_wait_delivery"].notna().any() else None
            if idx2 is not None:
                worst_bottleneck = str(hourly.loc[idx2, "hour"])

    row = {
        "total_orders":       total_orders,
        "total_revenue":      total_revenue,
        "total_labor_cost":   total_labor,
        "overall_splh":       splh,
        "labor_cost_pct":     labor_pct,
        "overall_naive_mae":  overall_naive_mae,
        "overall_dynamic_mae": overall_dynamic_mae,
        "peak_hour":          peak_hour,
        "worst_bottleneck_hour": worst_bottleneck,
        "orders_with_wait_data": len(comp),
    }
    return pd.DataFrame([row])


# ── Main entry point ──────────────────────────────────────────────────────────

def run_silver_to_gold() -> None:
    """Run the full Silver → Gold aggregation."""
    orders   = _read("orders.parquet")
    items    = _read("order_items.parquet")
    dispatch = _read("dispatch_events.parquet")
    staff    = _read("staff_changes.parquet")

    if orders is None:
        print("[silver_to_gold] No orders.parquet found — skipping Gold layer.")
        return

    hourly   = _hourly_summary(orders, staff)
    accuracy = _estimation_accuracy(orders)
    botts    = _bottleneck_log()
    kpis     = _daily_kpis(orders, hourly, items)

    _write_gold(hourly,   "hourly_summary.parquet")
    _write_gold(accuracy, "estimation_accuracy.parquet")
    _write_gold(botts,    "bottleneck_log.parquet")
    _write_gold(kpis,     "daily_kpis.parquet")


if __name__ == "__main__":
    run_silver_to_gold()
    print("[silver_to_gold] Done.")
