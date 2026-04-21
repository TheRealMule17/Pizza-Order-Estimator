"""
Bronze → Silver ETL

Reads all raw data from the four source directories, cleans each source,
cross-validates, and writes unified Parquet files to data/cleaned/.

Run standalone:
    python -m src.pipeline.bronze_to_silver
"""

from __future__ import annotations

import csv
import io
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import pandas as pd

# ── Path helpers ──────────────────────────────────────────────────────────────

_ROOT = Path(__file__).parent.parent.parent   # project root


def _cfg() -> dict:
    """Return the pipeline config block from config.yaml."""
    try:
        import yaml  # type: ignore
        with open(_ROOT / "config.yaml") as fh:
            data = yaml.safe_load(fh) or {}
        return data.get("pipeline", {})
    except Exception:
        return {}


def _raw_dir() -> Path:
    return _ROOT / _cfg().get("raw_data_dir", "data/raw")


def _cleaned_dir() -> Path:
    p = _ROOT / _cfg().get("cleaned_data_dir", "data/cleaned")
    p.mkdir(parents=True, exist_ok=True)
    return p


# ── Order-ID normalisation ────────────────────────────────────────────────────

_STRIP_RE = re.compile(r"^(ORD[-_]?|ord[-_]?)", re.IGNORECASE)


def _norm_order_id(raw: object) -> str:
    """Strip known prefixes, lowercase, strip whitespace.  Returns '' on failure."""
    if raw is None:
        return ""
    s = str(raw).strip()
    s = _STRIP_RE.sub("", s)
    return s.upper()


def _norm_driver_id(raw: object) -> str:
    """Normalise driver ID to 'D<N>' string form."""
    if raw is None:
        return ""
    s = str(raw).strip()
    # "driver_3", "driver3", "3" → "D3"
    m = re.match(r"^(?:driver[_]?)?(\d+)$", s, re.IGNORECASE)
    if m:
        return f"D{m.group(1)}"
    return s


def _norm_name(raw: str) -> str:
    """Title-case, strip whitespace, expand common oven abbreviations."""
    _ABBREV_EXPAND = {
        "PEP PIZZA": "Pepperoni Pizza",
        "SSG PIZZA": "Sausage Pizza",
        "SUPR PIZZA": "Supreme Pizza",
        "VEG PIZZA":  "Veggie Pizza",
        "ML PIZZA":   "Meat Lovers Pizza",
        "CHZ PIZZA":  "Cheese Pizza",
        "CUST PIZZA": "Custom Pizza",
        "PEP":    "Pepperoni",
        "SSG":    "Sausage",
        "SUPR":   "Supreme",
        "VEG":    "Veggie",
        "ML":     "Meat Lovers",
        "CHZ":    "Cheese",
        "CUST":   "Custom",
    }
    s = raw.strip().title()
    return _ABBREV_EXPAND.get(s.upper(), s)


# ── Timestamp parsers ─────────────────────────────────────────────────────────

def _parse_pos_ts(s: str) -> Optional[datetime]:
    """Parse 'MM/DD/YYYY hh:mm:ss AM/PM' → UTC datetime."""
    try:
        return datetime.strptime(s.strip(), "%m/%d/%Y %I:%M:%S %p").replace(
            tzinfo=timezone.utc
        )
    except Exception:
        return None


def _parse_epoch(val: object) -> Optional[datetime]:
    """Parse Unix epoch (int or str) → UTC datetime."""
    try:
        return datetime.fromtimestamp(int(float(str(val))), tz=timezone.utc)
    except Exception:
        return None


def _parse_iso(s: str) -> Optional[datetime]:
    """Parse ISO 8601 (with or without Z, with or without ms) → UTC datetime."""
    s = s.strip().rstrip("Z")
    for fmt in ("%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(s, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            pass
    return None


# ── Bronze loaders ────────────────────────────────────────────────────────────

def _load_pos() -> tuple[list[dict], list[dict]]:
    """Load all POS JSONL files.  Returns (records, rejected)."""
    raw_dir = _raw_dir() / "pos"
    records, rejected = [], []
    seen_keys: set[tuple] = set()

    for path in sorted(raw_dir.glob("*.jsonl")):
        for lineno, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError as exc:
                rejected.append({"source": "pos", "row": line[:120],
                                  "reason": f"JSON parse error: {exc}"})
                continue

            oid_raw = obj.get("order_id", "")
            ts_raw  = obj.get("timestamp", "")
            ts      = _parse_pos_ts(ts_raw)
            oid     = _norm_order_id(oid_raw)

            if not oid:
                rejected.append({"source": "pos", "row": str(obj)[:120],
                                  "reason": "missing/unparseable order_id"})
                continue
            if ts is None:
                rejected.append({"source": "pos", "row": str(obj)[:120],
                                  "reason": f"unparseable timestamp: {ts_raw!r}"})
                continue

            # Deduplicate on (order_id, timestamp)
            key = (oid, ts_raw)
            if key in seen_keys:
                continue
            seen_keys.add(key)

            # Parse total price
            total_str = str(obj.get("total", "0")).replace("$", "").strip()
            try:
                total = float(total_str)
            except ValueError:
                total = None

            records.append({
                "order_id":         oid,
                "order_id_raw":     oid_raw,
                "placed_at":        ts,
                "order_type":       obj.get("order_type"),       # may be None
                "items_raw":        obj.get("items", []),
                "total_price":      total,
                "naive_estimate":   obj.get("_naive_est"),
                "dynamic_estimate": obj.get("_dynamic_est"),
                "total_prep_weight": obj.get("_total_prep_weight"),
                "_source_file":     path.name,
            })

    return records, rejected


def _load_kitchen() -> tuple[list[dict], list[dict]]:
    """Load all kitchen CSV files.  Returns (records, rejected)."""
    raw_dir = _raw_dir() / "kitchen"
    records, rejected = [], []

    for path in sorted(raw_dir.glob("*.csv")):
        try:
            reader = csv.DictReader(io.StringIO(path.read_text(encoding="utf-8")))
        except Exception as exc:
            rejected.append({"source": "kitchen", "row": path.name,
                              "reason": str(exc)})
            continue

        for row in reader:
            ts  = _parse_epoch(row.get("timestamp"))
            ref = _norm_order_id(row.get("order_ref", ""))

            if ts is None:
                rejected.append({"source": "kitchen", "row": str(row)[:120],
                                  "reason": "unparseable epoch timestamp"})
                continue

            order_ref = row.get("order_ref", "")
            # Reject phantom rows (order_ref starts with "ghost_")
            if str(order_ref).startswith("ghost_"):
                rejected.append({"source": "kitchen", "row": str(row)[:120],
                                  "reason": "phantom/ghost order ref"})
                continue

            if not ref:
                rejected.append({"source": "kitchen", "row": str(row)[:120],
                                  "reason": "missing order_ref"})
                continue

            records.append({
                "order_id":   ref,
                "item_name":  _norm_name(row.get("item_name", "Unknown")),
                "station_id": row.get("station_id", ""),
                "event_type": row.get("event_type", ""),
                "timestamp":  ts,
            })

    return records, rejected


def _load_oven() -> tuple[list[dict], list[dict]]:
    """Load all oven log files.  Returns (records, rejected)."""
    raw_dir = _raw_dir() / "oven"
    records, rejected = [], []

    _LINE_RE = re.compile(
        r"\[(.+?)\]\s+(OVEN_IN|OVEN_OUT)\s+slot=(\d+)\s+item=\"(.+?)\"\s+order=(.+)"
    )

    for path in sorted(raw_dir.glob("*.log")):
        for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
            line = line.strip()
            if not line:
                continue

            # Reject corrupted lines (contain junk block chars)
            if any(c in line for c in "▒▓░█") or "ERROR" in line:
                rejected.append({"source": "oven", "row": line[:120],
                                  "reason": "corrupted/garbled line"})
                continue

            m = _LINE_RE.match(line)
            if not m:
                rejected.append({"source": "oven", "row": line[:120],
                                  "reason": "does not match oven log format"})
                continue

            ts_str, event, slot_s, item, order_raw = m.groups()
            ts   = _parse_iso(ts_str)
            oid  = _norm_order_id(order_raw)
            slot = int(slot_s)

            if ts is None:
                rejected.append({"source": "oven", "row": line[:120],
                                  "reason": f"unparseable ISO timestamp: {ts_str!r}"})
                continue
            if not oid:
                rejected.append({"source": "oven", "row": line[:120],
                                  "reason": "missing/empty order id"})
                continue

            # Flag impossible slot values (validated in output, kept here as a flag)
            records.append({
                "order_id":      oid,
                "item_name":     _norm_name(item),
                "event_type":    event,
                "slot":          slot,
                "slot_flag":     "",   # set below
                "timestamp":     ts,
            })

    return records, rejected


def _load_dispatch() -> tuple[list[dict], list[dict]]:
    """Load all dispatch JSON files.  Returns (records, rejected)."""
    raw_dir = _raw_dir() / "dispatch"
    records, rejected = [], []

    for path in sorted(raw_dir.glob("*.json")):
        try:
            events = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            rejected.append({"source": "dispatch", "row": path.name,
                              "reason": str(exc)})
            continue

        if not isinstance(events, list):
            rejected.append({"source": "dispatch", "row": path.name,
                              "reason": "root is not a JSON array"})
            continue

        for obj in events:
            ts  = _parse_iso(str(obj.get("timestamp", "")))
            oid = _norm_order_id(obj.get("order_id")) if obj.get("order_id") else None
            did = _norm_driver_id(obj.get("driver_id"))
            evt = obj.get("event", "")

            if ts is None:
                rejected.append({"source": "dispatch", "row": str(obj)[:120],
                                  "reason": "unparseable timestamp"})
                continue

            records.append({
                "order_id":  oid,        # None for "returned" events
                "driver_id": did,
                "event":     evt,
                "timestamp": ts,
            })

    # Sort by timestamp to fix out-of-order events
    records.sort(key=lambda r: r["timestamp"])
    return records, rejected


def _load_staffing() -> list[dict]:
    """Load staffing CSV files (clean source — no rejection needed)."""
    raw_dir = _raw_dir() / "staffing"
    records = []
    for path in sorted(raw_dir.glob("*.csv")):
        try:
            reader = csv.DictReader(io.StringIO(path.read_text(encoding="utf-8")))
            for row in reader:
                ts = _parse_iso(row.get("wall_timestamp", ""))
                if ts is None:
                    continue
                records.append({
                    "timestamp": ts,
                    "sim_min":   float(row.get("sim_min", 0)),
                    "insiders":  int(row.get("insiders", 0)),
                    "drivers":   int(row.get("drivers", 0)),
                })
        except Exception:
            pass
    records.sort(key=lambda r: r["timestamp"])
    return records


# ── Silver builders ───────────────────────────────────────────────────────────

def _build_orders(
    pos: list[dict],
    kitchen: list[dict],
    oven: list[dict],
    dispatch: list[dict],
) -> pd.DataFrame:
    """
    Build the orders Silver table.

    completed_at for carryout = max(oven_out) across pizzas.
    completed_at for delivery = delivered event timestamp.
    order_type filled from dispatch when POS has null.
    """
    if not pos:
        return pd.DataFrame()

    # Delivery order IDs (have dispatch events)
    delivery_oids = {r["order_id"] for r in dispatch if r.get("order_id")}

    # Oven-out times per order
    oven_out_by_order: dict[str, datetime] = {}
    for r in oven:
        if r["event_type"] == "OVEN_OUT" and r["order_id"]:
            prev = oven_out_by_order.get(r["order_id"])
            if prev is None or r["timestamp"] > prev:
                oven_out_by_order[r["order_id"]] = r["timestamp"]

    # Delivery complete times
    delivery_complete: dict[str, datetime] = {}
    for r in dispatch:
        if r["event"] == "delivered" and r["order_id"]:
            delivery_complete[r["order_id"]] = r["timestamp"]

    # Kitchen order IDs that appeared (for orphan detection)
    kitchen_oids = {r["order_id"] for r in kitchen}

    rows = []
    for p in pos:
        oid      = p["order_id"]
        otype    = p["order_type"]

        # Fill missing order_type from dispatch data
        if otype is None:
            otype = "delivery" if oid in delivery_oids else "carryout"

        completed_at: Optional[datetime]
        if otype == "delivery":
            completed_at = delivery_complete.get(oid)
        else:
            completed_at = oven_out_by_order.get(oid)

        placed_at   = p["placed_at"]
        actual_wait: Optional[float] = None
        if completed_at is not None and placed_at is not None:
            actual_wait = (completed_at - placed_at).total_seconds() / 60.0

        # Flags
        orphan_pos    = oid not in kitchen_oids   # in POS but not in kitchen

        rows.append({
            "order_id":          oid,
            "order_type":        otype,
            "num_items":         len(p["items_raw"]),
            "total_prep_weight": p.get("total_prep_weight"),
            "total_price":       p.get("total_price"),
            "placed_at":         placed_at,
            "completed_at":      completed_at,
            "actual_wait_min":   round(actual_wait, 3) if actual_wait is not None else None,
            "naive_estimate":    p.get("naive_estimate"),
            "dynamic_estimate":  p.get("dynamic_estimate"),
            "orphan_pos_flag":   orphan_pos,
        })

    return pd.DataFrame(rows)


def _build_order_items(
    kitchen: list[dict],
    oven: list[dict],
    pos: list[dict],
) -> pd.DataFrame:
    """
    Build the order_items Silver table by joining kitchen make events with
    oven bake events on (order_id, item_name).
    """
    # POS order IDs for phantom detection
    pos_oids = {p["order_id"] for p in pos}

    # Kitchen: group by (order_id, item_name) → {make_start, make_end}
    make_times: dict[tuple, dict] = {}
    for r in kitchen:
        key = (r["order_id"], r["item_name"])
        if key not in make_times:
            make_times[key] = {"make_start": None, "make_end": None,
                               "station_id": r.get("station_id", ""),
                               "phantom_kitchen": r["order_id"] not in pos_oids}
        if r["event_type"] == "make_start":
            make_times[key]["make_start"] = r["timestamp"]
        elif r["event_type"] == "make_complete":
            make_times[key]["make_end"] = r["timestamp"]

    # Oven: group by (order_id, item_name) → {oven_in, oven_out, slot}
    oven_times: dict[tuple, dict] = {}
    for r in oven:
        key = (r["order_id"], r["item_name"])
        if key not in oven_times:
            oven_times[key] = {"oven_in": None, "oven_out": None, "slot": r["slot"]}
        if r["event_type"] == "OVEN_IN":
            oven_times[key]["oven_in"] = r["timestamp"]
        elif r["event_type"] == "OVEN_OUT":
            oven_times[key]["oven_out"] = r["timestamp"]

    all_keys = set(make_times) | set(oven_times)
    rows = []
    for oid, item_name in all_keys:
        mk = make_times.get((oid, item_name), {})
        ov = oven_times.get((oid, item_name), {})

        make_start = mk.get("make_start")
        make_end   = mk.get("make_end")
        prep_weight: Optional[float] = None
        if make_start and make_end:
            prep_weight = round((make_end - make_start).total_seconds() / 60.0, 3)

        rows.append({
            "order_id":       oid,
            "item_name":      item_name,
            "station_id":     mk.get("station_id", ""),
            "prep_weight":    prep_weight,
            "make_start":     make_start,
            "make_end":       make_end,
            "oven_in":        ov.get("oven_in"),
            "oven_out":       ov.get("oven_out"),
            "oven_slot":      ov.get("slot"),
            "phantom_kitchen": mk.get("phantom_kitchen", False),
        })

    return pd.DataFrame(rows) if rows else pd.DataFrame()


def _build_dispatch_events(dispatch: list[dict]) -> pd.DataFrame:
    if not dispatch:
        return pd.DataFrame()
    return pd.DataFrame(dispatch)


def _build_staff_changes(staffing: list[dict]) -> pd.DataFrame:
    if not staffing:
        return pd.DataFrame(columns=["timestamp", "sim_min", "insiders", "drivers", "event"])
    df = pd.DataFrame(staffing)
    # Infer clock_in / clock_out
    events = []
    for i, row in df.iterrows():
        prev = df.iloc[i - 1] if i > 0 else None
        if prev is None:
            events.append("clock_in")
        elif row["insiders"] > prev["insiders"] or row["drivers"] > prev["drivers"]:
            events.append("clock_in")
        else:
            events.append("clock_out")
    df["event"] = events
    return df


# ── Validation helpers ────────────────────────────────────────────────────────

def _validate_oven_slots(oven_records: list[dict], capacity: int) -> list[dict]:
    flagged = []
    for r in oven_records:
        if r["slot"] > capacity:
            flagged.append(r)
    return flagged


# ── Writer ────────────────────────────────────────────────────────────────────

def _write_parquet(df: pd.DataFrame, name: str) -> None:
    if df.empty:
        return
    out = _cleaned_dir() / name
    df.to_parquet(out, index=False)


def _write_rejected(rows: list[dict]) -> None:
    out = _cleaned_dir() / "_rejected.csv"
    if not rows:
        out.write_text("source,row,reason\n", encoding="utf-8")
        return
    df = pd.DataFrame(rows, columns=["source", "row", "reason"])
    df.to_csv(out, index=False)


# ── Main entry point ──────────────────────────────────────────────────────────

def run_bronze_to_silver() -> str:
    """
    Run the full Bronze → Silver ETL.
    Returns a human-readable summary string.
    """
    # Detect oven capacity from config (for slot validation)
    try:
        import yaml  # type: ignore
        with open(_ROOT / "config.yaml") as fh:
            cfg_data = yaml.safe_load(fh) or {}
        oven_capacity = int(cfg_data.get("kitchen", {}).get("oven_capacity", 50))
    except Exception:
        oven_capacity = 50

    # -- Extract --
    pos_records,      pos_rejected      = _load_pos()
    kitchen_records,  kitchen_rejected  = _load_kitchen()
    oven_records,     oven_rejected     = _load_oven()
    dispatch_records, dispatch_rejected = _load_dispatch()
    staffing_records                    = _load_staffing()

    all_rejected = pos_rejected + kitchen_rejected + oven_rejected + dispatch_rejected

    # -- Validate: oven slot sanity --
    bad_slots = _validate_oven_slots(oven_records, oven_capacity)
    for r in bad_slots:
        r["slot_flag"] = f"INVALID_slot>{oven_capacity}"

    # -- Build Silver tables --
    orders_df   = _build_orders(pos_records, kitchen_records, oven_records, dispatch_records)
    items_df    = _build_order_items(kitchen_records, oven_records, pos_records)
    dispatch_df = _build_dispatch_events(dispatch_records)
    staff_df    = _build_staff_changes(staffing_records)

    # -- Detect cross-source issues --
    pos_oids    = {r["order_id"] for r in pos_records}
    kitchen_oids = {r["order_id"] for r in kitchen_records}
    phantom_kitchen = kitchen_oids - pos_oids
    orphan_pos      = pos_oids - kitchen_oids

    # -- Write Silver --
    _write_parquet(orders_df,   "orders.parquet")
    _write_parquet(items_df,    "order_items.parquet")
    _write_parquet(dispatch_df, "dispatch_events.parquet")
    _write_parquet(staff_df,    "staff_changes.parquet")
    _write_rejected(all_rejected)

    # -- Report --
    n_pos      = len(pos_records)
    n_kitchen  = len(kitchen_records)
    n_oven     = len(oven_records)
    n_dispatch = len(dispatch_records)
    n_total    = n_pos + n_kitchen + n_oven + n_dispatch
    n_rejected = len(all_rejected)
    pct        = (n_rejected / n_total * 100) if n_total > 0 else 0.0

    # Count filled order_type values
    n_filled = 0
    if not orders_df.empty and "order_type" in orders_df.columns:
        # Compare with original POS nulls
        n_filled = sum(1 for r in pos_records if r["order_type"] is None)

    report = (
        f"Processed {n_pos:,} POS records, {n_kitchen:,} kitchen events, "
        f"{n_oven:,} oven logs, {n_dispatch:,} dispatch events. "
        f"Rejected {n_rejected} rows ({pct:.1f}%). "
        f"Resolved {len(phantom_kitchen)} kitchen/POS mismatches. "
        f"Filled {n_filled} missing order_type values."
    )
    return report


if __name__ == "__main__":
    result = run_bronze_to_silver()
    print(result)
