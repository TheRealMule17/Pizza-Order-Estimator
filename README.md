# Pizza Order Estimator

A real-time simulation that exposes a fundamental flaw in how pizza chains estimate delivery times — and shows a better way.

---

## Why This Matters

Most pizza delivery systems calculate your ETA using a rolling average of recent order completion times. This works fine when traffic is steady, but completely breaks down during a dinner rush: every customer gets "20 minutes" even as dozens of orders pile up simultaneously, and a 6-pizza catering order gets the same estimate as a single pie. The result is a wave of angry customers who all expected 20 minutes and waited 45.

This project simulates a pizza shop's full order pipeline and compares the naive legacy approach against a queue-aware dynamic estimator — side by side, in real time.

---

## Architecture

```
pizza-order-estimator/
├── config.yaml              Tunable simulation parameters (edit this, not the code)
├── src/
│   ├── config.py            Config loader — reads config.yaml, provides typed `cfg` object
│   ├── models.py            Data classes: Pizza, Order, KitchenState
│   ├── estimators.py        NaiveEstimator (Model A) + DynamicEstimator (Model B)
│   ├── simulation.py        Order generator, make-line, oven, accuracy tracker
│   ├── app.py               Flask app + REST API endpoints
│   ├── dashboard.py         Rich TUI panels and layout
│   ├── main.py              Entry point — web UI by default, --tui for terminal
│   └── static/
│       ├── index.html       Single-page dashboard
│       ├── style.css        Dark theme with Domino's accent colours
│       └── app.js           Polling logic and DOM updates
└── tests/
    └── test_estimators.py   Unit tests for both estimation models
```

| Module | Responsibility |
|---|---|
| `config.py` | Loads `config.yaml` at startup; merges with built-in defaults so the simulation runs even if the file is missing. |
| `models.py` | Pure data — no logic. Defines `Pizza`, `Order`, `OrderStatus`, `KitchenState`. |
| `estimators.py` | Two isolated estimator classes. `NaiveEstimator` tracks a rolling average. `DynamicEstimator` projects the pipeline by simulating station and oven scheduling. |
| `simulation.py` | Runs in a background thread. Ticks a simulated clock forward, generates orders, advances pizzas through make-line → oven → delivery stages, records accuracy. |
| `app.py` | Flask app with five API endpoints. The simulation runs in a background thread; Flask serves requests from the main thread. |
| `dashboard.py` | Stateless render functions. Given a snapshot dict from `Simulation`, builds a Rich `Layout` for the TUI. |
| `main.py` | Wires everything together. Defaults to the web UI; pass `--tui` for the terminal version. |

---

## How to Install and Run

```bash
# 1. Clone and enter the repo
git clone <your-repo-url>
cd pizza-order-estimator

# 2. Install dependencies
pip install -r requirements.txt

# 3a. Run the web UI (default)
python -m src.main
# → open http://127.0.0.1:5000/

# 3b. Or run the original Rich terminal dashboard
python -m src.main --tui
```

### Web UI controls

| Button | Action |
|--------|--------|
| **Start** | Begin the simulation |
| **Stop** | Pause (state preserved) |
| **Reset** | Clear all orders and metrics |
| **Rush** | Trigger a ~3 sim-minute order flood |

### TUI keyboard controls

| Key | Action |
|-----|--------|
| `R` | Toggle rush mode |
| `Q` | Quit |

---

## Configuration

All simulation parameters live in `config.yaml` at the project root.
Edit that file and restart — no code changes needed.

```yaml
simulation:
  time_scale: 60          # 1 real second = 1 simulated minute

kitchen:
  make_line_stations: 3   # parallel prep stations
  oven_capacity: 8        # max pizzas baking simultaneously
  oven_time: 8.0          # minutes each pizza bakes

delivery:
  delivery_time: 10.0     # extra minutes for delivery orders
  delivery_ratio: 0.5     # fraction of orders that are delivery

orders:
  normal_interval_min: 30 # sim-seconds between orders (normal)
  normal_interval_max: 90
  rush_interval_min: 5    # sim-seconds between orders (rush)
  rush_interval_max: 15
  rush_duration: 180      # how long a rush lasts (sim-seconds)
  pizza_weights:          # probability weights for order size
    1: 35
    2: 30
    3: 15
    4: 10
    5: 5
    6: 3
    7: 2

estimator:
  naive_window: 10        # rolling-average window size
```

---

## How It Works

### Estimation models

**Naive (Legacy):** Keeps a rolling window of the last N completed order durations and returns that average for every new order — regardless of queue depth, order size, or current kitchen load.

**Dynamic:** Projects the actual pipeline by simulating when each pizza in the incoming order will clear the make line and oven, accounting for all orders already ahead of it.

### What to look for

- During **normal traffic**, both models perform similarly — the queue is short and the rolling average stays calibrated.
- After pressing **Rush**, orders flood in faster than the kitchen can process them. Watch the Naive model freeze on its last cached average while the Dynamic model's estimates climb in real time as it detects the growing backlog. Naive MAE will spike; Dynamic MAE stays comparatively stable.
- Large orders almost always get underestimated by the Naive model — Dynamic accounts for the extra make time and the serialisation cost when stations are busy.
- The **Accuracy** panel tracks MAE, average signed error (bias), and win count so you can see the aggregate picture across hundreds of orders.

---

## API Reference

| Method | Path | Description |
|--------|------|-------------|
| `GET`  | `/` | Serves the web dashboard |
| `POST` | `/api/start` | Start (or resume) the simulation |
| `POST` | `/api/stop` | Pause the simulation |
| `POST` | `/api/reset` | Clear all state and start fresh |
| `POST` | `/api/rush` | Trigger a rush period |
| `GET`  | `/api/state` | Full current state as JSON (polled every 1 s) |

---

## Data Logging

Every completed order is written to a timestamped CSV in `logs/`. The directory is created automatically on first run.

### Log files

| File | Description |
|------|-------------|
| `logs/order_log_<timestamp>.csv` | One row per completed order |
| `logs/order_log_<timestamp>.parquet` | Parquet version (written on stop/reset) |
| `logs/summary_<timestamp>.txt` | Plain-text run summary (written on stop/reset) |

Each run creates a new set of files (timestamp = wall-clock time at startup), so previous runs are never overwritten.

### CSV columns

| Column | Description |
|--------|-------------|
| `order_id` | Unique 8-character order ID |
| `order_type` | `carryout` or `delivery` |
| `num_items` | Number of pizzas in the order |
| `total_prep_weight` | Sum of make-line durations across all pizzas (sim-minutes) |
| `item_details` | JSON array — each pizza's name, prep weight, and price |
| `placed_at` | Sim-time the order was placed (minutes from sim start) |
| `completed_at` | Sim-time the order completed (oven exit for carryout, drop-off for delivery) |
| `actual_time_min` | Total wait from placement to completion (sim-minutes) |
| `naive_estimate_min` | Naive model's prediction at placement |
| `dynamic_estimate_min` | Dynamic model's prediction at placement |
| `naive_error_min` | `naive_estimate − actual` (positive = overestimate) |
| `dynamic_error_min` | `dynamic_estimate − actual` |
| `queue_depth_at_placement` | Orders in queue when this order was placed |
| `active_workers_at_placement` | Make-line workers busy at placement |
| `available_drivers_at_placement` | Free drivers at placement (delivery context) |
| `rush_active_at_placement` | `True` if a rush was running when order was placed |
| `driver_wait_min` | Minutes the order waited for a driver after exiting the oven (0 for carryouts) |
| `driver_id` | Driver that handled the delivery (`null` for carryouts) |
| `order_revenue` | Total revenue for the order based on pricing config |

### Converting to Parquet manually

```python
from src.logger import export_to_parquet
export_to_parquet("logs/order_log_2026-04-06_14-30-00.csv")
```

### Summary report format

```
=== Pizza Order Estimator — Run Summary ===
Run Duration: 2.4 simulated hours
Total Orders Completed: 147

--- Wait Times ---
Average Wait (Carryout): 11.2 min
Average Wait (Delivery): 22.8 min
Orders Over 20 min: 34 (23.1%)
...
--- Financials ---
Total Revenue: $1,842.00
SPLH: $87.71
Labor Cost %: 10.9%
```

---

## Pricing

Pizza prices are set in `config.yaml` under `pricing` and `pricing_tiers`:

| Type | Default price | Which pizzas |
|------|--------------|--------------|
| Normal | $10.00 | Cheese, Pepperoni, Sausage |
| Specialty | $15.00 | Veggie, Supreme |
| Custom | $10.00 + $1.00/topping | Random topping selections |

Labor cost tracking uses `labor.insider_hourly_wage` and `labor.driver_hourly_wage`. All workers and all drivers are costed for the full simulated duration (whether idle or not), matching real food-service accounting.

---

## Analytics (`/api/state → analytics`)

The `/api/state` response includes a live `analytics` object:

```json
{
  "analytics": {
    "orders_over_45_min": 2,
    "orders_over_30_min": 12,
    "orders_over_20_min": 34,
    "longest_wait_min": 48.3,
    "longest_wait_order_id": "A1B2C3D4",
    "avg_wait_carryout_min": 11.2,
    "avg_wait_delivery_min": 22.8,
    "total_revenue": 1842.00,
    "total_labor_cost": 201.60,
    "splh": 87.71,
    "labor_cost_percentage": 10.9,
    "naive_mae": 7.8,
    "dynamic_mae": 2.4,
    "naive_wins": 35,
    "dynamic_wins": 112,
    "orders_placed_during_rush": 63,
    "avg_wait_during_rush": 19.4,
    "avg_wait_outside_rush": 10.1
  }
}
```

---

## Data Pipeline

The simulator implements a **Medallion (Bronze → Silver → Gold)** architecture that captures realistic raw data streams, cleans them, and produces structured analytics tables — mirroring how a real restaurant data stack works.

The pipeline runs automatically in a background thread when the simulation stops (after a Full Day run or manual stop). It can also be run independently after any simulation session.

---

### Bronze — Raw Data Sources

Five raw data streams are written to `data/raw/` during the simulation, each emulating a real store system with its own format quirks and failure modes:

| Source | Directory | Format | Realistic quirks |
|--------|-----------|--------|-----------------|
| **POS** | `data/raw/pos/` | JSONL | American timestamps (`MM/DD/YYYY HH:MM:SS AM/PM`), `"$12.50"` price strings, 5% null `order_type`, case-mangled item names, 2% duplicate records |
| **Kitchen Display** | `data/raw/kitchen/` | CSV | Unix epoch timestamps, mangled order refs (`ord_`, lowercase), 3% missing `make_complete` events, phantom rows for ghost orders |
| **Oven Sensor** | `data/raw/oven/` | `.log` | ISO 8601 timestamps (40% missing timezone suffix), 1% corrupted lines with junk characters, 20% abbreviated pizza names, 1% impossible slot numbers |
| **Driver Dispatch** | `data/raw/dispatch/` | JSON array | ISO 8601+ms timestamps, driver IDs as integers 50% of the time, 2% out-of-order events, 5% missing driver-return events |
| **Staffing** | `data/raw/staffing/` | CSV | Clean — records each `set_insiders`/`set_drivers` change with wall timestamp and simulated minute offset |

All noise probabilities scale with the `pipeline.noise_level` setting (`none`, `normal`, `heavy`). Heavy mode multiplies all error rates by 2.5×.

---

### Silver — Cleaned & Joined Tables

```bash
python -m src.pipeline.bronze_to_silver
```

Reads all raw files from `data/raw/` and writes four clean Parquet tables to `data/cleaned/`:

| File | Contents |
|------|----------|
| `orders.parquet` | One row per order: normalized ID, order type, timestamps, wait times, naive and dynamic estimates, total price |
| `order_items.parquet` | One row per pizza: order ID, item ID, pizza type, make duration |
| `dispatch_events.parquet` | Driver assignments and deliveries: normalized order and driver IDs, event type, timestamp |
| `staff_changes.parquet` | Insider and driver counts at each staffing change, with wall and sim timestamps |

Key cleaning steps:

- **Order-ID normalization** — strips `ORD-`, `ord_`, `ORD_` prefixes and uppercases across all sources so records can be joined
- **POS deduplication** — exact duplicates on `(order_id, timestamp_string)` are dropped; only the first is kept
- **Phantom row rejection** — kitchen rows with `order_ref` starting with `"ghost_"` are flagged and excluded
- **Timestamp unification** — American POS strings, Unix epochs (kitchen), and ISO 8601 ±timezone (oven/dispatch) are all parsed to UTC `datetime`
- **Orphan detection** — items and dispatch events with no matching order are logged as rejected
- **Rejected rows** — written to `data/cleaned/_rejected.csv` with a `reason` column for auditability

**Example pipeline report output:**

```
[pipeline] Processed 318 POS records, 1271 kitchen events, 3 oven logs, 1 dispatch files → 312 orders, 1247 items, 284 dispatch events, 9 staff changes | rejected: 42
```

---

### Gold — Business Analytics Tables

```bash
python -m src.pipeline.silver_to_gold
```

Reads the Silver Parquet tables and writes four aggregated analytics tables to `data/analytics/`:

| File | Contents |
|------|----------|
| `hourly_summary.parquet` | One row per simulated hour: order count, avg wait by type, revenue, labor cost, SPLH, staffing levels, naive vs dynamic MAE |
| `estimation_accuracy.parquet` | One row per completed order: both estimates, actual wait, signed errors, and a `winner` column (`naive`/`dynamic`/`tie`) |
| `bottleneck_log.parquet` | Bottleneck events written by the simulation (make-line full, oven full, no drivers, extreme waits) |
| `daily_kpis.parquet` | Single-row headline table: total orders, revenue, labor cost, SPLH, labor cost %, overall MAE for both models, peak hour, worst bottleneck hour |

---

### Configuration

```yaml
pipeline:
  emit_raw_data:      true         # set false to skip all raw file writing
  raw_data_dir:       "data/raw"
  cleaned_data_dir:   "data/cleaned"
  analytics_data_dir: "data/analytics"
  noise_level:        "normal"     # "none" | "normal" | "heavy"
```

---

## Analytics Dashboard

A separate Streamlit app that reads the pipeline output and turns it into an interactive post-run analysis tool. It never writes to the data directories — it's purely read-only.

### Launching the dashboard

```bash
# Requires a completed simulation run with pipeline output in data/cleaned/ and data/analytics/
streamlit run src/dashboard/app.py
```

### Pages

| Page | What it shows |
|------|---------------|
| **Overview** | 8 headline KPI cards (total orders, revenue, SPLH, labor cost %, avg waits, MAE for both models), hourly order volume + wait time on a dual-axis chart, hourly revenue vs. labor cost |
| **Estimation Accuracy** | Per-order error scatter plot (dynamic vs. naive), overlapping error distributions, cumulative win-rate line chart; filterable by order type and traffic block |
| **Bottlenecks** | Event counts by type × traffic block, timeline scatter of when events clustered, staffing step chart with bottleneck markers, and an auto-generated worst-1-hour analysis with a plain-text recommendation |
| **Data Quality** | Pipeline summary cards (raw processed, accepted, rejected, rejection rate, order ID match rate), rejections by source (donut), top rejection reasons (bar), sample rejected rows table, and a data source consistency matrix showing orphaned/phantom records |
| **Raw Data Explorer** | Browse and download any Silver or Gold Parquet table as a sortable, filterable Streamlit dataframe |

### Requirements

- Run a full-day simulation and the pipeline first:
  ```bash
  python -m src.pipeline.bronze_to_silver
  python -m src.pipeline.silver_to_gold
  ```
- If no data exists the dashboard shows a friendly message on each page rather than crashing.

### Screenshots

*Overview — KPI cards and hourly charts*
`[screenshot placeholder]`

*Estimation Accuracy — scatter plot and distribution comparison*
`[screenshot placeholder]`

*Data Quality — rejection analysis and consistency matrix*
`[screenshot placeholder]`

> **Selling point:** The Data Quality page makes the Medallion pipeline work visible — it shows exactly how many records were cleaned, what was rejected, and why, with a sample of the raw rejected rows. This is the kind of observability that separates a toy pipeline from a production-grade one.

---

## Tech Stack

- **Python 3.10+**
- **[Flask](https://flask.palletsprojects.com/)** — web server and REST API
- **[Rich](https://github.com/Textualize/rich)** — optional terminal UI (`--tui` mode)
- **[PyYAML](https://pyyaml.org/)** — config file parsing
- **[pandas](https://pandas.pydata.org/) + [pyarrow](https://arrow.apache.org/docs/python/)** — CSV/Parquet log export
- **Vanilla HTML/CSS/JS** — no build tools, no frameworks
- **threading** — simulation loop and (in TUI mode) keyboard input run as daemon threads
