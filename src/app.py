"""
Flask web application for the Pizza Order Estimator.

Serves the frontend and exposes a small REST API that the browser polls
every second to update the live dashboard.
"""

from __future__ import annotations

import os

from typing import Optional

from flask import Flask, jsonify, send_from_directory

from .config import cfg
from .day_simulator import DaySimulator
from .simulation import Simulation

# ── App setup ─────────────────────────────────────────────────────────────────

_STATIC_DIR = os.path.join(os.path.dirname(__file__), "static")

app = Flask(__name__, static_folder=_STATIC_DIR, static_url_path="/static")
app.config["JSON_SORT_KEYS"] = False


def _make_sim() -> Simulation:
    return Simulation(
        time_scale=cfg.time_scale,
        num_workers=cfg.workers,
        oven_capacity=cfg.oven_capacity,
        naive_window=cfg.naive_window,
    )


# Module-level simulation instance (replaced on reset)
_sim: Simulation = _make_sim()
_day_sim: Optional[DaySimulator] = None


# ── Static page ───────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return send_from_directory(_STATIC_DIR, "index.html")


# ── API endpoints ─────────────────────────────────────────────────────────────

@app.route("/api/start", methods=["POST"])
def api_start():
    _sim.start()
    return jsonify({"status": "started"})


@app.route("/api/stop", methods=["POST"])
def api_stop():
    _sim.stop()
    return jsonify({"status": "stopped"})


@app.route("/api/reset", methods=["POST"])
def api_reset():
    global _sim
    _sim.reset()
    return jsonify({"status": "reset"})


@app.route("/api/rush", methods=["POST"])
def api_rush():
    _sim.trigger_rush()
    return jsonify({"status": "rush triggered"})


@app.route("/api/day-start", methods=["POST"])
def api_day_start():
    global _day_sim
    if _day_sim and _day_sim.is_running():
        return jsonify({"status": "already running"})
    _day_sim = DaySimulator(_sim)
    _day_sim.start()
    return jsonify({"status": "day started"})


@app.route("/api/day-stop", methods=["POST"])
def api_day_stop():
    global _day_sim
    if _day_sim:
        _day_sim.stop()
    return jsonify({"status": "day stopped"})


@app.route("/api/state", methods=["GET"])
def api_state():
    return jsonify(_sim.web_snapshot())


# ── Launch helper ─────────────────────────────────────────────────────────────

def run_web(host: str = "127.0.0.1", port: int = 5000) -> None:
    """Serve the Flask app. Simulation starts stopped — click Start to begin."""
    print(f"  Pizza Order Estimator → http://{host}:{port}/")
    app.run(host=host, port=port, debug=False, use_reloader=False)
