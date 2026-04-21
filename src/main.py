"""
Entry point for the Pizza Order Estimator.

Default mode: launches the Flask web UI (open http://127.0.0.1:5000/).
Use --tui to run the original Rich terminal dashboard instead.
"""

from __future__ import annotations

import argparse
import sys
import threading
import time

# ──────────────────────────────────────────────────────────────────────────────
# TUI helpers (only used in --tui mode)
# ──────────────────────────────────────────────────────────────────────────────

def _get_key_reader():
    """
    Return a function that blocks until a single keypress is available and
    returns the character.  Works on both Windows and Unix.
    """
    if sys.platform == "win32":
        import msvcrt

        def _read() -> str:
            ch = msvcrt.getwch()
            if ch in ("\x00", "\xe0"):
                msvcrt.getwch()  # discard scan code for special keys
                return ""
            return ch.lower()

    else:
        import tty
        import termios

        def _read() -> str:
            fd = sys.stdin.fileno()
            old = termios.tcgetattr(fd)
            try:
                tty.setraw(fd)
                ch = sys.stdin.read(1)
            finally:
                termios.tcsetattr(fd, termios.TCSADRAIN, old)
            return ch.lower()

    return _read


def _input_loop(sim, stop_event: threading.Event) -> None:
    """Daemon thread that reads keypresses and acts on them."""
    read_key = _get_key_reader()
    while not stop_event.is_set():
        try:
            ch = read_key()
        except Exception:
            break
        if ch == "r":
            sim.trigger_rush()
        elif ch in ("q", "\x03"):  # q or Ctrl-C
            stop_event.set()
            break


def run_tui(
    time_scale: float | None = None,
    num_workers: int | None = None,
    oven_capacity: int | None = None,
    refresh_rate: int = 4,
) -> None:
    """
    Launch the simulation and Rich terminal UI.

    All parameters default to values from config.yaml when not explicitly
    provided.
    """
    from rich.console import Console
    from rich.live import Live

    from .config import cfg
    from .dashboard import build_layout
    from .simulation import Simulation

    time_scale    = time_scale    if time_scale    is not None else cfg.time_scale
    num_workers   = num_workers   if num_workers   is not None else cfg.workers
    oven_capacity = oven_capacity if oven_capacity is not None else cfg.oven_capacity

    console = Console()
    sim = Simulation(
        time_scale=time_scale,
        num_workers=num_workers,
        oven_capacity=oven_capacity,
    )
    sim.start()

    stop_event = threading.Event()

    input_thread = threading.Thread(
        target=_input_loop,
        args=(sim, stop_event),
        daemon=True,
    )
    input_thread.start()

    try:
        with Live(
            build_layout(sim),
            console=console,
            refresh_per_second=refresh_rate,
            screen=True,
        ) as live:
            while not stop_event.is_set():
                live.update(build_layout(sim))
                time.sleep(1 / refresh_rate)
    except KeyboardInterrupt:
        pass
    finally:
        sim.stop()
        console.print("\n[bold green]Simulation stopped.[/]")


# Keep old name as alias for backwards compatibility
run = run_tui


# ──────────────────────────────────────────────────────────────────────────────
# Entry point
# ──────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Pizza Order Estimator — real-time queue-aware ETA comparison"
    )
    parser.add_argument(
        "--tui",
        action="store_true",
        help="Run the Rich terminal dashboard instead of the web UI",
    )
    parser.add_argument(
        "--host",
        default="127.0.0.1",
        help="Host for the web server (default: 127.0.0.1)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=5000,
        help="Port for the web server (default: 5000)",
    )
    args = parser.parse_args()

    if args.tui:
        run_tui()
    else:
        from .app import run_web
        run_web(host=args.host, port=args.port)
