"""
Rich TUI dashboard.

Renders a live terminal interface showing:
  - Kitchen status (make line + oven)
  - Order queue summary
  - Latest order with both estimates
  - Running accuracy comparison
  - Throughput and rush indicator
"""

from __future__ import annotations

from typing import Optional

from rich.columns import Columns
from rich.console import Console
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.progress_bar import ProgressBar
from rich.table import Table
from rich.text import Text

from .models import Order, OrderStatus, OrderType
from .simulation import AccuracyTracker, Simulation

# ──────────────────────────────────────────────────────────────────────────────
# Colour palette
# ──────────────────────────────────────────────────────────────────────────────

C_TITLE   = "bold white"
C_LABEL   = "dim white"
C_VALUE   = "bold cyan"
C_GOOD    = "bold green"
C_WARN    = "bold yellow"
C_BAD     = "bold red"
C_RUSH    = "bold red on dark_red"
C_NORMAL  = "bold green on dark_green"


def _fmt_min(minutes: Optional[float], *, show_sign: bool = False) -> str:
    """Format a float minute value for display."""
    if minutes is None:
        return "[dim]—[/]"
    sign = "+" if (show_sign and minutes > 0) else ""
    return f"{sign}{minutes:.1f} min"


def _error_style(err: Optional[float]) -> str:
    if err is None:
        return C_LABEL
    if abs(err) < 2:
        return C_GOOD
    if abs(err) < 5:
        return C_WARN
    return C_BAD


# ──────────────────────────────────────────────────────────────────────────────
# Panel builders
# ──────────────────────────────────────────────────────────────────────────────

def _kitchen_panel(snap: dict) -> Panel:
    """Visualise make-line stations and oven occupancy."""
    grid = Table.grid(padding=(0, 1))
    grid.add_column(style=C_LABEL, width=20)
    grid.add_column()

    busy   = snap["make_line_busy"]
    total  = snap["make_line_total"]
    # Build a block-character bar for the make line
    make_bar = ("█" * busy) + ("░" * (total - busy))
    make_label = f"[{C_VALUE}]{busy}[/]/[{C_VALUE}]{total}[/] busy"
    grid.add_row("Workers:", f"[yellow]{make_bar}[/]  {make_label}")

    # Make queue depth
    mq = snap["make_queue_depth"]
    grid.add_row("Make queue:", f"[{C_VALUE}]{mq}[/] pizza(s) waiting")

    # Oven
    ov_active = snap["oven_active"]
    ov_cap    = snap["oven_capacity"]
    ov_wait   = snap["oven_waiting"]
    ov_bar    = ("🍕" * ov_active) + ("▫️" * (ov_cap - ov_active))
    ov_label  = f"[{C_VALUE}]{ov_active}[/]/[{C_VALUE}]{ov_cap}[/] slots"
    grid.add_row("Oven:", f"{ov_bar}  {ov_label}")
    if ov_wait:
        grid.add_row("Oven backlog:", f"[{C_WARN}]{ov_wait}[/] pizza(s) waiting")

    return Panel(grid, title="[bold]Kitchen Status[/]", border_style="yellow")


def _queue_panel(snap: dict) -> Panel:
    """Show order counts by stage."""
    grid = Table.grid(padding=(0, 2))
    grid.add_column(style=C_LABEL)
    grid.add_column(style=C_VALUE, justify="right")

    grid.add_row("Waiting (queued):",     str(snap["queue_waiting"]))
    grid.add_row("In progress:",          str(snap["queue_in_progress"]))
    grid.add_row("Out for delivery:",     str(snap["queue_delivery"]))
    grid.add_row("──────────────────", "")
    grid.add_row("Total active:",         str(snap["orders_in_queue"]))
    grid.add_row("Total completed:",      str(snap["completed_orders"]))
    grid.add_row("Throughput:",           f"{snap['throughput']:.2f} ord/min")

    return Panel(grid, title="[bold]Order Queue[/]", border_style="blue")


def _rush_panel(rush_active: bool) -> Panel:
    if rush_active:
        content = Text("  🚨 RUSH MODE ACTIVE  ", style=C_RUSH, justify="center")
    else:
        content = Text("  ✅ Normal traffic  ", style=C_NORMAL, justify="center")
    return Panel(content, title="[bold]Status[/]", border_style="red" if rush_active else "green")


def _estimates_panel(recent_orders: list[Order]) -> Panel:
    """Table of recent orders showing both estimates vs. actual."""
    tbl = Table(
        show_header=True,
        header_style="bold magenta",
        border_style="dim",
        expand=True,
    )
    tbl.add_column("ID",       style="bold", width=9)
    tbl.add_column("Type",     width=9)
    tbl.add_column("Pizzas",   justify="center", width=7)
    tbl.add_column("Naive est", justify="right", width=11)
    tbl.add_column("Dynamic est", justify="right", width=13)
    tbl.add_column("Actual",   justify="right", width=9)
    tbl.add_column("Naive err", justify="right", width=10)
    tbl.add_column("Dyn err",  justify="right", width=9)
    tbl.add_column("Winner",   width=9)

    # Show most-recent first, up to 15 rows
    for order in reversed(recent_orders[-15:]):
        actual   = order.actual_duration
        n_err    = order.naive_error
        d_err    = order.dynamic_error

        if actual is not None and n_err is not None and d_err is not None:
            winner = (
                "[green]Dynamic[/]" if abs(d_err) < abs(n_err)
                else ("[red]Naive[/]"   if abs(n_err) < abs(d_err)
                      else "[dim]Tie[/]")
            )
        else:
            winner = "[dim]…[/]"

        type_str  = "🛵 Deliv" if order.order_type == OrderType.DELIVERY else "🏠 Carry"
        n_err_str = f"[{_error_style(n_err)}]{_fmt_min(n_err, show_sign=True)}[/]" if n_err is not None else "[dim]…[/]"
        d_err_str = f"[{_error_style(d_err)}]{_fmt_min(d_err, show_sign=True)}[/]" if d_err is not None else "[dim]…[/]"

        tbl.add_row(
            order.order_id,
            type_str,
            str(order.num_pizzas),
            _fmt_min(order.naive_estimate),
            _fmt_min(order.dynamic_estimate),
            _fmt_min(actual) if actual else "[dim]…[/]",
            n_err_str,
            d_err_str,
            winner,
        )

    return Panel(tbl, title="[bold]Recent Orders — Estimate vs. Actual[/]", border_style="magenta")


def _accuracy_panel(acc: AccuracyTracker) -> Panel:
    """Side-by-side accuracy stats for both models."""
    grid = Table.grid(padding=(0, 3))
    grid.add_column(style=C_LABEL, width=22)
    grid.add_column(justify="right", width=12)
    grid.add_column(justify="right", width=12)

    grid.add_row("", "[bold cyan]Naive[/]", "[bold cyan]Dynamic[/]")
    grid.add_row("─" * 22, "─" * 12, "─" * 12)

    def fmt_stat(val: Optional[float]) -> str:
        if val is None:
            return "[dim]—[/]"
        style = _error_style(val)
        return f"[{style}]{val:+.2f} min[/]"

    grid.add_row("MAE:",
                 fmt_stat(acc.naive_mae),
                 fmt_stat(acc.dynamic_mae))
    grid.add_row("Avg error (bias):",
                 fmt_stat(acc.naive_avg_error),
                 fmt_stat(acc.dynamic_avg_error))
    grid.add_row("Wins (closer):",
                 f"[green]{acc.naive_wins}[/]",
                 f"[green]{acc.dynamic_wins}[/]")
    grid.add_row("Samples:",
                 f"[{C_VALUE}]{acc.sample_count}[/]",
                 "")

    return Panel(grid, title="[bold]Accuracy Metrics[/]", border_style="cyan")


def _help_panel() -> Panel:
    txt = Text()
    txt.append("  [R] ", style="bold yellow")
    txt.append("Toggle rush mode    ")
    txt.append("  [Q] ", style="bold yellow")
    txt.append("Quit")
    return Panel(txt, title="[bold]Controls[/]", border_style="dim")


# ──────────────────────────────────────────────────────────────────────────────
# Main render function
# ──────────────────────────────────────────────────────────────────────────────

def build_layout(sim: Simulation) -> Layout:
    """Compose the full-screen layout from the simulation snapshot."""
    snap = sim.snapshot()

    layout = Layout()
    layout.split_column(
        Layout(name="header", size=3),
        Layout(name="top",    size=9),
        Layout(name="orders", minimum_size=10),
        Layout(name="bottom", size=10),
        Layout(name="help",   size=3),
    )

    # ── header ──────────────────────────────────────────────────────────────
    now_min = snap["now_min"]
    header_txt = Text(justify="center")
    header_txt.append("🍕  Pizza Order Estimator  🍕", style="bold white")
    header_txt.append(
        f"   sim-time: {now_min:.1f} min ({now_min/60:.2f} hrs)",
        style="dim white",
    )
    layout["header"].update(Panel(header_txt, border_style="dim"))

    # ── top row: kitchen | queue | rush ─────────────────────────────────────
    layout["top"].split_row(
        Layout(_kitchen_panel(snap), name="kitchen"),
        Layout(_queue_panel(snap),   name="queue"),
        Layout(_rush_panel(snap["rush_active"]), name="rush", size=24),
    )

    # ── orders table ─────────────────────────────────────────────────────────
    layout["orders"].update(_estimates_panel(snap["recent_orders"]))

    # ── bottom: accuracy stats ───────────────────────────────────────────────
    layout["bottom"].update(_accuracy_panel(snap["accuracy"]))

    # ── help bar ─────────────────────────────────────────────────────────────
    layout["help"].update(_help_panel())

    return layout
