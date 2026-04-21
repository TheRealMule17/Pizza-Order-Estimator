import streamlit as st
import plotly.graph_objects as go
import pandas as pd
import numpy as np
from datetime import timedelta
from src.dashboard.utils import (
    load_gold, load_silver, no_data_banner,
    COLORS, assign_traffic_block, TRAFFIC_BLOCKS, format_hour,
)


def render():
    st.header("Estimation Accuracy")

    acc = load_gold("estimation_accuracy")
    if acc is None or acc.empty:
        no_data_banner()
        return

    # Join placed_at from orders to get time-of-day axis
    orders_df = load_silver("orders")
    if orders_df is not None and "placed_at" in orders_df.columns:
        acc = acc.merge(orders_df[["order_id", "placed_at"]], on="order_id", how="left")
        if pd.api.types.is_datetime64_any_dtype(acc["placed_at"]):
            acc["hour_int"] = acc["placed_at"].dt.hour
            acc = acc.sort_values("placed_at").reset_index(drop=True)
        else:
            acc["hour_int"] = 12
    else:
        acc["hour_int"] = 12

    acc["traffic_block"] = acc["hour_int"].apply(assign_traffic_block)

    # ── Sidebar filters ───────────────────────────────────────────────────────
    with st.sidebar:
        st.subheader("Filters")
        order_types = ["All"] + sorted(acc["order_type"].dropna().unique().tolist())
        selected_type = st.selectbox("Order Type", order_types, key="est_order_type")
        blocks = ["All"] + [b for b in TRAFFIC_BLOCKS if b in acc["traffic_block"].unique()]
        selected_block = st.selectbox("Traffic Block", blocks, key="est_traffic_block")

    df = acc.copy()
    if selected_type != "All":
        df = df[df["order_type"] == selected_type]
    if selected_block != "All":
        df = df[df["traffic_block"] == selected_block]

    if df.empty:
        st.info("No orders match the selected filters.")
        return

    # ── Summary cards ─────────────────────────────────────────────────────────
    dynamic_mae = df["dynamic_error"].abs().mean()
    naive_mae = df["naive_error"].abs().mean()
    total = len(df)
    dynamic_wins = (df["winner"] == "dynamic").sum()
    naive_wins = (df["winner"] == "naive").sum()
    dynamic_bias = df["dynamic_error"].mean()
    naive_bias = df["naive_error"].mean()

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Dynamic MAE", f"{dynamic_mae:.2f} min")
    c2.metric(
        "Naive MAE", f"{naive_mae:.2f} min",
        delta=f"{naive_mae - dynamic_mae:+.2f} vs Dynamic",
        delta_color="inverse",
    )
    c3.metric("Dynamic Wins", f"{dynamic_wins} ({100 * dynamic_wins / total:.0f}%)")
    c4.metric("Naive Wins", f"{naive_wins} ({100 * naive_wins / total:.0f}%)")

    c5, c6 = st.columns(2)
    c5.metric(
        "Dynamic Avg Signed Error", f"{dynamic_bias:+.2f} min",
        help="Positive = tends to overestimate; negative = underestimate",
    )
    c6.metric(
        "Naive Avg Signed Error", f"{naive_bias:+.2f} min",
        help="Positive = tends to overestimate; negative = underestimate",
    )

    st.divider()

    # ── Chart 1: Error over time (scatter) ────────────────────────────────────
    st.subheader("Estimation Error Over Time")

    # Build hourly tick labels spanning the full operating day (15 slots)
    has_ts = "placed_at" in df.columns and pd.api.types.is_datetime64_any_dtype(df["placed_at"])
    if has_ts:
        first_hour = df["placed_at"].min().floor("h")
        tick_hours = [first_hour + timedelta(hours=i) for i in range(15)]
        tickvals = tick_hours
        ticktext = [format_hour(t) for t in tick_hours]
        x_vals = df["placed_at"]
        xaxis_range = [tick_hours[0] - timedelta(minutes=15),
                       tick_hours[-1] + timedelta(minutes=15)]
    else:
        tickvals = ticktext = xaxis_range = None
        x_vals = df.index.to_series()

    fig1 = go.Figure()
    fig1.add_hline(y=0, line_dash="dash", line_color="gray", opacity=0.5)

    for col_name, color, label in [
        ("dynamic_error", COLORS["dynamic"], "Dynamic"),
        ("naive_error", COLORS["naive"], "Naive"),
    ]:
        fig1.add_trace(go.Scatter(
            x=x_vals,
            y=df[col_name],
            mode="markers",
            name=label,
            marker=dict(color=color, size=5, opacity=0.65),
        ))
        if len(df) > 2:
            x_idx = np.arange(len(df))
            try:
                z = np.polyfit(x_idx, df[col_name].fillna(0).values, 1)
                p = np.poly1d(z)
                fig1.add_trace(go.Scatter(
                    x=x_vals,
                    y=p(x_idx),
                    mode="lines",
                    name=f"{label} trend",
                    line=dict(color=color, dash="dot", width=1.5),
                    showlegend=False,
                ))
            except Exception:
                pass

    xaxis_cfg = dict(title_text="Time of Day")
    if tickvals:
        xaxis_cfg.update(
            tickmode="array",
            tickvals=tickvals,
            ticktext=ticktext,
            range=xaxis_range,
        )
    fig1.update_layout(
        template="plotly_dark",
        height=400,
        yaxis_title="Error (min) — positive = overestimate",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )
    fig1.update_xaxes(**xaxis_cfg)
    st.plotly_chart(fig1, use_container_width=True)

    # ── Chart 2: Error distribution (overlapping histograms) ──────────────────
    st.subheader("Error Distribution")
    fig2 = go.Figure()
    for col_name, color, label in [
        ("dynamic_error", COLORS["dynamic"], "Dynamic"),
        ("naive_error", COLORS["naive"], "Naive"),
    ]:
        fig2.add_trace(go.Histogram(
            x=df[col_name],
            name=label,
            marker_color=color,
            opacity=0.70,
            nbinsx=30,
        ))
    fig2.update_layout(
        barmode="overlay",
        template="plotly_dark",
        height=350,
        xaxis_title="Error (min)",
        yaxis_title="Order Count",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )
    st.plotly_chart(fig2, use_container_width=True)

    # ── Chart 3: Cumulative win rate ──────────────────────────────────────────
    st.subheader("Cumulative Dynamic Win Rate")
    df_sorted = df.copy().reset_index(drop=True)
    df_sorted["is_dynamic_win"] = (df_sorted["winner"] == "dynamic").astype(int)
    df_sorted["cum_pct"] = df_sorted["is_dynamic_win"].expanding().mean() * 100
    df_sorted["order_num"] = range(1, len(df_sorted) + 1)

    fig3 = go.Figure()
    fig3.add_hline(y=50, line_dash="dash", line_color="gray", opacity=0.5,
                   annotation_text="50% baseline", annotation_position="bottom right")
    fig3.add_trace(go.Scatter(
        x=df_sorted["order_num"],
        y=df_sorted["cum_pct"],
        mode="lines",
        name="Dynamic Win Rate",
        line=dict(color=COLORS["dynamic"], width=2),
        fill="tozeroy",
        fillcolor="rgba(0,100,145,0.15)",
    ))
    fig3.update_layout(
        template="plotly_dark",
        height=300,
        xaxis_title="Order Number (chronological)",
        yaxis_title="Dynamic Win Rate (%)",
        yaxis=dict(range=[0, 100]),
    )
    st.plotly_chart(fig3, use_container_width=True)
