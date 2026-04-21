import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
from src.dashboard.utils import (
    load_gold, load_silver, no_data_banner,
    COLORS, BOTTLENECK_COLORS, format_hour, mins_to_time_str,
)


def render():
    st.header("Bottlenecks")

    bn = load_gold("bottleneck_log")
    hourly = load_gold("hourly_summary")
    staff = load_silver("staff_changes")

    if bn is None and hourly is None:
        no_data_banner()
        return

    if bn is None or bn.empty:
        st.info(
            "No bottleneck events recorded in this simulation run. "
            "The kitchen kept up with demand — great news!"
        )
        if hourly is not None and not hourly.empty:
            _staffing_chart(hourly, staff, pd.DataFrame())
        return

    # ── Chart 1: Bottleneck counts by type × traffic block ────────────────────
    st.subheader("Bottleneck Events by Type and Traffic Block")
    if "event_type" in bn.columns and "traffic_block_label" in bn.columns:
        counts = (
            bn.groupby(["traffic_block_label", "event_type"])
            .size()
            .reset_index(name="count")
        )
        fig1 = px.bar(
            counts,
            x="traffic_block_label",
            y="count",
            color="event_type",
            barmode="group",
            color_discrete_map=BOTTLENECK_COLORS,
            template="plotly_dark",
            labels={
                "traffic_block_label": "Traffic Block",
                "count": "Events",
                "event_type": "Type",
            },
        )
        fig1.update_layout(height=350, legend_title="Bottleneck Type")
        st.plotly_chart(fig1, use_container_width=True)
    elif "event_type" in bn.columns:
        counts = bn["event_type"].value_counts().reset_index()
        counts.columns = ["event_type", "count"]
        fig1 = px.bar(
            counts, x="event_type", y="count",
            color="event_type",
            color_discrete_map=BOTTLENECK_COLORS,
            template="plotly_dark",
            labels={"event_type": "Bottleneck Type", "count": "Events"},
        )
        fig1.update_layout(height=320, showlegend=False)
        st.plotly_chart(fig1, use_container_width=True)

    # ── Chart 2: Timeline scatter ─────────────────────────────────────────────
    st.subheader("Bottleneck Event Timeline")
    if "sim_time_min" in bn.columns and "event_type" in bn.columns:
        bn_plot = bn.copy()
        bn_plot["time_label"] = bn_plot["sim_time_min"].apply(mins_to_time_str)
        hover_cols = [c for c in ["details", "traffic_block_label", "queue_depth"] if c in bn_plot.columns]
        fig2 = px.scatter(
            bn_plot,
            x="sim_time_min",
            y="event_type",
            color="event_type",
            color_discrete_map=BOTTLENECK_COLORS,
            template="plotly_dark",
            hover_data=hover_cols or None,
            labels={"sim_time_min": "Simulated Time (min from open)", "event_type": "Bottleneck Type"},
            custom_data=["time_label"] if "time_label" in bn_plot.columns else None,
        )
        fig2.update_traces(marker=dict(size=10, opacity=0.85))
        fig2.update_layout(height=320, showlegend=False, xaxis_title="Simulated Time (min from store open)")
        st.plotly_chart(fig2, use_container_width=True)
    elif "simulated_time" in bn.columns and "event_type" in bn.columns:
        fig2 = px.scatter(
            bn,
            x="simulated_time",
            y="event_type",
            color="event_type",
            color_discrete_map=BOTTLENECK_COLORS,
            template="plotly_dark",
            labels={"simulated_time": "Simulated Time", "event_type": "Bottleneck Type"},
        )
        fig2.update_traces(marker=dict(size=10, opacity=0.85))
        fig2.update_layout(height=320, showlegend=False)
        st.plotly_chart(fig2, use_container_width=True)

    # ── Chart 3: Staffing overlay ─────────────────────────────────────────────
    _staffing_chart(hourly, staff, bn)

    # ── Worst period ──────────────────────────────────────────────────────────
    _worst_period(bn, hourly)


def _staffing_chart(hourly: pd.DataFrame, staff, bn: pd.DataFrame):
    st.subheader("Staffing Levels & Bottleneck Events")
    if hourly is None or hourly.empty:
        st.info("Hourly summary not available for staffing chart.")
        return

    df = hourly.copy()
    if pd.api.types.is_datetime64_any_dtype(df["hour"]):
        df["hour_label"] = df["hour"].apply(format_hour)
    else:
        df["hour_label"] = df["hour"].astype(str)

    fig = go.Figure()
    if "insiders_on_clock" in df.columns:
        fig.add_trace(go.Scatter(
            x=df["hour_label"], y=df["insiders_on_clock"],
            mode="lines", name="Insiders",
            line=dict(color=COLORS["carryout"], width=2, shape="hv"),
        ))
    if "drivers_on_clock" in df.columns:
        fig.add_trace(go.Scatter(
            x=df["hour_label"], y=df["drivers_on_clock"],
            mode="lines", name="Drivers",
            line=dict(color=COLORS["delivery"], width=2, shape="hv"),
        ))

    # Overlay bottleneck markers — use simulated_time if available
    if not bn.empty and "simulated_time" in bn.columns and "event_type" in bn.columns:
        for ev_type, grp in bn.groupby("event_type"):
            color = BOTTLENECK_COLORS.get(ev_type, "#9ca3af")
            fig.add_trace(go.Scatter(
                x=grp["simulated_time"],
                y=[0.3] * len(grp),
                mode="markers",
                name=ev_type,
                marker=dict(color=color, size=9, symbol="x"),
            ))

    fig.update_layout(
        template="plotly_dark",
        height=350,
        xaxis_title="Hour of Day",
        yaxis_title="Staff Count",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )
    st.plotly_chart(fig, use_container_width=True)


def _worst_period(bn: pd.DataFrame, hourly):
    st.subheader("Worst Period Analysis")
    if bn.empty or "sim_time_min" not in bn.columns:
        st.info("Insufficient data for worst period analysis.")
        return

    # Sliding 60-min window
    sim_min = bn["sim_time_min"].dropna()
    if sim_min.empty:
        st.info("No sim_time_min data available.")
        return

    window = 60
    best_count, best_start = 0, float(sim_min.min())
    for start in range(int(sim_min.min()), max(int(sim_min.max()) - window + 1, int(sim_min.min()) + 1), 5):
        count = int(((sim_min >= start) & (sim_min < start + window)).sum())
        if count > best_count:
            best_count, best_start = count, start

    worst_bn = bn[(bn["sim_time_min"] >= best_start) & (bn["sim_time_min"] < best_start + window)]
    start_str = mins_to_time_str(best_start)
    end_str = mins_to_time_str(best_start + window)

    col1, col2 = st.columns(2)
    with col1:
        st.markdown(f"**Worst Window:** {start_str} – {end_str}")
        st.markdown(f"**Total Events:** {best_count}")
        if "event_type" in worst_bn.columns:
            for ev, cnt in worst_bn["event_type"].value_counts().items():
                st.markdown(f"- **{ev}**: {cnt}")

    with col2:
        if hourly is not None and not hourly.empty:
            hour_idx = min(int(best_start) // 60, len(hourly) - 1)
            h_row = hourly.iloc[hour_idx]
            ins = h_row.get("insiders_on_clock")
            drv = h_row.get("drivers_on_clock")
            awt = h_row.get("avg_wait_delivery")
            if ins is not None and pd.notna(ins):
                st.markdown(f"**Insiders on clock:** {int(ins)}")
            if drv is not None and pd.notna(drv):
                st.markdown(f"**Drivers on clock:** {int(drv)}")
            if awt is not None and pd.notna(awt):
                st.markdown(f"**Avg Delivery Wait:** {float(awt):.1f} min")

        if "event_type" in worst_bn.columns and not worst_bn.empty:
            top = worst_bn["event_type"].mode().iloc[0]
            if top in ("no_drivers", "driver_queue_backup"):
                rec = f"Consider adding 1–2 drivers between {start_str}–{end_str} to reduce driver queue backup."
            elif top == "make_line_full":
                rec = f"Consider adding 1 insider between {start_str}–{end_str} to clear the make-line bottleneck."
            elif top == "oven_full":
                rec = f"Oven capacity was saturated between {start_str}–{end_str}. Consider staggering large orders."
            else:
                rec = f"Multiple bottleneck types clustered between {start_str}–{end_str}. Review overall staffing."
            st.info(f"**Recommendation:** {rec}")
