import streamlit as st
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
from datetime import timedelta
from src.dashboard.utils import (
    load_gold, load_silver, no_data_banner,
    fmt_currency, fmt_minutes, fmt_pct, safe_float,
    COLORS, format_hour,
)


def render():
    st.header("Overview")

    kpis = load_gold("daily_kpis")
    hourly = load_gold("hourly_summary")

    if kpis is None and hourly is None:
        no_data_banner()
        return

    # ── KPI Cards ────────────────────────────────────────────────────────────
    if kpis is not None and not kpis.empty:
        row = kpis.iloc[0]

        orders_df = load_silver("orders")
        avg_carryout = avg_delivery = None
        if orders_df is not None and not orders_df.empty and "actual_wait_min" in orders_df.columns:
            co = orders_df.loc[orders_df["order_type"] == "carryout", "actual_wait_min"].dropna()
            de = orders_df.loc[orders_df["order_type"] == "delivery", "actual_wait_min"].dropna()
            avg_carryout = co.mean() if len(co) > 0 else None
            avg_delivery = de.mean() if len(de) > 0 else None

        labor_pct = safe_float(row.get("labor_cost_pct"))
        naive_mae = safe_float(row.get("overall_naive_mae"))
        dynamic_mae = safe_float(row.get("overall_dynamic_mae"))
        splh_val = safe_float(row.get("overall_splh"))

        cols = st.columns(8)
        _kpi_card(cols[0], "Total Orders", str(int(row["total_orders"])) if pd.notna(row.get("total_orders")) else "—")
        _kpi_card(cols[1], "Total Revenue", fmt_currency(row.get("total_revenue")))
        _kpi_card(cols[2], "SPLH", fmt_currency(splh_val) if splh_val is not None else "—")
        _kpi_card(
            cols[3], "Labor Cost %",
            fmt_pct(labor_pct),
            status="red" if labor_pct is not None and labor_pct > 30 else "green",
        )
        _kpi_card(cols[4], "Avg Wait (Carryout)", fmt_minutes(avg_carryout))
        _kpi_card(cols[5], "Avg Wait (Delivery)", fmt_minutes(avg_delivery))
        _kpi_card(
            cols[6], "Dynamic MAE",
            fmt_minutes(dynamic_mae),
            status="red" if dynamic_mae is not None and dynamic_mae > 10 else "green",
        )
        _kpi_card(
            cols[7], "Naive MAE",
            fmt_minutes(naive_mae),
            status="red" if naive_mae is not None and naive_mae > 10 else "green",
        )

    st.divider()

    if hourly is None or hourly.empty:
        st.info("Hourly summary data not available.")
        return

    df = hourly.copy()
    if pd.api.types.is_datetime64_any_dtype(df["hour"]):
        # Fill all 15 store-hour slots (effective open through +14 h) so the
        # x-axis always spans the full operating day even mid-simulation.
        first_hour = df["hour"].min()
        tz = df["hour"].dt.tz
        all_hours = pd.date_range(start=first_hour, periods=15, freq="h", tz=tz)
        full_df = pd.DataFrame({"hour": all_hours})
        df = full_df.merge(df, on="hour", how="left")
        df["order_count"] = df["order_count"].fillna(0).astype(int)
        # float columns (wait times, revenue, etc.) stay NaN — lines break at empty hours
        df["hour_label"] = df["hour"].apply(format_hour)
        df["hour_int"] = df["hour"].dt.hour
    else:
        df["hour_label"] = df["hour"].astype(str)
        df["hour_int"] = 0

    # ── Dual-axis: Hourly Order Volume + Wait Time ────────────────────────────
    st.subheader("Hourly Order Volume & Wait Time")
    fig = make_subplots(specs=[[{"secondary_y": True}]])

    # Rush period shading — only for hours present in data
    hours_present = set(df["hour_int"].tolist())
    labels_by_hour = dict(zip(df["hour_int"], df["hour_label"]))
    for shade_start, shade_end, rush_label in [(11, 14, "Lunch Rush"), (18, 21, "Dinner Rush")]:
        shade_hrs = [h for h in range(shade_start, shade_end) if h in hours_present]
        if shade_hrs:
            fig.add_vrect(
                x0=labels_by_hour[shade_hrs[0]],
                x1=labels_by_hour[shade_hrs[-1]],
                fillcolor="rgba(255,255,255,0.05)",
                line_width=0,
                annotation_text=rush_label,
                annotation_position="top left",
                annotation_font_size=10,
            )

    fig.add_trace(
        go.Bar(
            x=df["hour_label"],
            y=df["order_count"],
            name="Orders",
            marker_color="rgba(99,110,250,0.65)",
        ),
        secondary_y=False,
    )

    for col_name, label, color in [
        ("avg_wait_carryout", "Avg Wait (Carryout)", COLORS["carryout"]),
        ("avg_wait_delivery", "Avg Wait (Delivery)", COLORS["delivery"]),
    ]:
        if col_name in df.columns:
            fig.add_trace(
                go.Scatter(
                    x=df["hour_label"],
                    y=df[col_name],
                    name=label,
                    line=dict(color=color, width=2),
                    mode="lines+markers",
                ),
                secondary_y=True,
            )

    fig.update_layout(
        template="plotly_dark",
        height=380,
        margin=dict(t=40, b=40),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )
    fig.update_yaxes(title_text="Order Count", secondary_y=False)
    fig.update_yaxes(title_text="Wait Time (min)", secondary_y=True)
    fig.update_xaxes(title_text="Hour of Day")
    st.plotly_chart(fig, use_container_width=True)

    # ── Revenue vs Labor Cost ─────────────────────────────────────────────────
    st.subheader("Hourly Revenue vs. Labor Cost")
    if "revenue" in df.columns and "labor_cost" in df.columns:
        fig2 = go.Figure()
        fig2.add_trace(go.Bar(
            x=df["hour_label"], y=df["revenue"],
            name="Revenue", marker_color=COLORS["revenue"],
        ))
        fig2.add_trace(go.Bar(
            x=df["hour_label"], y=df["labor_cost"],
            name="Labor Cost", marker_color=COLORS["labor"],
        ))
        fig2.update_layout(
            barmode="group",
            template="plotly_dark",
            height=350,
            margin=dict(t=20, b=40),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            xaxis_title="Hour of Day",
            yaxis_title="Amount ($)",
        )
        st.plotly_chart(fig2, use_container_width=True)
    else:
        st.info("Revenue/labor data not available in hourly summary.")


def _kpi_card(col, label: str, value: str, status: str = "neutral"):
    color_map = {"green": "#22c55e", "red": "#ef4444", "neutral": "#6b7280"}
    color = color_map.get(status, color_map["neutral"])
    col.markdown(
        f"""
        <div style="text-align:center;padding:12px 6px;background:#1a1a2e;
                    border-radius:8px;border-left:3px solid {color};margin-bottom:4px;">
            <div style="font-size:1.4rem;font-weight:700;color:#FAFAFA;line-height:1.2;">{value}</div>
            <div style="font-size:0.72rem;color:#9ca3af;margin-top:4px;">{label}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )
