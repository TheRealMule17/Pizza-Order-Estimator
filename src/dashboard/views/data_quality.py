import streamlit as st
import plotly.express as px
import pandas as pd
from src.dashboard.utils import load_silver, load_rejected, no_data_banner


def render():
    st.header("Data Quality")

    orders = load_silver("orders")
    items = load_silver("order_items")
    dispatch = load_silver("dispatch_events")
    staff = load_silver("staff_changes")
    rejected = load_rejected()

    has_silver = any(
        df is not None and not df.empty for df in [orders, items, dispatch, staff]
    )
    if not has_silver and rejected is None:
        no_data_banner()
        return

    # ── Pipeline summary cards ────────────────────────────────────────────────
    st.subheader("Pipeline Summary")

    total_accepted = sum(
        len(df) for df in [orders, items, dispatch, staff]
        if df is not None and not df.empty
    )
    total_rejected = len(rejected) if rejected is not None and not rejected.empty else 0
    total_raw = total_accepted + total_rejected
    rejection_rate = (total_rejected / total_raw * 100) if total_raw > 0 else 0.0

    match_rate = None
    if orders is not None and not orders.empty and "orphan_pos_flag" in orders.columns:
        match_rate = (1 - orders["orphan_pos_flag"].mean()) * 100

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Raw Records Processed", f"{total_raw:,}")
    c2.metric("Records Accepted (Silver)", f"{total_accepted:,}")
    c3.metric("Records Rejected", f"{total_rejected:,}")
    c4.metric("Rejection Rate", f"{rejection_rate:.1f}%")
    c5.metric("Order ID Match Rate", f"{match_rate:.1f}%" if match_rate is not None else "—")

    st.divider()

    # ── Rejection analysis ────────────────────────────────────────────────────
    if rejected is None or rejected.empty:
        st.success("All records passed validation — no rejections.", icon="✅")
    else:
        col_left, col_right = st.columns(2)

        with col_left:
            st.subheader("Rejections by Source")
            if "source" in rejected.columns:
                src = rejected["source"].value_counts().reset_index()
                src.columns = ["source", "count"]
                fig1 = px.pie(
                    src, names="source", values="count",
                    template="plotly_dark", hole=0.4,
                    color_discrete_sequence=["#ef4444", "#f97316", "#eab308", "#3b82f6"],
                )
                fig1.update_layout(height=320)
                st.plotly_chart(fig1, use_container_width=True)
            else:
                st.info("No 'source' column in rejected records.")

        with col_right:
            st.subheader("Top Rejection Reasons")
            reason_col = next(
                (c for c in rejected.columns if "reason" in c.lower()), None
            )
            if reason_col:
                reasons = rejected[reason_col].value_counts().head(10).reset_index()
                reasons.columns = ["reason", "count"]
                fig2 = px.bar(
                    reasons, x="count", y="reason", orientation="h",
                    template="plotly_dark",
                    color_discrete_sequence=["#E31837"],
                    labels={"count": "Count", "reason": ""},
                )
                fig2.update_layout(height=320, yaxis=dict(autorange="reversed"))
                st.plotly_chart(fig2, use_container_width=True)
            else:
                st.info("No rejection reason column found.")

        # ── Sample rejected rows ──────────────────────────────────────────────
        st.subheader("Sample Rejected Rows (first 20)")
        preferred = ["source", "raw_content", "rejection_reason"]
        if reason_col and reason_col not in preferred:
            preferred.append(reason_col)
        display_cols = [c for c in preferred if c in rejected.columns]
        if not display_cols:
            display_cols = rejected.columns.tolist()

        sample = rejected[display_cols].head(20).copy()
        for trunc_col in [c for c in ["raw_content", "raw_line"] if c in sample.columns]:
            sample[trunc_col] = sample[trunc_col].astype(str).str[:80] + "…"
        st.dataframe(sample, use_container_width=True, hide_index=True)

    st.divider()

    # ── Data source consistency matrix ────────────────────────────────────────
    st.subheader("Data Source Consistency")
    if orders is not None and not orders.empty:
        total_orders = len(orders)
        orphan_pos = int(orders["orphan_pos_flag"].sum()) if "orphan_pos_flag" in orders.columns else 0
        complete = total_orders - orphan_pos

        phantom_kitchen = 0
        if items is not None and not items.empty and "phantom_kitchen" in items.columns:
            phantom_kitchen = int(items["phantom_kitchen"].sum())

        dispatch_orders = 0
        if dispatch is not None and not dispatch.empty and "order_id" in dispatch.columns:
            dispatch_orders = dispatch["order_id"].dropna().nunique()

        pos_only = orphan_pos
        kitchen_no_oven = max(0, complete - dispatch_orders)

        rows = [
            ("Orders with data in all 4 sources (complete)", complete),
            ("POS + Kitchen present, no matching Oven/Dispatch", kitchen_no_oven),
            ("Orphaned POS orders (no kitchen data)", pos_only),
            ("Phantom kitchen events (no matching POS order)", phantom_kitchen),
            ("Unique orders appearing in Dispatch records", dispatch_orders),
        ]
        st.dataframe(
            pd.DataFrame(rows, columns=["Condition", "Count"]),
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.info("Order data not available for consistency matrix.")
