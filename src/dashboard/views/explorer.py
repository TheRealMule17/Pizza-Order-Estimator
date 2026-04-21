import streamlit as st
from src.dashboard.utils import load_silver, load_gold

SILVER_OPTIONS = ["orders", "order_items", "dispatch_events", "staff_changes"]
GOLD_OPTIONS = ["hourly_summary", "estimation_accuracy", "bottleneck_log", "daily_kpis"]


def render():
    st.header("Raw Data Explorer")
    st.caption("Browse the Parquet tables produced by the pipeline. All data is read-only.")

    tab_silver, tab_gold = st.tabs(["Silver Data (Cleaned)", "Gold Data (Analytics)"])

    with tab_silver:
        _table_explorer(SILVER_OPTIONS, load_silver, "silver")

    with tab_gold:
        _table_explorer(GOLD_OPTIONS, load_gold, "gold")


def _table_explorer(options: list, loader, prefix: str):
    selected = st.selectbox(
        "Select table",
        options,
        key=f"{prefix}_select",
        format_func=lambda x: x.replace("_", " ").title(),
    )

    df = loader(selected)

    if df is None:
        st.warning(
            f"`{selected}.parquet` was not found. Run the pipeline first to generate this table.",
            icon="⚠️",
        )
        return

    if df.empty:
        st.info(f"`{selected}` exists but contains no rows.")
        return

    st.caption(f"{len(df):,} rows × {len(df.columns)} columns")
    st.dataframe(df, use_container_width=True, height=520)

    csv_bytes = df.to_csv(index=False).encode("utf-8")
    st.download_button(
        label=f"⬇ Download {selected}.csv",
        data=csv_bytes,
        file_name=f"{selected}.csv",
        mime="text/csv",
        key=f"{prefix}_{selected}_dl",
    )
