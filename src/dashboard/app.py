import sys
from pathlib import Path

# Ensure project root is on sys.path when launched via `streamlit run src/dashboard/app.py`
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import streamlit as st

st.set_page_config(
    page_title="Pizza Order Estimator — Analytics",
    page_icon="🍕",
    layout="wide",
    initial_sidebar_state="expanded",
)

from src.dashboard.views import overview, estimation, bottlenecks, data_quality, explorer

PAGES = {
    "Overview": overview,
    "Estimation Accuracy": estimation,
    "Bottlenecks": bottlenecks,
    "Data Quality": data_quality,
    "Raw Data Explorer": explorer,
}

with st.sidebar:
    st.title("🍕 Pizza Analytics")
    st.caption("Post-run simulation analysis")
    st.divider()
    page_name = st.radio(
        "Navigate",
        list(PAGES.keys()),
        label_visibility="collapsed",
    )
    st.divider()
    st.caption("Requires a completed simulation run with pipeline output.")
    st.caption("`streamlit run src/dashboard/app.py`")

PAGES[page_name].render()
