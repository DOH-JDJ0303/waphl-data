import streamlit as st
import boto3
import pandas as pd
from dashboard.inspect import ui
import shared.ui

# === Config ===
SOURCE_BUCKET  = st.session_state.res_bucket
S3             = boto3.session.Session().client("s3")

# === Page ===
st.title("Inspect Results")

shared.ui.module_overview("This module allows you to inspect production results for release to internal partners.")

# === Getting Started ====
ui.getting_started()

# === Sidebar ===
with st.sidebar:
    if st.button("End Session", use_container_width=True):
        st.info("Ending session")
        ui.reset_state()

# === Queue File Selection (Optional) ====
ui.queue_files()

# === Result Inspection ====
ui.queue_results()

# === Submission ===
ui.check_submission()
ui.execute_submission()