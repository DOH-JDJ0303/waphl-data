import streamlit as st
import boto3
import pandas as pd
import math 
from dashboard.search import ui
import shared.ui
from shared.io_ops import RESULTS_PREFIX

# === Config ===
SOURCE_BUCKET = st.session_state.res_bucket
S3            = boto3.session.Session().client("s3")
URI           = f"s3://{SOURCE_BUCKET}/{RESULTS_PREFIX}"

# === Page ===
st.title("Search Results")

shared.ui.module_overview("This module allows you to search, filter, and export production results.")


filter_container = st.container()
try:
    partition_cols, all_cols = ui.get_table_meta(URI)
except Exception as e:
    st.error(f"Couldn't open the table: {e}")
    st.stop()


with filter_container:
    st.header("Filters")

    # --- partition selection -------------------------------------------------
    partition_col = None
    selected_values = []
    if partition_cols:
        partition_col = st.selectbox("Partition column", partition_cols)
        try:
            values = ui.get_partition_values(URI, partition_col)
        except Exception as e:
            st.error(f"Couldn't list partition values: {e}")
            st.stop()
        selected_values = st.multiselect(
            f"{partition_col} value(s)", values,
            help="Choose one or more. Leave empty to read every partition.",
        )
        if not selected_values:
            st.caption("⚠️ No partition selected — the whole table will be read.")
    else:
        st.caption("This table isn't partitioned; you can still pick columns.")

    # --- column selection ----------------------------------------------------
    selected_cols = st.multiselect(
        "Columns", all_cols,
        help="Leave empty to keep all columns.",
    )

    # --- page size -----------------------------------------------------------
    page_size = st.number_input("Rows per page", min_value=1, value=10, step=5)

    load = st.button("Load data", type="primary", use_container_width=True)

# Reading is gated on the button; the result lives in session_state so that
# changing the page or page size never triggers another read from S3.
if load:
    filters = (
        ((partition_col, "in", selected_values),)
        if partition_col and selected_values
        else None
    )
    try:
        df = ui.load_data(URI, filters, columns=selected_cols or None)
    except Exception as e:
        st.error(f"Read failed: {e}")
        st.stop()
    # remove:  if selected_cols: df = df[selected_cols]
    st.session_state["df"] = df

# --------------------------------------------------------------------------- #
# Results: summary, download, paginated preview
# --------------------------------------------------------------------------- #
df = st.session_state.get("df")

if df is None:
    st.info("Set your filters in the sidebar, then click **Load data**.")
    st.stop()

st.success(f"{len(df):,} rows × {df.shape[1]} columns")

st.download_button(
    "⬇️ Download CSV",
    data=ui.to_csv_bytes(df),
    file_name="filtered.csv",
    mime="text/csv",
)

total = len(df)
n_pages = max(1, math.ceil(total / page_size))
page = st.number_input("Page", min_value=1, max_value=n_pages, value=1, step=1)
start = (page - 1) * page_size
end = min(start + page_size, total)

st.dataframe(df.iloc[start:end], use_container_width=True)
st.caption(f"Showing rows {start + 1:,}–{end:,} of {total:,}  ·  page {page} of {n_pages}")