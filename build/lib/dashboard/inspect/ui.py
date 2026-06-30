import streamlit as st
import pandas as pd
import boto3
import uuid
import os
from datetime import datetime, timezone
import numpy as np
import time

from shared import io_ops, ui
from dashboard.inspect import result_table

from shared.io_ops import FILES_PREFIX, RESULTS_PREFIX
from shared.data_processing import (
    FILES_TABLE_MERGE_KEYS,
    FILES_TABLE_PARTITIONS,
    RESULTS_TABLE_MERGE_KEYS,
    RESULTS_TABLE_PARTITIONS,
)

# ============================================================================
# State Management
# ============================================================================

def reset_state(preserve_keys=None):
    """Reset session state while optionally preserving specific keys."""
    time.sleep(3)
    preserve_keys = set(preserve_keys or [])
    preserved = {k: st.session_state.get(k) for k in preserve_keys}
    
    for k in list(st.session_state.keys()):
        if k not in preserve_keys:
            del st.session_state[k]
    
    st.session_state.update(preserved)
    st.rerun()


# ============================================================================
# Queue Management
# ============================================================================

def check_queue():
    """
    Read and display queue files from S3, allowing row selection for processing.
    
    Returns:
        tuple: (processed_dataframe, original_queue_dataframe) or (None, queue_df)
    """
    # Set workflow
    source_bucket = st.session_state.get('res_bucket')
    if not source_bucket:
        return
    
    workflow = st.session_state.inspect_workflow
    if not workflow:
        st.warning("You must select a workflow before checking results.")
        return
    
    uri = f"s3://{source_bucket}/{FILES_PREFIX}"
    st.session_state.inspect_uri = uri
    filt = [
        ("workflow_alt", "=", workflow),
        ("inspected", "=", False),
    ]

    # Reset downstream dataframes
    st.session_state.df_queue      = pd.DataFrame()
    st.session_state.df_to_process = pd.DataFrame()
    st.session_state.df_inspect    = pd.DataFrame()
    st.session_state.df_edited     = pd.DataFrame()

    # Pull files
    try:
        st.session_state.df_queue = io_ops.read_delta_as_pandas(uri, filters = filt)
    except Exception as e:
        ui.push_error(f"Issue gathering data from {uri}:\n{e}")
        return
    
    # Return early if queue is empty
    if st.session_state.df_queue.empty:
        st.warning(f"{workflow} queue is empty!")
        return


def render_queue_table():
    """
    Render interactive queue selection interface.
    
    Returns:
        DataFrame: Subset of df_queue with selected rows
    """
    df_queue = st.session_state.df_queue
    if df_queue.empty:
        return
    with st.expander("Files in Queue (select rows to process)", expanded=False):
        df_view = df_queue.copy()
        df_view.reset_index(inplace=True)
        df_view.rename(columns={"index": "__rowid"}, inplace=True)

        # Initialize selection state
        num_rows = len(df_view)
        if "queue_selected_mask" not in st.session_state:
            st.session_state.queue_selected_mask = [True] * num_rows

        # Selection control buttons       
        c1, c2, _ = st.columns([1, 1, 6])
        with c1:
            if st.button("Select all"):
                st.session_state.queue_selected_mask = [True] * num_rows
        with c2:
            if st.button("Clear selection"):
                st.session_state.queue_selected_mask = [False] * num_rows

        # Add selection column and render editor
        df_view.insert(0, "selected", st.session_state.queue_selected_mask)
        edited = st.data_editor(
            df_view,
            use_container_width=True,
            num_rows="fixed",
            column_config={
                "selected": st.column_config.CheckboxColumn(
                    "selected",
                    help="Choose rows to send into gather.main()",
                    default=False
                ),
                "__rowid": st.column_config.NumberColumn(
                    "__rowid",
                    help="Internal row id",
                    disabled=True
                ),
            },
            disabled=[c for c in df_view.columns if c not in ("selected",)],
            key="queue_editor",
        )

        # Update session state and determine selected rows
        st.session_state.queue_selected_mask = edited["selected"].tolist()
        selected_ids = edited.loc[edited["selected"], "__rowid"].tolist()
        
        if selected_ids:
            df_to_process = df_queue.iloc[selected_ids]
            st.caption(f"Processing {len(df_to_process)} selected row(s).")
        else:
            df_to_process = pd.DataFrame()
            st.caption("No rows selected")
        
        st.session_state.df_to_process = df_to_process

        if df_to_process.empty:
            st.warning("No files selected from queue")
            st.stop()

        # Process selected rows
        if st.session_state.get('df_inspect', pd.DataFrame()).empty:
            try:
                result_table.main()
            except Exception as e:
                ui.push_error("Error: Problem loading the queue.")
                st.exception(e)
                st.stop()


# ============================================================================
# UI Components
# ============================================================================

def getting_started():
    source_bucket = st.session_state.get('res_bucket')
    if not source_bucket:
        return
    
    with st.container(border=True):
        st.subheader("Getting Started")
        st.markdown("##### Select a workflow")
        source_uri = f"s3://{source_bucket}/{FILES_PREFIX}"
        workflows = io_ops.delta_partition_values(source_uri, "workflow_alt")
        workflow = st.selectbox(
            f"Use the drop-down menu to select a workflow (source: {source_uri})",
            [""] + workflows,
        )
        if workflow:
            st.session_state.inspect_workflow = workflow

        if workflow:
            st.markdown("##### Check the results queue")
            if st.button("Check Queue"):
                check_queue()

def queue_files():
    df_queue = st.session_state.get('df_queue', pd.DataFrame())
    if df_queue.empty:
        return
    
    # === Inspection Overview ===
    with st.container(border=True):
        st.subheader("Select Queue Files (Optional)")
        st.write("Use the table below to include / exclude files from the queue. **Note:** This is often not recommended or necessary!")
        render_queue_table()

def results_metadata():
    with st.expander("Metadata"):
        # Data source
        st.write(f"**Data Source:** {st.session_state.get('inspect_uri', '')}")
        # Run summary
        with st.expander("Run Summary"):
            summary = pd.DataFrame()
            df_to_process = st.session_state.get("df_to_process", pd.DataFrame())
            
            if not df_to_process.empty:
                df_valid = df_to_process[
                    df_to_process['id_alt'].astype(str).str.strip().ne("") &
                    df_to_process['id_alt'].notna()
                ]

                # Count unique samples per run
                summary = (
                    df_valid
                    .groupby('run')['id_alt']
                    .nunique()
                    .reset_index(name='n_samples')
                    .sort_values('run')
                )
            st.dataframe(summary)
        # Workflow schema
        with st.expander("Workflow Scheme"):
            st.json(st.session_state.get('inspect_scheme', {}))
        # Warnings
        with st.expander("Warnings"):
            st.write(st.session_state.get('inspect_warnings', []))

def render_results():
    df_inspect = st.session_state.get('df_inspect', pd.DataFrame())
    if df_inspect.empty:
        ui.push_error("No files")
        return
    for c in ("accept", "reject"):
        if c not in df_inspect.columns:
            df_inspect[c] = False
        
    # Reorder columns with accept/reject first
    first = ["accept", "reject"]
    df_inspect = df_inspect[first + [c for c in df_inspect.columns if c not in first]]
    
    df_edited = st.data_editor(
        df_inspect,
        column_config={
            col: st.column_config.Column(disabled=True)
            for col in df_inspect.columns
            if col not in ["accept", "reject"]
        },
        num_rows="dynamic",
        use_container_width=True,
        key="editor",
    )

    col1, col2 = st.columns(2)
    with col1:
        if st.button("Submit"):
            st.session_state.submit_status = True
            st.session_state.df_edited = df_edited
    with col2:
        if st.button("Cancel"):
            st.session_state.submit_status = False

def queue_results():
    df_queue = st.session_state.get('df_queue', pd.DataFrame())
    if df_queue.empty:
        return
    
    # === Queue results
    with st.container(border=True):
        st.subheader("Inspect Results")
        st.write("Inspect each row below and decide if the result should be marked as `accept` or `reject`. Send a result back to the queue by unchecking both `accept` and `reject`. Press the **Submit** button when you are satisfied with your selection.")
        results_metadata()
        render_results()


# ============================================================================
# File Classification
# ============================================================================

def classify_file_decisions():
    """
    Classify files as decided or undecided based on accept/reject flags.
    
    A file is decided if ANY row has accept=True OR reject=True.
    A file is undecided if it appears in any row with both flags False/NaN.
    If the same file is undecided anywhere, it's removed from decided.
    
    Args:
        df: DataFrame with 'files_reportable', 'files_supplementary',
            'accept', and 'reject'

    Returns:
        tuple: (sorted list of decided files, sorted list of undecided files)
    """
    df = st.session_state.df_edited

    decided = set(st.session_state.super_files) if st.session_state.super_files else set()
    undecided = set()

    for _, row in df.iterrows():
        # Extract paths from both columns safely
        paths = []
        for col in ("files_reportable", "files_supplementary"):
            v = row.get(col, [])
            if isinstance(v, str):
                paths.append(v)
            elif isinstance(v, list):
                paths.extend(v)

        # Normalize paths: remove None/empty values, coerce to str
        paths = [
            str(p).strip()
            for p in paths
            if p is not None and str(p).strip() != ""
        ]

        # No paths? Continue
        if not paths:
            continue

        # Decision logic
        is_decided = bool(row.get("accept", False) or row.get("reject", False))

        if is_decided:
            decided.update(paths)
        else:
            undecided.update(paths)

    # Remove any file appearing as undecided anywhere
    decided -= undecided

    # Return sorted lists (only strings now)
    st.session_state.decided_files   = sorted(decided)
    st.session_state.undecided_files = sorted(undecided)


def split_queue_by_decision():
    """
    Split queue dataframe into decided and undecided subsets.
    
    Matching is based on the 'current' column in the dataframe:
    - If current ∈ decided_files → goes to df_decided
    - Otherwise → goes to df_undecided
    
    Args:
        decided_files: List of filepaths that have been decided
        undecided_files: List of filepaths that remain undecided
        
    Returns:
        tuple: (df_decided, df_undecided)
    """
    decided_files = st.session_state.decided_files
    decided_set = set(decided_files)
    
    df_queue = st.session_state.get("df_queue", pd.DataFrame()).copy()
    
    if df_queue.empty:
        ui.push_error("Original queue dataframe missing; cannot move source rows.")
        st.stop()
    
    if "current" not in df_queue.columns:
        ui.push_error("Queue dataframe is missing the 'current' column.")
        st.stop()

    # Split based on whether 'current' filepath is in decided set
    mask_decided = df_queue["current"].isin(decided_set)
    df_decided = df_queue[mask_decided].copy()
    df_undecided = df_queue[~mask_decided].copy()

    st.session_state.df_decided   = df_decided
    st.session_state.df_undecided = df_undecided


# ============================================================================
# Submission Processing
# ============================================================================

def execute_submission():
    source_bucket = st.session_state.get('res_bucket')
    submit_status_confirm = st.session_state.get('submit_status_confirm')

    if submit_status_confirm is None or not source_bucket:
        return
    elif submit_status_confirm is False:
        reset_state(preserve_keys=["SOURCE_BUCKET", "USER", "WORKFLOW"])
    elif submit_status_confirm is True:

        with st.status("Processing Submission"):
            user       = st.session_state.user
            df_edited  = st.session_state.df_edited
            df_decided = st.session_state.df_decided
            if not user:
                ui.push_error("User must be defined!")
                return
            try:
                now_iso = datetime.now(timezone.utc).isoformat()

                # --- results log ---
                df_results = df_edited[(df_edited["accept"]) | (df_edited["reject"])]
                if df_results.empty:
                    ui.push_error("Something went wrong!")
                    return

                df_results["inspected_by"] = user
                df_results["inspected_at"] = now_iso
                df_results['status'] = df_results['accept'].apply(lambda x: 'accept' if x else 'reject')
                df_results = df_results.astype("string")

                # --- files table: update inspected -> True ---
                if df_decided.empty:
                    ui.push_error("Something went wrong!")
                    return
                df_decided["inspected"] = True
                df_decided["inspected_by"] = user
                df_decided["inspected_at"] = now_iso

                # ensure bool really is bool (parquet/delta can get fussy)
                df_decided["inspected"] = df_decided["inspected"].astype(bool)

                # --- write changes to DB ---
                io_ops.write_delta(
                    df=df_results, 
                    uri=f"s3://{source_bucket}/{RESULTS_PREFIX}/",
                    key_cols=RESULTS_TABLE_MERGE_KEYS,
                    partition_by=RESULTS_TABLE_PARTITIONS
                )

                io_ops.write_delta(
                    df=df_decided,
                    uri=f"s3://{source_bucket}/{FILES_PREFIX}/",
                    key_cols=FILES_TABLE_MERGE_KEYS,
                    partition_by=FILES_TABLE_PARTITIONS
                )

                # --- celebrate! ---
                st.success("Submission successful!")
                st.balloons()
                reset_state()

            except Exception as e:
                ui.push_error(f"Submission failed: {e}")
                st.exception(e)
                st.stop()

def check_submission():
    submit_status = st.session_state.get("submit_status")

    if submit_status is None:
        return
    elif submit_status is False:
        reset_state(preserve_keys=["SOURCE_BUCKET", "USER", "WORKFLOW"])
    elif submit_status is True:
        with st.container(border=True):
            st.subheader("Check Submission (Optional)")
            st.write("Inspect the tables below to ensure the correct files leaving / heading back to the queue.")
            # Classify and split files
            classify_file_decisions()
            split_queue_by_decision()

            # Preview changes
            with st.expander("Files leaving the queue"):
                st.dataframe(st.session_state.get('df_decided', pd.DataFrame()))
            with st.expander("Files heading back to the queue"):
                st.dataframe(st.session_state.get('df_undecided', pd.DataFrame()))

            col1, col2 = st.columns(2)
            with col1:
                if st.button("Confirm Submission"):
                    st.session_state.submit_status_confirm = True
            with col2:
                if st.button("Cancel Submission"):
                    st.session_state.submit_status_confirm = False