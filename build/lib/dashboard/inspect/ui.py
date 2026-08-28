import streamlit as st
import pandas as pd
import boto3
import uuid
import os
from datetime import datetime, timezone
import numpy as np
import time

from collections import defaultdict

from shared import io_ops, ui
from dashboard.inspect import result_table, utils

from shared.io_ops import FILES_PREFIX, RESULTS_PREFIX
from shared.data_processing import (
    FILES_TABLE_MERGE_KEYS,
    FILES_TABLE_PARTITIONS,
    RESULTS_TABLE_MERGE_KEYS,
    RESULTS_TABLE_PARTITIONS,
)

QUEUE_TYPE_COL = "type" # Column in the files/queue table identifying each file's type.

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
def get_scheme_types():
    """Types the workflow scheme covers (deduped `type` values from
    reportable_files, computed by utils.extract_scheme_info).

    Reads inspect_reportable_file_types only. check_queue() pops inspect_scheme
    before this runs, so don't fall back to the raw scheme here.
    """
    types = st.session_state.get("inspect_reportable_file_types") or []
    return {str(t).strip() for t in types}


def _submit_auto_inspected(df_unlisted):
    """Write unlisted-type rows back to the files table as inspected, then
    pop a toast with the count."""
    source_bucket = st.session_state.get("res_bucket")
    user = st.session_state.get("user")
    if not source_bucket or not user:
        ui.push_message(
            "Skipped auto-submitting unlisted types (missing user or bucket).",
            type="warning",
        )
        return

    now_iso = datetime.now(timezone.utc).isoformat()
    df_unlisted["inspected"] = True
    df_unlisted["inspected_by"] = user
    df_unlisted["inspected_at"] = now_iso
    # ensure bool really is bool (parquet/delta can get fussy)
    df_unlisted["inspected"] = df_unlisted["inspected"].astype(bool)

    try:
        io_ops.write_delta(
            df=df_unlisted,
            uri=f"s3://{source_bucket}/{FILES_PREFIX}/",
            key_cols=FILES_TABLE_MERGE_KEYS,
            partition_by=FILES_TABLE_PARTITIONS,
        )
    except Exception as e:
        ui.push_message(f"Failed to auto-submit unlisted types: {e}")
        st.exception(e)
        return

    n = len(df_unlisted)
    st.toast(
        f"Auto-submitted {n} file{'s' if n != 1 else ''} with a type not in "
        f"the workflow scheme.",
        icon="📤",
    )


def auto_inspect_unlisted_types():
    """Drop rows from df_queue whose type isn't in the workflow scheme, mark
    them inspected, and write them back to the files table so they never reach
    df_view / the inspector."""
    df_queue = st.session_state.get("df_queue", pd.DataFrame())
    if df_queue.empty or QUEUE_TYPE_COL not in df_queue.columns:
        return

    allowed = get_scheme_types()
    if not allowed:
        # Couldn't resolve scheme types -> do NOT auto-submit the whole queue.
        ui.push_message(
            "No reportable file types resolved from the scheme; "
            "skipping auto-submission of unlisted types.",
            type="warning",
        )
        return

    types = utils.normalize_type_series(df_queue[QUEUE_TYPE_COL])
    unlisted_mask = ~types.isin(allowed)

    df_unlisted = df_queue[unlisted_mask].copy()
    st.session_state.df_queue = df_queue[~unlisted_mask].reset_index(drop=True)

    if not df_unlisted.empty:
        _submit_auto_inspected(df_unlisted)

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
        ui.push_message("You must select a workflow before checking results.", type="warning")
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

    # Reset all other state derived from a previous queue pull / workflow,
    # so nothing from a prior workflow's session leaks into this one.
    for key in (
        "queue_selected_mask",
        "df_inspect_working",
        "df_decided",
        "df_undecided",
        "decided_files",
        "undecided_files",
        "submit_status",
        "submit_status_confirm",
        "inspect_scheme",
        "inspect_warnings",
    ):
        st.session_state.pop(key, None)

# Pull files
    try:
        st.session_state.df_queue = io_ops.read_delta_as_pandas(uri, filters = filt)
    except Exception as e:
        ui.push_message(f"Issue gathering data from {uri}:\n{e}")
        return

    # Rows the gather lambda couldn't type (or that predate a scheme change)
    # come back as "other". Re-derive those from the origin URI before the
    # unlisted-type sweep, or anything reportable gets auto-submitted.
    st.session_state.df_queue = utils.type_from_origin(st.session_state.df_queue)

    st.write("DEBUG allowed:", sorted(get_scheme_types()))
    st.write("DEBUG queue types:", st.session_state.df_queue["type"].value_counts().to_dict())

    # Files whose type isn't in the workflow scheme can't be inspected, so mark
    # them inspected, push them back to the files table now, and keep them out
    # of the queue the user sees.
    auto_inspect_unlisted_types()

    # Return early if queue is empty
    if st.session_state.df_queue.empty:
        ui.push_message(f"The {workflow} queue if empty!", type="warning")
        return

def render_queue_table():
    """
    Render interactive queue selection interface.
    
    Returns:
        DataFrame: Subset of df_queue with selected rows
    """
    df_queue = st.session_state.get("df_queue", pd.DataFrame())
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
            if st.button("Select all", key="queue_select_all"):
                st.session_state.queue_selected_mask = [True] * num_rows
        with c2:
            if st.button("Clear selection", key="queue_clear_selection"):
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
            ui.push_message("No files selected from queue", type="warning")
            st.stop()

        utils.classify_files()

        # Process selected rows
        if st.session_state.get('df_inspect', pd.DataFrame()).empty:
            try:
                result_table.main()
            except Exception as e:
                ui.push_message("Error: Problem loading the queue.")
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

        workflows = utils.get_workflows(source_uri)
        workflow = st.selectbox(
            f"Use the drop-down menu to select a workflow (source: {source_uri})",
            [""] + workflows,
        )
        if workflow:
            st.session_state.inspect_workflow = workflow
            utils.select_scheme()
            st.markdown("##### Check the results queue")

            c1, c2, c3 = st.columns([1, 2, 1])

            with c1:
                if st.button("Check Queue"):
                    check_queue()

            with c2:
                st.text_input(
                    "Run to import",
                    key="import_run_input",
                    label_visibility="collapsed",
                    placeholder="Run to import",
                )

            with c3:
                if st.button("Import Run"):
                    import_run()

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

def render_results():
    df_inspect = st.session_state.get('df_inspect', pd.DataFrame())
    if df_inspect.empty:
        ui.push_message("No files")
        return
    for c in ("accept", "reject"):
        if c not in df_inspect.columns:
            df_inspect[c] = False

    # Reorder columns with accept/reject first, and normalize the index so it
    # lines up positionally with the data_editor's edited_rows keys.
    first = ["accept", "reject"]
    df_inspect = df_inspect[first + [c for c in df_inspect.columns if c not in first]]
    df_inspect = df_inspect.reset_index(drop=True)

    # Persist a working copy across reruns so select-all/clear-all and the
    # mutual-exclusion fixups stick instead of getting wiped each rerun.
    # check_queue() clears "df_inspect_working" whenever a fresh queue is
    # pulled, so this only (re)initializes on a genuinely new result set.
    if "df_inspect_working" not in st.session_state:
        st.session_state.df_inspect_working = df_inspect.copy()

    df_working = st.session_state.df_inspect_working

    # --- Select all / clear all controls ---
    c1, c2, c3, c4, _ = st.columns([1.3, 1.1, 1.3, 1.1, 4.2])
    with c1:
        if st.button("Select all accept", key="results_select_all_accept"):
            df_working["accept"] = True
            df_working["reject"] = False
    with c2:
        if st.button("Clear accept", key="results_clear_accept"):
            df_working["accept"] = False
    with c3:
        if st.button("Select all reject", key="results_select_all_reject"):
            df_working["reject"] = True
            df_working["accept"] = False
    with c4:
        if st.button("Clear reject", key="results_clear_reject"):
            df_working["reject"] = False

    df_edited = st.data_editor(
        df_working,
        column_config={
            col: st.column_config.Column(disabled=True)
            for col in df_working.columns
            if col not in ["accept", "reject"]
        },
        num_rows="dynamic",
        use_container_width=True,
        key="editor",
    )

    # --- Enforce accept/reject mutual exclusivity (neither selected is OK) ---
    # st.session_state["editor"] holds the raw widget diff for this run:
    # {"edited_rows": {row_idx: {col: new_value, ...}, ...}, ...}
    editor_state = st.session_state.get("editor", {})
    edited_rows = editor_state.get("edited_rows", {})

    needs_rerun = False
    for row_idx, changes in edited_rows.items():
        i = int(row_idx)
        if i >= len(df_edited):
            continue
        if changes.get("accept") is True and bool(df_edited.iloc[i]["reject"]):
            df_edited.iat[i, df_edited.columns.get_loc("reject")] = False
            needs_rerun = True
        if changes.get("reject") is True and bool(df_edited.iloc[i]["accept"]):
            df_edited.iat[i, df_edited.columns.get_loc("accept")] = False
            needs_rerun = True

    st.session_state.df_inspect_working = df_edited

    if needs_rerun:
        st.rerun()

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
    file_types = list(st.session_state.get("inspect_reportable_file_types", []))

    df_files   = st.session_state.get("df_processed", pd.DataFrame())
    df_results = st.session_state.get("df_edited", pd.DataFrame())

    undecided_mask = ~((df_results["accept"] == True) | (df_results["reject"] == True))
    undecided_files = (
        df_results.loc[undecided_mask, file_types]
        .stack()
        .explode()          # list cells -> one path per row; scalar cells pass through
        .dropna()
        .astype(str)
        .str.strip()
    )
    undecided_files = undecided_files[undecided_files != ""].tolist()

    # Map each undecided path back to its run and whether it's global (no id_alt).
    # Plain dicts instead of reindex()/groupby() so a path with no match in
    # df_files can't silently turn into an unusable NaN group key.
    lookup = (
        df_files[["current", "run", "id_alt"]]
        .assign(current=lambda d: d["current"].astype(str).str.strip())
        .drop_duplicates(subset="current", keep="last")
        .set_index("current")
    )
    run_by_path = lookup["run"].to_dict()
    global_by_path = (lookup["id_alt"].astype(str).str.strip() == "").to_dict()

    run_entries = defaultdict(list)
    for path in undecided_files:
        run = run_by_path.get(path)              # None if path isn't in df_files
        is_global = global_by_path.get(path, False)  # unmatched paths default non-global
        run_entries[run].append((path, is_global))

    # A run's undecided globals only get excused if every undecided path in
    # that run is global. Unmatched paths default to is_global=False, so they
    # (and any run they land in) are never mistakenly excused.
    excused = {
        path
        for entries in run_entries.values()
        if all(is_global for _, is_global in entries)
        for path, _ in entries
    }

    undecided_files = [p for p in undecided_files if p not in excused]

    df_undecided = df_files[df_files["current"].isin(undecided_files)].copy()
    df_decided = df_files[~df_files["current"].isin(undecided_files)].copy()

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
                ui.push_message("User must be defined!")
                return
            try:
                now_iso = datetime.now(timezone.utc).isoformat()

                # --- results log ---
                df_results = df_edited[(df_edited["accept"]) | (df_edited["reject"])]
                if df_results.empty:
                    ui.push_message("Something went wrong!")
                    return

                df_results["inspected_by"] = user
                df_results["inspected_at"] = now_iso
                df_results['status'] = df_results['accept'].apply(lambda x: 'accept' if x else 'reject')
                df_results = df_results.astype("string")

                # --- files table: update inspected -> True ---
                if df_decided.empty:
                    ui.push_message("Something went wrong!")
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
                ui.push_message(f"Submission failed: {e}")
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

def import_run():
    """Import a run into the queue for inspection. This is used when the queue is empty, and the user has not yet selected a workflow."""
    source_bucket = st.session_state.get('res_bucket')
    workflow = st.session_state.get("inspect_workflow")
    if not workflow:
        ui.push_message("You must select a workflow before importing a run.", type="warning")
        return
    run_to_import = st.session_state.get("import_run_input", "").strip()

    if not run_to_import:
        ui.push_message("Please enter a run to import.", type="warning")
        return
    try:
        uri = f"s3://{source_bucket}/{FILES_PREFIX}"

        # Only filter on the partition column
        df = io_ops.read_delta_as_pandas(
            uri,
            filters=[("workflow_alt", "=", workflow)],
        )

        # Filter by run in pandas
        df = df[df["run"] == run_to_import]

        if df.empty:
            ui.push_message(
                f"No files found for run '{run_to_import}' in workflow '{workflow}'.",
                type="warning",
            )
            return

        df = df.copy()  # Avoid SettingWithCopyWarning

        df["inspected"] = False
        df["inspected_by"] = None
        df["inspected_at"] = None

        io_ops.write_delta(
            df=df,
            uri=uri,
            key_cols=FILES_TABLE_MERGE_KEYS,
            partition_by=FILES_TABLE_PARTITIONS,
        )

        check_queue()
    except Exception as e:
        ui.push_message(f"Failed to import run {run_to_import}: {e}", type="error")
        st.exception(e)
    