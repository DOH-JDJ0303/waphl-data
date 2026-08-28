import streamlit as st
import boto3
import pandas as pd
import pyarrow as pa
import math
from collections import defaultdict
from deltalake import DeltaTable
from dashboard.editor import ui
import shared.ui
from shared.io_ops import RESULTS_PREFIX, FILES_PREFIX

# === Config ===
SOURCE_BUCKET = st.session_state.res_bucket
S3            = boto3.session.Session().client("s3")
PATH_COLUMN   = "current"          # column holding the s3:// URI for each row
NUMERIC_TYPES = {"byte", "short", "int", "integer", "long", "float", "double", "decimal"}
DEFAULT_RETENTION_HOURS = 168      # 7 days — Delta's default retention floor
EDIT_ROW_LIMIT = 500               # st.data_editor gets sluggish well before this
READ_MODE     = "🔍 Read-only"
WRITE_MODE    = "✏️ Read / write"


# --------------------------------------------------------------------------- #
# Delta helpers — mirrors the CLI script's behaviour
# --------------------------------------------------------------------------- #
def load_delta(uri):
    """Built on demand, never cached: a cached handle would go stale the moment
    a delete commits a new version."""
    return DeltaTable(uri, storage_options={"AWS_S3_ALLOW_UNSAFE_RENAME": "true"})


def column_type(dt, column):
    for f in dt.schema().fields:
        if f.name == column:
            return str(f.type).strip('"').lower()
    return None


def coerce_value(dt, column, value):
    """Free-text input is always a str; the read path needs it typed to match."""
    ctype = column_type(dt, column)
    try:
        if ctype == "boolean":
            return value.strip().lower() in ("true", "1", "yes", "t")
        if ctype in {"byte", "short", "int", "integer", "long"}:
            return int(value)
        if ctype in {"float", "double", "decimal"}:
            return float(value)
    except (ValueError, TypeError):
        pass
    return value


def build_predicate(dt, column, value):
    ctype = column_type(dt, column)
    if ctype in NUMERIC_TYPES or ctype == "boolean":
        return f"{column} = {value}"
    escaped = str(value).replace("'", "''")
    return f"{column} = '{escaped}'"


def full_predicate(dt, pairs):
    return " AND ".join(build_predicate(dt, c, v) for c, v in pairs)


def collect_paths(df, column=PATH_COLUMN):
    """s3:// URIs referenced by the matched rows, deduped and blank-stripped."""
    if df is None or column not in df.columns:
        return set()
    return {str(v).strip() for v in df[column].dropna().tolist() if str(v).strip()}


def diff_mask(original, edited):
    """Cell-level changes. NaN != NaN in pandas, so unchanged nulls have to be
    masked out explicitly or every null cell reads as edited."""
    aligned = edited.reindex(columns=original.columns)
    both_null = original.isna() & aligned.isna()
    return (original != aligned) & ~both_null


def changed_cells(original, edited):
    return diff_mask(original, edited).to_numpy().sum()


def changed_rows(original, edited):
    mask = diff_mask(original, edited)
    return edited.loc[mask.any(axis=1)].copy()


def arrow_schema(dt):
    """deltalake renamed this between versions; try both before giving up."""
    schema = dt.schema()
    for attr in ("to_arrow", "to_pyarrow"):
        if hasattr(schema, attr):
            return getattr(schema, attr)()
    return None


def merge_rows(dt, rows, key_col):
    """Write edited rows back by matching on key_col. Casting to the table's own
    schema first, since an edited frame can come back with object dtypes."""
    schema = arrow_schema(dt)
    source = pa.Table.from_pandas(rows, preserve_index=False)
    if schema is not None:
        source = source.cast(schema)
    (
        dt.merge(
            source=source,
            predicate=f"target.{key_col} = source.{key_col}",
            source_alias="source",
            target_alias="target",
        )
        .when_matched_update_all()
        .execute()
    )


def parse_s3_uri(uri):
    if not uri.startswith("s3://"):
        return None
    bucket, _, key = uri[5:].partition("/")
    if not bucket or not key:
        return None
    return bucket, key


def delete_s3_objects(paths):
    """Delete objects, grouped per bucket. Returns (deleted, errors, skipped)."""
    by_bucket = defaultdict(list)
    skipped = []
    for p in paths:
        parsed = parse_s3_uri(p)
        by_bucket[parsed[0]].append(parsed[1]) if parsed else skipped.append(p)

    deleted, errors = 0, []
    for bucket, keys in by_bucket.items():
        for i in range(0, len(keys), 1000):      # delete_objects caps at 1000/request
            chunk = keys[i:i + 1000]
            resp = S3.delete_objects(
                Bucket=bucket, Delete={"Objects": [{"Key": k} for k in chunk]}
            )
            deleted += len(resp.get("Deleted", []))
            errors += [f"{bucket}/{e.get('Key')}: {e.get('Message')}" for e in resp.get("Errors", [])]
    return deleted, errors, skipped


# === Page ===
st.title("Table Editor")

shared.ui.module_overview(
    "Search production tables by column or partition, preview and export the matching rows, "
    "and — in read / write mode — permanently remove those rows, optionally along with the "
    "S3 objects they reference."
)


@st.dialog("Entering read / write mode", dismissible=False)
def write_mode_warning():
    st.warning("**Read / write mode can permanently destroy data.**")
    st.markdown(
        "In this mode the page can:\n"
        "- delete rows from the Delta table\n"
        f"- delete the S3 objects those rows reference (on `{FILES_PREFIX}`)\n"
        "- vacuum the table, which discards the history that would let you undo any of it\n\n"
        "Nothing happens without confirmation, but nothing here is reversible either."
    )
    back, go = st.columns(2)
    if back.button("Stay read-only", use_container_width=True):
        # Writing to the widget's key before it renders is what actually moves
        # the radio back; clearing the ack alone would leave it on write.
        st.session_state["mode_select"] = READ_MODE
        st.session_state["acked_mode"] = None
        st.rerun()
    if go.button("I understand", type="primary", use_container_width=True):
        st.session_state["acked_mode"] = WRITE_MODE
        st.rerun()


# --------------------------------------------------------------------------- #
# Step 1: mode. Defaults to read-only; writing has to be chosen deliberately.
# --------------------------------------------------------------------------- #
mode = st.radio(
    "Mode",
    options=[READ_MODE, WRITE_MODE],
    horizontal=True,
    key="mode_select",
    help="Read-only cannot modify anything. Read / write adds the destructive controls.",
)

if mode == WRITE_MODE:
    # Warn on entry, and again on any later re-entry, since leaving clears the ack.
    if st.session_state.get("acked_mode") != WRITE_MODE:
        write_mode_warning()
        st.stop()          # keep the rest of the page dark until acknowledged
else:
    # Leaving write mode disarms anything already staged, so returning later
    # never lands on a pre-armed confirmation.
    st.session_state["acked_mode"] = None
    st.session_state["delete_armed"] = False
    st.session_state["confirm_delete"] = False
    st.session_state["edit_armed"] = False
    st.session_state["confirm_edit"] = False

# --------------------------------------------------------------------------- #
# Step 2: pick the table. Nothing below renders until a choice is made.
# index=None is what makes that possible — a selectbox with a plain options
# list always returns options[0], so there is no "unselected" state without it.
# --------------------------------------------------------------------------- #
table_choice = st.selectbox(
    "Table",
    options=[RESULTS_PREFIX, FILES_PREFIX],
    index=None,
    placeholder="Choose a table…",
)

if table_choice is None:
    st.info("Select a table to get started.")
    st.stop()

URI = f"s3://{SOURCE_BUCKET}/{table_choice}"
st.caption(f"Reading from `{URI}`")

try:
    partition_cols, all_cols = ui.get_table_meta(URI)
except Exception as e:
    st.error(f"Couldn't open the table: {e}")
    st.stop()

# Partitions first, then the remaining columns. Union rather than concat, since
# get_table_meta may or may not already list the partition keys in all_cols —
# this way a column can't appear twice with contradictory values.
partition_set = set(partition_cols)
filterable = list(partition_cols) + [c for c in all_cols if c not in partition_set]


@st.cache_data(show_spinner=False)
def partition_values(uri, column):
    """Partition values come from directory names, so this is cheap enough to
    offer as a dropdown. Cached because it re-runs on every widget interaction."""
    return ui.get_partition_values(uri, column)

# Switching tables invalidates everything downstream.
if st.session_state.get("previous_table") != table_choice:
    st.session_state["previous_table"] = table_choice
    st.session_state["filter_rows"] = [0]
    st.session_state["next_row_id"] = 1
    st.session_state.pop("df", None)
    st.session_state.pop("loaded_filters", None)
    st.session_state.pop("confirm_delete", None)
    st.session_state.pop("delete_armed", None)
    st.session_state.pop("confirm_edit", None)
    st.session_state.pop("edit_armed", None)

st.session_state.setdefault("filter_rows", [0])
st.session_state.setdefault("next_row_id", 1)

with st.container():
    st.header("Filters")

    # Each row is a column/value pair side by side. Partition values come from
    # directory names, so those get a dropdown; every other column falls back to
    # free text, since enumerating its distinct values means scanning the data.
    active = []
    for idx, row_id in enumerate(list(st.session_state["filter_rows"])):
        c_col, c_val, c_rm = st.columns([3, 5, 1], vertical_alignment="bottom")
        labels = "visible" if idx == 0 else "collapsed"

        column = c_col.selectbox(
            "Column / partition",
            filterable,
            index=None,
            placeholder="Choose a column / partition…",
            key=f"fcol_{table_choice}_{row_id}",
            label_visibility=labels,
        )

        # Value widgets are keyed on the column as well as the row, so switching
        # the column clears the old value instead of carrying something that
        # isn't valid for the new one.
        raw_value = None
        if column is None:
            c_val.text_input(
                "Equals",
                value="",
                placeholder="Choose a column first",
                disabled=True,
                key=f"fnull_{table_choice}_{row_id}",
                label_visibility=labels,
            )
        elif column in partition_set:
            try:
                raw_value = c_val.selectbox(
                    "Equals",
                    partition_values(URI, column),
                    index=None,
                    placeholder="Choose a value…",
                    key=f"fpart_{table_choice}_{row_id}_{column}",
                    label_visibility=labels,
                )
            except Exception as e:
                # Fall back to typing rather than losing the filter entirely.
                c_val.caption(f"Couldn't list values ({e}) — type one instead:")
                raw_value = c_val.text_input(
                    "Equals",
                    key=f"fpartfree_{table_choice}_{row_id}_{column}",
                    placeholder="value",
                    label_visibility="collapsed",
                )
        else:
            raw_value = c_val.text_input(
                "Equals",
                key=f"fval_{table_choice}_{row_id}_{column}",
                placeholder="value",
                label_visibility=labels,
            )

        if c_rm.button("✕", key=f"frm_{row_id}", help="Remove this filter"):
            st.session_state["filter_rows"].remove(row_id)
            st.rerun()

        if column and raw_value is not None and str(raw_value).strip():
            active.append((column, str(raw_value).strip()))

    if st.button("➕ Add filter"):
        st.session_state["filter_rows"].append(st.session_state["next_row_id"])
        st.session_state["next_row_id"] += 1
        st.rerun()

    seen = [c for c, _ in active]
    duplicates = {c for c in seen if seen.count(c) > 1}
    if duplicates:
        st.warning(
            f"{', '.join(sorted(duplicates))} filtered more than once — conditions are ANDed, "
            "so conflicting values return nothing."
        )

    if not active:
        st.caption("⚠️ No filters set — the whole table will be read.")

    page_size = st.number_input("Rows per page", min_value=1, value=10, step=5)

    load = st.button("Load data", type="primary", use_container_width=True)

# Reading is gated on the button; the result lives in session_state so that
# changing the page or page size never triggers another read from S3.
if load:
    try:
        dt = load_delta(URI)
        typed = [(c, coerce_value(dt, c, v)) for c, v in active]
        df = ui.load_data(URI, tuple((c, "=", v) for c, v in typed) or None)
    except Exception as e:
        st.error(f"Read failed: {e}")
        st.stop()
    st.session_state["df"] = df
    # Pinned so the delete acts on exactly what produced the frame on screen,
    # not on filter widgets the user may have edited since.
    st.session_state["loaded_filters"] = active
    # A fresh read invalidates anything staged against the previous frame.
    st.session_state["delete_armed"] = False
    st.session_state["edit_armed"] = False
    # Rotating the token gives st.data_editor a new key, discarding edits that
    # were made against the frame this read just replaced.
    st.session_state["load_token"] = st.session_state.get("load_token", 0) + 1

# --------------------------------------------------------------------------- #
# Results: summary, download, paginated preview
# --------------------------------------------------------------------------- #
df = st.session_state.get("df")

if df is None:
    st.info("Set your filters above, then click **Load data**.")
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

# --------------------------------------------------------------------------- #
# Delete — everything below is unreachable in read-only mode
# --------------------------------------------------------------------------- #
if mode != WRITE_MODE:
    st.caption(f"Read-only. Switch to **{WRITE_MODE}** at the top to modify this table.")
    st.stop()

@st.dialog("Confirm changes", dismissible=False)
def confirm_edit_dialog(rows, key_col):
    st.warning(f"**Writing {len(rows):,} edited row(s) back to the table.**")
    st.markdown(f"Rows are matched on `{key_col}`; matched rows are overwritten in full.")
    st.dataframe(rows, use_container_width=True)

    acknowledged = st.checkbox("I understand this overwrites the stored values.")

    cancel, go = st.columns(2)
    if cancel.button("Cancel", key="edit_dialog_cancel", use_container_width=True):
        st.session_state["confirm_edit"] = False
        st.rerun()

    if go.button("Save", type="primary", disabled=not acknowledged, use_container_width=True):
        try:
            merge_rows(load_delta(URI), rows, key_col)
        except Exception as e:
            st.session_state["confirm_edit"] = False
            st.session_state["edit_armed"] = False
            st.session_state["edit_result"] = ("error", f"Save failed, nothing written: {e}")
            st.rerun()

        st.session_state["confirm_edit"] = False
        st.session_state["edit_armed"] = False
        st.session_state["edit_result"] = ("ok", f"Saved {len(rows):,} row(s).")
        # The in-memory frame is now behind the table; force a fresh read.
        st.session_state.pop("df", None)
        st.session_state.pop("loaded_filters", None)
        partition_values.clear()
        st.rerun()


# # --------------------------------------------------------------------------- #
# # Edit — per-row writeback via merge
# # --------------------------------------------------------------------------- #
# st.divider()
# st.subheader("✏️ Edit rows")

# with st.container(border=True):
#     if total > EDIT_ROW_LIMIT:
#         st.info(
#             f"{total:,} rows loaded — editing is capped at {EDIT_ROW_LIMIT:,}. "
#             "Narrow the filters and reload to edit."
#         )
#     else:
#         # merge() matches on a predicate, so per-row writeback needs a column
#         # that identifies a row uniquely. Without one, an edit to a single row
#         # would be applied to every row sharing that value.
#         key_col = st.selectbox(
#             "Key column",
#             df.columns.tolist(),
#             index=None,
#             placeholder="Which column uniquely identifies a row?",
#             help="Used to match edited rows back to the table. Must be unique.",
#         )

#         if key_col is None:
#             st.caption("Choose a key column to start editing.")
#         elif df[key_col].duplicated().any():
#             dupes = int(df[key_col].duplicated().sum())
#             st.error(
#                 f"`{key_col}` has {dupes:,} duplicate value(s) in this result set, so it "
#                 "can't identify rows. Pick another column."
#             )
#         elif df[key_col].isna().any():
#             st.error(f"`{key_col}` contains nulls, which can't be matched on. Pick another column.")
#         else:
#             # The key is what rows are matched on, and partition values decide
#             # which files a row lives in — neither can be edited in place.
#             locked = sorted({key_col} | (partition_set & set(df.columns)))
#             st.caption(f"Locked: {', '.join(f'`{c}`' for c in locked)}")

#             edited = st.data_editor(
#                 df,
#                 use_container_width=True,
#                 num_rows="fixed",          # merge updates rows; it can't add or drop them
#                 disabled=locked,
#                 key=f"editor_{table_choice}_{st.session_state.get('load_token', 0)}",
#             )

#             changed = changed_rows(df, edited)
#             n_cells = int(changed_cells(df, edited))

#             if changed.empty:
#                 st.caption("No changes yet.")
#             else:
#                 st.warning(f"**{len(changed):,} row(s) changed** ({n_cells:,} cell(s)).")
#                 st.dataframe(changed, use_container_width=True)

#                 if not st.session_state.get("edit_armed"):
#                     if st.button("Review changes…", use_container_width=True):
#                         st.session_state["edit_armed"] = True
#                         st.session_state["confirm_delete"] = False
#                         st.rerun()
#                 else:
#                     cancel_col, save_col = st.columns(2)
#                     if cancel_col.button("← Cancel", key="edit_cancel", use_container_width=True):
#                         st.session_state["edit_armed"] = False
#                         st.rerun()
#                     if save_col.button("Save to table", type="primary", use_container_width=True):
#                         st.session_state["confirm_edit"] = True
#                         st.rerun()

#                 if st.session_state.get("confirm_edit"):
#                     confirm_edit_dialog(changed, key_col)

# edit_result = st.session_state.pop("edit_result", None)
# if edit_result:
#     (st.success if edit_result[0] == "ok" else st.error)(edit_result[1])

# --------------------------------------------------------------------------- #
# Delete
# --------------------------------------------------------------------------- #
st.divider()
st.subheader("🗑️ Delete these rows")

pinned = st.session_state.get("loaded_filters") or []

# Path count is computed regardless of the toggle so the number is on screen
# *before* the decision, not after it. The frame is already in memory, so this
# costs nothing.
all_paths = sorted(collect_paths(df))

# --- associated files: the consequential choice, so it gets its own panel ----
with st.container(border=True):
    st.markdown("### 📁 Associated files")

    if table_choice != FILES_PREFIX:
        # Only the files table owns the objects; a path in any other table is a
        # reference to something it doesn't control, so deleting from here would
        # break rows elsewhere with no record of it.
        also_delete_files = False
        st.caption(
            f"File deletion is only available on `{FILES_PREFIX}`. "
            f"On `{table_choice}` the rows are removed but the S3 objects are left alone."
        )
    elif PATH_COLUMN not in df.columns:
        also_delete_files = False
        st.caption(f"This table has no `{PATH_COLUMN}` column — nothing to delete.")
    elif not all_paths:
        also_delete_files = False
        st.caption(f"No rows reference a path in `{PATH_COLUMN}`.")
    else:
        st.markdown(
            f"These rows reference **{len(all_paths):,} file(s)** in S3 via `{PATH_COLUMN}`."
        )
        also_delete_files = st.toggle(
            "**Delete these files from S3 too**",
            value=False,
            help="Off: rows go, objects stay (and become orphaned). "
                 "On: the objects are permanently removed as well.",
        )
        if also_delete_files:
            st.error(f"**{len(all_paths):,} S3 object(s) will be permanently deleted.**")
            st.caption(f"For example: `{all_paths[0]}`")
        else:
            st.info("Rows only — the S3 objects stay where they are.")

file_paths = all_paths if also_delete_files else []

# --- vacuum ------------------------------------------------------------------
run_vacuum = st.toggle(
    "Vacuum the table afterwards",
    value=False,
    help="A delete only tombstones rows; the underlying Parquet stays until vacuumed. "
         "Vacuum reclaims that space and is what makes the delete unrecoverable.",
)

retention_hours = DEFAULT_RETENTION_HOURS
if run_vacuum:
    retention_hours = st.number_input(
        "Retention (hours)",
        min_value=0,
        value=DEFAULT_RETENTION_HOURS,
        step=24,
        help="Files newer than this are kept. Delta's default floor is 168h (7 days).",
    )
    if retention_hours < DEFAULT_RETENTION_HOURS:
        st.warning(
            f"Below the {DEFAULT_RETENTION_HOURS}h floor, so the retention check has to be "
            "disabled. This can break in-flight readers and destroys time travel to any "
            "version older than the retention window."
        )


@st.dialog("Confirm deletion", dismissible=False)
def confirm_delete_dialog(row_count, paths, filters, delete_files, vacuum, retention):
    st.error(
        f"**You are about to permanently delete {row_count:,} row(s)"
        + (f" and {len(paths):,} file(s)" if delete_files else "")
        + ".**"
    )
    st.markdown("This cannot be undone. Matching rows:")
    st.code(" AND ".join(f"{c} = {v}" for c, v in filters) or "(no filters — entire table)")
    if delete_files and paths:
        st.caption(f"Example object: `{paths[0]}`")
    if vacuum:
        st.markdown(
            f"Then vacuuming with **{retention}h** retention, which drops any table version "
            "older than that window."
        )

    acknowledged = st.checkbox("I understand this is permanent.")

    cancel, go = st.columns(2)
    if cancel.button("Cancel", use_container_width=True):
        st.session_state["confirm_delete"] = False
        st.session_state["delete_armed"] = False
        st.rerun()

    if go.button("Delete", type="primary", disabled=not acknowledged, use_container_width=True):
        try:
            dt = load_delta(URI)
            dt.delete(full_predicate(dt, filters))
        except Exception as e:
            st.session_state["confirm_delete"] = False
            st.session_state["delete_armed"] = False
            st.session_state["delete_result"] = ("error", f"Row delete failed: {e}")
            st.rerun()

        summary = f"Deleted {row_count:,} row(s)."
        if delete_files and paths:
            deleted, errors, skipped = delete_s3_objects(paths)
            summary += f" Deleted {deleted:,} file(s)."
            if skipped:
                summary += f" Skipped {len(skipped):,} non-s3:// path(s)."
            if errors:
                summary += f" {len(errors):,} object(s) failed: {errors[0]}"

        if vacuum:
            try:
                # dry_run defaults to True in the Python bindings — without this
                # it reports candidates and removes nothing.
                removed = dt.vacuum(
                    retention_hours=retention,
                    dry_run=False,
                    enforce_retention_duration=retention >= DEFAULT_RETENTION_HOURS,
                )
                summary += f" Vacuum removed {len(removed):,} file(s)."
            except Exception as e:
                summary += f" Rows were deleted, but vacuum failed: {e}"

        st.session_state["confirm_delete"] = False
        st.session_state["delete_armed"] = False
        st.session_state["delete_result"] = ("ok", summary)
        st.session_state.pop("df", None)
        st.session_state.pop("loaded_filters", None)
        # A delete can empty a partition, so the cached value lists are stale.
        partition_values.clear()
        st.rerun()


# --- arm / cancel / confirm --------------------------------------------------
# Two stages before the modal: review what is about to happen, and back out
# without ever pressing anything labelled "Delete".
if not pinned:
    st.warning("Deleting with no filters would empty the table. Add at least one filter and reload.")
elif not st.session_state.get("delete_armed"):
    if st.button("Review delete…", use_container_width=True):
        st.session_state["delete_armed"] = True
        st.rerun()
else:
    with st.container(border=True):
        st.markdown("#### Ready to delete")
        st.markdown(f"- **{total:,}** row(s) matching `{' AND '.join(f'{c} = {v}' for c, v in pinned)}`")
        if also_delete_files:
            st.markdown(f"- **{len(file_paths):,}** S3 object(s) from `{PATH_COLUMN}`")
        else:
            st.markdown("- No files — S3 objects will be left in place")
        if run_vacuum:
            st.markdown(f"- Vacuum at **{retention_hours}h** retention")

        cancel_col, delete_col = st.columns(2)
        if cancel_col.button("← Cancel", use_container_width=True):
            st.session_state["delete_armed"] = False
            st.rerun()
        if delete_col.button("Delete", type="primary", use_container_width=True):
            st.session_state["confirm_delete"] = True
            st.session_state["confirm_edit"] = False
            st.rerun()

if st.session_state.get("confirm_delete"):
    confirm_delete_dialog(
        total, file_paths, pinned, also_delete_files, run_vacuum, retention_hours
    )

result = st.session_state.pop("delete_result", None)
if result:
    (st.success if result[0] == "ok" else st.error)(result[1])