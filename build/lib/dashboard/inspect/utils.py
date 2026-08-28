import streamlit as st
from pathlib import Path
import json
from typing import Dict, Any
from shared import ui
import pandas as pd
import numpy as np
from shared import io_ops, file_typing
from shared.file_typing import FilePatternMatcher

QUEUE_ORIGIN_COL = "origin"


def get_matcher():
    """Matcher built from the pattern defs stashed by extract_scheme_info."""
    return FilePatternMatcher(
        st.session_state.get("inspect_reportable_files") or []
    )


def type_from_origin(df, matcher=None, overwrite=False, reads_by_extension=False):
    """Re-derive the `type` column from the origin URI.

    By default only rows that were never successfully classified (blank, or
    sitting at "other") are re-typed; rows carrying a real type are left
    alone. Pass overwrite=True to re-derive every row.
    """
    if df.empty or QUEUE_ORIGIN_COL not in df.columns:
        return df

    if matcher is None:
        matcher = get_matcher()
    if not matcher:
        # An empty matcher types everything as "other" -- with overwrite=True
        # that would wipe the column, so bail instead.
        ui.push_message(
            "No file patterns resolved from the scheme; skipping origin-based typing.",
            type="warning",
        )
        return df

    df = df.copy()

    runs = df["run"] if "run" in df.columns else pd.Series("", index=df.index)
    derived = pd.Series(
        [
            file_typing.classify_origin(o, r, matcher, reads_by_extension)
            for o, r in zip(df[QUEUE_ORIGIN_COL], runs)
        ],
        index=df.index,
        dtype="object",
    )

    if "type" not in df.columns or overwrite:
        df["type"] = derived
        return df

    existing = normalize_type_series(df["type"])
    stale = existing.map(file_typing.is_unclassified)
    df.loc[stale, "type"] = derived[stale]
    return df


@st.cache_data(ttl=600, show_spinner="Loading workflows...")
def get_workflows(source_uri: str) -> list[str]:
    return io_ops.delta_partition_values(source_uri, "workflow_alt")


def extract_scheme_info():
    """Get lowercased summary cols, reportable file types, and QC rules."""
    scheme = st.session_state.get("inspect_scheme")
    if scheme is None:
        # Don't leave a previous workflow's patterns in place -- they'd be
        # used to type the next queue pull.
        st.session_state.pop("inspect_reportable_files", None)
        st.session_state.pop("inspect_reportable_file_types", None)
        return None, None, None

    # Raw pattern defs, kept in session_state so classification still works
    # after check_queue() pops "inspect_scheme".
    defs = scheme.get("reportable_files", []) or []
    st.session_state["inspect_reportable_files"] = defs

    # Declared types, not matched types: an entry with a malformed pattern
    # should still count as an allowed type.
    file_types = {
        str(entry.get("type", "")).strip()
        for entry in defs
        if isinstance(entry, dict) and entry.get("type")
    }
    file_types.add(file_typing.READS_TYPE)

    summary_cols = {
        str(k).lower(): (
            [v.lower()] if isinstance(v, str)
            else [str(item).lower() for item in v]
        )
        for k, v in scheme.get("summary_columns", {}).items()
    }

    qc_criteria = []
    for rec in scheme.get("qc_criteria", []):
        if 'column' in rec:
            rec['column'] = str(rec['column']).lower()
            for alt, cols in summary_cols.items():
                rec['column'] = alt if rec['column'] in cols else rec['column']
            qc_criteria.append(rec)

    st.session_state["inspect_summary_cols"] = summary_cols
    st.session_state["inspect_qc_criteria"] = qc_criteria
    st.session_state["inspect_reportable_file_types"] = file_types


@st.cache_data(ttl=600, show_spinner="Loading scheme...")
def _load_scheme(workflow: str) -> dict:
    scheme_dir = Path("./schemes")
    for schema_file in scheme_dir.glob("*.json"):
        with open(schema_file) as f:
            data = json.load(f)
        if data.get("workflow") == workflow:
            return data
    return {}


def select_scheme():  # stays uncached — this is the part with side effects
    workflow = st.session_state.get("inspect_workflow")
    if not workflow:
        ui.push_message("No workflow selected")
        return
    data = _load_scheme(workflow)
    if not data:
        ui.push_message(f"No scheme found for workflow: {workflow!r}")
        return
    st.session_state["inspect_scheme"] = data
    extract_scheme_info()


@st.cache_data(show_spinner="Classifying files...", max_entries=10)
def _classify_files(df: pd.DataFrame, file_types: list[str]) -> pd.DataFrame:
    """Pure version: no session_state reads or writes."""
    file_types = list(file_types)

    for col in ("id", "id_alt", "run", "type", "current", "origin"):
        if col not in df.columns:
            df[col] = ""
        df[col] = df[col].where(df[col].notna(), "").astype(str).str.strip()

    is_sample = df["id_alt"] != ""
    grp = np.where(
        is_sample,
        "S|" + df["id"] + "|" + df["id_alt"] + "|" + df["run"] + "|" + df["origin"],
        "G|" + df["run"] + "|" + df["origin"],
    )
    df["_grp"] = grp

    if "timestamp" in df.columns:
        order = df["timestamp"].sort_values(na_position="first", kind="stable").index
    else:
        order = df.index

    superseded = (
        df.loc[order]
          .duplicated(subset="_grp", keep="last")
          .reindex(df.index, fill_value=False)
    )
    df.drop(columns="_grp", inplace=True)

    df["reportable"] = False
    df.loc[df["type"].isin(file_types) & ~superseded, "reportable"] = True
    return df


def classify_files() -> pd.DataFrame:
    df = st.session_state.get("df_to_process", pd.DataFrame())
    if df.empty:
        st.session_state["df_processed"] = df
        return df

    file_types = list(st.session_state.get("inspect_reportable_file_types", []))
    result = _classify_files(df.copy(), file_types)
    st.session_state["df_processed"] = result
    return result


def normalize_type_series(s: pd.Series) -> pd.Series:
    """Same normalization _classify_files applies to the `type` column:
    NaN -> "" -> stripped string."""
    return s.where(s.notna(), "").astype(str).str.strip()