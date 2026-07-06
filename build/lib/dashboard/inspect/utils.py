import streamlit as st
from pathlib import Path
import json
from typing import Dict, Any
from shared import ui
import pandas as pd
import numpy as np
from shared import io_ops

@st.cache_data(ttl=600, show_spinner="Loading workflows...")
def get_workflows(source_uri: str) -> list[str]:
    return io_ops.delta_partition_values(source_uri, "workflow_alt")

def extract_scheme_info():
    """Get lowercased summary cols, reportable file types, and QC rules."""
    scheme = st.session_state.get("inspect_scheme")
    if scheme is None:
        return None, None, None

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

    reportable_files = scheme.get("reportable_files", [])

    file_types = {
        str(entry.get("type", "")).strip()
        for entry in reportable_files
        if entry.get("type")
    }
    file_types.add("raw_reads")

    st.session_state["inspect_summary_cols"] = summary_cols
    st.session_state["inspect_qc_criteria"]  = qc_criteria
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