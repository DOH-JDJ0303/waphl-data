#!/usr/bin/env python3
import os
import sys
import json
from pathlib import Path
from typing import Dict, Any, List, Tuple, DefaultDict

import streamlit as st
import pandas as pd
import numpy as np
import fsspec
from collections import defaultdict

from shared import data_processing
from dashboard.inspect import workflow_specific_functions as wsf

# ---------- GLOBALS ----------
BASE_COLS = ["accept", "reject", "id_alt", "run", "workflow_alt"]


# ---------- CONFIG LOADING ----------
def select_scheme() -> Dict[str, Any]:
    """Load the single JSON scheme matching `workflow` from schemes/."""
    workflow = st.session_state.get("inspect_workflow")
    if workflow is None:
        st.error("No workflow selected")
        return
    
    scheme_dir = Path("./schemes")
    if not scheme_dir.is_dir():
        st.error(f"Scheme directory not found: {scheme_dir}")

    for schema_file in scheme_dir.glob("*.json"):
        with open(schema_file, "r") as f:
            data = json.load(f)
        if data.get("workflow") == workflow:
            st.toast(f"Using scheme {schema_file}")
            st.session_state.inspect_scheme = data
            return

    st.error(f"No scheme found for workflow: {workflow!r}")


# ---------- HELPERS ----------
def extract_scheme_info():
    """Get lowercased summary cols, reportable file types, and QC rules."""
    scheme = st.session_state.get("inspect_scheme")
    if scheme is None:
        return None, None, None
    
    summary_cols = { str(k).lower(): str(v).lower() for k, v in scheme.get("summary_columns", {}).items()}

    file_types: set = set()
    for rec in scheme.get("reportable_files", []):
        if isinstance(rec, dict):
            file_types.add(rec.get("type", "other"))

    qc_criteria = []
    for rec in scheme.get("qc_criteria", []):
        if 'column' in rec:
            rec['column'] = str(rec['column']).lower()
            for alt, col in summary_cols.items():
                rec['column'] = alt if rec['column'] == col.lower() else rec['column']
            qc_criteria.append(rec)
    
    return summary_cols, sorted(file_types), qc_criteria


def process_records(records):
    global_files     = defaultdict(lambda: defaultdict(list))   # key: run
    sample_files     = defaultdict(lambda: defaultdict(list))   # key: (id_alt, run)
    file_cache       = defaultdict(lambda: [])
    superceded_files = []

    # Sort by file timestamp
    sorted_records = sorted(records, key=lambda d: d.get("score", float("inf")))

    for row in sorted_records:
        sid        = row.get("id")
        id_alt     = row.get("id_alt")
        run        = row.get("run")
        reportable = bool(row.get("reportable", False))
        ftype_raw  = row.get("type", "other")
        ftype      = (ftype_raw or "other").strip().lower()

        # Get current and origin paths
        origin  = row.get("origin", "").strip()
        current = row.get("current", "").strip()

        if not run or not current or not origin:
            st.error(f"Missing run name or file path. sid={sid}, run={run!r}, current_path={current!r}, origin_path={current!r}")

        fgroup = ftype if reportable else "files_supplementary"

        if id_alt:     # sample-level
            key = (sid, id_alt, run)
            if origin in file_cache.get(key, []):
                superceded_files.append(current)
            else:
                sample_files[key][fgroup].append(current)
                file_cache[key].append(origin)
        else:          # run-level (global)
            key = run
            if origin in file_cache.get(key, []):
                superceded_files.append(current)
            else:
                global_files[key][fgroup].append(current)
                file_cache[key].append(origin)

    if superceded_files:
        st.session_state.get("inspect_warnings", []).append(f"The following files are superceded by a newer version and will be automatically marked 'reject': {superceded_files}")

    return global_files, sample_files, superceded_files


def apply_qc_row(row: pd.Series, criteria: List[Dict[str, Any]]) -> bool:
    """Return True if row passes all numeric min/max rules."""
    if "inspect_warnings" not in st.session_state:
        st.session_state.inspect_warnings = []

    for rule in criteria or []:
        col = rule.get("column", '').lower().strip()

        if not col or col not in row:
            st.session_state.inspect_warnings.append(f"apply_qc_row: Row {row.name}: {col!r} not in data")
            return False

        val = row[col]
        if pd.isna(val) or val is None:
            st.session_state.inspect_warnings.append(f"apply_qc_row: Row {row.name}: value is n/a or null")
            return False

        if rule.get("equals") is not None and str(val) != str(rule['equals']):
            return False

        if rule.get("min_value") is not None:
            try:
                val = pd.to_numeric(val)
                min_value = float(rule["min_value"])
            except Exception:
                return False
            if val < min_value:
                return False

        if rule.get("max_value") is not None:
            try:
                val = pd.to_numeric(val)
                max_value = float(rule["max_value"])
            except Exception:
                return False
            if val > max_value:
                return False

    return True


def build_rows(
    global_files: Dict,
    sample_files: Dict, 
    summary_cols: Dict,
    file_types: List[str],
    workflow: str,
    df_to_process: None,
) -> List[Dict[str, Any]]:
    """
    Build one row per sample; if multiple assemblies exist, make one row per assembly.
    For summary data: read ALL summary files for the sample and, for each summary column,
    join multiple values with ';' (deduplicated, order-preserving). Attach these merged
    summary values to every row for the sample.
    """
    rows: List[Dict[str, Any]] = []
    file_types_set = set(file_types)
    summary_filter_cols = [summary_cols['id'].lower()] if summary_cols.get('id') else []
    summary_filters_vals = []

    def _process_summary_data(
        paths: List[str],
        cols_lower: Dict[str, str],
        filter_vals: List[str] = [],
        filter_cols: List[str] = []
    ) -> Dict[str, Any]:
        # Always collect into lists keyed by the desired output fields
        collected: Dict[str, List[str]] = {out_key: [] for out_key in cols_lower}

        # Pair up filters safely (ignore extras on either side)
        filters = [
            (str(val) if val is not None else None, str(col).lower())
            for val, col in zip(filter_vals or [], filter_cols or [])
        ]

        any_rows_kept = False

        for p in paths or []:
            df = data_processing.read_table(p)
            if df is None or df.empty:
                continue

            df_filt = df.copy()
            df_filt.columns = [str(c).lower() for c in df_filt.columns]

            # Apply filters (substring match, case-sensitive by your original code;
            # switch to .str.contains(..., case=False) if you want case-insensitive)
            for val_str, col in filters:
                if not val_str or col not in df_filt.columns:
                    continue
                mask = df_filt[col].astype(str).str.contains(val_str, na=False)
                df_filt = df_filt[mask]

            if not df_filt.empty:
                any_rows_kept = True

            # Collect requested columns
            for out_key, src_col in cols_lower.items():
                src_col_l = str(src_col).lower()
                if src_col_l in df_filt.columns:
                    vals = (
                        df_filt[src_col_l]
                        .dropna()
                        .astype(str)
                        .tolist()
                    )
                    collected[out_key].extend(vals)

        # If no rows matched anywhere, return all summary fields as NaN
        if not any_rows_kept:
            return {out_key: np.nan for out_key in cols_lower}

        # Validate + merge
        merged: Dict[str, Any] = {}
        for field, vals in collected.items():
            # Deduplicate while preserving order
            seen = set()
            uniq = []
            for v in vals:
                if v not in seen:
                    seen.add(v)
                    uniq.append(v)

            if len(uniq) > 1:
                # Keep your strictness; include filters for easier debugging
                raise ValueError(
                    f"Summary field '{field}' has multiple unique values: {uniq}. "
                    f"Expected exactly one unique value. "
                    f"Filters used: {filter_vals}; {filter_cols}"
                )

            merged[field] = uniq[0] if uniq else np.nan

        return merged
    
    if workflow == 'vaper':
        sample_files, global_files = wsf.process_vaper(sample_files, global_files)
        summary_filter_cols = summary_filter_cols + ['reference']
    if workflow == 'mycosnp':
        sample_files, global_files = wsf.process_mycosnp(sample_files, global_files, df_to_process)

    final_rows = []
    for key, sfiles in sample_files.items():
        if len(key) == 3:
            sid, id_alt, run = key
            summary_filters_vals = [sid]
        else:
            sid, id_alt, run, ref_name = key
            summary_filters_vals = [sid, ref_name]

        row_files = sfiles.copy()

        # Attach global files
        g = global_files.get(run, {})
        # attach global files
        for k2, v2 in g.items():
            row_files.setdefault(k2, [])
            row_files[k2].extend(v2)

        # Gather full list of reportable files
        reportable_files = []
        for k, v in row_files.items():
            if k != 'files_supplementary':
                reportable_files.extend(v if isinstance(v, list) else [])
        
        row_files['files_reportable']    = reportable_files
        row_files['files_supplementary'] = row_files.get('files_supplementary', [])
    
        if 'summary' in row_files:
            summary_data = _process_summary_data(
                row_files['summary'],
                summary_cols, 
                filter_vals = summary_filters_vals, 
                filter_cols = summary_filter_cols
            )

            row_files = row_files | summary_data

        for k in summary_cols.keys():
            row_files.setdefault(k, np.nan)

        base = {
            "id": sid,
            "id_alt": id_alt,
            "run": run,
            "workflow_alt": workflow,
        }

        # overlay: base → row (files) → merged summary values
        full_row = {**base, **row_files}
        final_rows.append(full_row)

    return final_rows


def apply_qc(df_out: pd.DataFrame, qc_criteria: List[Dict[str, Any]]) -> pd.DataFrame:
    st.session_state.inspect_warnings = []
    df_out["accept"] = [apply_qc_row(r, qc_criteria) for _, r in df_out.iterrows()]
    df_out["reject"] = ~df_out["accept"]
    return df_out


def order_columns(df: pd.DataFrame, summary_cols_dict: Dict[str, str]) -> pd.DataFrame:
    summary_cols = list(summary_cols_dict.keys())
    present_base = [c for c in BASE_COLS if c in df.columns]
    other_cols = [c for c in df.columns if c not in present_base + summary_cols]
    return df[present_base + summary_cols + other_cols]


# ---------- MAIN ----------
def main():
    workflow = st.session_state.get("inspect_workflow")
    df_to_process = st.session_state.get("df_to_process", pd.DataFrame())

    if workflow is None or df_to_process.empty:
        return
    
    select_scheme()
    summary_cols, file_types, qc_criteria = extract_scheme_info()

    records = df_to_process.to_dict("records")
    global_files, sample_files, superceded_files = process_records(records)
    rows = build_rows(global_files, sample_files, summary_cols, file_types, workflow, df_to_process)

    df_out = pd.DataFrame(rows)
    if df_out.empty:
        return pd.DataFrame(columns=BASE_COLS + list(summary_cols.keys()))

    df_out = apply_qc(df_out, qc_criteria)
    df_out = order_columns(df_out, summary_cols)

    st.session_state.df_inspect  = df_out
    st.session_state.super_files = superceded_files

if __name__ == "__main__":
    print("This module exposes main(df, workflow) → DataFrame")
