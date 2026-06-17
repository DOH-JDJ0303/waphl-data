import re
import os
from typing import Any, Dict, Iterable, Optional

import pandas as pd
import fsspec

# =========================
# Table schemas & partitions
# =========================

# ----- All Files Table -----
FILES_TABLE_KEYS = [
    "id",
    "id_alt",
    "workflow",
    "workflow_alt",
    "run",
    "file",
    "timestamp",
    "reportable",
    "type",
    "origin",
    "current",
    "inspected",
]
FILES_TABLE_KEYS_MUTABLE = ["reportable", "inspected"]
FILES_TABLE_MERGE_KEYS = [k for k in FILES_TABLE_KEYS if k not in FILES_TABLE_KEYS_MUTABLE]
FILES_TABLE_PARTITIONS = ["workflow_alt", "reportable"]

# ----- Results Table -----
RESULTS_TABLE_KEYS = [
    "id",
    "id_alt",
    "workflow_alt",
    "run",
    "filepaths",
    "raw_reads",
    "assembly",
    "log",
    "summary",
    "status",
]
RESULTS_TABLE_KEYS_MUTABLE = ["status"]
RESULTS_TABLE_MERGE_KEYS = [k for k in RESULTS_TABLE_KEYS if k not in RESULTS_TABLE_KEYS_MUTABLE]
RESULTS_TABLE_PARTITIONS = ["workflow_alt", "status"]

# ----- Terra Cache -----
TERRA_CACHE_TABLE_KEYS = [
    "project",
    "workspace",
    "entity_type",
    "workflow_name",
    "submission_id",
    "submission_time",
    "transfer_status",
]
TERRA_CACHE_TABLE_KEYS_MUTABLE = ["transfer_status"]
TERRA_CACHE_TABLE_MERGE_KEYS = [
    k for k in TERRA_CACHE_TABLE_KEYS if k not in TERRA_CACHE_TABLE_KEYS_MUTABLE
]
TERRA_CACHE_TABLE_PARTITIONS = ["project", "workspace"]


# ==========
# Utilities
# ==========

def assign_alt_id(id_str: Optional[str], run: Optional[str]) -> Optional[str]:
    """
    Derive an alternate ID from `id_str` by matching known patterns.
    Falls back to removing a trailing run suffix and WA-suffixed segments.

    Returns None if `id_str` is falsy.
    """
    if not id_str:
        return None

    patterns = [r"(\d{4}JQ-\d{5})", r"(WA\d{7})"]
    for pattern in patterns:
        match = re.search(pattern, id_str)
        if match:
            return match.group(1)

    cleaned = re.sub(fr"[-_]?{re.escape(run or '')}$", "", id_str)
    cleaned = re.sub(r"-WA.*", "", cleaned)
    cleaned = cleaned.strip()
    return cleaned if cleaned else id_str


def assign_alt_workflow(workflow_str: Optional[str]) -> Optional[str]:
    """
    Map a workflow string to a known short name if it contains one of the
    recognized substrings. Otherwise, return the original string.
    """
    if not workflow_str:
        return None

    mapping = ("phoenix", "vaper", "mycosnp", "theiaprok", "recapp", "basespace")
    lowered = workflow_str.lower()
    for wf in mapping:
        if wf in lowered:
            return wf
    return workflow_str


def standardize_data(
    data: Dict[str, Any],
    default_columns: Optional[Iterable[str]] = None,
) -> Dict[str, Any]:
    """
    Enrich a record dict with standardized fields and defaults.

    - Sets id_alt, workflow_alt, inspected
    - Ensures any `default_columns` exist with None values if missing
    """
    data["id_alt"] = assign_alt_id(data.get("id", ""), data.get("run", ""))
    data["workflow_alt"] = assign_alt_workflow(data.get("workflow", ""))
    data["inspected"] = False

    if default_columns:
        for c in default_columns:
            if c not in data:
                data[c] = None

    return data


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize dtypes:
      - 'timestamp' -> int (coerce invalid to 0)
      - Fill NaN in object columns with empty strings
    """
    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_numeric(df["timestamp"], errors="coerce").fillna(0).astype(int)

    obj_cols = df.select_dtypes(include="object").columns
    if len(obj_cols) > 0:
        df = df.fillna({col: "" for col in obj_cols})

    return df


def extract_stem(filepath: str) -> str:
    """Strip compound extensions like .fa.gz, .tsv, etc."""
    stem = os.path.basename(filepath)
    # Stop after removing known archive/text extensions so we don't kill dots in names
    for _ in range(3):
        s, ext = os.path.splitext(stem)
        if ext.lower() in {".gz", ".bz2", ".xz", ".zip", ".bz"}:
            stem = s
            continue
        if ext.lower() in {".fa", ".fasta", ".fq", ".fastq", ".tsv", ".csv", ".txt"}:
            stem = s
            continue
        break
    return stem

def read_table(path: str) -> pd.DataFrame:
    """Read local or s3:// TSV/CSV, autodetect sep with a safe fallback."""
    try:
        lower = path.lower()
        sep = "\t" if lower.endswith((".tsv", ".txt")) else ("," if lower.endswith(".csv") else None)
        with fsspec.open(path, "rb") as fh:
            if sep is not None:
                return pd.read_csv(fh, sep=sep)
            # try tsv then csv
            try:
                return pd.read_csv(fh, sep="\t")
            except Exception:
                fh.seek(0)
                return pd.read_csv(fh, sep=",")
    except Exception as e:
        print(f"⚠️ Failed to read table {path}: {e}")
        return pd.DataFrame()
