from __future__ import annotations

import json
import os
from typing import Iterable, Optional, Sequence, Union

import boto3
import numpy as np
import pandas as pd
import pyarrow as pa
from botocore.exceptions import ClientError
from deltalake import DeltaTable, write_deltalake

import streamlit as st

# -----------------
# Constants / Paths
# -----------------

RAW_PREFIX         = "data"
FILES_PREFIX       = "tables/metadata/"
RESULTS_PREFIX     = "tables/results/"
TERRA_CACHE_PREFIX = "tables/cache/terra/"


# --------------
# Arrow Helpers
# --------------

def _to_arrow(
    records_or_df: Union[pd.DataFrame, Sequence[dict]],
    expected_cols: Optional[Iterable[str]] = None,
) -> pa.Table:
    """
    Convert records or a DataFrame to a PyArrow table with light type hygiene.
    Ensures:
      - boolean dtype for ('reportable', 'inspected') if present
      - int64 dtype for ('timestamp', 'last_run_time') if present
      - adds any `expected_cols` missing (with sensible defaults)
      - stable column order if `expected_cols` provided
    """
    df = records_or_df if isinstance(records_or_df, pd.DataFrame) else pd.DataFrame(records_or_df)

    # Type hygiene
    for c in ("reportable", "inspected"):
        if c in df.columns:
            df[c] = df[c].astype(bool)
    for c in ("timestamp", "last_run_time"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).astype("int64")

    # Enforce stable column order & presence
    if expected_cols:
        for col in expected_cols:
            if col not in df.columns:
                if col in ("reportable", "inspected", "file_exists"):
                    df[col] = False
                elif col in ("timestamp", "last_run_time"):
                    df[col] = 0
                else:
                    df[col] = ""
        df = df[list(expected_cols)]

    return pa.Table.from_pandas(df, preserve_index=False)


# ---------------------------------------------
# JSON normalization for complex dtype columns
# ---------------------------------------------

def _jsonify_if_complex(x):
    """
    Convert complex objects into compact, deterministic JSON (or scalars).
    Leaves plain scalars/strings as-is. Numpy scalars become Python scalars.
    """
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return None

    # Treat bytes as UTF-8 strings if present
    if isinstance(x, (bytes, bytearray)):
        try:
            return x.decode("utf-8")
        except Exception:
            return str(x)

    # Normalize numpy scalars
    if isinstance(x, np.generic):
        return x.item()

    # numpy arrays -> lists -> JSON
    if isinstance(x, np.ndarray):
        return json.dumps(x.tolist(), ensure_ascii=False, separators=(",", ":"), sort_keys=True)

    # Lists / tuples / sets / dicts -> JSON (sets become lists)
    if isinstance(x, (list, tuple, set, dict)):
        if isinstance(x, set):
            x = list(x)
        return json.dumps(x, ensure_ascii=False, separators=(",", ":"), sort_keys=True)

    return x


def _normalize_lists_to_json_strings(df: pd.DataFrame) -> pd.DataFrame:
    """
    For each column, if any value is list/tuple/dict/set/ndarray, convert ALL such
    values in that column to compact JSON strings. Keep None/NaN as None.
    """
    df = df.copy()
    for col in df.columns:
        s = df[col]
        needs_json = False

        # Quick sample check to avoid unnecessary mapping over entire column
        sample = s.values[:100] if len(s) > 100 else s.values
        for v in sample:
            if isinstance(v, (list, tuple, set, dict, np.ndarray)):
                needs_json = True
                break

        if needs_json:
            df[col] = s.map(_jsonify_if_complex)
        else:
            # Light pass to clean numpy scalars / bytes in object cols
            if s.dtype == "object":
                df[col] = s.map(_jsonify_if_complex)
    return df


# -----------------------
# Delta Lake I/O helpers
# -----------------------

def write_delta(
    df: pd.DataFrame,
    uri: str,
    key_cols: Optional[Iterable[str]] = None,
    partition_by: Optional[Iterable[str]] = None,
    schema_mode: str = "merge",
) -> None:
    """
    Upsert/append a DataFrame to a Delta table at `uri`.

    - Normalizes complex columns to JSON strings
    - Creates table on first write (append)
    - If `key_cols` are provided and present, perform MERGE (update/insert)
      else append.
    
    Args:
        df: DataFrame to write
        uri: Delta table URI
        key_cols: Columns to use as merge keys
        partition_by: Columns to partition by
        schema_mode: Schema evolution mode - "merge" (add new columns), 
                     "overwrite" (replace schema), or None (no evolution)
    """
    # 0) Normalize any complex types across ALL columns -> JSON strings
    df = _normalize_lists_to_json_strings(df)

    # 1) Convert to Arrow
    at = _to_arrow(df)

    # 2) Create-on-first-write?
    try:
        DeltaTable(uri)
        table_exists = True
    except Exception:
        table_exists = False

    if not table_exists:
        write_deltalake(
            uri, 
            at, 
            mode="append", 
            partition_by=partition_by,
            schema_mode=schema_mode,
        )
        return

    # 3) If keys missing, append with schema evolution
    if not key_cols or not set(key_cols).issubset(df.columns):
        write_deltalake(
            uri, 
            at, 
            mode="append", 
            partition_by=partition_by,
            schema_mode=schema_mode,
        )
        return

    # 4) MERGE (updates matched, inserts new)
    predicate = " AND ".join([f"target.{c} = source.{c}" for c in key_cols])
    (
        DeltaTable(uri)
        .merge(
            source=at,
            predicate=predicate,
            source_alias="source",
            target_alias="target",
        )
        .when_matched_update_all()
        .when_not_matched_insert_all()
        .execute()
    )


def read_delta_as_pandas(uri: str, filters: Optional[Sequence[tuple]] = None) -> pd.DataFrame:
    """
    Read a Delta table into a pandas DataFrame, with optional `filters`.
    """
    dt = DeltaTable(uri)
    if filters:
        return dt.to_pandas(filters=filters)
    return dt.to_pandas()


# ----------------
# S3 List Helpers
# ----------------

def list_prefix_dirs(bucket: str, prefix: str) -> list[str]:
    """
    Return sorted list of immediate directory names under an S3 prefix.
    """
    s3 = boto3.client("s3")
    prefix = str(prefix).rstrip("/") + "/"

    paginator = s3.get_paginator("list_objects_v2")
    dirs = set()

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter="/"):
        for cp in page.get("CommonPrefixes", []):
            full_prefix = cp["Prefix"]
            name = os.path.basename(full_prefix.rstrip("/"))
            if name:
                dirs.add(name)

    return sorted(dirs)


def list_s3_keys(bucket: str, prefix: str):
    """
    Yield all object keys under a given S3 prefix (handles pagination).
    """
    s3 = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            yield obj["Key"]


def s3_file_exists(bucket: str, key: str) -> bool:
    """
    Check if a file exists in an S3 bucket.

    Args:
        bucket: S3 bucket name.
        key: Full key (path) of the file within the bucket.

    Returns:
        True if file exists, False otherwise.
    """
    s3 = boto3.client("s3")
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        # 404 means the object does not exist
        if e.response.get("Error", {}).get("Code") == "404":
            return False
        # Re-raise other errors (e.g., permission issues)
        raise

