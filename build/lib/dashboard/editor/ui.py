from typing import Optional, Sequence

import pandas as pd
import streamlit as st
from deltalake import DeltaTable

from shared.io_ops import read_delta_as_pandas, delta_partition_values

# --------------------------------------------------------------------------- #
# Cached helpers — keep reruns cheap so paging never re-reads S3
# --------------------------------------------------------------------------- #
@st.cache_data(show_spinner=False)
def get_table_meta(uri: str):
    """Return (partition_columns, all_column_names) from table metadata only."""
    dt = DeltaTable(uri)
    partition_cols = list(dt.metadata().partition_columns)
    all_cols = [f.name for f in dt.schema().fields]
    return partition_cols, all_cols


@st.cache_data(show_spinner=False)
def get_partition_values(uri: str, partition_col: str):
    return delta_partition_values(uri, partition_col)


@st.cache_data(show_spinner="Reading Delta table…")
def load_data(uri, filters, columns=None, _storage_options=None):
    f = [tuple(x) for x in filters] if filters else None
    cols = list(columns) if columns else None
    return read_delta_as_pandas(uri, filters=f, columns=cols)


@st.cache_data(show_spinner=False)
def to_csv_bytes(df: pd.DataFrame) -> bytes:
    return df.to_csv(index=False).encode("utf-8")


def get_unique_values(uri, column_name):
    """
    Get unique values for a column in the dataset.
    
    Args:
        uri: S3 URI to the dataset
        column_name: Name of the column
    
    Returns:
        List of unique values sorted
    """
    import pyarrow.parquet as pq
    
    # Read the table metadata
    table = pq.read_table(uri)
    
    # Get unique values for the column
    if column_name in table.column_names:
        col_data = table.column(column_name).to_pylist()
        unique_vals = sorted(list(set([v for v in col_data if v is not None])))
        return unique_vals
    else:
        raise ValueError(f"Column {column_name} not found in table")

