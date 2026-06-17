import streamlit as st
import pandas as pd
from io import StringIO
from pathlib import Path
import fsspec
from typing import Optional
import uuid
import os

from shared import io_ops

USER = os.environ['USER']

REQ_COLS_1 = ['source_bucket', 'source_key', 'dest_bucket', 'dest_key']
REQ_COLS_2 = ['origin', 'current']
REQ_COLS_3 = ['source', 'destination']

SESSION_ID = f"{USER}-{uuid.uuid4().hex}"

# ---------------- I/O helpers ----------------
def read_table_any(src) -> pd.DataFrame:
    if src is None:
        return pd.DataFrame()

    if hasattr(src, "read") and hasattr(src, "name"):  # st.UploadedFile
        name = src.name.lower()
        raw = src.read().decode("utf-8")
        sep = "\t" if name.endswith(".tsv") else ","
        return pd.read_csv(StringIO(raw), sep=sep)

    path = str(src)
    sep = "\t" if path.lower().endswith(".tsv") else ","
    with fsspec.open(path, "rb") as f:
        return pd.read_csv(f, sep=sep)

def build_table_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = df.columns.str.lower()
    cols = df.columns

    if all(c in cols for c in REQ_COLS_1):
        df_out = df[REQ_COLS_1].copy()
    elif all(c in cols for c in REQ_COLS_2):
        df_out = pd.DataFrame()
        df_out[['source_bucket', 'source_key']] = df['origin'].str.extract(r"s3://([^/]+)/(.+)")
        df_out[['dest_bucket', 'dest_key']]     = df['current'].str.extract(r"s3://([^/]+)/(.+)")
    elif all(c in cols for c in REQ_COLS_3):
        df_out = pd.DataFrame()
        df_out[['source_bucket', 'source_key']] = df['source'].str.extract(r"s3://([^/]+)/(.+)")
        df_out[['dest_bucket', 'dest_key']]     = df['destination'].str.extract(r"s3://([^/]+)/(.+)")
    else:
        st.error(
            f"Dataframe must have one of the following column sets:\n"
            f"{REQ_COLS_1}\n{REQ_COLS_2}\n{REQ_COLS_3}"
        )
        return pd.DataFrame()

    # normalize keys
    df_out['source_key'] = df_out['source_key'].str.lstrip('/')
    df_out['dest_key']   = df_out['dest_key'].str.lstrip('/')
    return df_out

# ---------------- existence check ----------------
def check_source_files(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Adds a boolean 'source_exists' column to df and returns (df_with_flag, missing_rows_df).
    Deduplicates lookups for speed.
    """
    if df.empty:
        return df.assign(source_exists=pd.Series(dtype=bool)), df

    required = ['source_bucket', 'source_key']
    if not all(c in df.columns for c in required):
        st.error(f"Missing required columns: {required}")
        return df, pd.DataFrame(columns=required)

    # Deduplicate lookups
    pairs = (
        df[['source_bucket', 'source_key']]
        .drop_duplicates()
        .assign(source_key=lambda x: x['source_key'].str.lstrip('/'))
    )

    exists_map: dict[tuple[str, str], bool] = {}
    progress = st.progress(0.0, text="Checking S3 source files...")

    total = len(pairs)
    for i, row in enumerate(pairs.itertuples(index=False), start=1):
        bucket = row.source_bucket
        key    = row.source_key
        ok = False
        try:
            ok = io_ops.s3_file_exists(bucket, key)  # expected: returns True/False
        except Exception as e:
            # Treat exceptions as missing, but you could log e if you like
            ok = False
        exists_map[(bucket, key)] = ok
        progress.progress(i / total, text=f"Checking {i}/{total}")

    progress.empty()

    # map back
    df = df.copy()
    df['source_key'] = df['source_key'].str.lstrip('/')
    df['source_exists'] = [
        exists_map.get((b, k), False) for b, k in zip(df['source_bucket'], df['source_key'])
    ]

    missing = df.loc[~df['source_exists'], ['source_bucket', 'source_key']].copy()
    return df, missing

# ---------------- app ----------------
def main():
    st.title("Import a Table")

    local_file = st.file_uploader("Upload Local File", type=["csv", "tsv"])
    s3_file = st.text_input("Or load from S3 (s3://bucket/key.csv or .tsv)")

    if local_file and s3_file:
        st.warning("Please supply a local file OR an S3 file, not both.")
        st.stop()

    df: Optional[pd.DataFrame] = None

    if local_file:
        df = read_table_any(local_file)
    elif s3_file:
        path = s3_file.strip()
        if not (path.startswith("s3://") or Path(path).exists()):
            st.error("Provide a valid s3:// URI or a local filesystem path.")
            st.stop()
        try:
            df = read_table_any(path)
        except Exception as e:
            st.error(f"Failed to read file: {e}")
            st.stop()

    if df is not None:
        if df.empty:
            st.warning("The supplied file is empty!")
            return

        df = build_table_columns(df)
        if df.empty:
            return

        st.success(f"Loaded {len(df):,} rows × {len(df.columns)} columns")
        st.dataframe(df, use_container_width=True)

        # ---- Check existence
        df_checked, missing = check_source_files(df)

        n_missing = len(missing)
        if n_missing == 0:
            st.success("All source files exist in S3 ✅")
        else:
            st.error(f"{n_missing} source file(s) are missing from S3.")
            with st.expander("Show missing files", expanded=True):
                st.dataframe(missing, use_container_width=True)
                # download button
                csv_bytes = missing.to_csv(index=False).encode("utf-8")
                st.download_button(
                    "Download missing list (CSV)",
                    data=csv_bytes,
                    file_name=f"missing_sources_{SESSION_ID}.csv",
                    mime="text/csv",
                )

        # Optional: display with existence flag
        with st.expander("Table with existence flag", expanded=False):
            st.dataframe(df_checked, use_container_width=True)

if __name__ == "__main__":
    main()
