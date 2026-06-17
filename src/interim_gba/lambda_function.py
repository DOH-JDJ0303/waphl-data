#!/usr/bin/env python3
import os
import sys
import io
import boto3
import pandas as pd
from typing import Any

from shared import io_ops

# ----- Global Setup ----- #
RES_BUCKET = os.environ.get("RES_BUCKET")

SESSION = boto3.session.Session()
S3      = SESSION.client("s3")

RESULTS_PREFIX = io_ops.RESULTS_PREFIX

SOURCE_TABLE_URI   = f"s3://{RES_BUCKET}/{RESULTS_PREFIX}"
WORKFLOW_ALT_VALUE = "phoenix"
OUTPUT_KEY         = "tables/gba.csv"

# ----- Utility ----- #
def log_print(msg: Any) -> None:
    print(str(msg), flush=True)

def storage_options() -> dict:
    """Storage options for deltalake S3 access (credentials come from the role)."""
    region = os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION")
    return {"AWS_REGION": region} if region else {}

# ----- Core Logic ----- #
def read_table(uri: str, workflow_alt: str) -> pd.DataFrame:
    """Read a Delta table filtered to a single workflow_alt value."""
    log_print(f"Reading {uri} where workflow_alt={workflow_alt}")
    df = io_ops.read_delta_as_pandas(
        uri,
        filters=[("workflow_alt", "=", workflow_alt)],
        storage_options=storage_options(),
    )
    log_print(f"Rows read: {len(df)}")
    return df

def write_csv(df: pd.DataFrame, bucket: str, key: str) -> str:
    """Serialize a DataFrame to CSV and upload it to S3."""
    buf = io.StringIO()
    df.to_csv(buf, index=False)
    body = buf.getvalue().encode("utf-8")
    S3.put_object(Bucket=bucket, Key=key, Body=body, ContentType="text/csv")
    uri = f"s3://{bucket}/{key}"
    log_print(f"Wrote CSV ({len(body)} bytes, {len(df)} rows) to {uri}")
    return uri

# ----- Lambda Handler ----- #
def handler(event, context):
    try:
        df = read_table(SOURCE_TABLE_URI, WORKFLOW_ALT_VALUE)
    except Exception as e:
        log_print(f"Delta read failed: {e}")
        sys.exit(1)

    try:
        write_csv(df, RES_BUCKET, OUTPUT_KEY)
    except Exception as e:
        log_print(f"CSV write failed: {e}")
        sys.exit(1)

# ----- Optional Local Test ----- #
if __name__ == "__main__":
    handler({}, None)