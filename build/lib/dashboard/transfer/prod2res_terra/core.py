# core.py
import streamlit as st
import json
from pathlib import Path
import contextlib
import io
import os
import boto3

from shared import io_ops

TERRA_CACHE_PREFIX = io_ops.TERRA_CACHE_PREFIX


def run_handler() -> None:
    res_bucket = st.session_state.get('res_bucket')
    job_queue  = st.session_state.get('p2rt_job_queue')
    job_definition  = st.session_state.get('p2rt_job_definition')

    if not res_bucket or not job_queue or not job_definition:
        return

    os.environ.update({
        "DEST_BUCKET":   res_bucket,
        "JOB_QUEUE":     job_queue,
        "JOB_DEFINITION": job_definition
    })

    from prod2res_terra.gather import lambda_function

    stdout_buf = io.StringIO()
    stderr_buf = io.StringIO()

    with contextlib.redirect_stdout(stdout_buf), contextlib.redirect_stderr(stderr_buf):
        lambda_function.handler(None, None)

    if stdout_buf.getvalue():
        with st.expander("Stdout"):
            st.code(stdout_buf.getvalue())
    if stderr_buf.getvalue():
        with st.expander("Stderr"):
            st.warning(stderr_buf.getvalue())


def gather_workspace_cache() -> None:
    """Load and combine all cache CSVs for a workspace from S3."""
    res_bucket      = st.session_state.get("res_bucket")
    terra_project   = st.session_state.get("terra_project")
    terra_workspace = st.session_state.get("p2rt_terra_workspace")

    if not all([res_bucket, terra_project, terra_workspace]):
        return

    uri     = f"s3://{res_bucket}/{TERRA_CACHE_PREFIX}"
    filters = [
        ("project",   "=", terra_project),
        ("workspace", "=", terra_workspace),
    ]

    try:
        df = io_ops.read_delta_as_pandas(uri=uri, filters=filters)
        if "transfer_status" in df.columns:
            cols = ["transfer_status"] + [c for c in df.columns if c != "transfer_status"]
            df   = df[cols]
        st.session_state.p2rt_df_cache = df
    except Exception as e:
        st.error(f"Issue loading cache: {e}")
        st.stop()


def submit_batch_job() -> bool:
    """Submit an AWS Batch job for a Terra submission."""
    keys = [
        "terra_project", "p2rt_terra_workspace", "p2rt_job_queue", "p2rt_job_definition",
        "res_bucket", "gcs_credentials_uri", "p2rt_workflow_name",
        "p2rt_submissionentity", "p2rt_submissiontime", "p2rt_submissionid",
    ]
    s = st.session_state
    missing = [k for k in keys if k not in s]
    if missing:
        st.error(f"The following data is missing: {', '.join(missing)}")
        return

    batch    = boto3.session.Session().client("batch")
    job_name = f"{s.terra_project[:10]}_{s.p2rt_terra_workspace[:10]}_{s.p2rt_submissionid[:8]}"

    try:
        batch.submit_job(
            jobName=job_name,
            jobQueue=s.p2rt_job_queue,
            jobDefinition=s.p2rt_job_definition,
            containerOverrides={
                "environment": [
                    {"name": "TERRA_PROJECT",          "value": s.terra_project},
                    {"name": "TERRA_WORKSPACE",         "value": s.p2rt_terra_workspace},
                    {"name": "TERRA_SUBMISSIONID",      "value": s.p2rt_submissionid},
                    {"name": "TERRA_WORKFLOW",          "value": s.p2rt_workflow_name},
                    {"name": "TERRA_SUBMISSIONENTITY",  "value": s.p2rt_submissionentity},
                    {"name": "TERRA_SUBMISSIONTIME",    "value": s.p2rt_submissiontime},
                    {"name": "DEST_BUCKET",             "value": s.res_bucket},
                    {"name": "GCS_CREDENTIALS_URI",     "value": s.gcs_credentials_uri},
                ]
            },
        )
        st.success(f"Submitted batch job for submission {s.p2rt_submissionid}.")
        return
    except Exception as e:
        st.error(f"Failed to submit job for {s.p2rt_submissionid}: {e}")
        return