import os
import uuid
from pathlib import Path

import boto3
import streamlit as st

from dashboard import utils
from dashboard.transfer.prod2res import core
from shared import io_ops

# === CONFIG ===
WORKFLOW_PREFIX = utils.env("WORKFLOW_PREFIX", "workflow")
RUN_PREFIX      = utils.env("RUN_PREFIX", "runs")

S3 = boto3.session.Session().client("s3")

# === MAIN ===
def main():
    prod_bucket = st.session_state.get("prod_bucket")
    res_bucket  = st.session_state.get("res_bucket")
    user        = st.session_state.get("user")
    queue_url   = st.session_state.get("p2r_queue_url")

    workflow = run = trigger_key = None

    if prod_bucket:
        workflows = io_ops.list_prefix_dirs(prod_bucket, WORKFLOW_PREFIX)
        workflow = st.selectbox("Select a workflow", [""] + workflows)

    if workflow:
        st.session_state.p2r_workflow = workflow

        runs_dir = Path(WORKFLOW_PREFIX, workflow, RUN_PREFIX)
        st.session_state.p2r_runs_dir = runs_dir
        runs = io_ops.list_prefix_dirs(prod_bucket, runs_dir)
        run = st.selectbox("Select a run", [""] + runs)

    if run:
        st.session_state.p2r_run = run
        core.get_trigger_key()
        trigger_key = st.session_state.trigger_key

    if not trigger_key:
        return

    if not user:
        st.error("User not defined!")
        st.stop()

    session_id = f"{user}-{uuid.uuid4().hex}"
    st.session_state.p2r_session_id = session_id

    event = {
        "id": session_id,
        "detail": {
            "bucket": {"name": prod_bucket},
            "object": {"key": trigger_key},
        },
    }

    st.subheader("Event Preview")
    st.json(event)

    if not st.button("Transfer Data"):
        return

    os.environ.update({
        "SOURCE_BUCKET": prod_bucket,
        "DEST_BUCKET":   res_bucket,
        "QUEUE_URL":     queue_url,
    })

    with st.spinner("Running `prod2res.gather` lambda function handler..."):
        try:
            core.run_handler(event)
        except Exception as e:
            st.error("Data transfer failed!")
            st.error(e)
            st.stop()