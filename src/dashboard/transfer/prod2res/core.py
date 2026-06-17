import streamlit as st
import json
from pathlib import Path
import contextlib
import io

from shared import io_ops

def get_trigger_key():
    workflow    = st.session_state.p2r_workflow
    runs_dir    = st.session_state.p2r_runs_dir
    run         = st.session_state.p2r_run
    prod_bucket = st.session_state.prod_bucket

    if not workflow or not runs_dir or not run or not prod_bucket:
        return
    
    workflow_scheme = Path(f'schemes/{workflow}.json')
    if not workflow_scheme.exists():
        st.error(f"No file transfer scheme available for {workflow}")
        st.stop()
        
    with open(workflow_scheme, 'r') as f:
        data = json.load(f)
        trigger = data.get('trigger')

    if not trigger:
        st.error(f"Trigger file not found in {workflow_scheme}")
        st.stop()
        
    trigger_key = str(Path(runs_dir, run, trigger))
    st.session_state.trigger_key = trigger_key
    if io_ops.s3_file_exists(prod_bucket, trigger_key) == False:
        st.warning(
            f"Trigger file, s3://{prod_bucket}/{trigger_key}, does not exist for {run}. "
            "This could indicate a failed / incomplete run. Would you like to transfer anyway?"
            )
        if st.button("Attempt Transfer Anyway"):
            pass

def run_handler(event: dict) -> None:
    from prod2res.gather import lambda_function

    stdout_buf = io.StringIO()
    stderr_buf = io.StringIO()

    with contextlib.redirect_stdout(stdout_buf), contextlib.redirect_stderr(stderr_buf):
        lambda_function.handler(event, None)

    if stdout_buf.getvalue():
        st.code(stdout_buf.getvalue())
    if stderr_buf.getvalue():
        st.warning(stderr_buf.getvalue())
    