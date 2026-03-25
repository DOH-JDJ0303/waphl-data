# ui.py
import pandas as pd
import streamlit as st

from dashboard import utils
from dashboard.transfer.prod2res_terra import core


def select_workspace() -> None:
    raw       = st.session_state.get("terra_workspaces", "")
    workspaces = [w.strip() for w in raw.split(",") if w.strip()]
    st.session_state.p2rt_terra_workspace = st.selectbox("Select a Terra.bio workspace", [""] + workspaces)


def render_cache_editor() -> None:
    terra_workspace = st.session_state.get("p2rt_terra_workspace")
    df_cache        = st.session_state.get("p2rt_df_cache", pd.DataFrame())

    if df_cache.empty or not terra_workspace:
        return

    df_display = df_cache.copy()
    if "select" not in df_display.columns:
        df_display.insert(0, "select", False)

    st.subheader(f"`{terra_workspace}` Run Cache")
    st.session_state.p2rt_df_edited = st.data_editor(
        df_display,
        key="cache_editor",
        use_container_width=True,
        hide_index=True,
        column_config={
            "select":          st.column_config.CheckboxColumn("Select", default=False),
            "project":         st.column_config.TextColumn("project",         disabled=True),
            "workspace":       st.column_config.TextColumn("workspace",        disabled=True),
            "entity_type":     st.column_config.TextColumn("entity_type",      disabled=True),
            "workflow_name":   st.column_config.TextColumn("workflow_name",    disabled=True),
            "submission_id":   st.column_config.TextColumn("submission_id",    disabled=True),
            "submission_time": st.column_config.TextColumn("submission_time",  disabled=True),
            "transfer_status": st.column_config.TextColumn("transfer_status",  disabled=True),
        },
    )


def fill_from_row() -> None:
    df_edited = st.session_state.get("p2rt_df_edited", pd.DataFrame())
    if df_edited.empty or "select" not in df_edited.columns:
        return

    if st.button("Use selected row", type="primary", use_container_width=True):
        selected = df_edited[df_edited["select"] == True]

        if len(selected) == 0:
            st.error("Select a row to populate the fields below.")
            return
        if len(selected) > 1:
            st.error("Multiple rows selected — please select exactly one.")
            return

        row = selected.iloc[0]
        st.session_state["p2rt_workflow_name"] = str(row["workflow_name"])
        st.session_state["p2rt_submissionentity"] = str(row["entity_type"])
        st.session_state["p2rt_submissiontime"] = str(row["submission_time"])
        st.session_state["p2rt_submissionid"] = str(row["submission_id"])

        st.rerun()


def render_transfer_params() -> None:
    st.subheader("Transfer Parameters")
    st.write("Update the transfer parameters below manually or by selecting a workflow from the cache table above.")

    fields = {
        "p2rt_workflow_name":    "Workflow Name (e.g., 2_TheiaProk_Illumina_PE_WAPHL_PHB_yRnFTr1467o)",
        "p2rt_submissionentity": "Submission Entity (e.g., Cor_240212)",
        "p2rt_submissiontime":   "Submission Time (e.g., 1708464126)",
        "p2rt_submissionid":     "Submission ID (e.g., 2b1d138d-8d03-44c3-87db-f78adcfa1b48)",
    }

    for key, label in fields.items():
        if key not in st.session_state:
            st.session_state[key] = ""
        st.text_input(label, key=key)

    if st.button("Transfer Terra.bio Data"):
        missing = [label for key, label in fields.items() if not st.session_state.get(key)]
        if missing:
            st.error(f"Missing required fields: {', '.join(missing)}")
        else:
            core.submit_batch_job()

def update_workspace_cache():
    terra_workspaces = st.session_state.terra_workspaces
    if not terra_workspaces:
        return
    
    st.session_state.setdefault("cache_refresh", False)
    st.write(
        "Use the button below to check for new runs from Terra.bio. " 
        "This process utilizes the `prod2res_terra.gather` lambda function, " 
        "which deploys AWS Batch jobs for any new runs detected from Terra.bio. ")
    if st.button("Refresh workspace cache"):
        if st.session_state.get('cache_refresh', True):
            st.warning("Terra.bio workspace cache can only be updated once per session. This is to avoid performing duplicate submissions. ⚠️ Refresh the page to perform this action again ⚠️.")
            return
        with st.status("Refreshing workspace cache..."):
            core.run_handler()
            st.session_state.cache_refresh = True



def main() -> None:
    transfer_params_container = st.container(border=True)

    with st.container(border=True):
        st.subheader("Fill from Cache Table")
        update_workspace_cache()
        select_workspace()
        core.gather_workspace_cache()
        render_cache_editor()
        fill_from_row()

    with transfer_params_container:
        render_transfer_params()
    