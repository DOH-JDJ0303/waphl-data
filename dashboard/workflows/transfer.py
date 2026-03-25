import streamlit as st
from dashboard import utils, transfer
import shared.ui

# === Page ===
st.title("Transfer Data")
shared.ui.module_overview("Transfer of production-level results from AWS, Terra.bio, or a local machine to the result database located in an S3 bucket")

# === Getting Started ===
transfer.ui.getting_started()

# Display selected activity
submodule = st.session_state.get("data_source")
if submodule == "AWS Production Bucket":
    st.subheader("AWS Production Bucket")
    shared.ui.submodule_overview(
        "Transfers production results from an AWS S3 bucket using the <b>prod2res</b> module — "
        "first gathering results via the <b>prod2res.gather</b> lambda function, then copying them "
        "with the <b>prod2res.cp</b> lambda function. Requires a <b>manifest.csv</b> to be located in the run directory "
        "with columns <b>sample</b>, <b>fastq_1</b>, and <b>fastq_2</b> listing all samples in the run."
    )
    with st.sidebar:
        p2r_queue_url = st.text_input("Prod2Res Queue URL", value=utils.env("P2R_QUEUE_URL", ""))

        if p2r_queue_url:
            st.session_state.p2r_queue_url = p2r_queue_url
        else:
            st.warning(f"Missing: P2R_QUEUE_URL")

    if p2r_queue_url:
        transfer.prod2res.ui.main()
    
elif submodule == "Terra.bio":
    st.subheader("Terra.bio")
    shared.ui.submodule_overview(
        "Transfers production results from a Terra.bio workspace using the <b>prod2res_terra</b> module — "
        "send events to the <b>prod2res_terra.cp</b> batch function using Terra.bio run parameters."
    )
    p2rt_status = False
    with st.sidebar:

        p2rt_job_queue      = st.text_input("Prod2Res Terra Job Queue", value=utils.env("P2RT_JOB_QUEUE", ""))
        p2rt_job_definition = st.text_input("Prod2Res Terra Job Definition", value=utils.env("P2RT_JOB_DEFINITION", ""))
        gcs_credentials_uri = st.text_input("Google Cloud Credentials (URI)", value=utils.env("GOOGLE_CLOUD_CREDENTIALS", ""))
        terra_project       = st.text_input("Terra Project", value=utils.env("TERRA_PROJECT", ""))
        terra_workspaces    = st.text_input("Terra Workspaces", value=utils.env("TERRA_WORKSPACES", ""))

        if gcs_credentials_uri and p2rt_job_queue and p2rt_job_definition and terra_project and terra_workspaces:
            p2rt_status = True
            st.session_state.gcs_credentials_uri = gcs_credentials_uri
            st.session_state.p2rt_job_queue           = p2rt_job_queue
            st.session_state.p2rt_job_definition      = p2rt_job_definition
            st.session_state.terra_project            = terra_project
            st.session_state.terra_workspaces         = terra_workspaces
        else:
            p2rt_conf = {
                "Google Cloud Credentials": gcs_credentials_uri, 
                "Prod2Res Terra Job Queue": p2rt_job_queue, 
                "Prod2Res Terra Job Definition": p2rt_job_definition, 
                "Terra Project": terra_project, 
                "Terra Workspaces": terra_workspaces 
                }
            missing = [k for k, v in p2rt_conf.items() if not v]
            st.warning(f"Missing: {', '.join(missing)}")
    if p2rt_status == True:
        transfer.prod2res_terra.ui.main()

elif submodule == "Local CSV File":
    st.warning("Under construction 🦺. Please select a different data source!")
    # transfer.import_csv.ui.main()