import streamlit as st

def getting_started():
    with st.container(border=True):
        st.subheader("Getting Started")
        st.write("Use the drop-down menu to select the source of the production data you wish to transfer.")
        st.session_state.data_source = st.selectbox(
            "Data Source:",
            ["None", "AWS Production Bucket", "Terra.bio", "Local CSV File"],
            key="submodule_selector"
        )