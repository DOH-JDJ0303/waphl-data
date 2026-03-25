import streamlit as st
import os

def env(var, default=None):
    out = os.environ.get(var, default)
    if out is None:
        st.error(f"Required environmental variable not set: {var}")
    return out