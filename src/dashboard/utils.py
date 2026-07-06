import streamlit as st
import os
from importlib.metadata import version, PackageNotFoundError

def app_version() -> str:
    # Tries distribution names in order; adjust to match your pyproject name.
    for dist in ("waphl-data", "dashboard"):
        try:
            return version(dist)
        except PackageNotFoundError:
            continue
    return "unknown"

def env(var, default=None):
    out = os.environ.get(var, default)
    if out is None:
        st.error(f"Required environmental variable not set: {var}")
    return out