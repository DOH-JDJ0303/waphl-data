import streamlit as st
from dashboard import utils

st.set_page_config(layout="wide")

# === AWS Config ===
utils.env("AWS_ACCESS_KEY_ID")
utils.env("AWS_SECRET_ACCESS_KEY")
utils.env("AWS_DEFAULT_REGION")

# standardize session state for error messages and slot
st.session_state["error_messages"] = []
st.session_state["error_slot"]     = st.empty()

# === Sidebar Config ===
with st.sidebar:
    st.subheader("⚙️ Configuration")
    user = st.text_input("Username", value=utils.env("USER", ""))
    prod_bucket = st.text_input("Production Bucket", value=utils.env("PROD_BUCKET", ""))
    res_bucket = st.text_input("Results Bucket", value=utils.env("RES_BUCKET", ""))

    if user and prod_bucket and res_bucket:
        st.session_state.user        = user
        st.session_state.prod_bucket = prod_bucket
        st.session_state.res_bucket  = res_bucket
    else:
        missing = [k for k, v in {"Username": user, "Production Bucket": prod_bucket, "Results Bucket": res_bucket}.items() if not v]
        st.warning(f"Missing: {', '.join(missing)}")

# === App ===
if all(k in st.session_state for k in ("user", "prod_bucket", "res_bucket")):
    pg = st.navigation([
        st.Page("workflows/landing.py", title="Home", icon="🏠"),
        st.Page("workflows/search.py", title="Search Results", icon="🔍"),
        st.Page("workflows/inspect.py", title="Inspect Results", icon="📋"),
        st.Page("workflows/transfer.py", title="Transfer Data", icon="🚚"),
    ])
    pg.run()
else:
    st.info("👈 Please complete configuration in the sidebar to get started.")

if st.session_state.get("error_slot"):
        st.session_state["error_slot"] = st.empty()