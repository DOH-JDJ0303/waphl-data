import streamlit as st
from dashboard import utils

st.set_page_config(layout="wide")
st.caption(f"dashboard · v{utils.app_version()}")

# === AWS Config ===
utils.env("AWS_ACCESS_KEY_ID")
utils.env("AWS_SECRET_ACCESS_KEY")
utils.env("AWS_DEFAULT_REGION")

# --- Persistent message queues -------------------------------------------
# Initialize ONCE (only if absent) so queued messages survive across reruns
# until the user closes the modal. These must NOT be reset every run, or the
# modal would be cleared before it's ever seen.
st.session_state.setdefault("error_messages", [])
st.session_state.setdefault("warning_messages", [])
st.session_state.setdefault("dismissed_messages", set())

@st.dialog("Notifications", dismissible=False)
def _show_messages_dialog():
    """Blocking modal: shows queued errors/warnings, closes only via button."""
    errors   = st.session_state.get("error_messages", [])
    warnings = st.session_state.get("warning_messages", [])

    if errors:
        st.markdown("#### ❌ Errors")
        for msg in errors:
            st.error(msg)
    if warnings:
        st.markdown("#### ⚠️ Warnings")
        for msg in warnings:
            st.warning(msg)

    # Non-dismissable dialogs must be closed programmatically with st.rerun().
    if st.button("Close", type="primary", use_container_width=True):
            dismissed = st.session_state.setdefault("dismissed_messages", set())
            dismissed.update(st.session_state.get("error_messages", []))
            dismissed.update(st.session_state.get("warning_messages", []))
            st.session_state["error_messages"]   = []
            st.session_state["warning_messages"] = []
            st.rerun()


# Surface any queued messages as a blocking window before anything else runs.
if st.session_state["error_messages"] or st.session_state["warning_messages"]:
    _show_messages_dialog()

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
        # Left inline on purpose — see note below.
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