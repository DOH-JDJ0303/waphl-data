import streamlit as st

def module_overview(text):
    css = """
    .module-overview-wrapper {
        border-radius: 6px;
        overflow: hidden;
        margin-bottom: 1.5rem;
    }
    .module-overview-header {
        background-color: rgba(150, 140, 160, 0.18);
        padding: 0.45rem 1.25rem;
        font-size: 0.75rem;
        font-weight: 600;
        letter-spacing: 0.07em;
        text-transform: uppercase;
        opacity: 0.9;
    }
    .module-overview-body {
        background-color: rgba(150, 140, 160, 0.07);
        padding: 0.9rem 1.25rem;
        font-size: 0.95rem;
        line-height: 1.6;
        opacity: 0.85;
    }
    """
    st.html(f"""
        <style>{css}</style>
        <div class="module-overview-wrapper">
            <div class="module-overview-header">Module Overview</div>
            <div class="module-overview-body">{text}</div>
        </div>
    """)


def submodule_overview(text):
    css = """
    .submodule-overview-wrapper {
        border-radius: 6px;
        overflow: hidden;
        margin-bottom: 1.5rem;
    }
    .submodule-overview-header {
        background-color: rgba(120, 100, 140, 0.28);
        padding: 0.45rem 1.25rem;
        font-size: 0.75rem;
        font-weight: 600;
        letter-spacing: 0.07em;
        text-transform: uppercase;
        opacity: 0.9;
    }
    .submodule-overview-body {
        background-color: rgba(120, 100, 140, 0.13);
        padding: 0.9rem 1.25rem;
        font-size: 0.95rem;
        line-height: 1.6;
        opacity: 0.85;
    }
    """
    st.html(f"""
        <style>{css}</style>
        <div class="submodule-overview-wrapper">
            <div class="submodule-overview-header">Submodule Overview</div>
            <div class="submodule-overview-body">{text}</div>
        </div>
    """)