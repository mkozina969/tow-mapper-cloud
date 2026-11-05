"""Visual shell and session continuity for the existing Streamlit workflow."""

from io import BytesIO

import streamlit as st

import download_center


SECTIONS = {
    "Import Center": "↑",
    "Valeo PDF → Excel": "⇩",
    "Bosch Merger": "▤",
    "Dashboard": "⌂",
    "Crosswalk Mapping": "⇄",
    "UoM Conversion": "▦",
    "Download Center": "↓",
    "Suppliers": "▤",
    "Reports": "▥",
    "Settings": "⚙",
}
SECTION_NOTES = {
    "Suppliers": "Select suppliers in Import Center. A dedicated supplier directory is planned.",
    "Reports": "Historical processing reports are planned.",
    "Settings": "Crosswalk Mapping and UoM Conversion require the admin PIN.",
}
WORKSPACE_KEYS = (
    "_debug", "vendor_filter", "vendor_select", "supplier_col_select",
    "quantity_column", "unit_price_column", "chk_lock", "multiselect_export_cols",
    "export_invoice_label", "export_item_label", "export_location", "export_manual_date",
    "export_date_column", "export_date_format", "export_text_columns",
    "portal_document", "portal_blank", "custom_export_filename", "pallets_export_filename",
    "regular_export_filename", "portal_export_filename",
    "share_finished_exports",
)

STYLE = """
<style>
.stApp { background: #F5F7FA; }
[data-testid="stMainBlockContainer"] { max-width: 1500px; padding-top: 3.5rem; }
[data-testid="stSidebar"] { border-right: 1px solid #E2E8F0; }
.st-key-tow_sidebar_nav [role="radiogroup"] { gap: .3rem; }
.st-key-tow_sidebar_nav [role="radiogroup"] label {
    padding: .62rem .72rem; min-height: 2.6rem; border-radius: .4rem;
    border-left: 3px solid transparent; align-self: stretch;
}
.st-key-tow_sidebar_nav [role="radiogroup"] label:hover { background: #EAF2FB; }
.st-key-tow_sidebar_nav [role="radiogroup"] label:has(input:checked) {
    color: #0F2B5B; background: #E4EEFA; border-left-color: #0078D4; font-weight: 650;
}
.tow-brand {
    display: flex; flex-wrap: wrap; align-items: center; gap: .7rem 1rem;
    color: #FFF; background: #0F2B5B; border-radius: .55rem;
    padding: .9rem 1.15rem; margin-bottom: .9rem;
    box-shadow: 0 2px 5px #0f2b5b22;
}
.tow-brand-mark {
    display: grid; place-items: center; background: #0078D4; color: #FFF;
    width: 2.1rem; height: 2.1rem; border-radius: .38rem; font-weight: 700;
}
.tow-brand-name { font-weight: 700; font-size: 1.18rem; letter-spacing: -.01em; }
.tow-brand-subtitle { color: #DAE8FA; font-size: .88rem; }
.tow-preview-label { color: #64748B; font-size: .77rem; font-weight: 700;
    letter-spacing: .08em; text-transform: uppercase; margin-bottom: .45rem; }
.st-key-tow_workspace_card, .st-key-tow_activity_card,
.st-key-tow_recent_exports {
    background: white; border-radius: .65rem; box-shadow: 0 2px 6px #0f2b5b0c;
}
</style>
"""


def _preserve_workspace():
    # Streamlit drops widget keys for controls that are absent on a rerun.
    vendor_stamp_key = f"export_vendor_stamp_{st.session_state.get('vendor_select', '')}"
    for key in (*WORKSPACE_KEYS, vendor_stamp_key):
        if key in st.session_state:
            st.session_state[key] = st.session_state[key]


def render_shell():
    render_brand()
    navigation_key = f"tow_navigation_{st.session_state.get('tow_upload_epoch', 0)}"
    with st.sidebar:
        st.markdown('<div class="tow-preview-label">Workspace</div>', unsafe_allow_html=True)
        with st.container(key="tow_sidebar_nav"):
            section = st.radio(
                "Sections", list(SECTIONS), key=navigation_key,
                label_visibility="collapsed",
                format_func=lambda name: f"{SECTIONS[name]}   {name}",
            )
        st.divider()
        st.button("Reset session", on_click=_reset_session, help="Clear files, results, queued changes, and access for this browser session.")
        st.button("Logout", on_click=_reset_session)
        st.caption("TOW Mapper Pro · UI preview")

    if section != "Import Center":
        _preserve_workspace()
    return section


def render_brand():
    st.markdown(STYLE, unsafe_allow_html=True)
    st.markdown(
        '<div class="tow-brand"><span class="tow-brand-mark" aria-hidden="true">T</span>'
        '<span class="tow-brand-name">TOW Mapper Pro</span>'
        '<span class="tow-brand-subtitle">Supplier Data Management</span></div>',
        unsafe_allow_html=True,
    )


def render_preview_section(section):
    st.title(section)
    st.info(f"Section preview. {SECTION_NOTES[section]}")


def _open_section(section):
    navigation_key = f"tow_navigation_{st.session_state.get('tow_upload_epoch', 0)}"
    st.session_state[navigation_key] = section


def render_dashboard():
    st.title("Dashboard")
    st.caption("Import, map, and prepare supplier data for NAV.")
    st.markdown("#### Quick actions")
    actions = ("Import Center", "Crosswalk Mapping", "UoM Conversion", "Download Center")
    for column, name in zip(st.columns(len(actions)), actions):
        with column:
            st.button(f"{SECTIONS[name]}  {name}", on_click=_open_section, args=(name,), use_container_width=True)

    with st.container(border=True, key="tow_workspace_card"):
        st.markdown("#### Current processing session")
        if st.session_state.get("mapped_ready"):
            matched = len(st.session_state["matched"])
            unmatched = len(st.session_state["unmatched"])
            metrics = (
                ("Processed rows", matched + unmatched),
                ("Matched items", matched),
                ("Unmatched items", unmatched),
                ("UoM conversions applied", len(st.session_state.get("uom_audit", []))),
            )
            for column, (label, count) in zip(st.columns(len(metrics)), metrics):
                column.metric(label, f"{count:,}")
        else:
            st.caption("Run mapping in Import Center to see results for this session.")

    with st.container(border=True, key="tow_recent_exports"):
        download_center.render_recent_exports(st)

    with st.container(border=True, key="tow_activity_card"):
        st.markdown("#### Processing history")
        st.caption("Processing history is unavailable because completed runs are not recorded.")


def _reset_session():
    epoch = st.session_state.get("tow_upload_epoch", 0) + 1
    download_center.revoke_download_access(st)
    st.session_state.clear()
    st.session_state["tow_upload_epoch"] = epoch


def supplier_upload():
    upload_key = f"uploader_{st.session_state.get('tow_upload_epoch', 0)}"
    upload = st.file_uploader(
        "Drag & drop ili odaberi datoteku", type=["xlsx", "xls", "csv", "pdf"],
        key=upload_key, on_change=_clear_removed_upload, args=(upload_key,),
    )
    if upload is not None:
        st.session_state["tow_retained_upload"] = (upload.name, upload.getvalue())
        st.caption(f"Current file: {upload.name}")
        return upload

    saved = st.session_state.get("tow_retained_upload")
    if saved is None:
        return None

    name, data = saved
    st.caption(f"Current file: {name}")
    if st.button("Remove current file", key="tow_remove_retained_upload"):
        st.session_state.pop("tow_retained_upload", None)
        st.rerun()
    restored = BytesIO(data)
    restored.name = name
    return restored


def _clear_removed_upload(upload_key):
    upload = st.session_state.get(upload_key)
    if upload is None:
        st.session_state.pop("tow_retained_upload", None)
