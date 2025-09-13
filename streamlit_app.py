from __future__ import annotations

import os
import csv
from io import BytesIO
from pathlib import Path
from typing import Optional, Dict, List

import pandas as pd
import streamlit as st

# PDF parsing (optional)
try:
    import pdfplumber  # type: ignore
    _HAS_PDF = True
except Exception:
    _HAS_PDF = False

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

st.set_page_config(page_title="Supplier → TOW Mapper (Cloud DB)", layout="wide")

# =============================================================================
# Helpers
# =============================================================================
def _excel_bytes(dfs: Dict[str, pd.DataFrame]) -> bytes:
    bio = BytesIO()
    with pd.ExcelWriter(bio, engine="xlsxwriter") as w:
        for sheet, df in dfs.items():
            if isinstance(df, pd.DataFrame) and not df.empty:
                df.reset_index(drop=True).to_excel(w, index=False, sheet_name=sheet)
    return bio.getvalue()


def _engine() -> Engine:
    db_url = os.getenv("DATABASE_URL") or st.secrets.get("DATABASE_URL", "")
    if not db_url:
        st.error("DATABASE_URL nije postavljen (Secrets ili env).")
        st.stop()
    return create_engine(db_url, pool_pre_ping=True)


engine = _engine()


def _read_sql(query: str, params: dict | None = None) -> pd.DataFrame:
    with engine.connect() as conn:
        return pd.read_sql(text(query), conn, params=params or {})


@st.cache_data(show_spinner=False)
def _crosswalk_count() -> int:
    return int(_read_sql("SELECT COUNT(*) AS n FROM crosswalk").iloc[0]["n"])


@st.cache_data(show_spinner=False)
def _fetch_vendors(filter_q: str = "", limit: int = 500) -> List[str]:
    """Return vendor list with '' (GLOBAL) first; no LIKE when filter is blank."""
    filter_q = (filter_q or "").strip()
    if filter_q:
        q = """
            SELECT DISTINCT vendor_id
            FROM crosswalk
            WHERE vendor_id IS NOT NULL AND vendor_id ILIKE :like
            ORDER BY vendor_id
            LIMIT :lim
        """
        params = {"like": f"%{filter_q}%", "lim": limit}
    else:
        q = """
            SELECT DISTINCT vendor_id
            FROM crosswalk
            WHERE vendor_id IS NOT NULL
            ORDER BY vendor_id
            LIMIT :lim
        """
        params = {"lim": limit}
    df = _read_sql(q, params)
    vals = [str(x or "") for x in df["vendor_id"].tolist()]
    if "" not in vals:
        vals.insert(0, "")
    else:
        vals = [""] + [v for v in vals if v != ""]
    return vals


@st.cache_data(show_spinner=False)
def _load_vendor_names() -> Dict[str, str]:
    """Return {vendor_id -> vendor_name} from vendors table (if exists)."""
    try:
        df = _read_sql("SELECT vendor_id, vendor_name FROM vendors")
    except Exception:
        return {}
    df["vendor_id"] = df["vendor_id"].astype(str).str.strip().str.upper()
    df["vendor_name"] = df["vendor_name"].fillna("").astype(str).str.strip()
    return dict(zip(df["vendor_id"], df["vendor_name"]))


@st.cache_data(show_spinner=False)
def _columns_editor(default_order: List[str]) -> List[str]:
    """
    Show a robust column chooser that works across Streamlit versions.
    Returns the ordered list of columns to export.
    """
    cfg_default = pd.DataFrame({
        "column": default_order,
        "include": True,
        "order": list(range(1, len(default_order) + 1)),
    })

    st.markdown("### Choose export columns & order")

    _editor_kwargs = dict(hide_index=True, use_container_width=True)

    try:
        cfg = st.data_editor(
            cfg_default,
            **_editor_kwargs,
            column_config={
                "column": st.column_config.TextColumn("Column", disabled=True),
                "include": st.column_config.CheckboxColumn("Include"),
                "order": st.column_config.NumberColumn("Order", min_value=1, step=1),
            },
            help="Označi kolone i dodijeli redoslijed (1..N).",
        )
    except TypeError:
        st.info("Using a basic column editor (advanced column_config not supported on this deployment).")
        cfg = st.data_editor(cfg_default, **_editor_kwargs)

    cfg["order"] = pd.to_numeric(cfg["order"], errors="coerce")
    cfg = cfg.dropna(subset=["order"])
    cfg = cfg[cfg["include"]].sort_values(["order", "column"], kind="stable")
    export_cols = cfg["column"].tolist()
    return export_cols

# =============================================================================
# Header
# =============================================================================
st.title("Supplier → TOW Mapper (Cloud DB)")
with st.expander("How to use", expanded=False):
    st.markdown("""
1) Učitaj račun (Excel/CSV/PDF).  
2) Odaberi kolonu sa **supplier code** i pokreni mapping.  
3) Export: klasični (Matched+Unmatched) ili **Custom** (odabir kolona i redoslijeda).  
4) Admin (PIN): upsert, queue, apply i live search.
""")

st.toggle("Debug logs", key="_debug", value=False)

try:
    st.success(f"Crosswalk loaded (rows: {_crosswalk_count():,}) ✅")
except Exception as e:
    st.warning(f"Ne mogu pročitati broj redaka crosswalka: {e}")

# =============================================================================
# Vendor select: filter + refresh (+ vendor names)
# =============================================================================
vendors_map = _load_vendor_names()

st.markdown("**Vendor**")
cc1, cc2 = st.columns([3, 1])
with cc1:
    vendor_filter = st.text_input("Filter vendors (substring / prefix)", value="", key="vendor_filter")
with cc2:
    if st.button("Refresh list", key="btn_refresh_vendors"):
        st.cache_data.clear()

vendors = _fetch_vendors(vendor_filter)
prev_vendor = st.session_state.get("vendor_select", "")
if prev_vendor not in vendors:
    prev_vendor = ""  # GLOBAL

def _fmt_vendor(v: str) -> str:
    if v == "":
        return "GLOBAL (blank)"
    name = vendors_map.get(v.upper())
    return f"{v} — {name}" if name else v

vendor = st.selectbox(
    " ", options=vendors,
    index=vendors.index(prev_vendor) if prev_vendor in vendors else 0,
    key="vendor_select",
    format_func=_fmt_vendor,
    label_visibility="collapsed",
)
st.caption("Ostavi prazno za GLOBAL mapiranja (vrijedi za sve vendore).")

# =============================================================================
# Upload invoice
# =============================================================================
st.subheader("1) Upload supplier invoice (Excel / CSV / PDF)")
uploaded = st.file_uploader("Drag & drop ili odaberi datoteku", type=["xlsx", "xls", "csv", "pdf"], key="uploader")

preview_df: pd.DataFrame | None = None
if uploaded is not None:
    try:
        suffix = Path(uploaded.name).suffix.lower()
        if suffix in [".xlsx", ".xls"]:
            preview_df = pd.read_excel(uploaded)
        elif suffix == ".csv":
            content = uploaded.getvalue().decode("utf-8", errors="replace")
            try:
                dialect = csv.Sniffer().sniff(content.splitlines()[0])
                sep = dialect.delimiter
            except Exception:
                sep = ","
            preview_df = pd.read_csv(BytesIO(uploaded.getvalue()), sep=sep)
        elif suffix == ".pdf":
            if not _HAS_PDF:
                st.error("PDF parsing nije omogućen (pdfplumber nije instaliran).")
            else:
                tables = []
                with pdfplumber.open(BytesIO(uploaded.getvalue())) as pdf:
                    for page in pdf.pages:
                        try:
                            tbl = page.extract_table()
                            if tbl:
                                header, *rows = tbl
                                tables.append(pd.DataFrame(rows, columns=[str(x) for x in header]))
                        except Exception:
                            continue
                if tables:
                    preview_df = pd.concat(tables, ignore_index=True)
                else:
                    st.error("Nije detektirana tablica u PDF-u.")
        if isinstance(preview_df, pd.DataFrame):
            st.caption(f"📄 Kolone: {', '.join(map(str, preview_df.columns))}")
            st.dataframe(preview_df.head(200), use_container_width=True)
    except Exception as e:
        st.error(f"Greška pri čitanju datoteke: {e}")
        preview_df = None

# =============================================================================
# 2) Map to TOW
# =============================================================================
st.subheader("2) Map to TOW")

c1, c2 = st.columns([1, 1])
with c1:
    if st.button("Clear result", key="btn_clear_map"):
        for k in ["matched", "unmatched", "matched_cols", "unmatched_cols", "mapped_ready", "map_locked"]:
            st.session_state.pop(k, None)
with c2:
    st.session_state["map_locked"] = st.checkbox(
        "Lock mapping result (prevent reruns from clearing)",
        value=st.session_state.get("map_locked", True),
        key="chk_lock",
    )

if isinstance(preview_df, pd.DataFrame) and not preview_df.empty:
    supplier_col = st.selectbox(
        "Koja kolona sadrži SUPPLIER code?",
        options=list(preview_df.columns),
        index=0,
        key="supplier_col_select",
    )

    if st.button("Run mapping", type="primary", key="btn_run_mapping"):
        try:
            df_sup = preview_df.rename(columns={supplier_col: "supplier_id"}).copy()
            df_sup["supplier_id"] = df_sup["supplier_id"].astype(str).str.strip()
            vparam = (vendor or "").strip()

            df_map = _read_sql("""
                SELECT vendor_id, supplier_id, tow_code
                FROM crosswalk
                WHERE vendor_id = :v OR vendor_id = ''
            """, {"v": vparam})

            lookup = {(r["vendor_id"], r["supplier_id"]): r["tow_code"] for _, r in df_map.iterrows()}

            def resolve_tow(supp_code: str) -> Optional[str]:
                s = str(supp_code)
                return lookup.get((vparam, s)) or lookup.get(("", s)) or None

            df_out = df_sup.copy()
            df_out["tow"] = df_out["supplier_id"].map(resolve_tow)

            st.session_state["matched"] = df_out[df_out["tow"].notna()].copy()
            st.session_state["unmatched"] = df_out[df_out["tow"].isna()].copy()
            st.session_state["matched_cols"] = list(st.session_state["matched"].columns)
            st.session_state["unmatched_cols"] = list(st.session_state["unmatched"].columns)
            st.session_state["mapped_ready"] = True
            st.session_state["map_locked"] = True
            st.toast("Mapping complete ✅", icon="✅")
        except Exception as e:
            st.error(f"Mapping failed: {e}")
            st.session_state["mapped_ready"] = False

# Show result if available
if st.session_state.get("mapped_ready", False):
    matched = st.session_state["matched"]
    unmatched = st.session_state["unmatched"]

    st.success(f"Mapping complete → matched: {len(matched):,} | unmatched: {len(unmatched):,}")

    with st.expander("Preview: Matched (first 200 rows)", expanded=False):
        st.dataframe(matched.head(200), use_container_width=True)
    with st.expander("Preview: Unmatched (first 200 rows)", expanded=False):
        st.dataframe(unmatched.head(200), use_container_width=True)

    # -----------------------------------------------------------------------------
    # 2a) OLD / SIMPLE EXPORT
    # -----------------------------------------------------------------------------
    st.download_button(
        "Download Excel (Matched + Unmatched)",
        data=_excel_bytes({"Matched": matched, "Unmatched": unmatched}),
        file_name="mapping_result.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        key="dl_simple_both",
    )

    # -----------------------------------------------------------------------------
    # 2b) Custom export (add Invoice/Item/vendor_id/date + choose columns & order)
    # -----------------------------------------------------------------------------
    st.subheader("3) Custom export (add columns + choose order)")

    invoice_label = "Invoice"
    item_label = "Item"
    vendor_to_stamp = (vendor if vendor != "" else "GLOBAL")
    all_cols_now = list(dict.fromkeys(list(matched.columns) + list(unmatched.columns)))
    date_choice = "(none)"

    with st.expander("Optional: dodatne kolone (Invoice, Item, vendor_id, date)", expanded=True):
        cA, cB, cC = st.columns([1, 1, 1.3])
        with cA:
            invoice_label = st.text_input("Constant value for 'Invoice'", value="Invoice")
        with cB:
            item_label = st.text_input("Constant value for 'Item'", value="Item")
        with cC:
            vendor_to_stamp = st.text_input(
                "vendor_id to stamp on export",
                value=(vendor if vendor != "" else "GLOBAL")
            ).strip().upper() or "GLOBAL"

        date_options = ["(none)"] + all_cols_now
        date_choice = st.selectbox("Pick Date column from uploaded file (optional)", options=date_options, index=0)

    def _enrich(df: pd.DataFrame) -> pd.DataFrame:
        df2 = df.copy()
        df2["Invoice"] = invoice_label
        df2["Item"] = item_label
        df2["vendor_id"] = vendor_to_stamp
        if date_choice != "(none)" and date_choice in df2.columns:
            df2["date"] = df2[date_choice]
        return df2

    matched_en = _enrich(matched)
    unmatched_en = _enrich(unmatched)

    # Build preferred order
    all_cols = list(dict.fromkeys(list(matched_en.columns) + list(unmatched_en.columns)))
    preferred_first = [c for c in ["Invoice", "Item", "date", "vendor_id", "tow"] if c in all_cols]
    rest = [c for c in all_cols if c not in preferred_first]
    default_order = preferred_first + rest

    # Get ordered column selection
    export_cols = _columns_editor(default_order)
    if not export_cols:
        export_cols = default_order

    def _apply_selection(df: pd.DataFrame) -> pd.DataFrame:
        cols_in_df = [c for c in export_cols if c in df.columns]
        return df[cols_in_df] if cols_in_df else df

    matched_out = _apply_selection(matched_en)
    unmatched_out = _apply_selection(unmatched_en)

    with st.expander("Preview (custom): Matched", expanded=False):
        st.dataframe(matched_out.head(200), use_container_width=True)

    with st.expander("Preview (custom): Unmatched", expanded=False):
        st.dataframe(unmatched_out.head(200), use_container_width=True)

    st.download_button(
        "⬇️ Download Excel (custom columns & order)",
        data=_excel_bytes({"Matched": matched_out, "Unmatched": unmatched_out}),
        file_name="mapping_custom.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        key="dl_both_custom",
    )
else:
    st.info("Učitaj datoteku i pokreni mapping.")

# =============================================================================
# Admin (PIN -> unlock) + Queue/Direct + Live search (NEON DB)
# =============================================================================
st.divider()
st.subheader("Admin - Add / Queue / Apply Mappings - Live search")

# --- PIN gate (ENV/Secrets; trim) ---
expected_pin = os.getenv("ADMIN_PIN", st.secrets.get("ADMIN_PIN", ""))
expected_pin = str(expected_pin).strip()

c_pin1, c_pin2, c_pin3 = st.columns([5, 1, 2])
with c_pin1:
    pin_in = st.text_input("Admin PIN", type="password", key="admin_pin_input")
with c_pin2:
    if st.button("Unlock", key="btn_unlock"):
        st.session_state["admin_unlocked"] = (str(pin_in).strip() == expected_pin)
with c_pin3:
    st.caption(f"PIN configured: {'✅' if expected_pin else '❌'}")

if not st.session_state.get("admin_unlocked", False):
    st.info("Admin locked. Unesi PIN i klikni **Unlock**.")
    st.stop()

st.success("Admin unlocked.")

# ============== ADD A SINGLE MAPPING ==============
st.markdown("### Add a single mapping")

with st.form("admin_add_single"):
    c1, c2, c3 = st.columns(3)
    with c1:
        vendor_in = st.text_input("vendor_id (leave blank for GLOBAL)", value=st.session_state.get("vendor_prefill", ""))
    with c2:
        supp_in = st.text_input("supplier_id", value=st.session_state.get("supplier_prefill", ""))
    with c3:
        tow_in = st.text_input("tow_code", value=st.session_state.get("tow_prefill", ""))

    st.caption("Action…")
    action = st.radio(
        label="Action…",
        options=["Queue (downloadable CSV)", "Directly to DB (upsert)"],
        horizontal=True,
        key="admin_add_action"
    )

    submitted = st.form_submit_button("Add")
    if submitted:
        v = (vendor_in or "").strip()
        s = (supp_in or "").strip()
        t = (tow_in or "").strip()

        if not s:
            st.error("supplier_id is required.")
        else:
            if action.startswith("Queue"):
                queue_cols = ["vendor_id", "supplier_id", "tow_code"]
                if "updates_df" not in st.session_state:
                    st.session_state["updates_df"] = pd.DataFrame(columns=queue_cols)
                st.session_state["updates_df"] = pd.concat(
                    [st.session_state["updates_df"], pd.DataFrame([{"vendor_id": v, "supplier_id": s, "tow_code": t}])],
                    ignore_index=True
                )
                st.success("Queued. (updates.csv u dnu sekcije)")
            else:
                try:
                    with engine.begin() as conn:
                        conn.execute(text("""
                            INSERT INTO crosswalk (vendor_id, supplier_id, tow_code)
                            VALUES (:v, :s, :t)
                            ON CONFLICT (vendor_id, supplier_id)
                            DO UPDATE SET tow_code = EXCLUDED.tow_code
                        """), {"v": v, "s": s, "t": t})
                    st.success("Upsert OK.")
                except Exception as e:
                    st.error(f"DB error: {e}")

# --- Queue CSV (download/apply/clear) ---
st.markdown("### Apply queued CSV to DB")

if "updates_df" in st.session_state and not st.session_state["updates_df"].empty:
    dfq = st.session_state["updates_df"].copy()
    st.dataframe(dfq, use_container_width=True, height=200)

    qcsv = dfq.to_csv(index=False).encode("utf-8")
    st.download_button(
        "Download updates.csv",
        data=qcsv,
        file_name="updates.csv",
        mime="text/csv",
        key="dl_updates_csv"
    )

    c_apply1, c_apply2 = st.columns([1, 1])
    with c_apply1:
        if st.button("Apply updates.csv to DB", key="btn_apply_updates"):
            try:
                with engine.begin() as conn:
                    for _, r in dfq.iterrows():
                        conn.execute(text("""
                            INSERT INTO crosswalk (vendor_id, supplier_id, tow_code)
                            VALUES (:v, :s, :t)
                            ON CONFLICT (vendor_id, supplier_id)
                            DO UPDATE SET tow_code = EXCLUDED.tow_code
                        """), {
                            "v": str(r.get("vendor_id", "")),
                            "s": str(r.get("supplier_id", "")),
                            "t": str(r.get("tow_code", "")),
                        })
                st.success("updates.csv applied to DB.")
            except Exception as e:
                st.error(f"Apply failed: {e}")
    with c_apply2:
        if st.button("Clear queued items", key="btn_clear_updates"):
            st.session_state["updates_df"] = pd.DataFrame(columns=["vendor_id", "supplier_id", "tow_code"])
            st.info("Queue cleared.")
else:
    st.caption("No updates.csv yet.")

# ======================== LIVE SEARCH ========================
st.markdown("### Live search / inspect")

c_f1, c_f2, c_f3 = st.columns([2.2, 2.2, 1.2])
with c_f1:
    vendor_filter_live = st.text_input("vendor_id filter (blank = ALL)", value=st.session_state.get("ls_vendor", ""))
with c_f2:
    supp_filter_live = st.text_input("supplier_id search (exact or contains)", value=st.session_state.get("ls_supplier", ""))
with c_f3:
    exact = st.checkbox("Exact supplier match", value=st.session_state.get("ls_exact", False))

st.session_state["ls_vendor"] = vendor_filter_live
st.session_state["ls_supplier"] = supp_filter_live
st.session_state["ls_exact"] = exact

clauses, params = [], {}
if vendor_filter_live != "":
    clauses.append("c.vendor_id = :v1")
    params["v1"] = vendor_filter_live
if supp_filter_live != "":
    if exact:
        clauses.append("c.supplier_id = :s1")
        params["s1"] = supp_filter_live
    else:
        clauses.append("c.supplier_id ILIKE :s1")
        params["s1"] = f"%{supp_filter_live}%"

where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
q = f"""
    SELECT c.vendor_id, v.vendor_name, c.supplier_id, c.tow_code
    FROM crosswalk c
    LEFT JOIN vendors v ON v.vendor_id = c.vendor_id
    {where}
    ORDER BY c.vendor_id, c.supplier_id
    LIMIT 500
"""

try:
    df_res = _read_sql(q, params)
    st.caption(f"500 results shown (max 500) — shown: {len(df_res):,}")
    st.dataframe(df_res, use_container_width=True, height=260)

    st.markdown("##### Pick row as prefill")
    c_idx, c_btn = st.columns([1, 2])
    with c_idx:
        idx = st.number_input(" ", min_value=0, max_value=max(len(df_res)-1, 0), value=0, step=1, label_visibility="collapsed")
    with c_btn:
        if st.button("Prefill Admin form from row", key="btn_prefill"):
            if not df_res.empty:
                row = df_res.iloc[int(idx)]
                st.session_state["vendor_prefill"] = str(row.get("vendor_id", "") or "")
                st.session_state["supplier_prefill"] = str(row.get("supplier_id", "") or "")
                st.session_state["tow_prefill"] = str(row.get("tow_code", "") or "")
                st.success("Prefilled — skrolaj gore do 'Add a single mapping'.")
except Exception as e:
    st.error(f"Search failed: {e}")
