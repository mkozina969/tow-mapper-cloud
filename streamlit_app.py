from __future__ import annotations

import os
import csv
from io import BytesIO
from typing import Optional
from pathlib import Path

import pandas as pd
import streamlit as st
import pdfplumber
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

st.set_page_config(page_title="Supplier → TOW Mapper (Cloud DB)", layout="wide")

# =============================================================================
# Engine: Postgres in cloud via DATABASE_URL (mandatory)
# =============================================================================
def get_engine() -> Engine:
    db_url = os.getenv("DATABASE_URL") or st.secrets.get("DATABASE_URL", "")
    if not db_url:
        st.error("DATABASE_URL not configured. Set it in Streamlit Secrets or environment.")
        st.stop()
    return create_engine(db_url, pool_pre_ping=True)

engine = get_engine()

# =============================================================================
# Schema ensure & helpers
# =============================================================================
def ensure_schema(engine: Engine) -> tuple[str, str, Optional[str]]:
    """
    Ensure crosswalk table/constraints exist.
    Returns canonical names used by the app: (supplier_col, tow_col, vendor_col_or_None)
    """
    with engine.begin() as conn:
        # Base table (vendor_id NOT NULL; we use 'GLOBAL' as sentinel for blank)
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS crosswalk (
            tow_code    TEXT NOT NULL,
            supplier_id TEXT NOT NULL,
            vendor_id   TEXT NOT NULL
        )
        """))
        # Unique (vendor_id, supplier_id) — idempotent via DO block
        conn.execute(text("""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_constraint
                WHERE conname = 'uq_crosswalk_vendor_supplier'
                  AND conrelid = 'crosswalk'::regclass
            ) THEN
                ALTER TABLE crosswalk
                ADD CONSTRAINT uq_crosswalk_vendor_supplier UNIQUE (vendor_id, supplier_id);
            END IF;
        END
        $$;
        """))
    return ("supplier_id", "tow_code", "vendor_id")

def df_read_sql(query: str, params: dict | None = None) -> pd.DataFrame:
    with engine.connect() as conn:
        return pd.read_sql_query(text(query), conn, params=params)

def exec_sql(sql: str, params: dict | None = None):
    with engine.begin() as conn:
        conn.execute(text(sql), params or {})

# =============================================================================
# Small pandas helper (compat: pandas 1.x and 2.x)
# =============================================================================
def df_map(df: pd.DataFrame, func):
    """Element-wise map that works on all pandas versions."""
    return df.map(func) if hasattr(pd.DataFrame, "map") else df.applymap(func)

# =============================================================================
# PDF table extraction helpers
# =============================================================================
def _score_header(cols: list[str]) -> int:
    tokens = [
        "supplier", "supplier_id", "supplier code", "suppliercode",
        "ean", "sku", "article", "code", "šifra", "sifra", "catalog", "catalogue"
    ]
    s = 0
    for c in cols:
        cl = str(c or "").lower()
        s += sum(tok in cl for tok in tokens)
    return s

def extract_pdf_tables(file):
    """
    Return {"scanned": bool, "tables": list[{page:int, strategy:str, rows:list[list[str]]}]}
    Each 'rows' contains the raw table including a header row (at some index).
    """
    out = []
    strategies = [
        ("lines", {"vertical_strategy": "lines", "horizontal_strategy": "lines",
                   "intersection_x_tolerance": 5, "intersection_y_tolerance": 5}),
        ("text",  {"vertical_strategy": "text",  "horizontal_strategy": "text",
                   "text_x_tolerance": 2, "text_y_tolerance": 2}),
    ]
    file.seek(0)
    with pdfplumber.open(file) as pdf:
        any_text = False
        for pnum, page in enumerate(pdf.pages, start=1):
            try:
                if (page.extract_text() or "").strip():
                    any_text = True
            except Exception:
                pass
            for name, ts in strategies:
                try:
                    tables = page.extract_tables(table_settings=ts) or []
                except Exception:
                    tables = []
                for t in tables:
                    norm = []
                    for row in t:
                        if row is None:
                            continue
                        norm.append([("" if c is None else str(c)).strip() for c in row])
                    if len(norm) >= 2 and max(len(r) for r in norm) >= 2:
                        out.append({"page": pnum, "strategy": name, "rows": norm})
        if not out and not any_text:
            return {"scanned": True, "tables": []}
    return {"scanned": False, "tables": out}

def rows_to_dataframe(rows: list[list[str]], header_row_idx: int = 0) -> pd.DataFrame:
    """Convert raw rows into a DataFrame given which row is the header."""
    if not rows:
        return pd.DataFrame()
    width = max(len(r) for r in rows)
    padded = [r + [""] * (width - len(r)) for r in rows]
    header = [str(x or "").strip() for x in padded[header_row_idx]]
    body = [r for i, r in enumerate(padded) if i != header_row_idx]
    df = pd.DataFrame(body, columns=header)
    df = df.loc[:, (df.columns.astype(str).str.strip() != "")]
    df = df.loc[:, ~df.columns.duplicated()]
    df = df.fillna("")
    df = df_map(df, lambda v: str(v).replace("\n", " ").strip())
    return df

# =============================================================================
# Crosswalk loader
# =============================================================================
@st.cache_data(show_spinner=False)
def load_crosswalk() -> pd.DataFrame:
    supplier_col, tow_col, vendor_col = ensure_schema(engine)
    q = f"""
    SELECT {('vendor_id, ' if vendor_col else '')}
           {supplier_col} AS supplier_id,
           {tow_col} AS tow
    FROM crosswalk
    """
    df = df_read_sql(q)
    df["supplier_id"] = df["supplier_id"].astype(str).str.strip().str.upper()
    df["tow"] = df["tow"].astype(str).str.strip()
    if "vendor_id" in df.columns:
        df["vendor_id"] = df["vendor_id"].astype(str).str.strip().str.upper()
    return df

# =============================================================================
# UI: Help + cache button
# =============================================================================
with st.expander("How to use", expanded=True):
    st.markdown("""
1) This app reads/writes a **cloud Postgres** database → data survives restarts.  
2) Upload a supplier invoice (**Excel / CSV / PDF**), choose **Vendor** and supplier code column, then **Run mapping**.  
3) Use **Admin** to add/queue mappings and live-search the DB.  
4) Blank vendor is treated as **GLOBAL**, duplicates prevented by unique `(vendor_id, supplier_id)`.
""")

if st.button("♻️ Clear cache & re-run"):
    st.cache_data.clear()
    st.rerun()

# =============================================================================
# Load crosswalk
# =============================================================================
cw = load_crosswalk()
st.success(
    f"Crosswalk loaded | rows: {len(cw):,} | "
    f"vendors: {cw['vendor_id'].nunique() if 'vendor_id' in cw.columns else 'N/A'}"
)

# =============================================================================
# Vendor selector
# =============================================================================
vendor = "ALL"
if "vendor_id" in cw.columns:
    vendors = ["ALL"] + sorted(cw["vendor_id"].dropna().unique().tolist())
    vendor = st.selectbox("Vendor", vendors, index=0, help="Select a specific vendor (or GLOBAL) for vendor-specific mappings.")
else:
    st.caption("No vendor_id in crosswalk → using ALL.")
cw_for_vendor = cw if vendor == "ALL" or "vendor_id" not in cw.columns else cw[cw["vendor_id"] == vendor]

# =============================================================================
# Upload invoice
# =============================================================================
st.header("2) Upload supplier invoice (Excel / CSV / PDF)")
invoice_file = st.file_uploader(
    "Drag & drop or Browse",
    type=["xlsx", "xls", "csv", "pdf"],
    accept_multiple_files=False
)

invoice_df = None
if invoice_file:
    try:
        name = invoice_file.name.lower()
        if name.endswith((".xlsx", ".xls")):
            invoice_df = pd.read_excel(invoice_file, dtype=str)
        elif name.endswith(".csv"):
            # auto-detect delimiter , or ;
            invoice_file.seek(0)
            head = invoice_file.read(4096).decode("utf-8", errors="ignore")
            sep = ";" if head.count(";") >= head.count(",") else ","
            invoice_file.seek(0)
            invoice_df = pd.read_csv(
                invoice_file, engine="python", dtype=str, encoding="utf-8",
                on_bad_lines="skip", sep=sep
            )
        elif name.endswith(".pdf"):
            result = extract_pdf_tables(invoice_file)
            if isinstance(result, dict) and result.get("scanned"):
                st.error("This PDF looks like a scanned image (no text). OCR isn’t enabled yet. Export to CSV/Excel or share a sample and we’ll add OCR.")
            else:
                tables = result["tables"]
                if not tables:
                    st.warning("I couldn’t detect tabular data in this PDF. Try exporting the invoice to CSV/Excel.")
                else:
                    options = [
                        f"Table #{i+1} | page {t['page']} | {t['strategy']} | {len(t['rows'])}x{max(len(r) for r in t['rows'])}"
                        for i, t in enumerate(tables)
                    ]
                    choice = st.selectbox("Pick a table found in the PDF", options, index=0)
                    idx = options.index(choice)
                    chosen = tables[idx]

                    max_header = min(4, len(chosen["rows"]) - 1)
                    header_row_idx = st.number_input(
                        "Which row is the header?", min_value=0, max_value=max_header, value=0, step=1
                    )

                    invoice_df = rows_to_dataframe(chosen["rows"], header_row_idx)

                    def all_tables_to_excel_bytes():
                        bio = BytesIO()
                        with pd.ExcelWriter(bio, engine="xlsxwriter") as w:
                            for i, t in enumerate(tables):
                                df_i = rows_to_dataframe(t["rows"], header_row_idx=0)
                                sheet = f"p{t['page']}_{t['strategy']}_{i+1}"[:31]
                                df_i.to_excel(w, index=False, sheet_name=sheet)
                        return bio.getvalue()

                    st.download_button(
                        "Download ALL extracted PDF tables (Excel)",
                        data=all_tables_to_excel_bytes(),
                        file_name="pdf_tables.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    )
        else:
            st.error("Unsupported file type.")
            invoice_df = None
    except Exception as e:
        st.error(f"Failed to load invoice: {e}")
        invoice_df = None

    if invoice_df is not None and not invoice_df.empty:
        invoice_df = df_map(invoice_df, lambda v: "" if v is None else str(v))
        st.write("Preview:", invoice_df.head(10))
        st.caption(f"Rows: {len(invoice_df):,} | Columns: {list(invoice_df.columns)}")
    elif invoice_df is not None:
        st.warning("Loaded the file but didn’t find any rows.")

# =============================================================================
# Mapping
# =============================================================================
st.header("3) Map to TOW")

def suggest_supplier_column(cols):
    low = [c.lower() for c in cols]
    candidates = [
        "supplier_id", "supplier", "supplier code", "suppliercode", "supplier_cod",
        "code", "ean", "sku", "article", "catalog", "catalogue", "šifra", "sifra"
    ]
    for i, c in enumerate(low):
        if any(tok in c for tok in candidates):
            return i
    return 0

if invoice_df is not None and not invoice_df.empty:
    idx = suggest_supplier_column(invoice_df.columns)
    code_col = st.selectbox("Which column contains the SUPPLIER code?", options=list(invoice_df.columns), index=idx)

    if st.button("Run mapping"):
        try:
            left = invoice_df.copy()
            left["_supplier_id_norm"] = left[code_col].astype(str).str.strip().str.upper()

            right = cw_for_vendor.copy()
            right["_supplier_id_norm"] = right["supplier_id"].astype(str).str.strip().str.upper()

            merged = left.merge(
                right[["_supplier_id_norm", "tow"]],
                on="_supplier_id_norm", how="left"
            ).drop(columns=["_supplier_id_norm"])

            matched = merged[merged["tow"].notna()].copy()
            unmatched = merged[merged["tow"].isna()].copy()

            st.success(f"Mapping complete → matched: {len(matched):,} | unmatched: {len(unmatched):,}")

            with st.expander("Preview: Matched (first 200 rows)", expanded=False):
                st.dataframe(matched.head(200), use_container_width=True)
            with st.expander("Preview: Unmatched (first 200 rows)", expanded=False):
                st.dataframe(unmatched.head(200), use_container_width=True)

            def to_excel_bytes(d: dict) -> bytes:
                bio = BytesIO()
                with pd.ExcelWriter(bio, engine="xlsxwriter") as w:
                    for sheet, df in d.items():
                        df.to_excel(w, index=False, sheet_name=sheet)
                return bio.getvalue()

            st.download_button(
                "Download Excel (Matched + Unmatched)",
                data=to_excel_bytes({"Matched": matched, "Unmatched": unmatched}),
                file_name="mapping_result.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
        except Exception as e:
            st.error(f"Mapping failed: {e}")
else:
    st.info("Upload your supplier invoice to enable mapping.")

# =============================================================================
# Admin helpers (upsert/queue)
# =============================================================================
def upsert_mapping(vendor_id: str | None, supplier_id: str, tow_code: str) -> None:
    # normalize (FIX: use .upper() on Python strings, not pandas .str.upper)
    supplier_id = str(supplier_id or "").strip().upper()
    tow_code    = str(tow_code or "").strip()
    vendor_id   = str(vendor_id or "GLOBAL").strip().upper()  # GLOBAL sentinel for blank vendor

    sql = """
    INSERT INTO crosswalk (tow_code, supplier_id, vendor_id)
    VALUES (:tow, :sup, :ven)
    ON CONFLICT (vendor_id, supplier_id)
    DO UPDATE SET tow_code = EXCLUDED.tow_code
    """
    with engine.begin() as conn:
        conn.execute(text(sql), {"tow": tow_code, "sup": supplier_id, "ven": vendor_id})

def append_pending_csv(vendor_id: str, supplier_id: str, tow_code: str) -> None:
    write_header = not (Path("updates.csv")).exists()
    with open("updates.csv", "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["tow_code","supplier_id","vendor_id"])
        if write_header: w.writeheader()
        w.writerow({
            "tow_code": str(tow_code or "").strip(),
            "supplier_id": str(supplier_id or "").strip().upper(),
            "vendor_id": (str(vendor_id or "GLOBAL").strip().upper())
        })

def apply_pending(file_path: str = "updates.csv") -> int:
    p = Path(file_path)
    if not p.exists(): return 0
    df = pd.read_csv(p, dtype=str).fillna("")
    n = 0
    for _, r in df.iterrows():
        upsert_mapping(r.get("vendor_id","GLOBAL"), r.get("supplier_id",""), r.get("tow_code",""))
        n += 1
    return n

# =============================================================================
# Admin (PIN-gated) + Live search
# =============================================================================
with st.expander("🔐 Admin • Add / Queue / Apply Mappings • Live search", expanded=False):
    default_pin = os.environ.get("ST_ADMIN_PIN") or st.secrets.get("admin_pin", "letmein")
    col_pin, col_btn = st.columns([3,1])
    with col_pin:
        pin = st.text_input("Admin PIN", type="password", placeholder="Enter PIN to enable admin actions")
    with col_btn:
        ok = st.button("Unlock", use_container_width=True)

    if ok:
        if pin != default_pin:
            st.error("Incorrect PIN.")
        else:
            st.success("Admin unlocked.")
            st.session_state["admin_pin_ok"] = True

    if st.session_state.get("admin_pin_ok"):
        st.subheader("Add a single mapping")
        with st.form("admin_add_one"):
            c1, c2, c3 = st.columns(3)
            with c1:
                vendor_id_in = st.text_input("vendor_id (leave blank for GLOBAL)", value=st.session_state.pop("prefill_vendor_id",""), placeholder="e.g. DOB0000025 or leave blank")
            with c2:
                supplier_id_in = st.text_input("supplier_id", value=st.session_state.pop("prefill_supplier_id",""), placeholder="e.g. 0986356023")
            with c3:
                tow_code_in = st.text_input("tow_code", value=st.session_state.pop("prefill_tow_code",""), placeholder="e.g. 200183")

            mode = st.radio("Add to…", ["Queue (downloadable CSV)", "Directly to DB (upsert)"], horizontal=True)
            submitted = st.form_submit_button("Add")
            if submitted:
                if not (supplier_id_in and tow_code_in):
                    st.error("supplier_id and tow_code are required (vendor_id optional; blank = GLOBAL).")
                else:
                    try:
                        if mode.startswith("Queue"):
                            append_pending_csv(vendor_id_in, supplier_id_in, tow_code_in)
                            st.success(f"Queued locally (updates.csv): {(vendor_id_in or 'GLOBAL').upper()} / {supplier_id_in} → {tow_code_in}")
                        else:
                            upsert_mapping(vendor_id_in, supplier_id_in, tow_code_in)
                            st.success(f"Upserted to DB: {(vendor_id_in or 'GLOBAL').upper()} / {supplier_id_in} → {tow_code_in}")
                            st.cache_data.clear()
                    except Exception as e:
                        st.exception(e)

        st.subheader("Apply queued CSV to DB")
        cA, cB = st.columns([1,1])
        with cA:
            if st.button("Apply updates.csv to DB"):
                try:
                    n = apply_pending("updates.csv")
                    st.success(f"Applied {n} row(s) to DB.")
                    st.cache_data.clear()
                except Exception as e:
                    st.exception(e)
        with cB:
            try:
                data = Path("updates.csv").read_bytes()
                st.download_button("Download updates.csv", data=data, file_name="updates.csv", mime="text/csv")
            except Exception:
                st.caption("No updates.csv yet.")

        st.subheader("Live search / inspect")
        c1, c2, c3 = st.columns([2,2,1])
        with c1:
            vendor_q = st.text_input("Vendor filter (exact; blank = ALL)")
        with c2:
            supplier_q = st.text_input("supplier_id search (exact or contains)")
        with c3:
            exact = st.checkbox("Exact supplier match", value=True)

        clauses, params = [], {}
        if vendor_q.strip():
            clauses.append("vendor_id = :ven")
            params["ven"] = vendor_q.strip().upper()
        if supplier_q.strip():
            if exact:
                clauses.append("supplier_id = :sup")
                params["sup"] = supplier_q.strip().upper()
            else:
                clauses.append("supplier_id LIKE :sup")
                params["sup"] = f"%{supplier_q.strip().upper()}%"
        where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
        q = f"SELECT vendor_id, supplier_id, tow_code FROM crosswalk{where} ORDER BY vendor_id, supplier_id LIMIT 500"
        df_res = df_read_sql(q, params)
        st.caption(f"{len(df_res)} result(s) shown (max 500)")
        st.dataframe(df_res, use_container_width=True, height=260)

        if not df_res.empty:
            with st.form("prefill_form"):
                idx = st.number_input("Pick row # to prefill", min_value=0, max_value=len(df_res)-1, step=1, value=0)
                if st.form_submit_button("Prefill Admin form from row"):
                    row = df_res.iloc[int(idx)]
                    st.session_state["prefill_vendor_id"] = str(row.get("vendor_id","") or "")
                    st.session_state["prefill_supplier_id"] = str(row.get("supplier_id","") or "")
                    st.session_state["prefill_tow_code"] = str(row.get("tow_code","") or "")
                    st.success("Prefilled. Scroll up to 'Add a single mapping'.")
