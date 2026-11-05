"""Bosch merger adapted from mkozina969/bosch-packing-app (blob 5c7144d)."""

import os
import re
import unicodedata
from datetime import datetime
from io import BytesIO
from typing import Dict, List, Optional, Tuple
from zipfile import BadZipFile

from openpyxl.utils.exceptions import InvalidFileException
import pandas as pd
import streamlit as st


# --------------------------- Normalization helpers ---------------------------

def _strip_accents(s: str) -> str:
    if not isinstance(s, str):
        return s
    return "".join(c for c in unicodedata.normalize("NFKD", s) if not unicodedata.combining(c))


def _norm_key(s: str) -> str:
    """Lenient header normalization for matching across subtle naming differences."""
    if s is None:
        return ""
    s = str(s)
    s = _strip_accents(s).lower()
    s = s.replace("\n", " ")
    s = re.sub(r"[\s\-]+", "_", s)      # spaces & dashes → underscore
    s = re.sub(r"[^a-z0-9_]+", "", s)     # keep only a-z0-9_
    s = re.sub(r"_+", "_", s).strip("_") # collapse underscores
    return s


# --------------------------- Table detection ---------------------------

def _find_header_row_and_map(raw: pd.DataFrame, target_headers: List[str]) -> Tuple[Optional[int], Dict[str, str]]:
    """Return (header_row_index, mapping target→original_column_name) if all targets found on some row.
    Matching is done on normalized keys.
    """
    targets_norm = [_norm_key(t) for t in target_headers]
    uniq_targets_norm = list(dict.fromkeys(targets_norm))  # preserve order

    for i, row in raw.iterrows():
        values = [v if v is not None else "" for v in row.values]
        norm_map: Dict[str, str] = {}
        for v in values:
            k = _norm_key(v)
            if k and k not in norm_map:
                norm_map[k] = str(v)
        if all(tk in norm_map for tk in uniq_targets_norm):
            mapping: Dict[str, str] = {}
            for t, tk in zip(target_headers, targets_norm):
                mapping[t] = norm_map[tk]
            return i, mapping
    return None, {}


def _extract_table_any_sheet(xl: pd.ExcelFile, target_headers: List[str]) -> Tuple[pd.DataFrame, Dict[str, str]]:
    for name in xl.sheet_names:
        try:
            raw = xl.parse(name, header=None, dtype=object)
        except Exception:
            continue
        hdr_idx, mapping = _find_header_row_and_map(raw, target_headers)
        if hdr_idx is not None:
            tbl = raw.iloc[hdr_idx + 1 :].copy()
            tbl.columns = [str(c) for c in raw.iloc[hdr_idx].values]
            tbl = tbl.dropna(axis=1, how="all")
            return tbl, mapping
    return pd.DataFrame(), {}


def _extract_subset_from_file(file, targets: List[str], *, trim_ext: bool = False) -> pd.DataFrame:
    with pd.ExcelFile(file) as xl:
        tbl, mapping = _extract_table_any_sheet(xl, targets)

    out_cols: List[str] = []
    seen = set()
    for t in targets:
        if t not in seen:
            out_cols.append(t)
            seen.add(t)
    out = pd.DataFrame(columns=out_cols)

    for t in out_cols:
        src_col = mapping.get(t, None)
        if src_col and src_col in tbl.columns:
            out[t] = tbl[src_col]
        else:
            out[t] = pd.NA

    src_name = getattr(file, "name", "uploaded.xlsx")
    if trim_ext:
        src_name = os.path.splitext(src_name)[0]
    out["Source_File"] = src_name
    return out


TARGETS = {
    "Packing lists": ["ProductNumber", "DeliveredQuantity", "PkgIdentNumber_2", "PkgIdentNumber_1"],
    "Invoices": ["ProductNumber", "UnitPrice", "Quantity", "DesAdvRef_Date"],
}
MAX_FILES = 50


def _store_uploads(uploader_key):
    uploads = st.session_state[uploader_key] or []
    st.session_state["bosch_batch"] = (
        None if len(uploads) > MAX_FILES else [(file.name, file.getvalue()) for file in uploads]
    )


def _clear_uploads():
    st.session_state.pop("bosch_batch", None)
    st.session_state["bosch_upload_epoch"] = st.session_state.get("bosch_upload_epoch", 0) + 1


def render_bosch_merger():
    st.title("Bosch Merger")
    st.caption("Combine Bosch packing lists or invoices into one Excel workbook.")

    options = st.session_state.setdefault("bosch_options", {})
    modes = list(TARGETS)
    mode = st.radio(
        "Choose mode", modes, index=modes.index(options.get("mode", modes[0])),
        horizontal=True, key="bosch_mode",
    )
    is_invoice = mode == "Invoices"
    quantity_column = "Quantity" if is_invoice else "DeliveredQuantity"
    keep_source_file = st.checkbox(
        "Add Source_File column", value=options.get("source_file", True), key="bosch_source_file",
    )
    drop_key = "bosch_drop_invoice" if is_invoice else "bosch_drop_packing"
    drop_blank = st.checkbox(
        f"Drop rows with blank {quantity_column}", value=options.get(drop_key, False), key=drop_key,
    )
    parse_dates = False
    if is_invoice:
        parse_dates = st.checkbox(
            "Parse DesAdvRef_Date as date", value=options.get("parse_dates", True), key="bosch_parse_dates",
        )
        options["parse_dates"] = parse_dates
    options.update({"mode": mode, "source_file": keep_source_file, drop_key: drop_blank})
    st.button("Clear selected files", key="bosch_clear_uploads", on_click=_clear_uploads)
    uploader_key = (
        f"bosch_upload_{st.session_state.get('tow_upload_epoch', 0)}"
        f"_{st.session_state.get('bosch_upload_epoch', 0)}"
    )
    st.file_uploader(
        f"Upload {mode.lower()} .xlsx files (1–{MAX_FILES})",
        type=["xlsx"],
        accept_multiple_files=True,
        key=uploader_key,
        on_change=_store_uploads,
        args=(uploader_key,),
    )
    batch = st.session_state.get("bosch_batch", [])
    if batch is None:
        st.error(f"Please upload at most {MAX_FILES} files.")
        return
    if not batch:
        st.info(f"Upload 1–{MAX_FILES} .xlsx files to begin.")
        return

    parts = []
    included_names = []
    with st.spinner("Reading and extracting tables…"):
        for name, contents in batch:
            workbook = BytesIO(contents)
            workbook.name = name
            try:
                part = _extract_subset_from_file(workbook, TARGETS[mode], trim_ext=is_invoice)
            except (BadZipFile, InvalidFileException, KeyError, ValueError):
                st.warning(f"Could not read {name}. It was skipped.")
                continue
            if part.empty:
                st.warning(f"No {mode.lower()} rows found in {name}. It was skipped.")
                continue
            parts.append(part)
            included_names.append(name)
    if not parts:
        return

    with st.expander("Files included in this merge"):
        for name in included_names:
            st.write(name)

    merged = pd.concat(parts, ignore_index=True)
    if not keep_source_file:
        merged = merged.drop(columns="Source_File")
    merged[quantity_column] = pd.to_numeric(merged[quantity_column], errors="coerce")
    if is_invoice:
        merged["UnitPrice"] = pd.to_numeric(merged["UnitPrice"], errors="coerce")
    if parse_dates:
        merged["DesAdvRef_Date"] = pd.to_datetime(merged["DesAdvRef_Date"], errors="coerce")
    if drop_blank:
        merged = merged[merged[quantity_column].notna()].reset_index(drop=True)

    st.success(f"Merged rows: {len(merged):,}")
    st.dataframe(merged.head(50), width="stretch")
    buffer = BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        merged.to_excel(writer, index=False, sheet_name="Merged")
    kind = "invoices" if is_invoice else "packing"
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    st.download_button(
        "Download XLSX",
        data=buffer.getvalue(),
        file_name=f"bosch_{kind}_{timestamp}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        key="bosch_download",
        on_click="ignore",
    )
