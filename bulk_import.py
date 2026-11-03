"""Validated, transactional file imports for PIN-protected administration."""
import csv
from decimal import Decimal, InvalidOperation
from io import BytesIO
from pathlib import Path

import pandas as pd
from sqlalchemy import text


SCHEMAS = {
    "mapping": ("crosswalk", ["vendor_id", "supplier_id", "tow_code"]),
    "conversion": ("item_uom_conversion", ["vendor_id", "supplier_id", "divider", "active"]),
}
MAX_ROWS = 10000


def read_import(data, filename):
    suffix = Path(filename).suffix.lower()
    if suffix == ".xlsx":
        return pd.read_excel(BytesIO(data), dtype=str, keep_default_na=False, nrows=MAX_ROWS + 1)
    if suffix == ".csv":
        source = data.decode("utf-8-sig")
        try:
            sep = csv.Sniffer().sniff(source[:8192], delimiters=",;\t").delimiter
        except csv.Error:
            raise ValueError("Unable to detect CSV separator. Use comma, semicolon or tab.") from None
        return pd.read_csv(BytesIO(data), sep=sep, dtype=str, keep_default_na=False, encoding="utf-8-sig", nrows=MAX_ROWS + 1)
    raise ValueError("Upload an .xlsx or UTF-8 .csv file with a header row.")


def validate_import(frame, kind, allow_global=False):
    _, columns = SCHEMAS[kind]
    frame = frame.copy().fillna("")
    frame.columns = [str(c).strip().lower() for c in frame.columns]
    required = [c for c in columns if c != "active"]
    missing = [c for c in required if c not in frame.columns]
    if frame.columns.duplicated().any():
        raise ValueError("Duplicate column names are not allowed.")
    if missing:
        raise ValueError("Missing required columns: " + ", ".join(missing))
    if frame.empty or len(frame) > MAX_ROWS:
        raise ValueError(f"Import must contain 1–{MAX_ROWS:,} rows.")
    if "active" in columns and "active" not in frame:
        frame["active"] = "true"
    frame = frame[columns]
    errors, records, keys = [], [], set()
    for position, values in enumerate(frame.to_dict("records"), start=2):
        record = {c: str(v).strip() for c, v in values.items()}
        row_errors = []
        if not record["vendor_id"] and (kind == "conversion" or not allow_global):
            row_errors.append("vendor_id is required (GLOBAL mappings require the explicit option)")
        if not record["supplier_id"]:
            row_errors.append("supplier_id is required")
        if any(any(ord(char) < 32 for char in str(v)) for v in record.values()):
            row_errors.append("fields cannot contain control characters")
        key = (record["vendor_id"], record["supplier_id"])
        if key in keys:
            row_errors.append("duplicate vendor_id + supplier_id in this file")
        keys.add(key)
        if kind == "mapping":
            if not record["tow_code"]:
                row_errors.append("tow_code is required")
        else:
            try:
                divider = Decimal(record["divider"])
                if not divider.is_finite() or divider != divider.to_integral_value() or not 1 <= divider <= 2147483647:
                    raise InvalidOperation
                record["divider"] = int(divider)
            except (InvalidOperation, ValueError):
                row_errors.append("divider must be a positive integer (maximum 2147483647)")
            active = record["active"].lower()
            if active not in ("true", "false", "1", "0", "yes", "no"):
                row_errors.append("active must be true/false, yes/no or 1/0")
            else:
                record["active"] = active in ("true", "1", "yes")
        if row_errors:
            errors.append({"file_row": position, "supplier_id": record["supplier_id"], "error": "; ".join(row_errors)})
        records.append(record)
    return records, errors


def existing_records(engine, records, kind):
    table, columns = SCHEMAS[kind]
    found = {}
    with engine.connect() as conn:
        for start in range(0, len(records), 400):
            batch = records[start:start + 400]
            params, pairs = {}, []
            for i, record in enumerate(batch):
                params[f"v{i}"] = record["vendor_id"]
                params[f"s{i}"] = record["supplier_id"]
                pairs.append(f"(:v{i}, :s{i})")
            query = f"SELECT {', '.join(columns)} FROM {table} WHERE (vendor_id, supplier_id) IN ({', '.join(pairs)})"
            for row in conn.execute(text(query), params).mappings():
                found[(row["vendor_id"], row["supplier_id"])] = dict(row)
    return found


def import_records(engine, records, kind, update_existing=False, allow_global=False):
    # Revalidate at the write boundary; invalid batches never partially commit.
    clean, errors = validate_import(pd.DataFrame(records), kind, allow_global)
    if errors:
        raise ValueError("Validation failed. Resolve all file errors before importing.")
    table, columns = SCHEMAS[kind]
    action = "DO NOTHING"
    if update_existing:
        changed = [c for c in columns if c not in ("vendor_id", "supplier_id")]
        action = "DO UPDATE SET " + ", ".join(f"{c} = EXCLUDED.{c}" for c in changed)
        if kind == "conversion":
            action += ", updated_at = CURRENT_TIMESTAMP"
    statement = text(f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({', '.join(':' + c for c in columns)}) ON CONFLICT (vendor_id, supplier_id) {action}")
    affected = 0
    with engine.begin() as conn:
        for start in range(0, len(clean), 400):
            result = conn.execute(statement, clean[start:start + 400])
            affected += max(result.rowcount, 0)
    return affected


def render_bulk_import(engine, kind):
    import streamlit as st
    _, columns = SCHEMAS[kind]
    label = "conversion rules" if kind == "conversion" else "supplier-to-TOW mappings"
    with st.expander(f"Bulk import {label} — XLSX / CSV"):
        st.caption("Files need the template column headers. Maximum 10,000 rows; the first worksheet is used for XLSX. Keep item codes as text in Excel to preserve leading zeros. CSV must use UTF-8.")
        example = {"vendor_id": "VENDOR_EXAMPLE", "supplier_id": "001.234"}
        example.update({"divider": 4, "active": True} if kind == "conversion" else {"tow_code": "TOW_EXAMPLE"})
        template = pd.DataFrame([example], columns=columns)
        st.download_button("Download CSV template", template.to_csv(index=False).encode("utf-8-sig"), file_name=f"{kind}_import_template.csv", mime="text/csv", key=f"bulk_{kind}_csv_template")
        buffer = BytesIO()
        template.to_excel(buffer, index=False, engine="xlsxwriter")
        st.download_button("Download XLSX template", buffer.getvalue(), file_name=f"{kind}_import_template.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key=f"bulk_{kind}_xlsx_template")
        st.caption("Template rows are examples. Replace them with your own items before importing.")
        upload = st.file_uploader(f"Upload {label}", type=["xlsx", "csv"], key=f"bulk_{kind}_upload")
        allow_global = False
        if kind == "mapping":
            allow_global = st.checkbox("Allow blank vendor_id for GLOBAL mappings", key="bulk_mapping_global")
        update = st.checkbox("Update existing records with values from this file", value=False, key=f"bulk_{kind}_update")
        if upload is None:
            return
        try:
            records, errors = validate_import(read_import(upload.getvalue(), upload.name), kind, allow_global)
            if errors:
                st.error(f"{len(errors)} invalid rows. Nothing has been imported.")
                st.dataframe(pd.DataFrame(errors), use_container_width=True)
                st.download_button("Download validation errors", pd.DataFrame(errors).to_csv(index=False).encode("utf-8-sig"), file_name=f"{kind}_import_errors.csv", key=f"bulk_{kind}_errors")
                return
            existing = existing_records(engine, records, kind)
            count_existing = len(existing)
            st.info(f"{len(records)} valid rows: {len(records) - count_existing} new, {count_existing} existing ({'will be updated' if update else 'will be skipped'}).")
            preview = []
            for record in records:
                key = (record["vendor_id"], record["supplier_id"])
                before = existing.get(key)
                entry = {**record, "action": "insert" if before is None else ("update" if update else "skip")}
                if before is not None:
                    entry.update({f"current_{c}": before[c] for c in columns if c not in ("vendor_id", "supplier_id")})
                preview.append(entry)
            st.dataframe(pd.DataFrame(preview), use_container_width=True)
            st.caption("Import applies the whole validated file in one transaction. It does not delete records.")
            if st.button(f"Import {label} to Neon", type="primary", key=f"bulk_{kind}_apply"):
                affected = import_records(engine, records, kind, update, allow_global)
                st.cache_data.clear()
                st.session_state["mapped_ready"] = False
                st.success(f"Import completed: {affected} records written. Rerun invoice mapping to use the updated database.")
        except Exception as exc:
            st.error(f"Bulk import failed; no partial batch was committed: {exc}")
