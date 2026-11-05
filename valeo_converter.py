"""Valeo PDF conversion adapted from mkozina969/valeo-streamlit (blob 69e55ee)."""

from collections import defaultdict
from io import BytesIO
import re

import pandas as pd
import pdfplumber
from pdfminer.pdfexceptions import PDFException
from pdfplumber.utils.exceptions import MalformedPDFException, PdfminerException
import streamlit as st


# ---------- helpers ----------
def eu_to_float(s: str):
    s = str(s).strip().replace(".", "").replace(",", ".")
    try:
        return float(s)
    except Exception:
        return None

def read_all_text(pdf):
    return "\n".join([(p.extract_text() or "") for p in pdf.pages])

# ---------- Valeo INVOICE (unchanged) ----------
def parse_valeo_invoice_text(full_text:str) -> pd.DataFrame:
    rows = []
    current_inv = None
    inv_re = re.compile(r"\b(695\d{6})\b")  # Valeo invoice number

    for raw_line in (l for l in full_text.splitlines() if l.strip()):
        m_inv = inv_re.search(raw_line)
        if m_inv:
            current_inv = m_inv.group(1)

        low = raw_line.lower()
        if low.startswith((
            "your order:", "delivery note:", "goods value",
            "vat rate", "transport cost", "currency",
            "total gross value", "net price without vat"
        )):
            continue

        tok = raw_line.split()
        if len(tok) < 7:
            continue

        # last two are Net Price, Tot. Net
        if not (re.fullmatch(r"[\d\.,]+", tok[-1]) and re.fullmatch(r"[\d\.,]+", tok[-2])):
            continue

        # find "... Qty(int) Orig(AA) Customs(6-8d) ..."
        j = None
        for k in range(len(tok)-3, 1, -1):
            if (re.fullmatch(r"[A-Z]{2}", tok[k]) and
                k+1 < len(tok) and re.fullmatch(r"\d{6,8}", tok[k+1]) and
                re.fullmatch(r"\d+", tok[k-1])):
                j = k
                break
        if j is None:
            continue

        # supplier_id = first numeric token BEFORE Qty
        supplier_token = None
        for t in tok[:j-1]:
            if re.fullmatch(r"\d+", t):
                supplier_token = t
                break
        if not supplier_token:
            continue

        qty = int(tok[j-1])
        net_price = eu_to_float(tok[-2])
        tot_net   = eu_to_float(tok[-1])
        rows.append([supplier_token, qty, net_price, tot_net, current_inv])

    return pd.DataFrame(rows, columns=["Supplier_ID","Qty","Net Price","Tot. Net Value","InvoiceNo"])

# ---------- Valeo PACKING (header-locked, order-preserving, cross-page) ----------
def parse_valeo_packing_pdf(pdf) -> pd.DataFrame:
    """
    Process only pages with 'PACKING LIST'. For each such page:
      - detect header row where 'VALEO' and 'Quantity' sit on the same line;
      - build tight x-windows from the header tokens themselves;
      - iterate lines BELOW the header, top->bottom:
          * update current parcel on '<6+ digits> ... PALLET'
          * capture supplier_id (4–8 digits) INSIDE VALEO window
          * capture qty (1–4 digits) INSIDE Quantity window
        If both present and we have a current parcel -> append row.
    Keeps duplicates and preserves exact order across pages.
    """
    rows = []
    current_parcel = None

    PALLET_WORD = re.compile(r"\b(PALLET|CARTON)\b", re.IGNORECASE)
    PARCEL_ID   = re.compile(r"^\d{6,}$")
    SUPPLIER_ID = re.compile(r"^\d{4,8}$")
    INT_ONLY    = re.compile(r"^\d{1,4}$")
    SKIP_LINE   = re.compile(r"(dimensions|total\s+net\s+weight|total\s+gross\s+weight|type\s+of\s+parcel)",
                             re.IGNORECASE)

    for page in pdf.pages:
        page_text = page.extract_text() or ""
        if "PACKING LIST" not in page_text.upper():
            continue

        words = page.extract_words(use_text_flow=True, keep_blank_chars=False)
        if not words:
            continue

        # Group into visual lines
        line_map = defaultdict(list)
        for w in words:
            line_map[round(w["top"], 1)].append(w)
        lines = [(y, sorted(ws, key=lambda x: x["x0"])) for y, ws in sorted(line_map.items(), key=lambda kv: kv[0])]

        # Find header (must contain 'Quantity' and 'Valeo' on the same y)
        header = None
        for y, ws in lines:
            texts_lower = [w["text"].strip().lower() for w in ws]
            if "quantity" in texts_lower and any("valeo" in t for t in texts_lower):
                header = (y, ws)
                break
        if not header:
            # skip page if header can't be found (safer than guessing)
            continue

        header_y = header[0]
        qty_hdrs   = [w for w in header[1] if w["text"].strip().lower() == "quantity"]
        valeo_hdrs = [w for w in header[1] if "valeo" in w["text"].strip().lower()]

        # tight windows from header tokens
        qty_win   = (min(w["x0"] for w in qty_hdrs) - 1,  max(w["x1"] for w in qty_hdrs) + 60) if qty_hdrs else (page.width*0.75, page.width*0.95)
        valeo_win = (min(w["x0"] for w in valeo_hdrs) - 10, max(w["x1"] for w in valeo_hdrs) + 20) if valeo_hdrs else (page.width*0.45, page.width*0.7)

        # Walk lines in order *below* the header
        for y, ws in lines:
            if y <= header_y:
                continue
            if SKIP_LINE.search(" ".join(w["text"] for w in ws)):
                continue

            # detect & set parcel if this line has PALLET
            texts = [w["text"] for w in ws]
            if any(PALLET_WORD.search(t) for t in texts):
                parcel_tokens = [w["text"] for w in ws if PARCEL_ID.match(w["text"])]
                if parcel_tokens:
                    current_parcel = parcel_tokens[0]
                # NOTE: do not 'continue' here — the same line can contain the first item

            if not current_parcel:
                # top-of-page lines before the first PALLET belong to previous page's parcel
                # so if we still don't have one, skip until first PALLET on the document
                continue

            # supplier inside VALEO window
            supplier_tokens = [w["text"] for w in ws
                               if valeo_win[0] <= w["x0"] <= valeo_win[1] and SUPPLIER_ID.match(w["text"])]
            if not supplier_tokens:
                continue
            supplier_id = supplier_tokens[0]

            # quantity inside Quantity window (pure int, <= 4 digits)
            qty_tokens = [w["text"] for w in ws
                          if qty_win[0] <= w["x0"] <= qty_win[1] and INT_ONLY.match(w["text"])]
            if not qty_tokens:
                continue
            quantity = int(qty_tokens[-1])  # rightmost int in Quantity column

            rows.append([current_parcel, supplier_id, quantity])

    return pd.DataFrame(rows, columns=["Parcel N°","VALEO Material N°","Quantity"]).reset_index(drop=True)

# ---------- autodetect ----------
def autodetect(pdf):
    text = read_all_text(pdf)
    inv_df  = parse_valeo_invoice_text(text)
    pack_df = parse_valeo_packing_pdf(pdf)
    return inv_df, pack_df


RESULTS_KEY = "valeo_pdf_results"
XLSX_MIME = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"


def convert_pdf(data):
    with pdfplumber.open(BytesIO(data)) as pdf:
        return autodetect(pdf)


def _workbook(rows, sheet):
    buffer = BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        rows.to_excel(writer, index=False, sheet_name=sheet)
    return buffer.getvalue()


def _clear_results():
    st.session_state.pop(RESULTS_KEY, None)


def render_valeo_converter():
    st.title("Valeo PDF → Excel")
    st.caption("Automatically detect invoice and packing lines and download separate Excel files.")
    uploads = st.file_uploader(
        "Upload one or more Valeo PDFs",
        type=["pdf"],
        accept_multiple_files=True,
        key=f"valeo_pdfs_{st.session_state.get('tow_upload_epoch', 0)}",
        on_change=_clear_results,
    )

    if uploads and RESULTS_KEY not in st.session_state:
        results = []
        for upload in uploads:
            try:
                invoice, packing = convert_pdf(upload.getvalue())
            except (PDFException, PdfminerException, MalformedPDFException, ValueError):
                results.append((upload.name, None, None))
            else:
                results.append((upload.name, invoice, packing))
        st.session_state[RESULTS_KEY] = results

    results = st.session_state.get(RESULTS_KEY, [])
    if not results:
        st.info("Upload one or more Valeo PDFs.")
        return

    if not uploads:
        st.button("Clear previous Valeo results", on_click=_clear_results)

    for index, (name, invoice, packing) in enumerate(results):
        st.divider()
        st.subheader(f"File: {name}")
        if invoice is None:
            st.error("This PDF could not be read. Check that it opens and is not password protected.")
            continue
        if invoice.empty and packing.empty:
            st.warning("No invoice or packing lines detected — is this a Valeo PDF?")
            continue

        stem = name.rsplit(".", 1)[0]
        if not invoice.empty:
            st.write(f"**Invoice lines detected:** {len(invoice)} rows")
            st.dataframe(invoice, width="stretch", height=320)
            st.download_button(
                "Download Invoice XLSX",
                data=_workbook(invoice, "InvoiceLines"),
                file_name=f"{stem}_invoice_lines.xlsx",
                mime=XLSX_MIME,
                key=f"valeo_invoice_download_{index}",
                on_click="ignore",
            )
        if not packing.empty:
            total_quantity = int(packing["Quantity"].sum())
            st.write(f"**Packing lines detected:** {len(packing)} rows (Σ Quantity = {total_quantity})")
            st.dataframe(packing, width="stretch", height=320)
            st.download_button(
                "Download Packing XLSX",
                data=_workbook(packing, "PackingLines"),
                file_name=f"{stem}_packing_lines.xlsx",
                mime=XLSX_MIME,
                key=f"valeo_packing_download_{index}",
                on_click="ignore",
            )
