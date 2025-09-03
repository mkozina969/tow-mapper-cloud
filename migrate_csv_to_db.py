import argparse
from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine, text

# -----------------------------------------------------------------------------
# Parse arguments
# -----------------------------------------------------------------------------
ap = argparse.ArgumentParser()
ap.add_argument("--csv", required=True)
ap.add_argument("--db", required=True, help="SQLAlchemy URL (postgresql+psycopg2://user:pass@host:port/db)")
ap.add_argument("--rebuild", action="store_true")
args = ap.parse_args()

# -----------------------------------------------------------------------------
# DB setup
# -----------------------------------------------------------------------------
engine = create_engine(args.db, pool_pre_ping=True)

with engine.begin() as conn:
    if args.rebuild:
        conn.execute(text("DROP TABLE IF EXISTS crosswalk"))
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS crosswalk (
            tow_code    TEXT NOT NULL,
            supplier_id TEXT NOT NULL,
            vendor_id   TEXT
        )
    """))
    conn.execute(text("""
        CREATE UNIQUE INDEX IF NOT EXISTS ix_crosswalk_vendor_supplier
        ON crosswalk (vendor_id, supplier_id)
    """))

# -----------------------------------------------------------------------------
# CSV reader with delimiter + encoding detection
# -----------------------------------------------------------------------------
def read_csv(p: Path) -> pd.DataFrame:
    encodings = ("utf-8-sig", "utf-8", "latin-1")
    first_line = None
    chosen_enc = None

    # Detect encoding + peek header
    for enc in encodings:
        try:
            with open(p, "r", encoding=enc) as f:
                for line in f:
                    if line.strip():
                        first_line = line
                        chosen_enc = enc
                        break
            if first_line is not None:
                break
        except Exception:
            continue
    if first_line is None:
        raise RuntimeError("Cannot detect CSV encoding/header.")

    # Detect delimiter
    sep = ";" if (";" in first_line and first_line.count(";") >= first_line.count(",")) else ","

    try:
        df = pd.read_csv(p, dtype=str, encoding=chosen_enc, sep=sep, on_bad_lines="skip")
    except Exception as e:
        raise RuntimeError(f"Failed to read CSV with enc={chosen_enc}, sep='{sep}': {e}")

    # Fallback if pandas treated everything as one column
    if df.shape[1] == 1 and ";" in df.columns[0]:
        df = pd.read_csv(p, dtype=str, encoding=chosen_enc, sep=";", on_bad_lines="skip")

    return df.fillna("")

# -----------------------------------------------------------------------------
# Load CSV
# -----------------------------------------------------------------------------
df = read_csv(Path(args.csv))
cols = {c.lower().strip(): c for c in df.columns}

tow = cols.get("tow_code") or cols.get("tow")
sup = cols.get("supplier_id") or cols.get("supplier_code")
ven = cols.get("vendor_id")

if not tow or not sup:
    raise SystemExit(f"CSV needs tow/tow_code and supplier_id/supplier_code. Found: {list(df.columns)}")

out = pd.DataFrame({
    "tow_code": df[tow].astype(str).str.strip(),
    "supplier_id": df[sup].astype(str).str.strip().str.upper(),
    "vendor_id": (df[ven].astype(str).str.strip().str.upper() if ven else "")
})
out = out.drop_duplicates(subset=["vendor_id", "supplier_id"], keep="last")

# -----------------------------------------------------------------------------
# Insert into DB
# -----------------------------------------------------------------------------
sql = text("""
INSERT INTO crosswalk (tow_code, supplier_id, vendor_id)
VALUES (:tow, :sup, :ven)
ON CONFLICT (vendor_id, supplier_id)
DO UPDATE SET tow_code = EXCLUDED.tow_code
""")

with engine.begin() as conn:
    conn.execute(sql, out.to_dict(orient="records"))

print(f"Loaded {len(out)} rows into crosswalk.")
