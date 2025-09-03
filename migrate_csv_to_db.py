import argparse
from pathlib import Path
import math
import pandas as pd
from sqlalchemy import create_engine, text

# -----------------------------------------------------------------------------
# Args
# -----------------------------------------------------------------------------
ap = argparse.ArgumentParser()
ap.add_argument("--csv", required=True)
ap.add_argument("--db", required=True, help="SQLAlchemy URL (postgresql+psycopg2://user:pass@host:port/db)")
ap.add_argument("--rebuild", action="store_true")
ap.add_argument("--chunk", type=int, default=5000, help="rows per batch insert")
args = ap.parse_args()

# -----------------------------------------------------------------------------
# DB init
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
# CSV reader (detect delimiter + encoding)
# -----------------------------------------------------------------------------
def read_csv(p: Path) -> pd.DataFrame:
    encodings = ("utf-8-sig", "utf-8", "latin-1")
    first_line = None
    chosen_enc = None

    for enc in encodings:
        try:
            with open(p, "r", encoding=enc) as f:
                for line in f:
                    if line.strip():
                        first_line = line
                        chosen_enc = enc
                        break
            if first_line:
                break
        except Exception:
            continue
    if not first_line:
        raise RuntimeError("Cannot detect CSV encoding/header.")

    sep = ";" if first_line.count(";") >= first_line.count(",") else ","

    df = pd.read_csv(p, dtype=str, encoding=chosen_enc, sep=sep, on_bad_lines="skip")
    # Fallback if everything ended up in one column header with ';'
    if df.shape[1] == 1 and ";" in df.columns[0]:
        df = pd.read_csv(p, dtype=str, encoding=chosen_enc, sep=";", on_bad_lines="skip")
    return df.fillna("")

# -----------------------------------------------------------------------------
# Transform
# -----------------------------------------------------------------------------
df = read_csv(Path(args.csv))
cols = {c.lower().strip(): c for c in df.columns}

tow_col = cols.get("tow_code") or cols.get("tow")
sup_col = cols.get("supplier_id") or cols.get("supplier_code")
ven_col = cols.get("vendor_id")

if not tow_col or not sup_col:
    raise SystemExit(f"CSV needs tow/tow_code and supplier_id/supplier_code. Found: {list(df.columns)}")

out = pd.DataFrame({
    "tow_code": df[tow_col].astype(str).str.strip(),
    "supplier_id": df[sup_col].astype(str).str.strip().str.upper(),
    "vendor_id": (df[ven_col].astype(str).str.strip().str.upper() if ven_col else "")
})

# Normalize blanks → NULL for vendor_id
out["vendor_id"] = out["vendor_id"].apply(lambda x: None if x == "" else x)
out = out.drop_duplicates(subset=["vendor_id", "supplier_id"], keep="last")

# -----------------------------------------------------------------------------
# Upsert SQL (placeholders match dict keys)
# -----------------------------------------------------------------------------
sql = text("""
INSERT INTO crosswalk (tow_code, supplier_id, vendor_id)
VALUES (:tow_code, :supplier_id, :vendor_id)
ON CONFLICT (vendor_id, supplier_id)
DO UPDATE SET tow_code = EXCLUDED.tow_code
""")

# -----------------------------------------------------------------------------
# Execute in chunks
# -----------------------------------------------------------------------------
n = len(out)
if n == 0:
    print("Nothing to load (0 rows after cleaning).")
else:
    records = out.to_dict(orient="records")
    chunk = max(1, args.chunk)
    total_batches = math.ceil(n / chunk)
    done = 0
    with engine.begin() as conn:
        for i in range(0, n, chunk):
            batch = records[i:i+chunk]
            conn.execute(sql, batch)
            done += len(batch)
            print(f"Upserted {done}/{n} rows...", flush=True)
    print(f"Loaded {n} rows into crosswalk.")
