import argparse
from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine, text

ap = argparse.ArgumentParser()
ap.add_argument("--csv", required=True)
ap.add_argument("--db", required=True, help="SQLAlchemy URL (postgresql+psycopg2://user:pass@host:port/db)")
ap.add_argument("--rebuild", action="store_true")
args = ap.parse_args()

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

def read_csv(p: Path) -> pd.DataFrame:
    for enc in ("utf-8","latin-1"):
        try:
            return pd.read_csv(p, dtype=str, encoding=enc, on_bad_lines="skip")
        except Exception:
            continue
    raise RuntimeError("Cannot read CSV")

df = read_csv(Path(args.csv)).fillna("")
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
out = out.drop_duplicates(subset=["vendor_id","supplier_id"], keep="last")

sql = text("""
INSERT INTO crosswalk (tow_code, supplier_id, vendor_id)
VALUES (:tow, :sup, :ven)
ON CONFLICT (vendor_id, supplier_id)
DO UPDATE SET tow_code = EXCLUDED.tow_code
""")

with engine.begin() as conn:
    conn.execute(sql, out.to_dict(orient="records"))

print(f"Loaded {len(out)} rows into crosswalk.")
