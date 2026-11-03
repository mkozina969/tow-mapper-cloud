CREATE TABLE IF NOT EXISTS item_uom_conversion (
    id BIGSERIAL PRIMARY KEY,
    vendor_id TEXT NOT NULL CHECK (btrim(vendor_id) <> ''),
    supplier_id TEXT NOT NULL CHECK (btrim(supplier_id) <> ''),
    divider INTEGER NOT NULL CHECK (divider > 0),
    active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (vendor_id, supplier_id)
);
