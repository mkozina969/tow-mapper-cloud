from io import BytesIO
import unittest
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.pool import StaticPool
from bulk_import import read_import, validate_import, existing_records, import_records


class BulkImportTests(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine("sqlite://", poolclass=StaticPool, connect_args={"check_same_thread": False})
        with self.engine.begin() as conn:
            conn.execute(text("CREATE TABLE crosswalk (vendor_id TEXT, supplier_id TEXT, tow_code TEXT CHECK(tow_code <> 'DB_REJECT'), UNIQUE(vendor_id,supplier_id))"))
            conn.execute(text("CREATE TABLE item_uom_conversion (vendor_id TEXT, supplier_id TEXT, divider INTEGER CHECK(divider>0), active BOOLEAN, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, UNIQUE(vendor_id,supplier_id))"))

    def tearDown(self):
        self.engine.dispose()

    def test_csv_xlsx_preserve_text_and_optional_active(self):
        data = b"vendor_id;supplier_id;divider\r\nV1;026.782;6\r\n"
        frame = read_import(data, "rules.csv")
        records, errors = validate_import(frame, "conversion")
        self.assertFalse(errors)
        self.assertEqual(records[0], {"vendor_id": "V1", "supplier_id": "026.782", "divider": 6, "active": True})
        buffer = BytesIO()
        frame.to_excel(buffer, index=False)
        self.assertEqual(validate_import(read_import(buffer.getvalue(), "rules.xlsx"), "conversion")[0], records)
        na = read_import(b"vendor_id,supplier_id,tow_code\nV1,NA,000123\n", "mapping.csv")
        self.assertEqual(na.iloc[0]["supplier_id"], "NA")
        self.assertEqual(na.iloc[0]["tow_code"], "000123")

    def test_invalid_batch_and_duplicates(self):
        frame = pd.DataFrame([{"vendor_id": "V1", "supplier_id": "A", "divider": 3}, {"vendor_id": "V1", "supplier_id": "A", "divider": 0}])
        _, errors = validate_import(frame, "conversion")
        self.assertEqual(errors[0]["file_row"], 3)
        self.assertIn("duplicate", errors[0]["error"])
        with self.assertRaises(ValueError):
            import_records(self.engine, frame.to_dict("records"), "conversion")
        with self.engine.connect() as conn:
            self.assertEqual(conn.scalar(text("SELECT count(*) FROM item_uom_conversion")), 0)

    def test_insert_skip_update_and_vendor_isolation(self):
        records = [{"vendor_id": "V1", "supplier_id": "A", "tow_code": "TOW1"}]
        self.assertEqual(import_records(self.engine, records, "mapping"), 1)
        records[0]["tow_code"] = "TOW2"
        self.assertEqual(import_records(self.engine, records, "mapping"), 0)
        self.assertEqual(existing_records(self.engine, records, "mapping")[("V1", "A")]["tow_code"], "TOW1")
        self.assertEqual(import_records(self.engine, records, "mapping", True), 1)
        records[0]["vendor_id"] = "V2"
        self.assertEqual(import_records(self.engine, records, "mapping"), 1)
        self.assertEqual(existing_records(self.engine, records, "mapping")[("V2", "A")]["tow_code"], "TOW2")

    def test_conversion_update_disable(self):
        records = [{"vendor_id": "V1", "supplier_id": "026.782", "divider": 6, "active": True}]
        import_records(self.engine, records, "conversion")
        records[0].update(divider=4, active=False)
        import_records(self.engine, records, "conversion", True)
        current = existing_records(self.engine, records, "conversion")[("V1", "026.782")]
        self.assertEqual(current["divider"], 4)
        self.assertFalse(current["active"])

    def test_database_failure_rolls_back_entire_batch(self):
        records = [{"vendor_id": "V1", "supplier_id": "A", "tow_code": "OK"}, {"vendor_id": "V1", "supplier_id": "B", "tow_code": "DB_REJECT"}]
        with self.assertRaises(Exception):
            import_records(self.engine, records, "mapping")
        self.assertEqual(existing_records(self.engine, records, "mapping"), {})

    def test_global_requires_explicit_opt_in(self):
        records = [{"vendor_id": "", "supplier_id": "A", "tow_code": "GLOBAL"}]
        with self.assertRaises(ValueError):
            import_records(self.engine, records, "mapping")
        self.assertEqual(import_records(self.engine, records, "mapping", allow_global=True), 1)


if __name__ == "__main__":
    unittest.main()
