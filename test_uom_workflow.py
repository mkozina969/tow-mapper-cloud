"""Exercise the real Streamlit mapping/export path against an isolated SQLite fixture."""
from decimal import Decimal
from io import BytesIO
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import pandas as pd
from sqlalchemy import create_engine, text
import streamlit as st
from streamlit.testing.v1 import AppTest


class WorkflowTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(dir=Path.cwd(), prefix=".uom-test-")
        self.url = "sqlite:///" + str(Path(self.temp.name) / "fixture.db")
        engine = create_engine(self.url)
        with engine.begin() as conn:
            conn.execute(text("CREATE TABLE crosswalk (vendor_id TEXT, supplier_id TEXT, tow_code TEXT)"))
            conn.execute(text("INSERT INTO crosswalk VALUES ('V1','A','TOW-A'), ('V1','B','TOW-B'), ('V2','A','OTHER-A')"))
            conn.execute(text("CREATE TABLE vendors (vendor_id TEXT, vendor_name TEXT)"))
            conn.execute(text("CREATE TABLE item_uom_conversion (vendor_id TEXT, supplier_id TEXT, divider INTEGER, active BOOLEAN)"))
            conn.execute(text("INSERT INTO item_uom_conversion VALUES ('V1','A',3,TRUE), ('V1','B',4,FALSE), ('V2','A',6,TRUE)"))
        engine.dispose()
        self.env = patch.dict(os.environ, {"DATABASE_URL": self.url, "ADMIN_PIN": "test-only"})
        self.env.start()
        st.cache_data.clear()
        st.cache_resource.clear()
        self.upload = BytesIO(b"code,qty,price\nA,12,2.50\nB,8,1.20\nUNKNOWN,2,3.00\n")
        self.upload.name = "original.csv"
        self.exports = {}
        self.engines = []
        original_download = st.download_button

        def tracked_engine(*args, **kwargs):
            instance = create_engine(*args, **kwargs)
            self.engines.append(instance)
            return instance

        def capture(label, data, **kwargs):
            self.exports[kwargs.get("key")] = data
            return original_download(label, data, **kwargs)

        self.patches = [patch.object(st, "file_uploader", return_value=self.upload), patch.object(st, "download_button", side_effect=capture), patch("sqlalchemy.create_engine", side_effect=tracked_engine)]
        for item in self.patches:
            item.start()
        self.app = AppTest.from_file("streamlit_app.py", default_timeout=30).run()

    def tearDown(self):
        for item in reversed(self.patches):
            item.stop()
        self.env.stop()
        st.cache_resource.clear()
        st.cache_data.clear()
        for instance in self.engines:
            instance.dispose()
        self.temp.cleanup()

    def select(self, label, value):
        next(item for item in self.app.selectbox if item.label == label).select(value).run()

    def map_invoice(self):
        self.select("Quantity column", "qty")
        self.select("Unit price column", "price")
        self.app.button(key="btn_run_mapping").click().run()

    def test_mapping_export_rerun_and_vendor_isolation(self):
        self.app.selectbox(key="vendor_select").select("V1").run()
        self.map_invoice()
        self.assertFalse(list(self.app.exception))
        self.assertTrue(self.app.session_state["mapped_ready"])
        matched = self.app.session_state["matched"]
        self.assertEqual(matched.iloc[0]["qty"], Decimal(4))
        self.assertEqual(matched.iloc[0]["price"], Decimal("7.50"))
        self.assertEqual(matched.iloc[1]["qty"], 8)  # Disabled rule.
        exported = pd.read_excel(BytesIO(self.exports["dl_simple_both"]), sheet_name=None)
        self.assertEqual(list(exported), ["Matched", "Unmatched"])
        self.assertEqual(exported["Matched"].iloc[0]["price"], 7.5)
        self.assertEqual(exported["Matched"].iloc[0]["qty"], 4)
        self.assertEqual(exported["Unmatched"].iloc[0]["supplier_id"], "UNKNOWN")
        self.app.button(key="btn_run_mapping").click().run()
        self.assertEqual(self.app.session_state["matched"].iloc[0]["qty"], Decimal(4))
        self.app.selectbox(key="vendor_select").select("V2").run()
        self.assertFalse(self.app.session_state["mapped_ready"])
        self.app.button(key="btn_run_mapping").click().run()
        self.assertEqual(self.app.session_state["matched"].iloc[0]["qty"], Decimal(2))

    def test_nondivisible_blocks_export(self):
        self.upload.seek(0)
        self.upload.truncate()
        self.upload.write(b"code,qty,price\nA,4,2.50\n")
        self.upload.seek(0)
        self.app.selectbox(key="vendor_select").select("V1").run()
        self.exports.clear()
        self.map_invoice()
        self.assertFalse(self.app.session_state["mapped_ready"])
        self.assertTrue(any("not divisible" in item.value for item in self.app.error))
        self.assertNotIn("dl_simple_both", self.exports)


if __name__ == "__main__":
    unittest.main()
