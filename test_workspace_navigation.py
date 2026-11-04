"""Regression checks for the feature-branch navigation and access boundaries."""

import os
from pathlib import Path
import unittest
from unittest.mock import patch

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.pool import StaticPool
import streamlit as st
from streamlit.testing.v1 import AppTest

APP = Path(__file__).with_name("streamlit_app.py")


class WorkspaceTests(unittest.TestCase):
    def setUp(self):
        st.cache_data.clear()
        st.cache_resource.clear()
        self.engine = create_engine(
            "sqlite://", poolclass=StaticPool, connect_args={"check_same_thread": False}
        )
        self.addCleanup(self.engine.dispose)
        with self.engine.begin() as conn:
            conn.execute(text("CREATE TABLE vendors (vendor_id TEXT, vendor_name TEXT)"))
            conn.execute(text("INSERT INTO vendors VALUES ('DEMO', 'Example Supplier')"))
            conn.execute(text("CREATE TABLE crosswalk (vendor_id TEXT, supplier_id TEXT, tow_code TEXT)"))
            conn.execute(text("INSERT INTO crosswalk VALUES ('', 'DEMO-1', 'GLOBAL-1'), ('DEMO', 'DEMO-1', 'LOCAL-1'), ('', 'DEMO-2', 'GLOBAL-2')"))
            conn.execute(text("CREATE TABLE item_uom_conversion (vendor_id TEXT, supplier_id TEXT, divider INT, active BOOLEAN)"))
            conn.execute(text("INSERT INTO item_uom_conversion VALUES ('DEMO', 'DEMO-1', 6, TRUE)"))

        self.enterContext(patch.dict(os.environ, {"DATABASE_URL": "sqlite://", "ADMIN_PIN": "fixture-pin"}))
        self.factory = self.enterContext(patch("sqlalchemy.create_engine", return_value=self.engine))
        self.enterContext(patch("streamlit_sortables.sort_items", side_effect=lambda columns, **kwargs: columns))
        self.app = AppTest.from_file(str(APP), default_timeout=30)
        self.app.secrets["download_center"] = {"enabled": True, "upload_key": "fixture-upload-key-123456789"}

    def navigate(self, name):
        self.app.radio(key="tow_navigation").set_value(name).run()
        self.assertFalse(self.app.exception)

    def button(self, label):
        return next(button for button in self.app.button if button.label == label)

    def unlock(self, pin):
        next(widget for widget in self.app.text_input if widget.label == "Admin PIN").set_value(pin)
        self.button("Unlock").click().run()
        self.assertFalse(self.app.exception)

    def test_protected_sections_require_pin_before_database_access(self):
        for section in ("Crosswalk Mapping", "UoM Conversion"):
            app = AppTest.from_file(str(APP), default_timeout=30)
            app.session_state["tow_navigation"] = section
            self.app = app.run()
            self.assertFalse(self.app.exception)
            self.unlock("wrong-pin")
            self.assertTrue(any("not accepted" in error.value for error in self.app.error))
            self.assertFalse(self.app.dataframe)
            self.assertFalse(any(button.label in {"Add", "Save rule"} for button in self.app.button))
        self.factory.assert_not_called()

    def test_import_round_trips_and_session_reset(self):
        self.app.session_state["tow_retained_upload"] = (
            "packing.csv", b"ProductNumber,Quantity,Pallet\nDEMO-1,24,PAL01\nDEMO-2,3,PAL02\n"
        )
        self.app.run()
        self.assertEqual(self.app.radio(key="tow_navigation").value, "Import Center")
        self.app.text_input(key="shared_upload_key").set_value("fixture-upload-key-123456789").run()
        self.app.checkbox(key="share_finished_exports").check().run()
        self.app.selectbox(key="vendor_select").set_value("DEMO").run()
        self.app.selectbox(key="quantity_column").set_value("Quantity").run()
        self.app.button(key="btn_run_mapping").click().run()
        self.assertEqual(self.app.session_state["matched"]["tow"].tolist(), ["LOCAL-1", "GLOBAL-2"])
        self.assertEqual(self.app.session_state["matched"]["Quantity"].tolist(), [4, 3])
        self.app.multiselect(key="multiselect_export_cols").set_value(["tow", "Quantity", "Pallet"]).run()
        self.button("Apply changes").click().run()
        self.app.text_input(key="pallets_export_filename").set_value("demo_pallets").run()

        for section in ("Crosswalk Mapping", "UoM Conversion"):
            self.navigate(section)
            if section == "Crosswalk Mapping":
                self.unlock("fixture-pin")
                self.app.button(key="btn_prefill").click().run()
                self.assertEqual(self.app.session_state["supplier_prefill"], "DEMO-1")
                self.app.session_state["updates_df"] = pd.DataFrame(
                    [{"vendor_id": "DEMO", "supplier_id": "PENDING", "tow_code": "PENDING-TOW"}]
                )
            self.assertTrue(self.app.dataframe)
            self.navigate("Import Center")
            self.assertTrue(self.app.session_state["mapped_ready"])
            self.assertEqual(self.app.session_state["tow_retained_upload"][0], "packing.csv")
            self.assertEqual(self.app.session_state["export_cols"], ["tow", "Quantity", "Pallet"])
            self.assertTrue(self.app.session_state["columns_applied"])
            self.assertEqual(self.app.text_input(key="pallets_export_filename").value, "demo_pallets")
            self.assertTrue(self.app.checkbox(key="share_finished_exports").value)
            self.assertTrue(any(button.label == "Download Pallets PRN" for button in self.app.get("download_button")))

        with self.engine.connect() as conn:
            self.assertEqual(conn.execute(text("SELECT count(*) FROM crosswalk")).scalar_one(), 3)
        self.assertEqual(len(self.app.session_state["updates_df"]), 1)

        self.app.selectbox(key="vendor_select").set_value("").run()
        self.app.button(key="btn_run_mapping").click().run()
        self.assertEqual(self.app.session_state["matched"]["tow"].tolist(), ["GLOBAL-1", "GLOBAL-2"])
        self.assertEqual(self.app.session_state["matched"]["Quantity"].tolist(), [24, 3])
        self.navigate("Settings")
        self.button("Reset session").click().run()
        self.assertFalse(self.app.exception)
        for key in ("admin_unlocked", "tow_retained_upload", "matched", "export_cols", "updates_df"):
            self.assertNotIn(key, self.app.session_state)
        self.navigate("UoM Conversion")
        self.button("Unlock")
        self.assertFalse(self.app.dataframe)


if __name__ == "__main__":
    unittest.main()
