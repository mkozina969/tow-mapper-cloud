"""Regression checks for the feature-branch navigation and access boundaries."""

from datetime import datetime
from io import BytesIO
import os
from pathlib import Path
import unittest
from unittest.mock import patch

import pandas as pd
from openpyxl import Workbook, load_workbook
from sqlalchemy import create_engine, text
from sqlalchemy.pool import StaticPool
import streamlit as st
from streamlit.testing.v1 import AppTest
from test_mapper_access import sign_in, start_authorized

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
        self.app.secrets["download_center"] = {"enabled": True}

    def navigate(self, name):
        next(radio for radio in self.app.radio if radio.label == "Sections").set_value(name).run()
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
            app.session_state["tow_navigation_0"] = section
            self.app = start_authorized(app)
            self.assertFalse(self.app.exception)
            self.unlock("wrong-pin")
            self.assertTrue(any("not accepted" in error.value for error in self.app.error))
            self.assertFalse(self.app.dataframe)
            self.assertFalse(any(button.label in {"Add", "Save rule"} for button in self.app.button))
        self.factory.assert_not_called()

    def test_valeo_section_opens_without_database_access(self):
        self.app.session_state["tow_navigation_0"] = "Valeo PDF → Excel"
        self.app.run()
        self.assertFalse(self.app.get("file_uploader"))

        start_authorized(self.app)
        self.assertFalse(self.app.exception)
        self.assertEqual(self.app.title[0].value, "Valeo PDF → Excel")
        uploader = self.app.get("file_uploader")[0]
        self.assertEqual(uploader.label, "Upload one or more Valeo PDFs")
        uploader.set_value(("broken.pdf", b"not a PDF", "application/pdf")).run()
        self.assertFalse(self.app.exception)
        self.assertIn("could not be read", self.app.error[0].value)
        self.factory.assert_not_called()

    def test_import_round_trips_and_session_reset(self):
        self.app.session_state["tow_retained_upload"] = (
            "packing.csv", b"ProductNumber,Quantity,Pallet\nDEMO-1,24,PAL01\nDEMO-2,3,PAL02\n"
        )
        start_authorized(self.app)
        self.assertEqual(next(radio for radio in self.app.radio if radio.label == "Sections").value, "Import Center")
        self.assertFalse(any(field.label == "Team upload key" for field in self.app.text_input))
        self.app.checkbox(key="share_finished_exports").check().run()
        self.app.selectbox(key="vendor_select").set_value("DEMO").run()
        self.app.selectbox(key="quantity_column").set_value("Quantity").run()
        self.app.button(key="btn_run_mapping").click().run()
        self.assertEqual(self.app.session_state["matched"]["tow"].tolist(), ["LOCAL-1", "GLOBAL-2"])
        self.assertEqual(self.app.session_state["matched"]["Quantity"].tolist(), [4, 3])
        self.app.multiselect(key="multiselect_export_cols").set_value(["tow", "Quantity", "Pallet"]).run()
        self.button("Apply changes").click().run()
        self.app.text_input(key="pallets_export_filename").set_value("demo_pallets").run()

        for section in ("Crosswalk Mapping", "UoM Conversion", "Valeo PDF → Excel", "Bosch Merger"):
            self.navigate(section)
            if section == "Crosswalk Mapping":
                self.unlock("fixture-pin")
                self.app.button(key="btn_prefill").click().run()
                self.assertEqual(self.app.session_state["supplier_prefill"], "DEMO-1")
                self.app.session_state["updates_df"] = pd.DataFrame(
                    [{"vendor_id": "DEMO", "supplier_id": "PENDING", "tow_code": "PENDING-TOW"}]
                )
            if section in ("Crosswalk Mapping", "UoM Conversion"):
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
        for key in ("tow_app_access_grant", "admin_unlocked", "tow_retained_upload", "matched", "export_cols", "updates_df"):
            self.assertNotIn(key, self.app.session_state)
        self.assertFalse(self.app.radio)
        sign_in(self.app)
        self.navigate("UoM Conversion")
        self.button("Unlock")
        self.assertFalse(self.app.dataframe)

    def test_bosch_modes_export_and_keep_their_own_session_state(self):
        def workbook(headers, *rows):
            book = Workbook()
            book.active.append(["Cover sheet"])
            sheet = book.create_sheet("Bosch data")
            sheet.append(["Report"])
            sheet.append(headers)
            for row in rows:
                sheet.append(row)
            contents = BytesIO()
            book.save(contents)
            return contents.getvalue()

        def export_rows(mock_download):
            export = mock_download.call_args.kwargs
            book = load_workbook(BytesIO(export["data"]))
            self.assertEqual(book.sheetnames, ["Merged"])
            return export["file_name"], list(book["Merged"].values)

        self.app.session_state["tow_navigation_0"] = "Bosch Merger"
        self.app.run()
        self.assertFalse(self.app.get("file_uploader"))
        start_authorized(self.app)
        self.assertFalse(self.app.exception)
        mime = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"

        packing_headers = ("ProductNumber", "DeliveredQuantity", "PkgIdentNumber_2", "PkgIdentNumber_1")
        packing_row = ("001234", 4, "0000456", "0000789")
        packing = workbook(packing_headers, packing_row)
        with patch.object(st, "download_button", wraps=st.download_button) as download:
            self.app.get("file_uploader")[0].set_value(
                [("packing-a.xlsx", packing, mime), ("packing-b.xlsx", packing, mime)]
            ).run()
            self.assertFalse(self.app.exception)
            filename, rows = export_rows(download)
            self.assertTrue(filename.startswith("bosch_packing_"))
            self.assertEqual(rows, [
                (*packing_headers, "Source_File"),
                (*packing_row, "packing-a.xlsx"),
                (*packing_row, "packing-b.xlsx"),
            ])

            self.app.radio(key="bosch_mode").set_value("Invoices").run()
            invoice_headers = ("ProductNumber", "UnitPrice", "Quantity", "DesAdvRef_Date")
            invoice = workbook(invoice_headers, ("001234", 12.5, 2, "2026-09-01"), ("001234", 7, None, "2026-09-02"))
            self.app.get("file_uploader")[0].set_value(
                [("invoice-001.xlsx", invoice, mime), ("damaged.xlsx", b"not a workbook", mime)]
            ).run()
            self.assertFalse(self.app.exception)
            self.assertTrue(any("damaged.xlsx" in warning.value for warning in self.app.warning))
            filename, rows = export_rows(download)
            self.assertTrue(filename.startswith("bosch_invoices_"))
            self.assertEqual(rows[0], (*invoice_headers, "Source_File"))
            self.assertEqual(rows[1], ("001234", 12.5, 2, datetime(2026, 9, 1), "invoice-001"))
            self.assertEqual(len(rows), 3)

            self.app.checkbox(key="bosch_drop_invoice").check().run()
            self.app.checkbox(key="bosch_source_file").uncheck().run()
            self.navigate("Valeo PDF → Excel")
            self.navigate("Bosch Merger")
            self.assertEqual(export_rows(download)[1], [
                invoice_headers, ("001234", 12.5, 2, datetime(2026, 9, 1)),
            ])
            self.button("Clear selected files").click().run()
            self.assertFalse(self.app.get("download_button"))
            self.assertNotIn("bosch_batch", self.app.session_state)

        self.button("Logout").click().run()
        self.assertNotIn("bosch_options", self.app.session_state)
        self.factory.assert_not_called()


if __name__ == "__main__":
    unittest.main()
