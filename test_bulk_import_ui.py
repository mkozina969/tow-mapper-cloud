from io import BytesIO
import unittest
from unittest.mock import patch
import streamlit as st
from streamlit.testing.v1 import AppTest
from sqlalchemy import text
import test_bulk_import as fixtures


class BulkImportUiTests(unittest.TestCase):
    def setUp(self):
        self.fixture = fixtures.BulkImportTests()
        self.fixture.setUp()

    def tearDown(self):
        self.fixture.tearDown()

    def test_preview_then_click_import_for_both_sections(self):
        for kind, payload, table, label in (
            ("mapping", b"vendor_id,supplier_id,tow_code\nV1,001.234,TOW-NEW\n", "crosswalk", "supplier-to-TOW mappings"),
            ("conversion", b"vendor_id;supplier_id;divider;active\nV1;026.782;6;true\n", "item_uom_conversion", "conversion rules"),
        ):
            with self.subTest(kind=kind):
                upload = BytesIO(payload)
                upload.name = "batch.csv"
                with patch.object(st, "file_uploader", return_value=upload), patch("sqlalchemy.create_engine", return_value=self.fixture.engine):
                    app = AppTest.from_string(f"from sqlalchemy import create_engine\nfrom bulk_import import render_bulk_import\nrender_bulk_import(create_engine('sqlite://'), '{kind}')").run()
                    self.assertFalse(list(app.exception))
                    with self.fixture.engine.connect() as conn:
                        self.assertEqual(conn.scalar(text(f"SELECT count(*) FROM {table}")), 0)
                    app.button(key=f"bulk_{kind}_apply").click().run()
                    self.assertFalse(list(app.exception))
                    self.assertTrue(any("Import completed" in item.value for item in app.success))
                    with self.fixture.engine.connect() as conn:
                        self.assertEqual(conn.scalar(text(f"SELECT count(*) FROM {table}")), 1)
                    # Rerun + import again: existing record is skipped by default.
                    app.run()
                    app.button(key=f"bulk_{kind}_apply").click().run()
                    self.assertTrue(any("0 records written" in item.value for item in app.success))

    def test_invalid_upload_has_no_import_button(self):
        upload = BytesIO(b"vendor_id,supplier_id,divider\nV1,026.782,0\n")
        upload.name = "invalid.csv"
        with patch.object(st, "file_uploader", return_value=upload), patch("sqlalchemy.create_engine", return_value=self.fixture.engine):
            app = AppTest.from_string("from sqlalchemy import create_engine\nfrom bulk_import import render_bulk_import\nrender_bulk_import(create_engine('sqlite://'), 'conversion')").run()
            self.assertTrue(any("invalid rows" in item.value for item in app.error))
            self.assertFalse(any(b.key == "bulk_conversion_apply" for b in app.button))

    def test_import_sections_are_behind_existing_admin_pin(self):
        from test_uom_workflow import WorkflowTests
        workflow = WorkflowTests()
        workflow.setUp()
        try:
            self.assertFalse(any("Bulk import" in section.label for section in workflow.app.expander))
            workflow.app.text_input(key="admin_pin_input").set_value("wrong").run()
            workflow.app.button(key="btn_unlock").click().run()
            self.assertFalse(any("Bulk import" in section.label for section in workflow.app.expander))
            workflow.app.text_input(key="admin_pin_input").set_value("test-only").run()
            workflow.app.button(key="btn_unlock").click().run()
            self.assertFalse(list(workflow.app.exception))
            self.assertEqual(sum("Bulk import" in section.label for section in workflow.app.expander), 2)
        finally:
            workflow.tearDown()


if __name__ == "__main__":
    unittest.main()

