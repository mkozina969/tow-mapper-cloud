"""Download Center checks using synthetic object storage and Streamlit sessions."""

from datetime import datetime, timedelta, timezone
from io import BytesIO
from pathlib import Path
from threading import Event
from types import SimpleNamespace
import sys
import unittest
from unittest.mock import patch

import pandas as pd
from streamlit.testing.v1 import AppTest

import download_center as dc


APP = Path(__file__).with_name("streamlit_app.py")
UPLOAD_KEY = "fixture-upload-key-12345678"
DOWNLOAD_KEY = "fixture-download-key-12345678"
DELETE_KEY = "fixture-delete-key-12345678"
LEGACY = "exports/" + "a" * 32 + "/packing.prn"
FLAT = "exports/invoice_20261104-103000_abcdefabcdef.xlsx"
EXPIRED = "exports/" + "b" * 32 + "/old.csv"
OTHER = "private/unrelated.csv"
BYTES = b"\x00original\xff\r\n"


class FakeStorage:
    def __init__(self):
        now = datetime.now(timezone.utc)
        self.objects = {
            LEGACY: (now - timedelta(days=1), BYTES),
            FLAT: (now - timedelta(hours=1), b"workbook"),
            EXPIRED: (now - timedelta(days=7), b"expired"),
            OTHER: (now, b"unrelated"),
        }
        self.lists = self.reads = 0
        self.deleted = []

    def get_paginator(self, operation):
        assert operation == "list_objects_v2"
        return self

    def paginate(self, *, Bucket, Prefix):
        assert Bucket == dc.BUCKET and Prefix == dc.PREFIX
        self.lists += 1
        records = [
            {"Key": key, "LastModified": uploaded, "Size": len(payload)}
            for key, (uploaded, payload) in self.objects.items()
        ]
        return [{"Contents": records[:2]}, {"Contents": records[2:]}]

    def get_object(self, *, Bucket, Key):
        assert Bucket == dc.BUCKET
        self.reads += 1
        uploaded, payload = self.objects[Key]
        return {"LastModified": uploaded, "ContentLength": len(payload), "Body": BytesIO(payload)}

    def head_object(self, *, Bucket, Key):
        assert Bucket == dc.BUCKET
        return {"LastModified": self.objects[Key][0]}

    def delete_object(self, *, Bucket, Key):
        assert Bucket == dc.BUCKET
        self.deleted.append(Key)
        del self.objects[Key]


class StorageTests(unittest.TestCase):
    def setUp(self):
        self.storage = FakeStorage()

    def test_list_formats_filters_and_retention(self):
        files = list(dc.list_exports(self.storage))
        self.assertEqual({o["Key"] for o in files}, {LEGACY, FLAT})
        self.assertEqual([o["Key"] for o in dc.list_exports(self.storage, expired=True)], [EXPIRED])
        self.assertEqual([o["Key"] for o in dc.filter_exports(files, "PACK", "PRN")], [LEGACY])
        self.assertEqual(dc.filter_exports(files, "PACK", "CSV"), [])
        self.assertEqual(dc.original_filename(FLAT), "invoice.xlsx")
        self.assertEqual(dc.original_filename(LEGACY), "packing.prn")
        self.assertEqual(self.storage.reads, 0)
        exact_expiry = self.storage.objects[LEGACY][0] + dc.RETENTION
        with self.assertRaisesRegex(ValueError, "expired"):
            dc.read_export(self.storage, LEGACY, now=exact_expiry)

    def test_binary_download_missing_and_revocation(self):
        grant = Event()
        expiration = datetime.now(timezone.utc) + timedelta(days=6)
        with patch.object(dc, "_read_client", return_value=self.storage):
            self.assertEqual(dc._download({}, grant, LEGACY, expiration), BYTES)
            with self.assertRaisesRegex(ValueError, "no longer available"):
                dc._download({}, grant, LEGACY, expiration - timedelta(days=8))
            with self.assertRaisesRegex(ValueError, "unavailable"):
                dc._download({}, grant, "exports/" + "c" * 32 + "/missing.prn", expiration)
            grant.set()
            with self.assertRaisesRegex(ValueError, "no longer available"):
                dc._download({}, grant, LEGACY, expiration)
        self.assertEqual(self.storage.reads, 2)

    def test_reject_path_traversal_and_oversized_files(self):
        for key in (OTHER, "exports/../secret.csv", "exports/%2e%2e/file.csv",
                    LEGACY + "/extra", "exports/" + "a" * 32 + "/../file.prn"):
            with self.subTest(key=key):
                with self.assertRaisesRegex(ValueError, "Invalid export key"):
                    dc.read_export(self.storage, key)
                with self.assertRaisesRegex(ValueError, "Invalid export key"):
                    dc.delete_export(self.storage, key)
        self.assertFalse(self.storage.deleted)
        self.assertEqual(self.storage.reads, 0)
        body = BytesIO(b"short")
        metadata = {"LastModified": datetime.now(timezone.utc), "ContentLength": dc.MAX_BYTES + 1, "Body": body}
        with patch.object(self.storage, "get_object", return_value=metadata):
            with self.assertRaisesRegex(ValueError, "25 MB"):
                dc.read_export(self.storage, LEGACY)
        self.assertTrue(body.closed)


class DownloadPageTests(unittest.TestCase):
    def setUp(self):
        self.storage = FakeStorage()
        self.app = AppTest.from_file(str(APP), default_timeout=30)
        self.app.secrets["download_center"] = {
            "enabled": True, "upload_key": UPLOAD_KEY, "download_key": DOWNLOAD_KEY,
            "delete_key": DELETE_KEY, "endpoint": "https://synthetic.aws.neon.tech",
            "region": "eu-central-1", "access_key_id": "fake", "secret_access_key": "fake",
        }
        self.app.session_state["tow_navigation_0"] = "Download Center"
        self.enterContext(patch.dict(sys.modules, {
            "boto3": SimpleNamespace(client=lambda *args, **kwargs: self.storage),
            "botocore.config": SimpleNamespace(Config=lambda **kwargs: None),
        }))
        self.app.run()
        self.assertFalse(self.app.exception)

    def unlock(self, label, value):
        next(widget for widget in self.app.text_input if widget.label == label).set_value(value)
        next(button for button in self.app.button if button.label == "Unlock").click().run()
        self.assertFalse(self.app.exception)

    def navigate(self, label):
        next(widget for widget in self.app.radio if widget.label == "Sections").set_value(label).run()
        self.assertFalse(self.app.exception)

    def test_keys_are_separate_and_dashboard_metadata_is_protected(self):
        self.assertEqual(self.storage.lists, 0)
        self.assertFalse(self.app.dataframe)
        self.unlock("Download access key", UPLOAD_KEY)
        self.assertEqual(self.storage.lists, 0)
        self.navigate("Dashboard")
        self.assertFalse(self.app.dataframe)
        self.assertEqual(self.storage.lists, 0)
        next(button for button in self.app.button if "Download Center" in button.label).click().run()
        self.unlock("Download access key", DOWNLOAD_KEY)
        self.assertEqual(self.app.dataframe[0].value["Type"].tolist(), ["XLSX", "PRN"])
        self.assertEqual(self.storage.reads, 0)
        self.app.selectbox(key="tow_download_type").set_value("PRN").run()
        self.assertEqual(self.app.dataframe[0].value["Type"].tolist(), ["PRN"])
        self.app.session_state["mapped_ready"] = True
        self.app.session_state["matched"] = pd.DataFrame({"tow": ["fixture"]})
        self.app.session_state["unmatched"] = pd.DataFrame()
        self.app.session_state["export_cols"] = ["tow"]
        self.navigate("Dashboard")
        self.assertEqual(self.app.metric[0].value, "1")
        self.assertEqual(self.app.session_state["export_cols"], ["tow"])
        grant = self.app.session_state[dc.DOWNLOAD_GRANT]
        next(button for button in self.app.button if button.label == "Reset session").click()
        # Patch the real app database factory for the default Import Center rerun.
        from sqlalchemy import create_engine, text
        from sqlalchemy.pool import StaticPool
        fixture_db = create_engine("sqlite://", poolclass=StaticPool, connect_args={"check_same_thread": False})
        self.addCleanup(fixture_db.dispose)
        with fixture_db.begin() as conn:
            conn.execute(text("CREATE TABLE crosswalk (vendor_id TEXT, supplier_id TEXT, tow_code TEXT)"))
        with patch("sqlalchemy.create_engine", return_value=fixture_db), patch.dict("os.environ", {"DATABASE_URL": "sqlite://"}):
            self.app.run()
        self.assertFalse(self.app.exception)
        self.assertTrue(grant.is_set())
        self.assertNotIn(dc.DOWNLOAD_GRANT, self.app.session_state)
        self.assertNotIn("matched", self.app.session_state)
        self.navigate("Download Center")
        self.assertFalse(self.app.dataframe)

    def test_delete_requires_distinct_key_and_explicit_confirmation(self):
        self.unlock("Download access key", DOWNLOAD_KEY)
        next(widget for widget in self.app.selectbox if widget.label == "Select an export").set_value(LEGACY).run()
        self.assertEqual(self.storage.reads, 0)
        self.assertFalse(any(button.label == "Delete file" for button in self.app.button))
        self.unlock("Delete access key", DOWNLOAD_KEY)
        self.assertFalse(self.storage.deleted)
        self.unlock("Delete access key", DELETE_KEY)
        self.assertTrue(next(button for button in self.app.button if button.label == "Delete file").disabled)
        next(box for box in self.app.checkbox if box.label == "Confirm deletion of this file").check().run()
        next(button for button in self.app.button if button.label == "Delete file").click().run()
        self.assertFalse(self.app.exception)
        self.assertEqual(self.storage.deleted, [LEGACY])
        self.assertEqual(self.app.dataframe[0].value["Type"].tolist(), ["XLSX"])


if __name__ == "__main__":
    unittest.main()
