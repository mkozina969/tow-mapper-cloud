import io
import os
import unittest
from contextlib import redirect_stderr, redirect_stdout
from datetime import datetime, timedelta, timezone
from unittest.mock import Mock, patch

import cleanup_download_center as cleanup


class StagingCleanupTest(unittest.TestCase):
    def setUp(self):
        now = datetime.now(timezone.utc)
        self.expired = "exports/synthetic_20200101-000000_0123456789ab.csv"
        self.current = "exports/current_20200101-000000_0123456789ab.csv"
        self.unrelated = "exports/unrelated.csv"
        self.objects = {
            self.expired: now - timedelta(days=8),
            self.current: now,
            self.unrelated: now - timedelta(days=8),
        }
        self.storage = Mock()
        self.storage.get_paginator.return_value.paginate.side_effect = lambda **_: [
            {"Contents": [{"Key": key, "LastModified": modified}
                          for key, modified in self.objects.items()]}
        ]
        self.storage.delete_object.side_effect = lambda Bucket, Key: self.objects.pop(Key)
        self.env = {
            "AWS_ENDPOINT_URL_S3": cleanup.STAGING_ENDPOINT,
            "AWS_REGION": cleanup.STAGING_REGION,
            "AWS_ACCESS_KEY_ID": "test-id",
            "AWS_SECRET_ACCESS_KEY": "test-secret",
        }

    def run_cleanup(self, *flags):
        log = io.StringIO()
        with patch.dict(os.environ, self.env, clear=True), redirect_stdout(log), \
                patch.object(cleanup, "client", return_value=self.storage), \
                patch("sys.argv", ["cleanup_download_center.py", "--staging-only", *flags]):
            cleanup.main()
        return log.getvalue()

    def test_dry_run_and_apply_preserve_current_and_unrelated_objects(self):
        self.assertIn("Dry run: eligible objects: 1", self.run_cleanup())
        self.assertEqual(set(self.objects), {self.expired, self.current, self.unrelated})
        self.assertIn("Deleted: 1", self.run_cleanup("--apply"))
        self.storage.delete_object.assert_called_once_with(Bucket=cleanup.BUCKET, Key=self.expired)
        self.assertEqual(set(self.objects), {self.current, self.unrelated})
        self.assertIn("Deleted: 0", self.run_cleanup("--apply"))

    def test_refuses_other_branches_and_missing_credentials_before_connecting(self):
        for name, value in [
            ("AWS_ENDPOINT_URL_S3", cleanup.STAGING_ENDPOINT.replace(cleanup.STAGING_BRANCH, "br-other")),
            ("AWS_ENDPOINT_URL_S3", cleanup.STAGING_ENDPOINT + "/another-path"),
            ("AWS_REGION", "us-east-1"),
            ("AWS_SECRET_ACCESS_KEY", ""),
        ]:
            with self.subTest(name=name), patch.dict(os.environ, {**self.env, name: value}, clear=True), \
                    patch.object(cleanup, "client") as connect, redirect_stderr(io.StringIO()), \
                    patch("sys.argv", ["cleanup_download_center.py", "--staging-only", "--apply"]):
                with self.assertRaises(SystemExit):
                    cleanup.main()
                connect.assert_not_called()
