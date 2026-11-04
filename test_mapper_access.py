"""Exercise the public landing page and workspace access boundary."""

from pathlib import Path
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from streamlit.testing.v1 import AppTest

import mapper_access

APP = Path(__file__).with_name("streamlit_app.py")
FIXTURE_KEY = "test-only-app-key-not-a-production-secret"


def sign_in(app, key=FIXTURE_KEY):
    app.text_input(key=mapper_access.KEY_INPUT).set_value(key)
    return next(button for button in app.button if button.label == "Sign in").click().run()


def start_authorized(app):
    app.secrets["APP_ACCESS_KEY"] = FIXTURE_KEY
    return sign_in(app.run())


class AppAccessTests(unittest.TestCase):
    def setUp(self):
        self.db = self.enterContext(patch("sqlalchemy.create_engine"))
        self.storage = Mock()
        if "boto3" in sys.modules:
            self.enterContext(patch.object(sys.modules["boto3"], "client", self.storage))
        else:
            sys.modules["boto3"] = SimpleNamespace(client=self.storage)
            self.addCleanup(sys.modules.pop, "boto3")
        self.app = AppTest.from_file(str(APP), default_timeout=30)
        self.app.secrets["APP_ACCESS_KEY"] = FIXTURE_KEY

    def assert_locked(self):
        self.assertFalse(self.app.exception)
        self.assertFalse(self.app.radio)
        self.assertFalse(self.app.dataframe)
        self.assertFalse(self.app.get("file_uploader"))
        self.assertFalse(self.app.get("download_button"))
        self.db.assert_not_called()
        self.storage.assert_not_called()

    def test_anonymous_navigation_and_missing_or_short_secret_fail_closed(self):
        for page in ("Import Center", "Dashboard", "Crosswalk Mapping", "UoM Conversion", "Download Center"):
            self.app.session_state["tow_navigation_0"] = page
            self.app.run()
            self.assert_locked()
        for config in ({}, {"APP_ACCESS_KEY": "short"}):
            self.app.secrets.clear()
            self.app.secrets.update(config)
            self.app.run()
            sign_in(self.app, config.get("APP_ACCESS_KEY", FIXTURE_KEY))
            self.assert_locked()

    def test_wrong_keys_trigger_cooldown_and_valid_key_works_afterward(self):
        self.app.session_state["tow_navigation_0"] = "Settings"
        self.app.run()
        with patch("mapper_access.monotonic", return_value=100):
            for _ in range(5):
                sign_in(self.app, "incorrect")
                self.assert_locked()
            self.assertTrue(self.app.warning)
            sign_in(self.app)
            self.assert_locked()
        with patch("mapper_access.monotonic", return_value=131):
            sign_in(self.app)
        self.assertFalse(self.app.exception)
        self.assertEqual(self.app.radio[0].value, "Settings")
        state = self.app.session_state
        if mapper_access.KEY_INPUT in state:
            self.assertEqual(state[mapper_access.KEY_INPUT], "")

    def test_logout_and_key_rotation_revoke_access(self):
        self.app.session_state["tow_navigation_0"] = "Settings"
        start_authorized(self.app)
        self.app.session_state["tow_retained_upload"] = ("test.csv", b"test")
        self.app.session_state["admin_unlocked"] = True
        next(b for b in self.app.button if b.label == "Logout").click().run()
        self.assert_locked()
        for key in (mapper_access.GRANT, "tow_retained_upload", "admin_unlocked"):
            self.assertNotIn(key, self.app.session_state)
        self.app.session_state["tow_navigation_1"] = "Settings"
        sign_in(self.app)
        self.app.secrets["APP_ACCESS_KEY"] = "a-different-test-only-app-key-after-rotation"
        self.app.run()
        self.assert_locked()
