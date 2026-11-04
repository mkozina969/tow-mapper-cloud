"""Server-side access gate for the TOW Mapper Pro workspace."""

from hashlib import sha256
from hmac import compare_digest
from time import monotonic

GRANT = "tow_app_access_grant"
KEY_INPUT = "tow_app_access_input"
ATTEMPTS = "tow_app_access_attempts"
RETRY_AT = "tow_app_access_retry_at"
FAILED = "tow_app_access_failed"


def _expected(st):
    try:
        key = st.secrets.get("APP_ACCESS_KEY")
    except FileNotFoundError:
        return None
    if not isinstance(key, str) or len(key) < 32:
        return None
    return sha256(key.encode()).digest()


def has_app_access(st):
    expected = _expected(st)
    grant = st.session_state.get(GRANT)
    return expected is not None and isinstance(grant, bytes) and compare_digest(grant, expected)


def _submit(st):
    state = st.session_state
    supplied = state.get(KEY_INPUT, "")
    state[KEY_INPUT] = ""
    if monotonic() < state.get(RETRY_AT, 0):
        return
    expected = _expected(st)
    if expected is not None and compare_digest(sha256(supplied.encode()).digest(), expected):
        state[GRANT] = expected
        for key in (ATTEMPTS, RETRY_AT, FAILED):
            state.pop(key, None)
        return
    attempts = state.get(ATTEMPTS, 0) + 1
    state[ATTEMPTS] = attempts
    state[FAILED] = True
    if attempts >= 5:
        state[RETRY_AT] = monotonic() + 30
        state[ATTEMPTS] = 0


def render_access_form(st):
    st.write("Enter your application access key to open the workspace.")
    if _expected(st) is None:
        st.info("Workspace access is currently unavailable.")
    elif monotonic() < st.session_state.get(RETRY_AT, 0):
        st.warning("Too many attempts. Wait 30 seconds, then try again.")
    elif st.session_state.get(FAILED):
        st.error("Access key not accepted.")
    with st.form("tow_app_access"):
        st.text_input("Application access key", type="password", key=KEY_INPUT, max_chars=1024)
        st.form_submit_button("Sign in", on_click=_submit, args=(st,))
