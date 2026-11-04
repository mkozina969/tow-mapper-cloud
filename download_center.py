"""Private seven-day transfer of finished Tow Mapper exports."""
from hmac import compare_digest
from datetime import datetime, timedelta, timezone
import re
from urllib.parse import urlparse
from uuid import uuid4

BUCKET = "tow-mapper-exports"
PREFIX = "exports/"
RETENTION = timedelta(days=7)
MAX_BYTES = 25 * 1024 * 1024
EXPORT_FILENAME = r"[A-Za-z0-9_.-]{1,120}\.(?:xlsx|csv|prn)"


def client(config):
    import boto3
    from botocore.config import Config
    endpoint = config["endpoint"]
    parsed = urlparse(endpoint)
    if parsed.scheme != "https" or not parsed.hostname or not parsed.hostname.endswith(".aws.neon.tech") or parsed.username or parsed.password:
        raise ValueError("Configure the HTTPS Neon storage endpoint.")
    return boto3.client("s3", endpoint_url=endpoint, region_name=config["region"],
                        aws_access_key_id=config["access_key_id"], aws_secret_access_key=config["secret_access_key"],
                        config=Config(signature_version="s3v4", s3={"addressing_style": "path"},
                                      request_checksum_calculation="when_required", response_checksum_validation="when_required"))


def save_export(storage, filename, data, mime):
    if not re.fullmatch(EXPORT_FILENAME, filename) or not 0 < len(data) <= MAX_BYTES:
        raise ValueError("Only XLSX, CSV, and PRN exports up to 25 MB can be shared.")
    stem, extension = filename.rsplit(".", 1)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    key = f"{PREFIX}{stem}_{timestamp}_{uuid4().hex[:12]}.{extension}"
    storage.put_object(Bucket=BUCKET, Key=key, Body=data, ContentType=mime, CacheControl="private, no-store")
    return key


def list_exports(storage, now=None, expired=False):
    cutoff = (now or datetime.now(timezone.utc)) - RETENTION
    legacy_key = rf"{PREFIX}[a-f0-9]{{32}}/{EXPORT_FILENAME}"
    flat_key = rf"{PREFIX}[A-Za-z0-9_.-]{{1,120}}_[0-9]{{8}}-[0-9]{{6}}_[a-f0-9]{{12}}\.(?:xlsx|csv|prn)"
    for page in storage.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=PREFIX):
        for obj in page.get("Contents", []):
            if re.fullmatch(rf"(?:{legacy_key}|{flat_key})", obj["Key"]):
                if (obj["LastModified"] <= cutoff) == expired:
                    yield obj


def _enabled(st):
    try:
        return st.secrets.get("download_center", {}).get("enabled", False) is True
    except FileNotFoundError:
        return False


def _access(st):
    expected = st.secrets["download_center"].get("upload_key", "")
    provided = st.session_state.get("shared_upload_key", "")
    return (isinstance(expected, str) and len(expected) >= 24
            and isinstance(provided, str)
            and compare_digest(expected.encode(), provided.encode()))


def export_callback(filename, data, mime):
    import streamlit as st
    if not _enabled(st) or not _access(st) or not st.session_state.get("share_finished_exports", False):
        return None

    def save():
        if not _enabled(st) or not _access(st) or not st.session_state.get("share_finished_exports", False):
            return
        try:
            key = save_export(client(st.secrets["download_center"]), filename, data, mime)
            st.session_state["last_shared_export"] = key
            st.toast("Saved to private Neon storage.")
        except Exception:
            st.error("Could not save the shared copy. Your local download is still available.")
    return save


def render_shared_storage():
    import streamlit as st
    if not _enabled(st):
        return
    with st.expander("Save exports to Neon storage"):
        st.text_input("Team upload key", type="password", key="shared_upload_key")
        if not _access(st):
            st.caption("Enter the team upload key to enable shared exports.")
            return
        st.checkbox("Save a shared copy when I download a mapping export", key="share_finished_exports")
        st.caption("Retrieve shared files from the private tow-mapper-exports bucket using your Neon storage access.")
        if st.session_state.get("last_shared_export"):
            st.code(st.session_state["last_shared_export"], language=None)
