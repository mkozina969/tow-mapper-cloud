"""Private seven-day transfer of finished Tow Mapper exports."""
from datetime import datetime, timedelta, timezone
import re
from urllib.parse import urlparse
from uuid import uuid4

BUCKET = "tow-mapper-exports"
PREFIX = "exports/"
RETENTION = timedelta(days=7)
MAX_BYTES = 25 * 1024 * 1024


def authorized(claims, tenant_id, allowed_ids):
    try:
        return (isinstance(allowed_ids, (list, tuple)) and bool(tenant_id) and claims.get("tid") == tenant_id
                and bool(claims.get("oid")) and claims["oid"] in allowed_ids
                and float(claims.get("exp", 0)) > datetime.now(timezone.utc).timestamp())
    except (TypeError, ValueError):
        return False


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
    if not re.fullmatch(r"[A-Za-z0-9_.-]{1,120}\.(xlsx|csv)", filename) or not 0 < len(data) <= MAX_BYTES:
        raise ValueError("Only XLSX and CSV exports up to 25 MB can be shared.")
    key = f"{PREFIX}{uuid4().hex}/{filename}"
    storage.put_object(Bucket=BUCKET, Key=key, Body=data, ContentType=mime, CacheControl="private, no-store")
    return key


def list_exports(storage, now=None, expired=False):
    cutoff = (now or datetime.now(timezone.utc)) - RETENTION
    for page in storage.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=PREFIX):
        for obj in page.get("Contents", []):
            if re.fullmatch(r"exports/[a-f0-9]{32}/[A-Za-z0-9_.-]{1,120}\.(xlsx|csv)", obj["Key"]):
                if (obj["LastModified"] <= cutoff) == expired:
                    yield obj


def _enabled(st):
    try:
        return st.secrets.get("download_center", {}).get("enabled", False) is True
    except FileNotFoundError:
        return False


def _access(st):
    config = st.secrets["download_center"]
    claims = st.user.to_dict() if st.user.is_logged_in else {}
    return authorized(claims, config.get("tenant_id"), config.get("allowed_user_ids"))


def export_callback(filename, data, mime):
    import streamlit as st
    if not _enabled(st) or not _access(st) or not st.session_state.get("share_finished_exports", False):
        return "rerun"

    def save():
        if not _enabled(st) or not _access(st):
            return
        try:
            save_export(client(st.secrets["download_center"]), filename, data, mime)
            st.toast("Saved to Download Center for seven days.")
        except Exception:
            st.error("Could not save the shared copy. Your local download is still available.")
    return save


def render_download_center():
    import streamlit as st
    if not _enabled(st):
        return
    with st.expander("Download Center"):
        if not st.user.is_logged_in:
            if st.button("Sign in with company Microsoft account"):
                st.login("microsoft")
            return
        if st.button("Sign out of Download Center"):
            st.logout()
            return
        if not _access(st):
            st.warning("This Microsoft account is not authorized for the Download Center. Sign out and use an approved account.")
            return
        st.checkbox("Save a shared copy when I download an Excel or Portal CSV export", key="share_finished_exports")
        try:
            storage = client(st.secrets["download_center"])
            items = sorted(list_exports(storage), key=lambda row: row["LastModified"], reverse=True)
            if not items:
                st.info("No shared exports are available.")
                return
            choices = {obj["Key"]: obj for obj in items}
            key = st.selectbox("Shared exports", list(choices), format_func=lambda k: f"{k.rsplit('/', 1)[-1]} - expires {(choices[k]['LastModified'] + RETENTION):%Y-%m-%d %H:%M UTC}")
            if st.button("Prepare download") and _access(st) and choices[key]["LastModified"] + RETENTION > datetime.now(timezone.utc):
                url = storage.generate_presigned_url("get_object", Params={"Bucket": BUCKET, "Key": key, "ResponseContentDisposition": f'attachment; filename="{key.rsplit("/", 1)[-1]}"'}, ExpiresIn=min(60, max(1, int(((choices[key]["LastModified"] + RETENTION) - datetime.now(timezone.utc)).total_seconds()))))
                st.link_button("Download shared file (link valid up to 1 minute)", url)
        except Exception:
            st.error("The Download Center is temporarily unavailable.")
