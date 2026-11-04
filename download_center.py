"""Private seven-day transfer of finished Tow Mapper exports."""
from functools import partial
from hmac import compare_digest
from threading import Event
from time import monotonic
from datetime import datetime, timedelta, timezone
import re
from urllib.parse import urlparse
from uuid import uuid4
from zoneinfo import ZoneInfo

BUCKET = "tow-mapper-exports"
PREFIX = "exports/"
RETENTION = timedelta(days=7)
MAX_BYTES = 25 * 1024 * 1024
EXPORT_STEM = r"[A-Za-z0-9_.-]{1,120}"
EXPORT_EXTENSIONS = r"xlsx|csv|prn"
EXPORT_FILENAME = rf"{EXPORT_STEM}\.(?:{EXPORT_EXTENSIONS})"
LEGACY_KEY = re.compile(rf"{PREFIX}[a-f0-9]{{32}}/(?P<filename>{EXPORT_FILENAME})")
FLAT_KEY = re.compile(rf"{PREFIX}(?P<stem>{EXPORT_STEM})_[0-9]{{8}}-[0-9]{{6}}_[a-f0-9]{{12}}\.(?P<extension>{EXPORT_EXTENSIONS})")
DOWNLOAD_GRANT = "tow_download_grant"
DELETE_GRANT = "tow_delete_grant"
FILE_LIST = "tow_download_metadata"
PAGE_SIZE = 25
MIME_TYPES = {
    "XLSX": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    "CSV": "text/csv",
    "PRN": "application/octet-stream",
}


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
    for page in storage.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=PREFIX):
        for obj in page.get("Contents", []):
            if original_filename(obj["Key"]) is not None:
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


def original_filename(key):
    """Recognize only the two object key layouts written by Tow Mapper."""
    if not isinstance(key, str):
        return None
    if match := LEGACY_KEY.fullmatch(key):
        return match["filename"]
    if match := FLAT_KEY.fullmatch(key):
        return f"{match['stem']}.{match['extension']}"
    return None


def _active(timestamp, now):
    return timestamp + RETENTION > now


def _require_export_key(key):
    if original_filename(key) is None:
        raise ValueError("Invalid export key.")


def read_export(storage, key, now=None):
    _require_export_key(key)
    clock = (lambda: now) if now is not None else (lambda: datetime.now(timezone.utc))
    obj = storage.get_object(Bucket=BUCKET, Key=key)
    with obj["Body"] as body:
        if not _active(obj["LastModified"], clock()):
            raise ValueError("This export has expired.")
        if not 0 < obj["ContentLength"] <= MAX_BYTES:
            raise ValueError("The export is empty or exceeds 25 MB.")
        data = body.read(MAX_BYTES + 1)
        if len(data) != obj["ContentLength"]:
            raise ValueError("The export could not be read in full.")
        if not _active(obj["LastModified"], clock()):
            raise ValueError("This export has expired.")
    return data


def delete_export(storage, key, now=None):
    _require_export_key(key)
    obj = storage.head_object(Bucket=BUCKET, Key=key)
    if not _active(obj["LastModified"], now or datetime.now(timezone.utc)):
        raise ValueError("This export has expired.")
    storage.delete_object(Bucket=BUCKET, Key=key)


def _valid_key(config, name, supplied):
    expected = config.get(name, "")
    other_names = ("upload_key",) if name == "download_key" else ("upload_key", "download_key")
    return (
        isinstance(expected, str)
        and len(expected) >= 24
        and isinstance(supplied, str)
        and compare_digest(expected.encode(), supplied.encode())
        and all(expected != config.get(other) for other in other_names)
    )


def _grant(st, state_key):
    grant = st.session_state.get(state_key)
    return grant if grant and not grant.is_set() else None


def revoke_download_access(st):
    for key in (DOWNLOAD_GRANT, DELETE_GRANT):
        grant = st.session_state.pop(key, None)
        if grant is not None:
            grant.set()
    st.session_state.pop(FILE_LIST, None)


def _read_client(config):
    access_id = config.get("read_access_key_id")
    secret = config.get("read_secret_access_key")
    if access_id is None and secret is None:
        return client(config)
    if not access_id or not secret:
        raise ValueError("Configure both read credential fields.")
    return client({
        "endpoint": config["endpoint"], "region": config["region"],
        "access_key_id": access_id, "secret_access_key": secret,
    })


def _metadata(st, config, refresh=False):
    if not _grant(st, DOWNLOAD_GRANT):
        raise PermissionError("Download access is required.")
    cached = st.session_state.get(FILE_LIST)
    if refresh or not cached or monotonic() >= cached[0]:
        objects = sorted(
            list_exports(_read_client(config)),
            key=lambda obj: (obj["LastModified"], obj["Key"]),
            reverse=True,
        )
        st.session_state[FILE_LIST] = cached = (monotonic() + 30, objects)
    now = datetime.now(timezone.utc)
    return [obj for obj in cached[1] if _active(obj["LastModified"], now)]


def filter_exports(objects, search="", file_type="All"):
    return [
        obj for obj in objects
        if search.strip().casefold() in obj["Key"].rsplit("/", 1)[-1].casefold()
        and (file_type == "All" or obj["Key"].rsplit(".", 1)[-1].upper() == file_type)
    ]


def _rows(objects):
    local_zone = ZoneInfo("Europe/Zagreb")
    now = datetime.now(timezone.utc)

    def display_time(value):
        return value.astimezone(local_zone).strftime("%d %b %Y, %H:%M %Z")

    rows = []
    for obj in objects:
        uploaded = obj["LastModified"]
        expiration = uploaded + RETENTION
        remaining = max(0, int((expiration - now).total_seconds()))
        days, hours = divmod(remaining // 3600, 24)
        rows.append({
            "File name": obj["Key"].rsplit("/", 1)[-1],
            "Type": obj["Key"].rsplit(".", 1)[-1].upper(),
            "Size": f"{obj['Size'] / 1024:,.1f} KB",
            "Uploaded (Zagreb)": display_time(uploaded),
            "Expires (Zagreb)": display_time(expiration),
            "Remaining": f"{days}d {hours}h" if days or hours else "<1h",
        })
    return rows


def _download(config, grant, key, listed_expiration):
    # Deferred download runs outside the Streamlit script thread. The session
    # grant is revoked in place so old buttons cannot fetch after a reset.
    if grant.is_set() or datetime.now(timezone.utc) >= listed_expiration:
        raise ValueError("This download is no longer available.")
    try:
        data = read_export(_read_client(config), key)
    except Exception:
        raise ValueError("The export is unavailable. Refresh the Download Center.") from None
    if grant.is_set():
        raise ValueError("This download is no longer available.")
    return data


def _unlock(st, config, name, state_key, label):
    with st.form(f"tow_unlock_{name}", clear_on_submit=True):
        supplied = st.text_input(label, type="password")
        submitted = st.form_submit_button("Unlock", type="primary")
    if submitted:
        if _valid_key(config, name, supplied):
            st.session_state[state_key] = Event()
            st.rerun()
        st.error("The access key was not accepted.")


def render_recent_exports(st):
    st.markdown("#### Recent stored exports")
    if not _enabled(st) or not _grant(st, DOWNLOAD_GRANT):
        st.caption("Unlock Download Center to see shared exports.")
        return
    try:
        rows = _rows(_metadata(st, st.secrets["download_center"])[:5])
        if rows:
            st.dataframe(rows, hide_index=True, use_container_width=True)
        else:
            st.caption("No unexpired shared exports.")
    except Exception:
        st.warning("Shared exports are temporarily unavailable.")


def render_download_center(st):
    st.title("Download Center")
    st.caption("Retrieve finished exports for NAV. Files are available for seven days.")
    if not _enabled(st):
        st.info("Shared storage is disabled for this application.")
        return
    config = st.secrets["download_center"]
    if not _grant(st, DOWNLOAD_GRANT):
        download_key = config.get("download_key")
        if not isinstance(download_key, str) or len(download_key) < 24:
            st.info("Download access has not been configured.")
            return
        _unlock(st, config, "download_key", DOWNLOAD_GRANT, "Download access key")
        return

    filters, controls = st.columns([3, 1])
    with filters:
        search = st.text_input("Search by filename", key="tow_download_search")
    with controls:
        file_type = st.selectbox("File type", ["All", *MIME_TYPES], key="tow_download_type")
    refresh = st.button("Refresh file list")
    st.button("Lock Download Center", on_click=revoke_download_access, args=(st,))
    try:
        objects = filter_exports(_metadata(st, config, refresh), search, file_type)
    except Exception:
        st.error("Could not list shared exports. Check the storage configuration or try refreshing.")
        return
    if not objects:
        st.info("No available exports match these filters.")
        return

    if len(objects) > PAGE_SIZE:
        pages = (len(objects) + PAGE_SIZE - 1) // PAGE_SIZE
        page = st.selectbox("Page", range(1, pages + 1))
        objects = objects[(page - 1) * PAGE_SIZE:page * PAGE_SIZE]
    rows = _rows(objects)
    st.dataframe(rows, hide_index=True, use_container_width=True)
    by_key = {obj["Key"]: (obj, row) for obj, row in zip(objects, rows)}
    key = st.selectbox(
        "Select an export", list(by_key), index=None,
        format_func=lambda value: (
            f"{by_key[value][1]['File name']} · {by_key[value][1]['Uploaded (Zagreb)']}"
        ),
    )
    if key is None:
        return
    obj, selected_row = by_key[key]
    name = original_filename(key)
    if 0 < obj["Size"] <= MAX_BYTES:
        st.download_button(
            "Download file", data=partial(_download, dict(config), _grant(st, DOWNLOAD_GRANT), key, obj["LastModified"] + RETENTION),
            file_name=name, mime=MIME_TYPES[selected_row["Type"]], on_click="ignore", type="primary",
        )
    else:
        st.warning("This export is empty or exceeds the download size limit.")

    delete_key = config.get("delete_key")
    if isinstance(delete_key, str) and len(delete_key) >= 24:
        with st.expander("Delete export"):
            if not _grant(st, DELETE_GRANT):
                _unlock(st, config, "delete_key", DELETE_GRANT, "Delete access key")
            else:
                st.caption(f"Selected file: {selected_row['File name']}")
                confirmed = st.checkbox("Confirm deletion of this file", key=f"tow_confirm_delete_{key}")
                requested = st.button("Delete file", key=f"tow_delete_{key}", disabled=not confirmed)
                if requested and confirmed and _grant(st, DELETE_GRANT):
                    try:
                        delete_export(client(config), key)
                    except Exception:
                        st.error("Could not delete this export. Refresh the list and check its availability.")
                    else:
                        st.session_state.pop(FILE_LIST, None)
                        st.toast("Export deleted.")
                        st.rerun()
