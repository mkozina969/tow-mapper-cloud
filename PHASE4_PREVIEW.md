# Tow Mapper Pro preview and export retention

Deploy **a separate Streamlit Community Cloud app** using
`mkozina969/tow-mapper-cloud`, branch `feature/tow-mapper-pro-ui`, and entry
point `streamlit_app.py`. Choose a separate app URL. Use a staging database
and a staging Neon storage endpoint when exercising uploads or deletion.
The app expects a private bucket called `tow-mapper-exports` on the branch
identified by the endpoint. It does not provision buckets.

In the preview app settings, open **Secrets** and retain the app's usual
database and admin settings. Add the section below. Replace placeholders
only in the Streamlit Secrets editor or an ignored local
`.streamlit/secrets.toml` file.

```toml
DATABASE_URL = "STAGING_POSTGRES_CONNECTION_STRING"
ADMIN_PIN = "STAGING_ADMIN_PIN"

[download_center]
enabled = true
endpoint = "HTTPS_STAGING_NEON_S3_ENDPOINT"
region = "STAGING_NEON_REGION"
access_key_id = "STAGING_STORAGE_WRITE_TOKEN_ID"
secret_access_key = "STAGING_STORAGE_WRITE_SECRET"
upload_key = "REPLACE_WITH_RANDOM_UPLOAD_KEY"
download_key = "REPLACE_WITH_DISTINCT_RANDOM_DOWNLOAD_KEY"

# Optional, recommended for listing and retrieval. Supply both fields together.
read_access_key_id = "STAGING_STORAGE_READ_TOKEN_ID"
read_secret_access_key = "STAGING_STORAGE_READ_SECRET"

# Optional. Omit to disable in-app deletion.
delete_key = "REPLACE_WITH_DISTINCT_RANDOM_DELETE_KEY"
```

Generate three distinct random keys with at least 24 characters each.
The upload key enables the existing save-copy workflow. The download key
unlocks shared filenames and retrieval for that browser session. To delete,
unlock Download Center, unlock deletion separately, select the exact file,
and confirm it. Reset session revokes both access grants.

A separate Neon `storage:read` credential is recommended for browsing and
download. Omitting both read fields uses the existing storage credential.
Upload and confirmed deletion use the existing `storage:write` credential
because Neon groups PutObject and DeleteObject in that scope. The app
restricts both user-facing actions to recognized export keys. Credentials
stay on the server, and the bucket stays private. Streamlit transfers
downloads through its own browser session controls. The app does not
generate S3 URLs. Shared keys do not identify individual users.

On the local computer, run the usual export with **Save a shared copy**
enabled. On the remote ERP computer, open the same preview URL in the browser,
open **Download Center**, and enter the download key. Choose the file and
click **Download file**. The downloaded bytes match the stored object.
Timestamps display in Europe/Zagreb. Supplier is omitted because the
existing object records do not contain reliable supplier metadata.

## Schedule seven-day cleanup

The app hides expired objects and checks their age again on download.
The existing cleanup script has a dry-run default. Scheduling its
`--apply` mode is a separate operational task. Neon reports object deletion
on the current branch as soft deletion.

On an always-on Linux host with cron:

1. Check out the desired branch and create a Python virtual environment
   in the checkout. Install dependencies with
   `./.venv/bin/python -m pip install -r requirements.txt`.
2. Create `/etc/tow-mapper/cleanup.env` readable only by the scheduler
   account. Put shell assignments in it for `AWS_ENDPOINT_URL_S3`,
   `AWS_REGION`, `AWS_ACCESS_KEY_ID`, and `AWS_SECRET_ACCESS_KEY`.
   Use a Neon `storage:write` credential scoped to the intended branch.
3. Run the following as that account from the checkout. It reports eligible
   objects without removing them.

   ```sh
   set -a
   . /etc/tow-mapper/cleanup.env
   set +a
   ./.venv/bin/python cleanup_download_center.py
   ```

4. In that account's crontab, add this line after replacing the checkout
   and log paths with absolute paths accessible to the account.

   ```cron
   5 * * * * /bin/sh -c 'set -a; . /etc/tow-mapper/cleanup.env || exit 1; set +a; cd /opt/tow-mapper-cloud && ./.venv/bin/python cleanup_download_center.py --apply' >> /var/log/tow-mapper-cleanup.log 2>&1
   ```

5. Confirm the first hourly run in the log and verify the scheduler's exit
   status. Configure each branch's scheduler with its own endpoint and
   credentials. Keep secrets out of the crontab and logs.
