# Download Center setup

The private `tow-mapper-exports` bucket is on the `towmapper-aws` production branch. The feature remains hidden until `[download_center].enabled` is set to true. Configure sign-in and verify cleanup before enabling it.

1. Have your Microsoft Entra administrator register a single-tenant web application with redirect URI `https://tow-mapper-cloud-9rwtfeavbpeqj8cjyycw5c.streamlit.app/oauth2callback`. Record the tenant ID, app client ID, and the object IDs of approved users in that tenant. Create an app client secret.
2. Set up a Neon storage credential for the production branch. Enter the credentials directly in Streamlit Secrets; never commit them to GitHub or paste them into chat. Preserve the existing database and Admin settings.
3. Add the following sections to Streamlit Secrets. Replace placeholders and keep `enabled = false` until the cleanup job is ready. Use a random cookie secret of at least 32 bytes.

```toml
[auth]
redirect_uri = "https://tow-mapper-cloud-9rwtfeavbpeqj8cjyycw5c.streamlit.app/oauth2callback"
cookie_secret = "REPLACE_WITH_RANDOM_SECRET"

[auth.microsoft]
client_id = "ENTRA_APP_CLIENT_ID"
client_secret = "ENTRA_APP_CLIENT_SECRET"
server_metadata_url = "https://login.microsoftonline.com/TENANT_ID/v2.0/.well-known/openid-configuration"

[download_center]
enabled = false
tenant_id = "ENTRA_TENANT_ID"
allowed_user_ids = ["APPROVED_ENTRA_USER_OBJECT_ID"]
endpoint = "https://br-ancient-rice-b21pya8w.storage.c-6.eu-central-1.aws.neon.tech"
region = "eu-central-1"
access_key_id = "NEON_STORAGE_ACCESS_KEY_ID"
secret_access_key = "NEON_STORAGE_SECRET_ACCESS_KEY"
```

4. Configure an hourly job on a trusted scheduler with `AWS_ENDPOINT_URL_S3`, `AWS_REGION`, `AWS_ACCESS_KEY_ID`, and `AWS_SECRET_ACCESS_KEY` injected securely. Run `python cleanup_download_center.py` first to verify access without deleting anything; configure `python cleanup_download_center.py --apply` as the recurring command. Use a separate scheduler credential if available. The job touches only files in this bucket under the app's export prefix. Neon does not enforce S3 lifecycle expiry; the scheduled job performs the storage deletion operation. Neon documents this operation as a soft delete on the current branch.
5. Enable the center. Test with a synthetic invoice: sign in as an approved user, opt in to save a shared copy, and click an Excel or Portal CSV download. On the ERP computer, sign in, open Download Center, select the file and prepare a short-lived download link. Verify that an unapproved user cannot list or download files. The file disappears from the center after seven days; an hourly cleanup removes it from the live bucket on the next run.
