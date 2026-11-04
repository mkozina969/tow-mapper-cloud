# Separate TOW Mapper Pro production launch

Deploy `mkozina969/tow-mapper-cloud`, branch `feature/tow-mapper-pro-ui`,
entry point `streamlit_app.py`. Verify the feature branch tip matches the
approved access-gate commit before deploying. Use Python 3.13 and request
the subdomain `tow-mapper-pro`. Confirm the revision and URL at launch.
Deploying from this public repository makes the app public by default.
Stop before pressing Deploy until publication is explicitly approved.

In the new app's **Advanced settings → Secrets**, enter the configuration
below. Use the connection string for the database that contains the existing
crosswalk on `towmapper-aws → production`. Use a dedicated read-only login
where one already exists for the initial verification. A read-only login
supports mapping and searches. Admin writes require a separately authorized
login with appropriate privileges. Use a new Admin PIN and distinct,
random upload and download keys of at least 24 characters. Create dedicated
production storage credentials in Neon, one storage write and one storage
read, scoped to the production branch. Paste values directly into Streamlit.

```toml
DATABASE_URL = "PRODUCTION_DATABASE_CONNECTION_STRING"
ADMIN_PIN = "NEW_PRO_ADMIN_PIN"
APP_ACCESS_KEY = "NEW_RANDOM_APP_KEY_AT_LEAST_32_CHARACTERS"

[download_center]
enabled = true
endpoint = "https://br-ancient-rice-b21pya8w.storage.c-6.eu-central-1.aws.neon.tech"
region = "eu-central-1"
access_key_id = "PRO_STORAGE_WRITE_ACCESS_KEY_ID"
secret_access_key = "PRO_STORAGE_WRITE_SECRET"
read_access_key_id = "PRO_STORAGE_READ_ACCESS_KEY_ID"
read_secret_access_key = "PRO_STORAGE_READ_SECRET"
upload_key = "NEW_PRO_UPLOAD_KEY"
download_key = "NEW_PRO_DOWNLOAD_KEY"
```

Generate the application key in a password manager with at least 32 random
characters. Use a different value for each access layer. Enter the key only
in the new app's Streamlit Secrets. The workspace stays locked if the key is
missing or too short. Five incorrect attempts pause submissions in that
browser session for 30 seconds. Logout and Reset session clear workspace
files, results, and access grants. Changing the configured key invalidates
existing grants on the next rerun after the new secret takes effect.

The tested branch uses the existing private production bucket
`tow-mapper-exports` and its `exports/` prefix. Both apps can use that same
storage. Separate credentials and app keys do not partition the files.
Omit `delete_key` to keep in-app deletion off. Leave
`ENABLE_STAGING_EXPORT_CLEANUP` unset and leave production scheduling off.
The app still enforces seven-day eligibility at listing and download time.

After launch, verify the URL and revision. Open a fresh browser session and
confirm it shows only the branded sign-in page, including when a protected
page is requested directly. Verify a wrong key is rejected, the correct key
opens the workspace, and Logout and Reset session return to sign-in.
Use an approved synthetic invoice
with known supplier and GLOBAL matches and an existing UoM rule. Compare the
Excel, Pallets PRN, Regular PRN, and Portal CSV output to the staging baseline.
Verify the Admin PIN gate without submitting changes. Verify that Download
Center hides metadata when locked, rejects the upload key, opens with the
download key, and relocks after session reset. Use a synthetic shared copy
for storage verification. Verify expiry with an already aged synthetic
fixture where available and record any checks that remain pending.

To roll back, remove access to the new app in **App settings → Sharing** or
delete the new app from its own management menu. Verify the app name and URL
before using either control. Revoke only the dedicated credentials issued
for Pro if retiring it. The original Streamlit app is the fallback.
