# Transfer exports through private Neon storage

No Microsoft registration or Streamlit sign-in integration is needed. The storage feature stays hidden until enabled.

1. Create a production-branch Neon credential with `storage:write` for Streamlit. Keep it server-side. Generate a random team upload key of at least 24 characters and distribute it only to authorized staff.
2. Add this section in Streamlit Secrets, preserving existing settings. Keep it disabled until cleanup is configured.

```toml
[download_center]
enabled = false
upload_key = "REPLACE_WITH_RANDOM_TEAM_UPLOAD_KEY"
endpoint = "https://br-ancient-rice-b21pya8w.storage.c-6.eu-central-1.aws.neon.tech"
region = "eu-central-1"
access_key_id = "NEON_STORAGE_TOKEN_ID"
secret_access_key = "NEON_STORAGE_S3_SECRET_ACCESS_KEY"
```

3. Configure an hourly scheduler with the four storage environment variables `AWS_ENDPOINT_URL_S3`, `AWS_REGION`, `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`. Use a separate write-scoped credential if available. Run `python cleanup_download_center.py` to dry-run, then schedule `python cleanup_download_center.py --apply`. Neon does not enforce S3 lifecycle expiry. The job deletes expired app exports from the live bucket on the next hourly run after seven days. Neon describes its delete operation as a soft delete on the current branch.
4. Enable storage. In Streamlit, open **Save exports to Neon storage**, enter the team upload key, and select the shared-copy option. Download an Excel or Portal CSV file. The app shows the stored object path after a successful save.
5. On the ERP computer, browse the private `tow-mapper-exports` bucket under `exports/` and download the saved object. Use the Neon Console if your existing project permissions support object downloads. Otherwise, configure an S3-compatible client with path-style addressing, the endpoint above, and a separately issued `storage:read` credential. Do not give staff the Streamlit write credential.

Read credentials cover storage on the anchor branch and its descendants, not a single bucket. They do not grant database access. Check which buckets are on the branch before distributing them. Validate the flow with a synthetic export first. Do not commit or paste credentials into chat.
