# TOW Mapper Pro Phase 5 release checklist

**Decision: NOT READY.** The staging preview and live file transfers passed.
The hourly cleanup job and deletion of an expired object in staging still
require live verification.

The [staging preview](https://tow-mapper-pro-preview.streamlit.app/) used the
synthetic DEMO invoice. Excel (5,414 bytes), Pallets PRN (204 bytes), Regular
PRN (84 bytes), and Portal CSV (114 bytes) were saved to staging Neon and
retrieved through Download Center. Each retrieved copy matched its original
export's SHA-256 hash.

## Isolated staging and manual Streamlit deployment

Neon project **tow-mapper-pro-staging** (`patient-wave-61923224`) has a
`staging` branch, a private `tow-mapper-exports` bucket, and synthetic
database rows. The branch ID is `br-delicate-truth-b9ptbns0`. Its test vendor
`DEMO` has an item `DEMO-782` mapped to `00042` with divider 6. Item
`DEMO-GLOBAL` resolves through the GLOBAL crosswalk to `00043`.

1. Sign in to [Streamlit Community Cloud](https://share.streamlit.io/) with
   the GitHub account that owns the app. Select **Create app** and **Deploy
   a public app from GitHub** or the corresponding GitHub deployment option.
   Choose a new URL, for example `tow-mapper-pro-staging.streamlit.app` if
   available. Use repository `mkozina969/tow-mapper-cloud`, branch
   `feature/tow-mapper-pro-ui`, and file `streamlit_app.py`.
2. In [the staging Neon project](https://console.neon.tech/app/projects/patient-wave-61923224),
   use **Connect → Postgres database** on the **staging** branch for the staging
   connection string. Use **Object storage → Connect → Storage** for its S3
   endpoint and region. Create a branch-scoped `storage:write` credential for
   shared uploads and a separate `storage:read` credential for listing and
   downloads. Use a separate staging credential for the cleanup job.
3. In Streamlit **Advanced settings → Secrets**, fill in the
   [Phase 4 TOML template](PHASE4_PREVIEW.md). Verify `DATABASE_URL` targets
   staging, `endpoint` comes from the staging Storage panel, `region` is the
   staging region, and storage credentials are staging-only. Set `enabled`
   to true. Generate distinct random upload, download, and optional delete
   keys of at least 24 characters. Use a separate staging admin PIN.
   Paste values directly into Secrets. Select a supported Python version
   compatible with the requirements, such as 3.13. Click **Deploy**.
4. Confirm the new URL opens **Import Center** and the staging crosswalk loads.
   Limit preview access to testers where available. Use synthetic data in
   staging even if the preview is accessible publicly.

[Streamlit deployment instructions](https://docs.streamlit.io/deploy/streamlit-community-cloud/deploy-your-app/deploy)
· [Neon storage credential scopes](https://neon.com/docs/storage/authentication)

## Verify the real staging transfer

Save the following as `phase5-synthetic-invoice.csv`.

```csv
ProductNumber,Quantity,UnitPrice,Pallet
DEMO-782,24,1.50,PAL0001
DEMO-GLOBAL,3,2.00,PAL0002
```

1. In preview Import Center, select `DEMO`, upload the file, and select
   `ProductNumber`, `Quantity`, and `UnitPrice` as the matching and conversion
   columns. Run mapping. Confirm two matched rows. The first should be
   TOW `00042`, quantity 4, unit price 9.00, line value 36.00. The second
   should use TOW `00043`, quantity 3, and unit price 2.00.
2. In **Save exports to Neon storage**, enter the staging upload key and check
   **Save a shared copy**. Download the Excel result. For both PRNs, apply
   `tow`, `Quantity`, `Pallet` in that order and download each format.
   For Portal CSV, set an invoice number such as `PHASE5-DEMO`, enable
   **PortalBlank**, set a manual date, and apply the columns in this order:
   `PortalInvoiceNo`, `Invoice`, `Item`, `PortalBlank`, `tow`, `Quantity`,
   `UnitPrice`, `date`, `Location`, `vendor_id`. Download the Portal CSV.
3. In a separate browser profile or on the remote ERP computer, open the
   same preview URL. Verify the upload key is refused by Download Center.
   Unlock with the download key and retrieve each export. Compare the SHA-256
   of each local and remote copy. All pairs should match.

   ```powershell
   Get-FileHash -Algorithm SHA256 -LiteralPath 'C:\path\to\export.xlsx'
   ```

4. Confirm the download key is rejected by the admin PIN gate and by the
   delete gate. If deletion is configured, delete an expendable **staging**
   export using the separate key and confirmation. Check it disappears from
   the list and staging bucket. Reset the session and confirm both
   download and admin access must be unlocked again.

## GitHub Actions cleanup

The staging workflow runs at minute 7 UTC each hour once enabled. It pins
the Neon staging branch endpoint and region before connecting and reuses
the existing seven-day retention script. Its logs show the branch, mode and
counts without credentials or filenames.

1. In Neon, open **tow-mapper-pro-staging → staging → Object storage →
   Connect → Storage** and create a dedicated branch-scoped storage write key.
2. In GitHub, open **Settings → Environments → New environment** and create
   `neon-staging-cleanup`. Under **Deployment branches and tags**, choose
   **Selected branches and tags** and allow `feature/tow-mapper-pro-ui`.
   Add `main` only after approval. Under **Environment secrets**, add
   `NEON_STAGING_STORAGE_ACCESS_KEY_ID` and
   `NEON_STAGING_STORAGE_SECRET_ACCESS_KEY` from Neon. Under **Environment
   variables**, set `NEON_STAGING_STORAGE_ENDPOINT` to
   `https://br-delicate-truth-b9ptbns0.storage.c-7.eu-central-1.aws.neon.tech`.
3. A feature-branch push runs credential-free tests with expired, current,
   and unrelated object fixtures. GitHub requires both scheduled and
   manually dispatched workflows to exist on the default branch. Keep
   `main` as default.
4. After approval and merge, open **Actions → Staging Neon export cleanup →
   Run workflow** and choose **dry-run**. Record the exact keys of an expired
   synthetic export, a current export, and an unrelated object. Age is based
   on Neon LastModified, so allow seven days from upload. Verify dry run
   leaves all three present. Run again with **apply**, then verify only the
   expired key disappears. Save the run URLs and before/after bucket listings.
5. To activate hourly staging cleanup, open **Settings → Secrets and
   variables → Actions → Variables** and add repository variable
   `ENABLE_STAGING_EXPORT_CLEANUP` with value `true`. Check two hourly runs.
   Remove the variable or disable the workflow to stop hourly cleanup.
   This workflow remains pinned to staging after merge.

[GitHub Actions trigger rules](https://docs.github.com/en/actions/reference/workflows-and-actions/events-that-trigger-workflows)

## Validation and limitations

- Full local suite: 31 passed. The legacy admin test was updated to navigate
  to both dedicated admin sections.
- Local browser tests from the Phase 4 implementation transferred an
  identical Excel file across two independent sessions and retrieved an
  identical PRN. The CSV formatter passed its automated tests. Real Neon
  browser transfers for all formats remain pending.
- Excel output matched main for worksheets, values, types, formatting, and
  leading zeros. Quantity and price conversion, packing-list conversion,
  crosswalk fallback, bulk admin imports, navigation, deletion permissions,
  and session reset passed local tests.
- In one local run, parsing, mapping and UoM conversion took 1.66 s for
  10,000 rows (240 KB) and 9.12 s for 50,000 rows (1.20 MB). XLSX generation
  took 1.26 s on main versus 1.33 s on feature for 10,000 rows, and 7.87 s
  versus 8.58 s for 50,000 rows. Both branches use identical workbook
  generation code. This was a single sequential sample, and network,
  Cloud memory pressure, and production-scale GLOBAL lookup were outside it.
- The public repo exposes code and configuration examples. A filename and
  pattern scan found no committed local secrets files or credential
  patterns in reachable history. This was a targeted scan, not a full
  secret audit. Keep data, endpoint credentials and configuration in
  managed secrets. Consider a private repo with restricted collaborators
  and secret scanning before production. Shared app keys provide no
  individual audit trail. Streamlit serves a download after authorization,
  and bytes already received by a browser remain with that browser.
- Desktop and laptop layouts use the existing Streamlit shell. On small
  screens, the wide tables and export configuration may require horizontal
  scrolling. Complete the production acceptance test on desktop or laptop.

## Merge and rollback after approval

1. Confirm the real staging transfer and hourly cleanup evidence above.
   Record the tested feature commit, current main commit, and production
   Streamlit configuration in the release record. Keep a secure copy of
   the current production Secrets in the approved secrets store.
2. Open a PR from `feature/tow-mapper-pro-ui` into `main` and review the
   complete diff. Obtain explicit production approval before merging. Squash
   merge so rollback has one release commit. If the current production
   Streamlit deployment tracks `main`, merging can start an automatic
   redeploy immediately. Plan the merge and secure settings update in the
   approved change window.
3. Set production-only Streamlit Secrets in the live app, using a separate
   download key and read credential. Set an optional separate delete key
   only if deletion is approved. Verify the app builds and test the
   approved production smoke workflow. Keep the staging app separate.
4. To roll back, revert the squash commit on `main` through a new PR,
   restore the previous production Secrets if changed, and verify the live
   app has redeployed from the revert. Stop any newly enabled production
   cleanup schedule if required by the rollback plan. Reverting code does
   not restore any objects already deleted by cleanup.

Phase 5 changes are confined to this guide, the legacy test, and Git ignore
rules for local environment files.
