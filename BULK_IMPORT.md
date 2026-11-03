# Import multiple items from XLSX or CSV

Unlock Admin with the existing PIN. Each of the two admin sections has its own **Bulk import** expander and downloadable CSV/XLSX templates.

## Supplier-to-TOW mappings

Required headers: `vendor_id`, `supplier_id`, `tow_code`.

## Conversion rules

Required headers: `vendor_id`, `supplier_id`, `divider`. Optional header: `active` (defaults to true when the column is omitted). Use true/false, yes/no or 1/0. Dividers must be positive integers. Vendor IDs are always required for conversion rules.

## Workflow

1. Download a template and replace the example row with your items. Keep identifiers as text in Excel, especially codes with leading zeros. XLSX uses the first worksheet; CSV must be UTF-8 with comma, semicolon or tab delimiters. Files must include headers and contain at most 10,000 rows.
2. Upload the file into the appropriate section. All rows are validated; errors show file row numbers and can be downloaded as CSV. Duplicate vendor/item keys within a file must be resolved before import.
3. Review the preview. Existing records are skipped by default. Select **Update existing records with values from this file** if you intend to replace the existing TOW code or conversion settings; current values appear alongside incoming values.
4. Click **Import … to Neon** to commit the validated file. One transaction covers the entire batch, so a database error rolls everything back. No records are deleted.
5. Rerun invoice mapping to use the updated database.

For mappings only, blank vendor IDs mean GLOBAL. Check the explicit GLOBAL option to allow these rows. Missing items from the uploaded file do not disable or delete database records.

Updating conversion rules can disable them by setting `active` to false. Omitting the active column while updating sets it to true.
