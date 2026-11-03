# Packaging conversion

1. Deploy this branch and run `migrations/001_item_uom_conversion.sql` against the same Neon database as the app. Alternatively unlock Admin and click **Initialize UoM table**. Mapping fails closed until the table exists.
2. In Admin → UoM Conversion, enter the exact vendor ID, supplier item code, integer divider and active status. Save the same keys to edit or disable a rule.
3. Select the vendor, upload the original invoice and select supplier, quantity and unit price columns. Click Run mapping. Active exact-vendor rules divide quantity and multiply price; no matching rule leaves the row unchanged. Existing mapping/export columns are preserved.
4. Review the conversion audit and export normally. Invalid numbers or non-divisible quantities block the batch. Correct the invoice or rule and rerun.

Calculations and line-value checks use Decimal. Excel exports numeric cells using Excel's usual floating-point precision. Source uploads are never modified; every rerun starts from the source. Exported files must not be uploaded as original supplier invoices, since their unchanged column format cannot identify prior conversion.

Example: 12 PCS at 2.50 with divider 3 becomes 4 SET at 7.50; total remains 30.00. No example rules are inserted automatically.

Validation: `python -m unittest test_uom_conversion -v`.
