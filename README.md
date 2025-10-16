# Gene Based Variant Filtering Workflow Updates
## Oct 2025
### Summary of Changes
1. **Delta Table Integration**
   - Several annotation datasets are now read directly from **Delta tables**, enabling better performance, scalability, and schema evolution:
     - **ClinVar**: Previously a tarred Parquet file; now a Delta table.
2. **HGMD update**
   - `2025Q1` → `2025Q2`
3. **Table Consequence**
   - Applied filter: `F.col('picked') == True` to reduce duplicated entries.
   - Additional logic implemented to handle remaining duplicate rows for the same variant in the same subject. Rows are now prioritized based on the mrna column, favoring entries with non-None values.