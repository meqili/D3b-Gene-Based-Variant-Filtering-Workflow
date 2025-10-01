# Gene Based Variant Filtering Workflow Updates
## Oct 2025
### Summary of Changes
**Delta Table Integration**
   - Several annotation datasets are now read directly from **Delta tables**, enabling better performance, scalability, and schema evolution:
     - **ClinVar**: Previously a tarred Parquet file; now a Delta table.