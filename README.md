# The problem
Azure Data Factory recently released the new Snowflake connector, with an [streamlined upgrade process](https://learn.microsoft.com/en-us/azure/data-factory/connector-snowflake?tabs=data-factory#upgrade-the-snowflake-linked-service).

However, after changing to the new connector, [it has been reported that lookup activities randomly output nothing](https://stackoverflow.com/questions/79013977/adf-lookup-activity-sometimes-not-returning-value-after-changing-to-the-new-snow).

# The solution
Script activities, however, are more reliable and don't produce empty outputs as they inject raw SQL in Snowflake. So a potential solution is to replace every lookup activity by an equivalent script activity that has the exact same functionality.

To do so, we resolved to incrementally process every pipeline in our ADF repository by capturing JSONs, swapping lookups by scripts and modifying every mention in the pipeline related to the old lookup activity, including:
- Lookup output being passed as a list (e.g. to iterables)
- Lookup output being indexed (e.g. to access specific output rows)
- Lookup output being limited to firstRow (e.g. when calculating deltas using a MAX function)

# Usage
1. Locate the script into a new folder at the root of the ADF repository, at the same level than "pipeline" folder.
2. Pass the pipeline's names to be processed in a .txt located in the new folder with the format `<pipeline_name>.json` (no double-quotes).
3. Execute the script.
4. Incrementally add new pipelines to the .txt (the script handles duplicates).
