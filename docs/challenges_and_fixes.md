# Challenges and Fixes

## 1. DBFS to Unity Catalog Volumes Migration

The original course material used legacy DBFS paths like:
    dbfs:/FileStore/GlobalRetail/bronze_layer/customer_data/customer.csv

Current Databricks environments use Unity Catalog Volumes:
    /Volumes/main/globalretail/raw/customer.csv

**Fix:** Replaced all hardcoded DBFS paths with configurable paths
via environment variables (GR_RAW_ROOT, GR_ARCHIVE_ROOT) in
src/global_retail/config.py. The pipeline now works on both
legacy DBFS and Unity Catalog without code changes.

## 2. Archive Path Timestamp Bug

The original notebook archive logic used:
    datetime.datetime.now().strftime("%Y%m%d%H%M%s")

The lowercase %s is Unix epoch seconds, not zero-padded seconds.
This produces paths like 20240405143017126543 instead of the
intended 20240405143017.

**Fix:** Corrected to %S (uppercase) for zero-padded seconds in
the archive filename generation.

## 3. Transaction File Path Mismatch

The notebook referenced transaction_snappy.parquet but the actual
file was named transaction.snappy.parquet.

**Fix:** Standardized on transaction.snappy.parquet in config.py
with a configurable override.

## 4. Gold Layer Query Gaps

The original notebook gold layer only computed SUM(total_amount).
The refactored src/ version adds order counts, units sold, and
average order value for richer reporting.

## 5. Incremental Load Pattern

The notebooks had watermark logic duplicated across every silver
notebook. Refactored into a single get_last_watermark() utility
in src/global_retail/utils.py.