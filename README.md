# Global Retail — Databricks Medallion Pipeline

This project is a full retail data engineering pipeline that I built on Databricks to practice modern lakehouse patterns end to end. It starts from raw customer, product and transaction files in CSV, JSON and Parquet format, loads them into Delta Lake using the Bronze Silver Gold medallion architecture, and ends with a Power BI dashboard that answers real business questions around daily sales, category performance and customer segmentation.
The pipeline was originally a classroom exercise on DBFS, but I rewrote it to follow the modern Unity Catalog Volumes approach, refactored the notebook logic into a clean Python package under src/, added environment driven configuration so the same code runs locally and on Databricks, and wrote PyTest unit tests that spin up a local Spark session so the silver transformations can be validated without a cluster.
Every stage does one clear job. Bronze lands the raw data as Delta with an ingestion timestamp. Silver applies cleaning rules, validation and enrichment, and uses incremental MERGE with a watermark so it only processes new rows. Gold builds the reporting tables on top of silver and feeds Power BI. The goal was to treat it like a real production pipeline rather than a notebook demo.

End-to-end **retail data engineering pipeline** built on **Databricks**,
**PySpark**, and **Delta Lake**, following the **Bronze / Silver / Gold**
medallion architecture, with reporting in **Power BI**.

> Adapted from a legacy DBFS-based course implementation to the modern
> **Unity Catalog Volumes** approach.

---

## Architecture

Raw files (CSV / JSON / Parquet)
│

            append + ingestion_timestamp
  BRONZE    ───────────────────────────────>  Delta tables (raw)

│
   incremental MERGE on watermark
             cleaning · validation · enrichment
  SILVER    ───────────────────────────────>  Delta tables (curated)

│
                     aggregations

   GOLD    ───────────────────────────────>  Reporting tables

│

Power BI

## Project Structure
global-retail-databricks-pipeline/
├── src/
│   ├── global_retail/      # shared config, constants, utils
│   ├── bronze/             # raw → bronze loaders
│   ├── silver/             # bronze → silver transformations
│   └── gold/               # silver → gold aggregations
├── notebooks/              # original Databricks notebooks
├── tests/                  # PyTest unit tests with local Spark
├── data_samples/  
      └── raw/
            ├── customer.csv
            ├── product.json
            └── transaction.snappy.parquet
      └── processed/
            ├── GlobalRetail_Bronze/ customer, products, transactions in .csv format
            ├── GlobalRetail_Silver/ customer, orders, products in .csv
            └── GlobalRetail_Gold/ sales category, daily sales in csv format
├── docs/                   # architecture, dashboards, screenshots
├── pyproject.toml
├── requirements.txt
└── README.md

## Running locally
```bash
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
pytest -q
```

## Running on Databricks

1. Upload `src/` as a **Repo** (or build a wheel and install on cluster).
2. Place raw files in a **Unity Catalog Volume**, e.g.
   `/Volumes/main/globalretail/raw/`.
3. Set environment variables on the cluster:
   - `GR_RAW_ROOT=/Volumes/main/globalretail/raw`
   - `GR_ARCHIVE_ROOT=/Volumes/main/globalretail/archive`
4. Schedule jobs in this order:
   1. `bronze.customer_loader` · `bronze.product_loader` · `bronze.transaction_loader`
   2. `silver.customer_transform` · `silver.product_transform` · `silver.order_transform`
   3. `gold.daily_sales` · `gold.category_sales`

## Code quality
```bash
black src tests
isort src tests
pylint src
pytest
```

## Power BI dashboards

See [`docs/dashboard_notes.md`](docs/dashboard_notes.md) for the full
DAX measures, modeling steps, and dashboard layout used to build the
final Power BI report.

## Tech Stack

Databricks · PySpark · Delta Lake · Spark SQL · Unity Catalog Volumes ·
Power BI · Python 3.10+

## License

MIT
