"""Project-wide constants for the Global Retail pipeline.

Centralizes database names, table names, and schema definitions so that
they can be reused across bronze, silver, and gold layers without magic
strings scattered throughout the codebase.
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# Databases (schemas) — one per medallion layer
# ---------------------------------------------------------------------------
BRONZE_DB: str = "globalretail_bronze"
SILVER_DB: str = "globalretail_silver"
GOLD_DB: str = "globalretail_gold"

# ---------------------------------------------------------------------------
# Bronze tables
# ---------------------------------------------------------------------------
BRONZE_CUSTOMER_TABLE: str = "bronze_customer"
BRONZE_PRODUCT_TABLE: str = "bronze_products"
BRONZE_TRANSACTION_TABLE: str = "bronze_transactions"

# ---------------------------------------------------------------------------
# Silver tables
# ---------------------------------------------------------------------------
SILVER_CUSTOMER_TABLE: str = "silver_customers"
SILVER_PRODUCT_TABLE: str = "silver_products"
SILVER_ORDER_TABLE: str = "silver_orders"

# ---------------------------------------------------------------------------
# Gold tables
# ---------------------------------------------------------------------------
GOLD_DAILY_SALES_TABLE: str = "gold_daily_sales"
GOLD_CATEGORY_SALES_TABLE: str = "gold_category_sales"

# ---------------------------------------------------------------------------
# Common columns
# ---------------------------------------------------------------------------
INGESTION_TS_COL: str = "ingestion_timestamp"
LAST_UPDATED_COL: str = "last_updated"

# Default watermark when no rows have been processed yet.
DEFAULT_WATERMARK: str = "1900-01-01T00:00:00.000+00:00"
