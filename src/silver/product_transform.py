"""Silver layer transformation for products."""

from __future__ import annotations

from pyspark.sql import SparkSession

from global_retail.constants import (
    BRONZE_DB,
    BRONZE_PRODUCT_TABLE,
    SILVER_DB,
    SILVER_PRODUCT_TABLE,
)
from global_retail.utils import (
    ensure_database,
    get_last_watermark,
    get_logger,
    get_spark,
)

LOGGER = get_logger(__name__)

CREATE_SILVER_PRODUCTS_SQL = f"""
CREATE TABLE IF NOT EXISTS {SILVER_DB}.{SILVER_PRODUCT_TABLE} (
    product_id STRING,
    name STRING,
    category STRING,
    brand STRING,
    price DOUBLE,
    stock_quantity INT,
    rating DOUBLE,
    is_active BOOLEAN,
    price_category STRING,
    stock_status STRING,
    last_updated TIMESTAMP
) USING DELTA
"""


def create_silver_table(spark: SparkSession) -> None:
    """Create the silver product table if it does not yet exist."""
    ensure_database(spark, SILVER_DB)
    spark.sql(CREATE_SILVER_PRODUCTS_SQL)


def build_incremental_view(spark: SparkSession, watermark: str) -> None:
    """Build a cleaned, enriched temp view of new products."""
    spark.sql(f"""
        CREATE OR REPLACE TEMPORARY VIEW silver_incremental_products AS
        SELECT
            product_id,
            name,
            category,
            brand,
            CASE WHEN price < 0 THEN 0 ELSE price END AS price,
            CASE WHEN stock_quantity < 0 THEN 0
                 ELSE stock_quantity END AS stock_quantity,
            CASE
                WHEN rating < 0 THEN 0
                WHEN rating > 5 THEN 5
                ELSE rating
            END AS rating,
            is_active,
            CASE
                WHEN price > 1000 THEN 'Premium'
                WHEN price > 100  THEN 'Standard'
                ELSE 'Budget'
            END AS price_category,
            CASE
                WHEN stock_quantity = 0  THEN 'Out of Stock'
                WHEN stock_quantity < 10 THEN 'Low Stock'
                WHEN stock_quantity < 50 THEN 'Moderate Stock'
                ELSE 'Sufficient Stock'
            END AS stock_status,
            CURRENT_TIMESTAMP() AS last_updated
        FROM {BRONZE_DB}.{BRONZE_PRODUCT_TABLE}
        WHERE ingestion_timestamp > '{watermark}'
          AND name IS NOT NULL
          AND category IS NOT NULL
        """)


def merge_into_silver(spark: SparkSession) -> None:
    """Merge the incremental view into the silver product table."""
    spark.sql(f"""
        MERGE INTO {SILVER_DB}.{SILVER_PRODUCT_TABLE} AS target
        USING silver_incremental_products AS source
        ON target.product_id = source.product_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """)


def run() -> None:
    """Execute the product silver transformation job."""
    spark = get_spark()
    create_silver_table(spark)

    watermark = get_last_watermark(
        spark, f"{SILVER_DB}.{SILVER_PRODUCT_TABLE}"
    )
    LOGGER.info("Product silver watermark: %s", watermark)

    build_incremental_view(spark, watermark)
    merge_into_silver(spark)
    LOGGER.info("Silver product merge complete.")


if __name__ == "__main__":
    run()
