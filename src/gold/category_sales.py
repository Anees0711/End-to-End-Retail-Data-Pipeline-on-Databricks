"""Gold layer aggregation: sales rolled up by product category."""

from __future__ import annotations

from global_retail.constants import (
    GOLD_CATEGORY_SALES_TABLE,
    GOLD_DB,
    SILVER_DB,
    SILVER_ORDER_TABLE,
    SILVER_PRODUCT_TABLE,
)
from global_retail.utils import ensure_database, get_logger, get_spark

LOGGER = get_logger(__name__)


def run() -> None:
    """Materialize the category sales gold table."""
    spark = get_spark()
    ensure_database(spark, GOLD_DB)

    LOGGER.info("Building %s.%s", GOLD_DB, GOLD_CATEGORY_SALES_TABLE)
    spark.sql(f"""
        CREATE OR REPLACE TABLE {GOLD_DB}.{GOLD_CATEGORY_SALES_TABLE}
        USING DELTA AS
        SELECT
            p.category                       AS product_category,
            COUNT(DISTINCT o.transaction_id) AS order_count,
            SUM(o.quantity)                  AS units_sold,
            SUM(o.total_amount)              AS category_total_sales,
            AVG(o.total_amount)              AS avg_order_value
        FROM {SILVER_DB}.{SILVER_ORDER_TABLE} AS o
        JOIN {SILVER_DB}.{SILVER_PRODUCT_TABLE} AS p
          ON o.product_id = p.product_id
        WHERE o.order_status = 'Completed'
        GROUP BY p.category
        ORDER BY category_total_sales DESC
        """)
    LOGGER.info("Gold category sales build complete.")


if __name__ == "__main__":
    run()
