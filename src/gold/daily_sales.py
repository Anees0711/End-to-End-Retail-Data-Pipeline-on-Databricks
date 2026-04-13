"""Gold layer aggregation: daily total sales."""

from __future__ import annotations

from global_retail.constants import (
    GOLD_DAILY_SALES_TABLE,
    GOLD_DB,
    SILVER_DB,
    SILVER_ORDER_TABLE,
)
from global_retail.utils import ensure_database, get_logger, get_spark

LOGGER = get_logger(__name__)


def run() -> None:
    """Materialize the daily sales gold table."""
    spark = get_spark()
    ensure_database(spark, GOLD_DB)

    LOGGER.info("Building %s.%s", GOLD_DB, GOLD_DAILY_SALES_TABLE)
    spark.sql(f"""
        CREATE OR REPLACE TABLE {GOLD_DB}.{GOLD_DAILY_SALES_TABLE}
        USING DELTA AS
        SELECT
            transaction_date,
            COUNT(DISTINCT transaction_id) AS daily_order_count,
            SUM(quantity)                  AS daily_units_sold,
            SUM(total_amount)              AS daily_total_sales,
            AVG(total_amount)              AS avg_order_value
        FROM {SILVER_DB}.{SILVER_ORDER_TABLE}
        WHERE order_status = 'Completed'
        GROUP BY transaction_date
        ORDER BY transaction_date
        """)
    LOGGER.info("Gold daily sales build complete.")


if __name__ == "__main__":
    run()
