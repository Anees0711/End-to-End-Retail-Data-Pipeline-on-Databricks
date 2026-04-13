"""Bronze layer loader for raw product JSON data."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import current_timestamp

from global_retail.config import PipelineConfig, load_config
from global_retail.constants import (
    BRONZE_DB,
    BRONZE_PRODUCT_TABLE,
    INGESTION_TS_COL,
)
from global_retail.utils import ensure_database, get_logger, get_spark

LOGGER = get_logger(__name__)


def read_raw_products(spark: SparkSession, source_path: str) -> DataFrame:
    """Read raw products JSON file into a DataFrame.

    Args:
        spark: Active Spark session.
        source_path: Path to the raw product JSON file.

    Returns:
        DataFrame with the raw product catalog.
    """
    LOGGER.info("Reading raw products JSON from %s", source_path)
    return spark.read.option("multiLine", "true").json(source_path)


def run(config: PipelineConfig | None = None) -> None:
    """Execute the product bronze ingestion job."""
    config = config or load_config()
    spark = get_spark()
    ensure_database(spark, BRONZE_DB)

    raw_df = read_raw_products(spark, config.product_path)
    stamped_df = raw_df.withColumn(INGESTION_TS_COL, current_timestamp())

    fqn = f"{BRONZE_DB}.{BRONZE_PRODUCT_TABLE}"
    LOGGER.info("Appending products to %s", fqn)
    stamped_df.write.format("delta").mode("append").saveAsTable(fqn)
    LOGGER.info("Bronze product load complete.")


if __name__ == "__main__":
    run()
