"""Bronze layer: raw data ingestion into Delta tables."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import current_timestamp

from global_retail.config import PipelineConfig, load_config
from global_retail.constants import (
    BRONZE_CUSTOMER_TABLE,
    BRONZE_DB,
    INGESTION_TS_COL,
)
from global_retail.utils import ensure_database, get_logger, get_spark

LOGGER = get_logger(__name__)


def read_raw_customers(spark: SparkSession, source_path: str) -> DataFrame:
    """Read the raw customer CSV file into a DataFrame.

    Args:
        spark: Active Spark session.
        source_path: Path to the raw customer CSV file.

    Returns:
        DataFrame containing the raw customer rows.
    """
    LOGGER.info("Reading raw customer CSV from %s", source_path)
    return spark.read.csv(source_path, header=True, inferSchema=True)


def add_ingestion_timestamp(df: DataFrame) -> DataFrame:
    """Add an ``ingestion_timestamp`` column with the current time."""
    return df.withColumn(INGESTION_TS_COL, current_timestamp())


def write_to_bronze(df: DataFrame, table_name: str) -> None:
    """Append a DataFrame to a bronze Delta table.

    Args:
        df: DataFrame to persist.
        table_name: Unqualified bronze table name.
    """
    fqn = f"{BRONZE_DB}.{table_name}"
    LOGGER.info("Appending %d rows to %s", df.count(), fqn)
    df.write.format("delta").mode("append").saveAsTable(fqn)


def run(config: PipelineConfig | None = None) -> None:
    """Execute the customer bronze ingestion job."""
    config = config or load_config()
    spark = get_spark()
    ensure_database(spark, BRONZE_DB)

    raw_df = read_raw_customers(spark, config.customer_path)
    stamped_df = add_ingestion_timestamp(raw_df)
    write_to_bronze(stamped_df, BRONZE_CUSTOMER_TABLE)

    LOGGER.info("Bronze customer load complete.")


if __name__ == "__main__":
    run()
