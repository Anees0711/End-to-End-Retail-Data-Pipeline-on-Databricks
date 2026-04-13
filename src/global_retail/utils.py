"""Shared utilities for the Global Retail pipeline.

Provides logging setup, Spark session helpers, and incremental-load
watermark logic that are reused by every layer of the medallion
architecture.
"""

from __future__ import annotations

import logging
from typing import Optional

from pyspark.sql import SparkSession

from global_retail.constants import DEFAULT_WATERMARK


def get_logger(name: str) -> logging.Logger:
    """Return a configured module-level logger.

    Args:
        name: Logger name, typically ``__name__`` of the calling module.

    Returns:
        A configured :class:`logging.Logger` writing to stderr at INFO.
    """
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        logger.propagate = False
    return logger


def get_spark(app_name: str = "global-retail-pipeline") -> SparkSession:
    """Return an active :class:`SparkSession`, creating one if needed.

    On Databricks, the active session is reused. Locally, a new session
    is built with Delta Lake support enabled.

    Args:
        app_name: Application name registered with Spark.

    Returns:
        A live :class:`SparkSession`.
    """
    builder = (
        SparkSession.builder.appName(app_name)
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )
    return builder.getOrCreate()


def get_last_watermark(
    spark: SparkSession,
    table_fqn: str,
    timestamp_col: str = "last_updated",
) -> str:
    """Return the maximum timestamp from a table, or a default sentinel.

    Used to drive incremental loads from bronze to silver layers.

    Args:
        spark: Active Spark session.
        table_fqn: Fully-qualified table name (``database.table``).
        timestamp_col: Column to compute the maximum timestamp from.

    Returns:
        ISO-8601 timestamp string suitable for substitution into a SQL
        ``WHERE`` clause.
    """
    row = spark.sql(
        f"SELECT MAX({timestamp_col}) AS last_processed FROM {table_fqn}"
    ).collect()[0]
    last_processed: Optional[object] = row["last_processed"]
    if last_processed is None:
        return DEFAULT_WATERMARK
    return str(last_processed)


def ensure_database(spark: SparkSession, database: str) -> None:
    """Create a database if it does not already exist.

    Args:
        spark: Active Spark session.
        database: Name of the database/schema to create.
    """
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")
