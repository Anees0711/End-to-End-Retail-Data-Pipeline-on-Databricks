"""Runtime configuration for the Global Retail pipeline.

Reads environment variables (with sensible defaults) so the same code
runs locally, in Databricks DBFS, and on Unity Catalog Volumes without
modification.
"""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class PipelineConfig:
    """Immutable runtime configuration for all pipeline layers.

    Attributes:
        raw_root: Root path containing raw input files.
        archive_root: Root path where ingested raw files are archived.
        customer_file: Filename of the raw customer CSV.
        product_file: Filename of the raw product JSON.
        transaction_file: Filename of the raw transaction Parquet.
    """

    raw_root: str
    archive_root: str
    customer_file: str = "customer.csv"
    product_file: str = "products.json"
    transaction_file: str = "transaction.snappy.parquet"

    @property
    def customer_path(self) -> str:
        """Full path to the customer source file."""
        return f"{self.raw_root.rstrip('/')}/{self.customer_file}"

    @property
    def product_path(self) -> str:
        """Full path to the product source file."""
        return f"{self.raw_root.rstrip('/')}/{self.product_file}"

    @property
    def transaction_path(self) -> str:
        """Full path to the transaction source file."""
        return f"{self.raw_root.rstrip('/')}/{self.transaction_file}"


def load_config() -> PipelineConfig:
    """Build a :class:`PipelineConfig` from environment variables.

    Environment variables:
        GR_RAW_ROOT: Root directory containing raw input files.
        GR_ARCHIVE_ROOT: Root directory used to archive ingested files.

    Returns:
        A frozen :class:`PipelineConfig` instance.
    """
    raw_root = os.getenv(
        "GR_RAW_ROOT",
        "/Volumes/main/globalretail/raw",
    )
    archive_root = os.getenv(
        "GR_ARCHIVE_ROOT",
        "/Volumes/main/globalretail/archive",
    )
    return PipelineConfig(raw_root=raw_root, archive_root=archive_root)
