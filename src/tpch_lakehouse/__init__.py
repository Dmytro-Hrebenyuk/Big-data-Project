"""TPC-H lakehouse: bronze -> silver (3NF + data quality) -> gold (Regional Manager)."""

from tpch_lakehouse.config import Config, TABLES

__all__ = ["Config", "TABLES"]
