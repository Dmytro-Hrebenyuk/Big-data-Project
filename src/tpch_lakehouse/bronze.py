"""Bronze: land the source exactly as it is. No renames, no casts, no filters.

Only two metadata columns are added so every row can be traced back:
    _ingested_at   when this run copied the row
    _source_table  fully-qualified source it came from
"""

from __future__ import annotations

from pyspark.sql import functions as F

from tpch_lakehouse.config import TABLES, Config, create_schemas, write_table


def ingest_table(spark, cfg: Config, table: str) -> None:
    df = (
        spark.table(cfg.source(table))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_table", F.lit(cfg.source(table)))
    )
    # Full snapshot overwrite: the source is a static snapshot, so re-running is idempotent.
    write_table(df, cfg, "bronze", table)


def run(spark, cfg: Config) -> None:
    create_schemas(spark, cfg)
    for table in TABLES:
        ingest_table(spark, cfg, table)
        print(f"bronze: {cfg.source(table)} -> {cfg.table('bronze', table)}")
