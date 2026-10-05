"""Generate TPC-H data locally with DuckDB's dbgen and expose it to Spark as tpch_src.*

Local development only. In Databricks the source is samples.tpch and this script is
never used.
"""

from __future__ import annotations

from pathlib import Path

import duckdb

from tpch_lakehouse.config import TABLES


def generate(spark, out_dir: Path, scale_factor: float = 0.01, schema: str = "tpch_src") -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute(f"CALL dbgen(sf={scale_factor})")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    for table in TABLES:
        path = out_dir / f"{table}.parquet"
        con.execute(f"COPY {table} TO '{path}' (FORMAT PARQUET)")
        spark.read.parquet(str(path)).write.mode("overwrite").format("parquet").saveAsTable(f"{schema}.{table}")
    con.close()
