"""Run the whole pipeline on a laptop:  uv run python scripts/run_local.py [scale_factor]

Uses local Spark + parquet tables instead of Databricks + Delta. Same code path as the
notebooks; only the Config differs.
"""

from __future__ import annotations

import sys
from pathlib import Path

from pyspark.sql import SparkSession

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from generate_local_data import generate  # noqa: E402

from tpch_lakehouse import Config, bronze, gold, monitoring, questions, silver, validation  # noqa: E402

LOCAL = Path(__file__).resolve().parents[1] / ".local"


def local_spark() -> SparkSession:
    return (
        SparkSession.builder.master("local[*]").appName("tpch-local")
        .config("spark.sql.warehouse.dir", str(LOCAL / "warehouse"))
        .config("spark.driver.extraJavaOptions", f"-Dderby.system.home={LOCAL / 'derby'}")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .enableHiveSupport()
        .getOrCreate()
    )


LOCAL_CONFIG = Config(
    catalog=None, env="local", source_catalog=None, source_schema="tpch_src",
    table_format="parquet", apply_constraints=False,
)

if __name__ == "__main__":
    sf = float(sys.argv[1]) if len(sys.argv) > 1 else 0.01
    spark = local_spark()
    spark.sparkContext.setLogLevel("ERROR")
    cfg = LOCAL_CONFIG

    generate(spark, LOCAL / "data", sf)
    bronze.run(spark, cfg)
    silver.run(spark, cfg)
    silver.summary(spark, cfg).show()
    gold.run(spark, cfg)
    validation.run(spark, cfg).show(100, truncate=False)
    for name in questions.QUESTIONS:
        print(name)
        questions.ask(spark, cfg, name).show(20, truncate=False)
    monitoring.run(spark, cfg).filter("alert IS NOT NULL").orderBy("order_month").show(50, truncate=False)
