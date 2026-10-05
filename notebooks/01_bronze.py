# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Bronze
# MAGIC Copy the 8 TPC-H source tables **as is**. The only additions are two metadata columns
# MAGIC (`_ingested_at`, `_source_table`).

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

# The source documents itself - worth a read before modelling anything.
try:
    with open("/dbfs/databricks-datasets/tpch/README.md") as f:
        displayHTML(f"<pre>{f.read()}</pre>")
except Exception as exc:  # serverless / restricted clusters may not mount /dbfs
    print(f"README not readable here: {exc}")

# COMMAND ----------

from tpch_lakehouse import bronze

bronze.run(spark, cfg)

# COMMAND ----------

display(spark.sql(f"SHOW TABLES IN {cfg.schema('bronze')}"))
