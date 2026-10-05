# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Silver
# MAGIC Typed, cleanly named, **3NF** tables with **enforced data quality**.
# MAGIC
# MAGIC * each table is declared in `tpch_lakehouse.silver.SPECS` (columns, PK, NOT NULL, CHECK, FK)
# MAGIC * rows that break a rule go to `<table>_quarantine` together with the rules they broke
# MAGIC * NOT NULL / CHECK / PK / FK constraints are then put on the Delta tables

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

from tpch_lakehouse import silver

constraint_log = silver.run(spark, cfg)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Did anything get quarantined?
# MAGIC `bronze_rows = silver_rows + quarantined_rows` must hold for every table.

# COMMAND ----------

display(silver.summary(spark, cfg))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Constraints applied to the tables
# MAGIC PK / FK are *informational* in Unity Catalog - they document the model and draw the ER diagram in
# MAGIC Catalog Explorer. The enforcement is done by the pipeline rules above.

# COMMAND ----------

display(spark.createDataFrame(constraint_log, "statement string, result string"))
