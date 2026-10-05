# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Gold (Regional Manager)
# MAGIC A small star schema plus aggregates:
# MAGIC
# MAGIC | table | grain | used for |
# MAGIC |---|---|---|
# MAGIC | `dim_geography` | nation | nation → region hierarchy |
# MAGIC | `dim_customer`, `dim_supplier`, `dim_part` | one row per entity | Q2, labels |
# MAGIC | `fact_sales` | order line | Q3 (cross-region share) |
# MAGIC | `agg_region_revenue_monthly` | region × month | Q3 (ranking), monitoring |
# MAGIC | `agg_part_region_revenue` | part × region | Q1 |
# MAGIC | `agg_part_coverage` | part | Q4 |

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

from tpch_lakehouse import gold

gold.run(spark, cfg)

# COMMAND ----------

display(spark.table(cfg.table("gold", "agg_region_revenue_monthly")).orderBy("order_month", "revenue_rank"))
