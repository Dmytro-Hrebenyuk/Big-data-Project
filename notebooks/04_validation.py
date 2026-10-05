# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Validation
# MAGIC Each check is one SQL statement that returns the **number of violations** (0 = pass).
# MAGIC Results are appended to `<ops>.dq_results`. The notebook **fails** if any check fails, so a job
# MAGIC stops before anyone looks at wrong numbers.
# MAGIC
# MAGIC | group | proves |
# MAGIC |---|---|
# MAGIC | COMPLETENESS | no rows lost or duplicated source → bronze → silver → gold |
# MAGIC | GEOGRAPHY | every customer & supplier → valid nation → valid region |
# MAGIC | REVENUE | regional revenue sums to the total, identical in all three layers, no double counting |

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

from tpch_lakehouse import validation

results = validation.run(spark, cfg, fail_on_error=False)
display(results.orderBy("check_group", "check_name"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### The same net revenue in every layer

# COMMAND ----------

display(spark.sql(f"""
    SELECT 'bronze' AS layer, {validation.BRONZE_REVENUE} AS net_revenue FROM {cfg.table('bronze', 'lineitem')}
    UNION ALL
    SELECT 'silver', {validation.SILVER_REVENUE} FROM {cfg.table('silver', 'lineitem')}
    UNION ALL
    SELECT 'gold (fact)', SUM(net_revenue) FROM {cfg.table('gold', 'fact_sales')}
    UNION ALL
    SELECT 'gold (sum of regions)', SUM(revenue) FROM {cfg.table('gold', 'agg_region_revenue_monthly')}
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Demo: do the rules actually catch bad data?
# MAGIC We only have clean pre-production data, so we **inject** broken rows into a copy of bronze `customer`
# MAGIC (unknown nation, duplicate key, missing nation) and run the silver rules on it. Nothing is written.

# COMMAND ----------

from pyspark.sql import functions as F
from tpch_lakehouse import silver

bronze_customer = spark.table(cfg.table("bronze", "customer"))
sample = bronze_customer.limit(3).collect()
bad_rows = spark.createDataFrame(
    [
        sample[0].asDict() | {"c_custkey": -1, "c_nationkey": 999},    # nation does not exist
        sample[1].asDict(),                                              # duplicate primary key
        sample[2].asDict() | {"c_custkey": -2, "c_nationkey": None},   # nation missing
    ],
    schema=bronze_customer.schema,
)
display(
    silver.evaluate_rules(spark, cfg, "customer", bronze_df=bronze_customer.unionByName(bad_rows))
    .filter(F.size("_failed_rules") > 0)
    .select("customer_key", "name", "nation_key", "_failed_rules")
)

# COMMAND ----------

failed = results.filter("status = 'FAIL'").count()
assert failed == 0, f"{failed} data quality check(s) failed - see the table above"
print("All data quality checks passed.")
