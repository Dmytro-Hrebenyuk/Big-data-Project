# Databricks notebook source
# MAGIC %md
# MAGIC # 06 · Monitoring & alerting — revenue by region over time
# MAGIC * **Monitor:** `gold.agg_region_revenue_monthly` (revenue, share and rank per region per month)
# MAGIC * **Alert:** on the latest *complete* month a region either
# MAGIC   * **VOLUME** — deviates ≥ 5% in revenue **per day** from its trailing 3-month average, or
# MAGIC   * **RANK** — changes rank *and* its revenue share moved ≥ 1 percentage point from its trailing
# MAGIC     3-month average (rank alone flips almost every month — the regions are within ~1% of each other).

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter

from tpch_lakehouse import monitoring

REGION_COLORS = {"AFRICA": "#E69F00", "AMERICA": "#0072B2", "ASIA": "#D55E00",
                 "EUROPE": "#009E73", "MIDDLE EAST": "#CC79A7"}

monthly = spark.table(cfg.table("gold", "agg_region_revenue_monthly"))
pdf = (monthly.filter("is_complete_month")
       .selectExpr("region_name", "order_month", "CAST(revenue_per_day AS DOUBLE) AS revenue_per_day", "revenue_rank")
       .orderBy("order_month").toPandas())

# COMMAND ----------

fig, axes = plt.subplots(2, 1, figsize=(11, 6.5), sharex=True, gridspec_kw={"height_ratios": [2, 1]})
for region, grp in pdf.groupby("region_name"):
    axes[0].plot(grp.order_month, grp.revenue_per_day, label=region, color=REGION_COLORS[region], linewidth=1.2)
    axes[1].plot(grp.order_month, grp.revenue_rank, color=REGION_COLORS[region], linewidth=1.2)
axes[0].yaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v / 1e6:,.1f}M"))
axes[0].set_title("Net revenue per day by region (complete months only)")
axes[0].legend(ncol=5, frameon=False, fontsize=8, loc="lower center")
axes[1].invert_yaxis(); axes[1].set_yticks([1, 2, 3, 4, 5]); axes[1].set_title("Revenue rank (1 = largest)")
for ax in axes:
    ax.spines[["top", "right"]].set_visible(False)
plt.tight_layout(); plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### How noisy is "normal"? (this is how the thresholds were chosen)

# COMMAND ----------

scored = monitoring.run(spark, cfg)
display(scored.selectExpr(
    "count(*)                                  AS region_months",
    "round(stddev(volume_change), 4)           AS stddev_volume_change",
    "round(max(abs(volume_change)), 4)         AS max_abs_volume_change",
    "round(stddev(share_change), 4)            AS stddev_share_change",
    "round(max(abs(share_change)), 4)          AS max_abs_share_change",
    "sum(CASE WHEN rank_change <> 0 THEN 1 ELSE 0 END)      AS region_months_with_rank_change",
    "sum(CASE WHEN alert IS NOT NULL THEN 1 ELSE 0 END)     AS alerts_fired_in_history",
))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Alerts on the real data — latest complete month

# COMMAND ----------

display(scored.filter("is_latest_month").orderBy("revenue_rank"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Demo: would the alert catch a real problem?
# MAGIC We only have healthy pre-production data, so we cut ASIA's revenue in the latest complete month by 30%
# MAGIC **in memory** and run the exact same rule.

# COMMAND ----------

shocked = monitoring.simulate_shock(monthly, region="ASIA", factor=0.70)
display(
    monitoring.evaluate_alerts(shocked, cfg.volume_change_threshold, cfg.share_shift_threshold)
    .filter("is_latest_month").orderBy("revenue_rank")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Databricks SQL alert
# MAGIC Create an alert on this query (also in `sql/alert_region_revenue.sql`) with condition **`alerts > 0`**,
# MAGIC scheduled after the pipeline job.

# COMMAND ----------

print(f"""SELECT count(*) AS alerts,
       concat_ws('; ', collect_list(concat(region_name, ': ', alert))) AS details
FROM {cfg.table('gold', 'region_revenue_alerts')}
WHERE is_latest_month AND alert IS NOT NULL""")
