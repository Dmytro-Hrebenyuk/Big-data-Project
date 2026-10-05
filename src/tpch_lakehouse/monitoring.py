"""Monitoring + alerting: revenue by region over time.

The monitored series is gold.agg_region_revenue_monthly. Two alert rules run on
COMPLETE months only (a partial month always looks like a crash):

  VOLUME  revenue per day deviates from the region's trailing 3-month average by at
          least ``volume_change_threshold`` (default 5%). Per day, because February is
          ~10% shorter than January and would otherwise alert every year.
  RANK    the region's revenue rank changed versus the previous month AND its revenue
          share moved at least ``share_shift_threshold`` (default 1 percentage point)
          away from its trailing 3-month average. The five TPC-H regions sit within ~1%
          of each other, so the rank flips almost every month by pure noise; the share
          condition keeps only rank changes that are material.

How the defaults were profiled is in the README ("Monitoring and alerting").
"""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from tpch_lakehouse.config import Config, write_table


def evaluate_alerts(monthly: DataFrame, volume_threshold: float, share_threshold: float) -> DataFrame:
    """Score every (region, complete month). Pure function of its input, so it can be
    demonstrated on artificially modified data."""
    by_month = Window.partitionBy("order_month")
    by_region = Window.partitionBy("region_key").orderBy("order_month")
    trailing3 = by_region.rowsBetween(-3, -1)

    scored = (
        monthly.filter("is_complete_month")
        .withColumn("revenue", F.col("revenue").cast("double"))
        .withColumn("revenue_per_day", F.col("revenue") / F.col("days_in_month"))
        # rank and share are recomputed here so modified input is scored on the modified values
        .withColumn("revenue_rank", F.rank().over(by_month.orderBy(F.col("revenue").desc())))
        .withColumn("revenue_share", F.col("revenue") / F.sum("revenue").over(by_month))
        .withColumn("baseline_months", F.count("*").over(trailing3))
        .withColumn("baseline_revenue_per_day", F.avg("revenue_per_day").over(trailing3))
        .withColumn("baseline_share", F.avg("revenue_share").over(trailing3))
        .withColumn("previous_rank", F.lag("revenue_rank").over(by_region))
        .withColumn("volume_change", F.col("revenue_per_day") / F.col("baseline_revenue_per_day") - 1)
        .withColumn("share_change", F.col("revenue_share") - F.col("baseline_share"))
        .withColumn("rank_change", F.col("revenue_rank") - F.col("previous_rank"))
        .withColumn("is_latest_month", F.col("order_month") == F.max("order_month").over(Window.partitionBy(F.lit(1))))
    )
    has_baseline = F.col("baseline_months") == 3
    volume_alert = has_baseline & (F.abs("volume_change") >= volume_threshold)
    rank_alert = has_baseline & (F.col("rank_change") != 0) & (F.abs("share_change") >= share_threshold)
    alert = F.concat_ws(
        ", ",
        F.when(volume_alert & (F.col("volume_change") < 0), "VOLUME_DROP"),
        F.when(volume_alert & (F.col("volume_change") > 0), "VOLUME_SPIKE"),
        F.when(rank_alert & (F.col("rank_change") > 0), "RANK_LOSS"),
        F.when(rank_alert & (F.col("rank_change") < 0), "RANK_GAIN"),
    )
    return scored.select(
        "region_key", "region_name", "order_month", "is_latest_month",
        F.col("revenue").cast("decimal(22,2)").alias("revenue"),
        F.round("revenue_per_day", 2).alias("revenue_per_day"),
        F.round("volume_change", 4).alias("volume_change"),
        F.round("revenue_share", 4).alias("revenue_share"),
        F.round("share_change", 4).alias("share_change"),
        "revenue_rank", "previous_rank", "rank_change",
        F.when(alert != "", alert).alias("alert"),     # NULL = healthy
    )


def simulate_shock(monthly: DataFrame, region: str, factor: float) -> DataFrame:
    """Multiply one region's revenue in the latest complete month by ``factor``."""
    latest = monthly.filter("is_complete_month").agg(F.max("order_month")).first()[0]
    hit = (F.col("region_name") == region) & (F.col("order_month") == F.lit(latest))
    return monthly.withColumn(
        "revenue", F.when(hit, F.col("revenue") * F.lit(factor)).otherwise(F.col("revenue"))
    )


def run(spark, cfg: Config) -> DataFrame:
    """Score all history and persist it; the SQL alert reads the latest month from it."""
    monthly = spark.table(cfg.table("gold", "agg_region_revenue_monthly"))
    scored = evaluate_alerts(monthly, cfg.volume_change_threshold, cfg.share_shift_threshold)
    write_table(scored, cfg, "gold", "region_revenue_alerts")
    return spark.table(cfg.table("gold", "region_revenue_alerts"))
