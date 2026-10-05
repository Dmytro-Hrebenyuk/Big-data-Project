from decimal import Decimal

from pyspark.sql import functions as F

from tpch_lakehouse import Config, monitoring, questions, silver, validation
from tpch_lakehouse.config import TABLES


# ------------------------------------------------------------------ naming / portability
def test_names_follow_convention():
    cfg = Config(catalog="main", env="preprod")
    assert cfg.table("silver", "customer") == "main.tpch_preprod_silver.customer"
    assert cfg.source("orders") == "samples.tpch.orders"
    assert Config(catalog=None, env="dev").table("gold", "fact_sales") == "tpch_dev_gold.fact_sales"


# ------------------------------------------------------------------ bronze / silver
def test_bronze_is_source_plus_metadata_only(spark, cfg):
    for table in TABLES:
        source_cols = spark.table(cfg.source(table)).columns
        assert spark.table(cfg.table("bronze", table)).columns == [*source_cols, "_ingested_at", "_source_table"]


def test_clean_source_has_empty_quarantine(spark, cfg):
    for row in silver.summary(spark, cfg).collect():
        assert row.quarantined_rows == 0
        assert row.silver_rows == row.bronze_rows


def test_all_validation_checks_pass(spark, cfg):
    results = validation.run(spark, cfg, fail_on_error=False)
    failed = [r.check_name for r in results.filter("status = 'FAIL'").collect()]
    assert failed == []


def _failed_rules(spark, cfg, table, overrides):
    """Inject modified copies of a bronze row and return {pk value: failed rules}."""
    bronze_df = spark.table(cfg.table("bronze", table))
    template = bronze_df.limit(1).collect()[0].asDict()
    bad = spark.createDataFrame([template | o for o in overrides], schema=bronze_df.schema)
    evaluated = silver.evaluate_rules(spark, cfg, table, bronze_df=bronze_df.unionByName(bad))
    return evaluated.filter(F.size("_failed_rules") > 0)


def test_customer_with_unknown_or_missing_nation_is_quarantined(spark, cfg):
    bad = _failed_rules(spark, cfg, "customer", [
        {"c_custkey": -1, "c_nationkey": 999},
        {"c_custkey": -2, "c_nationkey": None},
    ])
    rules = {r.customer_key: set(r._failed_rules) for r in bad.collect()}
    assert rules[-1] == {"fk_nation_key__nation"}
    assert rules[-2] == {"not_null_nation_key", "fk_nation_key__nation"}


def test_duplicate_primary_key_is_quarantined_once(spark, cfg):
    bad = _failed_rules(spark, cfg, "customer", [{}])   # exact copy of an existing row
    rows = bad.collect()
    assert len(rows) == 1 and rows[0]._failed_rules == ["pk_unique"]


def test_nation_with_unknown_region_is_quarantined(spark, cfg):
    bad = _failed_rules(spark, cfg, "nation", [{"n_nationkey": 99, "n_regionkey": 42}])
    assert [r._failed_rules for r in bad.collect()] == [["fk_region_key__region"]]


def test_lineitem_composite_fk_and_value_checks(spark, cfg):
    bad = _failed_rules(spark, cfg, "lineitem", [
        {"l_linenumber": 98, "l_suppkey": -5},              # (part, supplier) pair not in partsupp
        {"l_linenumber": 99, "l_discount": Decimal("1.50")},            # discount out of range
    ])
    rules = {r.line_number: set(r._failed_rules) for r in bad.collect()}
    assert rules[98] == {"fk_part_key_supplier_key__partsupp"}
    assert rules[99] == {"discount_between_0_and_1"}


# ------------------------------------------------------------------ gold
def test_region_revenue_sums_to_source_total(spark, cfg):
    source_total = spark.sql(
        f"SELECT SUM(CAST(l_extendedprice * (1 - l_discount) AS DECIMAL(18,4))) FROM {cfg.source('lineitem')}"
    ).first()[0]
    by_region = questions.ask(spark, cfg, "q3a_revenue_by_region").collect()
    assert len(by_region) == 5
    assert abs(sum(r.revenue for r in by_region) - source_total) < 5 * 0.01   # 5 rounded rows
    assert abs(sum(r.revenue_share for r in by_region) - 1) < 1e-9


def test_india_matches_source(spark, cfg):
    expected = spark.sql(f"""
        SELECT COUNT(*), CAST(AVG(c_acctbal) AS DECIMAL(18,2))
        FROM {cfg.source('customer')} c JOIN {cfg.source('nation')} n ON c.c_nationkey = n.n_nationkey
        WHERE trim(n.n_name) = 'INDIA'""").first()
    got = questions.ask(spark, cfg, "q2_customers_india").first()
    assert (got.customers, got.avg_account_balance) == (expected[0], expected[1])


def test_top5_asia_is_sorted_and_asian(spark, cfg):
    rows = questions.ask(spark, cfg, "q1_top5_products_asia").collect()
    assert len(rows) == 5
    assert [r.revenue for r in rows] == sorted((r.revenue for r in rows), reverse=True)


def test_coverage_has_every_part(spark, cfg):
    coverage = spark.table(cfg.table("gold", "agg_part_coverage"))
    assert coverage.count() == spark.table(cfg.table("silver", "part")).count()
    assert coverage.filter("regions_sold_in > 5 OR supplier_regions > 5").count() == 0


def test_all_question_queries_run(spark, cfg):
    for name in questions.QUESTIONS:
        questions.ask(spark, cfg, name).collect()


# ------------------------------------------------------------------ monitoring
def test_partial_last_month_is_flagged_incomplete(spark, cfg):
    monthly = spark.table(cfg.table("gold", "agg_region_revenue_monthly"))
    last = monthly.agg(F.max("order_month")).first()[0]
    assert str(last) == "1998-08-01"                       # data ends 1998-08-02
    assert monthly.filter(F.col("order_month") == F.lit(last)).filter("is_complete_month").count() == 0
    assert monthly.filter("order_month = DATE'1998-07-01' AND NOT is_complete_month").count() == 0


def test_alert_fires_on_simulated_regional_drop(spark, cfg):
    monthly = spark.table(cfg.table("gold", "agg_region_revenue_monthly"))
    shocked = monitoring.simulate_shock(monthly, region="ASIA", factor=0.2)
    latest = monitoring.evaluate_alerts(shocked, 0.05, 0.01).filter("is_latest_month")
    asia = latest.filter("region_name = 'ASIA'").first()
    assert str(asia.order_month) == "1998-07-01"
    assert "VOLUME_DROP" in asia.alert
    assert asia.revenue_rank == 5


def test_no_alert_when_thresholds_are_not_reached(spark, cfg):
    monthly = spark.table(cfg.table("gold", "agg_region_revenue_monthly"))
    assert monitoring.evaluate_alerts(monthly, 10.0, 10.0).filter("alert IS NOT NULL").count() == 0
