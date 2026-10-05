"""Validation: prove, across all three layers, that regional numbers can be trusted.

Every check is one SQL statement that returns a single number: the count of
VIOLATIONS. 0 = pass. Results are appended to <ops>.dq_results on every run so the
history of data quality is itself queryable.

Check groups
    COMPLETENESS   nothing lost or duplicated between source -> bronze -> silver -> gold
    GEOGRAPHY      customer / supplier -> nation -> region always resolves  (profile rule 1)
    REVENUE        regional revenue sums correctly, no double counting      (profile rule 2)
"""

from __future__ import annotations

from datetime import datetime, timezone

from tpch_lakehouse.config import TABLES, Config, write_table
from tpch_lakehouse.silver import SPECS

# The same definition as gold.fact_sales.net_revenue, applied to the raw source columns.
BRONZE_REVENUE = "SUM(CAST(l_extendedprice * (1 - l_discount) AS DECIMAL(18,4)))"
SILVER_REVENUE = "SUM(CAST(extended_price * (1 - discount) AS DECIMAL(18,4)))"


def build_checks(cfg: Config) -> list[tuple[str, str, str, str]]:
    """Return (group, check name, description, SQL returning the number of violations)."""
    n = cfg.names()
    checks: list[tuple[str, str, str, str]] = []
    add = lambda group, name, desc, sql: checks.append((group, name, desc, sql.format(**n)))  # noqa: E731

    # ---------------------------------------------------------------- COMPLETENESS
    for t in TABLES:
        add("COMPLETENESS", f"bronze_matches_source__{t}",
            "bronze has exactly the source row count",
            f"SELECT abs((SELECT count(*) FROM {cfg.source(t)}) - (SELECT count(*) FROM {{bronze}}.{t}))")
        add("COMPLETENESS", f"silver_plus_quarantine_equals_bronze__{t}",
            "no row silently dropped: silver + quarantine = bronze",
            f"SELECT abs((SELECT count(*) FROM {{bronze}}.{t}) - (SELECT count(*) FROM {{silver}}.{t})"
            f" - (SELECT count(*) FROM {{silver}}.{t}_quarantine))")
        pk = ", ".join(SPECS[t].primary_key)
        add("COMPLETENESS", f"silver_pk_unique__{t}",
            f"primary key ({pk}) is unique in silver",
            f"SELECT count(*) FROM (SELECT {pk} FROM {{silver}}.{t} GROUP BY {pk} HAVING count(*) > 1)")
    add("COMPLETENESS", "gold_fact_rowcount_equals_silver_lineitem",
        "fact joins neither dropped nor duplicated a line item",
        "SELECT abs((SELECT count(*) FROM {silver}.lineitem) - (SELECT count(*) FROM {gold}.fact_sales))")
    add("COMPLETENESS", "gold_fact_grain_unique",
        "fact_sales has one row per (order_key, line_number)",
        "SELECT count(*) FROM (SELECT 1 FROM {gold}.fact_sales GROUP BY order_key, line_number HAVING count(*) > 1)")
    add("COMPLETENESS", "gold_dim_customer_keeps_all_customers",
        "customers without orders are still in dim_customer",
        "SELECT abs((SELECT count(*) FROM {silver}.customer) - (SELECT count(*) FROM {gold}.dim_customer))")

    # ---------------------------------------------------------------- GEOGRAPHY
    # Checked on the RAW data (bronze) - this is the statement about the source -
    # and again on silver, where it must hold by construction.
    add("GEOGRAPHY", "bronze_customer_has_valid_nation",
        "every source customer points at an existing nation",
        "SELECT count(*) FROM {bronze}.customer c LEFT ANTI JOIN {bronze}.nation n ON c.c_nationkey = n.n_nationkey")
    add("GEOGRAPHY", "bronze_supplier_has_valid_nation",
        "every source supplier points at an existing nation",
        "SELECT count(*) FROM {bronze}.supplier s LEFT ANTI JOIN {bronze}.nation n ON s.s_nationkey = n.n_nationkey")
    add("GEOGRAPHY", "bronze_nation_has_valid_region",
        "every source nation points at an existing region",
        "SELECT count(*) FROM {bronze}.nation n LEFT ANTI JOIN {bronze}.region r ON n.n_regionkey = r.r_regionkey")
    add("GEOGRAPHY", "silver_customer_has_valid_nation",
        "every silver customer resolves to a nation",
        "SELECT count(*) FROM {silver}.customer c LEFT ANTI JOIN {silver}.nation n ON c.nation_key = n.nation_key")
    add("GEOGRAPHY", "silver_supplier_has_valid_nation",
        "every silver supplier resolves to a nation",
        "SELECT count(*) FROM {silver}.supplier s LEFT ANTI JOIN {silver}.nation n ON s.nation_key = n.nation_key")
    add("GEOGRAPHY", "silver_nation_has_valid_region",
        "every silver nation resolves to a region",
        "SELECT count(*) FROM {silver}.nation n LEFT ANTI JOIN {silver}.region r ON n.region_key = r.region_key")
    add("GEOGRAPHY", "geography_quarantine_is_empty",
        "no region / nation / customer / supplier row was quarantined",
        "SELECT (SELECT count(*) FROM {silver}.region_quarantine) + (SELECT count(*) FROM {silver}.nation_quarantine)"
        " + (SELECT count(*) FROM {silver}.customer_quarantine) + (SELECT count(*) FROM {silver}.supplier_quarantine)")
    add("GEOGRAPHY", "gold_nation_in_exactly_one_region",
        "a nation maps to one region only (otherwise revenue would be double counted)",
        "SELECT count(*) FROM (SELECT nation_key FROM {gold}.dim_geography GROUP BY nation_key HAVING count(*) > 1)")
    add("GEOGRAPHY", "gold_fact_has_both_regions",
        "every sale has a customer region and a supplier region",
        "SELECT count(*) FROM {gold}.fact_sales WHERE customer_region_key IS NULL OR supplier_region_key IS NULL")

    # ---------------------------------------------------------------- REVENUE
    # Exact decimal arithmetic -> layers must agree to the cent (tolerance 0.01).
    tol = "0.01"
    add("REVENUE", "net_revenue_bronze_equals_silver",
        "total net revenue is identical in bronze and silver",
        f"SELECT CASE WHEN abs((SELECT {BRONZE_REVENUE} FROM {{bronze}}.lineitem)"
        f" - (SELECT {SILVER_REVENUE} FROM {{silver}}.lineitem)) > {tol} THEN 1 ELSE 0 END")
    add("REVENUE", "net_revenue_silver_equals_gold",
        "total net revenue is identical in silver and gold fact",
        f"SELECT CASE WHEN abs((SELECT {SILVER_REVENUE} FROM {{silver}}.lineitem)"
        f" - (SELECT SUM(net_revenue) FROM {{gold}}.fact_sales)) > {tol} THEN 1 ELSE 0 END")
    add("REVENUE", "regions_sum_to_total",
        "sum of per-region revenue = company total (no gap, no double count)",
        f"SELECT CASE WHEN abs((SELECT SUM(revenue) FROM {{gold}}.agg_region_revenue_monthly)"
        f" - (SELECT SUM(net_revenue) FROM {{gold}}.fact_sales)) > {tol} THEN 1 ELSE 0 END")
    add("REVENUE", "part_region_sums_to_total",
        "sum of per-part-per-region revenue = company total",
        f"SELECT CASE WHEN abs((SELECT SUM(revenue) FROM {{gold}}.agg_part_region_revenue)"
        f" - (SELECT SUM(net_revenue) FROM {{gold}}.fact_sales)) > {tol} THEN 1 ELSE 0 END")
    add("REVENUE", "each_sale_counted_in_one_region",
        "line items across regions add up to the fact row count",
        "SELECT abs((SELECT SUM(line_items) FROM {gold}.agg_region_revenue_monthly) - (SELECT count(*) FROM {gold}.fact_sales))")
    add("REVENUE", "region_revenue_matches_independent_recompute",
        "per-region revenue recomputed straight from bronze matches gold (per region)",
        f"""SELECT count(*) FROM (
                SELECT r.r_name AS region_name, {BRONZE_REVENUE} AS revenue
                FROM {{bronze}}.lineitem l
                JOIN {{bronze}}.orders   o ON l.l_orderkey  = o.o_orderkey
                JOIN {{bronze}}.customer c ON o.o_custkey   = c.c_custkey
                JOIN {{bronze}}.nation   n ON c.c_nationkey = n.n_nationkey
                JOIN {{bronze}}.region   r ON n.n_regionkey = r.r_regionkey
                GROUP BY r.r_name
            ) b
            FULL OUTER JOIN (
                SELECT region_name, SUM(revenue) AS revenue
                FROM {{gold}}.agg_region_revenue_monthly GROUP BY region_name
            ) g ON trim(b.region_name) = g.region_name
            WHERE b.revenue IS NULL OR g.revenue IS NULL OR abs(b.revenue - g.revenue) > {tol}""")
    add("REVENUE", "monthly_shares_sum_to_one",
        "regional revenue shares add up to 100% in every month",
        "SELECT count(*) FROM (SELECT order_month FROM {gold}.agg_region_revenue_monthly"
        " GROUP BY order_month HAVING abs(SUM(revenue_share) - 1) > 1e-9)")
    return checks


def run(spark, cfg: Config, fail_on_error: bool = True):
    """Run all checks, persist the results, and (optionally) fail the job."""
    run_at = datetime.now(timezone.utc).replace(tzinfo=None)
    rows = []
    for group, name, description, sql in build_checks(cfg):
        violations = int(spark.sql(sql).first()[0] or 0)
        rows.append((run_at, group, name, description, violations, "PASS" if violations == 0 else "FAIL"))

    results = spark.createDataFrame(
        rows,
        "run_at timestamp, check_group string, check_name string, description string, "
        "violations long, status string",
    )
    write_table(results, cfg, "ops", "dq_results", mode="append")

    failed = [r for r in rows if r[5] == "FAIL"]
    print(f"validation: {len(rows) - len(failed)}/{len(rows)} checks passed")
    if failed and fail_on_error:
        details = "\n".join(f"  - [{r[1]}] {r[2]}: {r[4]} violation(s)" for r in failed)
        raise AssertionError(f"{len(failed)} data quality check(s) failed:\n{details}")
    return results
