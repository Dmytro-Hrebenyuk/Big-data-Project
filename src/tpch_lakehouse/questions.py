"""The Regional Manager's business questions - answered from gold only.

Kept as named SQL templates so the notebook, the tests and the dashboard all run the
exact same query (one definition per number).
"""

from __future__ import annotations

from tpch_lakehouse.config import Config

QUESTIONS: dict[str, str] = {
    # Q1 - What are the top 5 products by revenue in Asia?
    "q1_top5_products_asia": """
        SELECT p.part_key, p.part_name, p.brand, p.type,
               CAST(a.revenue AS DECIMAL(18,2)) AS revenue,
               a.line_items
        FROM {gold}.agg_part_region_revenue a
        JOIN {gold}.dim_part p ON a.part_key = p.part_key
        WHERE a.region_name = 'ASIA'
        ORDER BY a.revenue DESC, p.part_key
        LIMIT 5
    """,
    # Q2 - How many customers are in India, and what is their average account balance?
    "q2_customers_india": """
        SELECT COUNT(*)                                     AS customers,
               CAST(AVG(account_balance) AS DECIMAL(18,2))  AS avg_account_balance
        FROM {gold}.dim_customer
        WHERE nation_name = 'INDIA'
    """,
    # Q3a - Which region has the highest total revenue?
    "q3a_revenue_by_region": """
        SELECT region_name,
               CAST(SUM(revenue) AS DECIMAL(22,2))                      AS revenue,
               CAST(SUM(revenue) / SUM(SUM(revenue)) OVER () AS DOUBLE) AS revenue_share,
               RANK() OVER (ORDER BY SUM(revenue) DESC)                 AS revenue_rank
        FROM {gold}.agg_region_revenue_monthly
        GROUP BY region_name
        ORDER BY revenue DESC
    """,
    # Q3b - What share of sales happens between a customer and a supplier in different regions?
    "q3b_cross_region_share": """
        SELECT CAST(SUM(CASE WHEN is_cross_region THEN net_revenue ELSE 0 END) AS DECIMAL(22,2)) AS cross_region_revenue,
               CAST(SUM(net_revenue) AS DECIMAL(22,2))                                           AS total_revenue,
               CAST(SUM(CASE WHEN is_cross_region THEN net_revenue ELSE 0 END) / SUM(net_revenue) AS DOUBLE) AS cross_region_share
        FROM {gold}.fact_sales
    """,
    "q3b_cross_region_share_by_region": """
        SELECT customer_region AS region_name,
               CAST(SUM(CASE WHEN is_cross_region THEN net_revenue ELSE 0 END) / SUM(net_revenue) AS DOUBLE) AS cross_region_share
        FROM {gold}.fact_sales
        GROUP BY customer_region
        ORDER BY customer_region
    """,
    # Q4 - Are there products sold in only one or two regions?
    "q4_coverage_distribution": """
        SELECT regions_sold_in,
               COUNT(*)                            AS products,
               CAST(AVG(line_items) AS DOUBLE)     AS avg_line_items_per_product
        FROM {gold}.agg_part_coverage
        GROUP BY regions_sold_in
        ORDER BY regions_sold_in
    """,
    "q4_products_in_1_or_2_regions": """
        SELECT part_key, part_name, brand, regions_sold_in, sold_in_regions,
               line_items, CAST(revenue AS DECIMAL(18,2)) AS revenue, supplier_regions
        FROM {gold}.agg_part_coverage
        WHERE regions_sold_in BETWEEN 1 AND 2
        ORDER BY regions_sold_in, line_items DESC, part_key
    """,
    # Q4 (supply side) - from how many regions can each product be sourced?
    "q4_supplier_region_distribution": """
        SELECT supplier_regions, COUNT(*) AS products
        FROM {gold}.agg_part_coverage
        GROUP BY supplier_regions
        ORDER BY supplier_regions
    """,
}


def sql(name: str, cfg: Config) -> str:
    return QUESTIONS[name].format(**cfg.names())


def ask(spark, cfg: Config, name: str):
    return spark.sql(sql(name, cfg))
