"""Gold: a small star schema + aggregates shaped for the Regional Manager.

Definitions used everywhere (see README, "Definitions"):
    net revenue      = extended_price * (1 - discount)            (tax excluded)
    region of a sale = region of the CUSTOMER who placed the order
    cross-region     = customer region <> supplier region
    period           = order date (not ship date)

Tables are built in order; each entry is (table name, SELECT statement).
"""

from __future__ import annotations

from tpch_lakehouse.config import Config, create_schemas, write_table

GOLD_TABLES: list[tuple[str, str]] = [
    # ------------------------------------------------------------------ dimensions
    ("dim_geography", """
        SELECT n.nation_key,
               n.name       AS nation_name,
               r.region_key,
               r.name       AS region_name
        FROM {silver}.nation n
        JOIN {silver}.region r ON n.region_key = r.region_key
    """),
    ("dim_customer", """
        SELECT c.customer_key,
               c.name AS customer_name,
               c.market_segment,
               c.account_balance,
               g.nation_key, g.nation_name, g.region_key, g.region_name
        FROM {silver}.customer c
        JOIN {gold}.dim_geography g ON c.nation_key = g.nation_key
    """),
    ("dim_supplier", """
        SELECT s.supplier_key,
               s.name AS supplier_name,
               s.account_balance,
               g.nation_key, g.nation_name, g.region_key, g.region_name
        FROM {silver}.supplier s
        JOIN {gold}.dim_geography g ON s.nation_key = g.nation_key
    """),
    ("dim_part", """
        SELECT part_key, name AS part_name, manufacturer, brand, type, size, container, retail_price
        FROM {silver}.part
    """),
    # ------------------------------------------------------------------ fact
    # Grain: one row per order line (order_key, line_number) - same grain as lineitem.
    # Every join goes to a table where the join key is unique, so rows can neither be
    # duplicated nor (thanks to silver FKs) lost. Validation proves both.
    ("fact_sales", """
        SELECT l.order_key,
               l.line_number,
               o.order_date,
               CAST(date_trunc('MONTH', o.order_date) AS DATE) AS order_month,
               o.customer_key,
               l.part_key,
               l.supplier_key,
               c.nation_key   AS customer_nation_key,
               c.nation_name  AS customer_nation,
               c.region_key   AS customer_region_key,
               c.region_name  AS customer_region,
               s.nation_key   AS supplier_nation_key,
               s.nation_name  AS supplier_nation,
               s.region_key   AS supplier_region_key,
               s.region_name  AS supplier_region,
               c.region_key <> s.region_key AS is_cross_region,
               l.quantity,
               l.extended_price,
               l.discount,
               CAST(l.extended_price * (1 - l.discount) AS DECIMAL(18,4)) AS net_revenue
        FROM {silver}.lineitem l
        JOIN {silver}.orders   o ON l.order_key    = o.order_key
        JOIN {gold}.dim_customer c ON o.customer_key = c.customer_key
        JOIN {gold}.dim_supplier s ON l.supplier_key = s.supplier_key
    """),
    # ------------------------------------------------------------------ aggregates
    # Monitoring table: revenue by region over time.
    ("agg_region_revenue_monthly", """
        WITH bounds AS (
            SELECT min(order_date) AS min_date, max(order_date) AS max_date FROM {gold}.fact_sales
        ),
        monthly AS (
            SELECT customer_region_key AS region_key,
                   customer_region     AS region_name,
                   order_month,
                   SUM(net_revenue)             AS revenue,
                   COUNT(DISTINCT order_key)    AS orders,
                   COUNT(DISTINCT customer_key) AS active_customers,
                   COUNT(*)                     AS line_items
            FROM {gold}.fact_sales
            GROUP BY customer_region_key, customer_region, order_month
        )
        SELECT m.region_key, m.region_name, m.order_month,
               m.revenue, m.orders, m.active_customers, m.line_items,
               day(last_day(m.order_month)) AS days_in_month,
               -- a month only counts once the data covers all of it (no partial periods)
               (m.order_month >= b.min_date AND last_day(m.order_month) <= b.max_date) AS is_complete_month,
               CAST(m.revenue / day(last_day(m.order_month)) AS DECIMAL(18,2)) AS revenue_per_day,
               RANK() OVER (PARTITION BY m.order_month ORDER BY m.revenue DESC) AS revenue_rank,
               CAST(m.revenue / SUM(m.revenue) OVER (PARTITION BY m.order_month) AS DOUBLE) AS revenue_share
        FROM monthly m CROSS JOIN bounds b
    """),
    # Top products by region (Q1) and regional coverage (Q4).
    ("agg_part_region_revenue", """
        SELECT part_key,
               customer_region_key AS region_key,
               customer_region     AS region_name,
               SUM(net_revenue)          AS revenue,
               SUM(quantity)             AS quantity,
               COUNT(*)                  AS line_items,
               COUNT(DISTINCT order_key) AS orders
        FROM {gold}.fact_sales
        GROUP BY part_key, customer_region_key, customer_region
    """),
    # One row per part (including parts never sold): where it sells, where it is sourced.
    ("agg_part_coverage", """
        WITH sold AS (
            SELECT part_key,
                   COUNT(*)                         AS regions_sold_in,
                   concat_ws(', ', sort_array(collect_list(region_name))) AS sold_in_regions,
                   SUM(line_items)                  AS line_items,
                   SUM(revenue)                     AS revenue
            FROM {gold}.agg_part_region_revenue
            GROUP BY part_key
        ),
        supply AS (
            SELECT ps.part_key,
                   COUNT(*)                     AS suppliers,
                   COUNT(DISTINCT s.region_key) AS supplier_regions
            FROM {silver}.partsupp ps
            JOIN {gold}.dim_supplier s ON ps.supplier_key = s.supplier_key
            GROUP BY ps.part_key
        )
        SELECT p.part_key, p.part_name, p.brand, p.type,
               COALESCE(sold.regions_sold_in, 0)   AS regions_sold_in,
               COALESCE(sold.sold_in_regions, '')  AS sold_in_regions,
               COALESCE(sold.line_items, 0)        AS line_items,
               COALESCE(sold.revenue, 0)           AS revenue,
               COALESCE(supply.suppliers, 0)        AS suppliers,
               COALESCE(supply.supplier_regions, 0) AS supplier_regions
        FROM {gold}.dim_part p
        LEFT JOIN sold   ON p.part_key = sold.part_key
        LEFT JOIN supply ON p.part_key = supply.part_key
    """),
]


def run(spark, cfg: Config) -> None:
    create_schemas(spark, cfg)
    for table, sql in GOLD_TABLES:
        write_table(spark.sql(sql.format(**cfg.names())), cfg, "gold", table)
        print(f"gold: built {cfg.table('gold', table)}")
