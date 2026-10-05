# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · Business questions — Regional Manager
# MAGIC Every answer is read from **gold only**. The SQL lives in `tpch_lakehouse.questions`, so this notebook,
# MAGIC the tests and any dashboard run the same statement.
# MAGIC
# MAGIC **Definitions** — *net revenue* = `extended_price × (1 − discount)`; a sale belongs to the
# MAGIC **customer's** region; period = order date.

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter, PercentFormatter

from tpch_lakehouse import questions

REGION_COLORS = {"AFRICA": "#E69F00", "AMERICA": "#0072B2", "ASIA": "#D55E00",
                 "EUROPE": "#009E73", "MIDDLE EAST": "#CC79A7"}
millions = FuncFormatter(lambda v, _: f"{v / 1e6:,.0f}M")
billions = FuncFormatter(lambda v, _: f"{v / 1e9:,.1f}B")


def ask(name):
    """Print the SQL (so the audience sees the code) and return the result as pandas."""
    print(questions.sql(name, cfg))
    pdf = questions.ask(spark, cfg, name).toPandas()
    for col in pdf.columns:                       # Decimal -> float for plotting
        if pdf[col].dtype == object and col not in ("part_name", "brand", "type", "region_name", "sold_in_regions"):
            pdf[col] = pdf[col].astype(float)
    return pdf

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q1 · What are the top 5 products by revenue in Asia?

# COMMAND ----------

q1 = ask("q1_top5_products_asia")
display(q1)

# COMMAND ----------

fig, ax = plt.subplots(figsize=(9, 3.5))
labels = [f"#{k} · {n}" for k, n in zip(q1.part_key, q1.part_name)]
ax.barh(labels[::-1], q1.revenue[::-1], color=REGION_COLORS["ASIA"])
ax.xaxis.set_major_formatter(millions)
ax.set_xlabel("Net revenue")
ax.set_title("Top 5 products by net revenue — ASIA")
ax.spines[["top", "right"]].set_visible(False)
plt.tight_layout(); plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q2 · How many customers are in India, and what is their average account balance?

# COMMAND ----------

q2 = ask("q2_customers_india")
display(q2)

# COMMAND ----------

# Context: India next to the other Asian nations.
asia = spark.sql(f"""
    SELECT nation_name, COUNT(*) AS customers, CAST(AVG(account_balance) AS DOUBLE) AS avg_account_balance
    FROM {cfg.table('gold', 'dim_customer')} WHERE region_name = 'ASIA'
    GROUP BY nation_name ORDER BY nation_name
""").toPandas()
fig, axes = plt.subplots(1, 2, figsize=(10, 3.2))
for ax, col, title in zip(axes, ["customers", "avg_account_balance"], ["Customers", "Average account balance"]):
    colors = [REGION_COLORS["ASIA"] if n == "INDIA" else "#BBBBBB" for n in asia.nation_name]
    ax.bar(asia.nation_name, asia[col], color=colors)
    ax.set_title(f"{title} — Asian nations (India highlighted)")
    ax.spines[["top", "right"]].set_visible(False)
    ax.tick_params(axis="x", labelsize=8)
plt.tight_layout(); plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q3 · Which region has the highest total revenue, and what share of sales is cross-region?

# COMMAND ----------

q3a = ask("q3a_revenue_by_region")
display(q3a)

# COMMAND ----------

q3b = ask("q3b_cross_region_share")
display(q3b)
q3b_by_region = ask("q3b_cross_region_share_by_region")

# COMMAND ----------

fig, axes = plt.subplots(1, 2, figsize=(11, 3.6))
bars = axes[0].bar(q3a.region_name, q3a.revenue, color=[REGION_COLORS[r] for r in q3a.region_name])
axes[0].bar_label(bars, labels=[f"{v / 1e9:.2f}B" for v in q3a.revenue], fontsize=9)
axes[0].yaxis.set_major_formatter(billions)
axes[0].set_title("Total net revenue by region")
axes[1].bar(q3b_by_region.region_name, q3b_by_region.cross_region_share,
            color=[REGION_COLORS[r] for r in q3b_by_region.region_name])
axes[1].axhline(q3b.cross_region_share[0], color="black", linestyle="--", linewidth=1)
axes[1].text(4.45, q3b.cross_region_share[0], f" overall {q3b.cross_region_share[0]:.1%}", va="bottom", ha="right")
axes[1].yaxis.set_major_formatter(PercentFormatter(1.0))
axes[1].set_ylim(0, 1)
axes[1].set_title("Share of revenue supplied from another region")
for ax in axes:
    ax.spines[["top", "right"]].set_visible(False)
    ax.tick_params(axis="x", labelsize=8)
plt.tight_layout(); plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q4 · Are there products sold in only one or two regions? What might that indicate?
# MAGIC Two views of "coverage":
# MAGIC * **demand side** — in how many regions do *customers* buy the product
# MAGIC * **supply side** — from how many regions can the product be *sourced* (`partsupp` → supplier region)

# COMMAND ----------

q4 = ask("q4_coverage_distribution")
display(q4)

# COMMAND ----------

display(ask("q4_products_in_1_or_2_regions"))

# COMMAND ----------

q4s = ask("q4_supplier_region_distribution")
display(q4s)

# COMMAND ----------

fig, axes = plt.subplots(1, 2, figsize=(10, 3.4), sharey=True)
for ax, pdf, col, title in [
    (axes[0], q4, "regions_sold_in", "Regions a product is SOLD in"),
    (axes[1], q4s, "supplier_regions", "Regions a product is SOURCED from"),
]:
    full = {i: 0 for i in range(0, 6)} | dict(zip(pdf[col].astype(int), pdf["products"]))
    bars = ax.bar([str(k) for k in full], list(full.values()),
                  color=["#D55E00" if k <= 2 else "#BBBBBB" for k in full])
    ax.bar_label(bars, fmt="{:,.0f}", fontsize=8)
    ax.set_title(title); ax.set_xlabel("number of regions")
    ax.spines[["top", "right"]].set_visible(False)
axes[0].set_ylabel("products")
plt.tight_layout(); plt.show()
