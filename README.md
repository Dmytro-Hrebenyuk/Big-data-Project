# TPC-H Lakehouse — Regional Manager

Group Assignment 1. A bronze → silver → gold lakehouse on Databricks for the TPC-H data
(`samples.tpch`), built to answer the Regional Manager's questions.

- **Presentation:** `<link>`
- **Team:** `Shumylovych Matvii, Hrebenyuk Dmytro`

## Contents

| File | What it is |
|---|---|
| `tpch_lakehouse_regional.ipynb` | The whole solution in one notebook |
| `docs/` | Silver ER diagram and result charts |
| `pyproject.toml`, `uv.lock` | uv project |

## How to run

1. In Databricks, add this repo as a Git folder (or import the notebook).
2. Open `tpch_lakehouse_regional.ipynb` and choose **Run all**.

Tables are created as `<catalog>.tpch_<env>_<layer>.<table>`. The catalog, environment and
source are notebook widgets (defaults: `workspace`, `dev`, `samples.tpch`), so the notebook
runs in any workspace without code changes. Re-running is safe.

## Layers

| Layer | Content |
|---|---|
| Bronze | The 8 source tables as is, plus `_ingested_at` and `_source_table` |
| Silver | The same 8 entities in 3NF, typed and renamed, with quality rules. Rows that fail a rule go to `<table>_quarantine` |
| Gold | Star schema (`fact_sales` and four dimensions) and three aggregates by region, month and product |

![Silver ER diagram](docs/er_silver.png)

## Definitions

| Term | Definition |
|---|---|
| Net revenue | `extended_price × (1 − discount)`, tax excluded |
| Region of a sale | The customer's region |
| Cross-region sale | Customer region differs from supplier region |
| Period | Order date, complete months only |

## Validation

43 checks across all three layers, each returning a count of violations. The notebook fails
if any check is violated.

- **Geography:** every customer and supplier resolves to a valid nation, every nation to a valid region.
- **Revenue:** regional revenue sums to the total and is identical in bronze, silver and gold.
- **Completeness:** no rows lost or duplicated between layers.

## Monitoring

Revenue by region per month, in `agg_region_revenue_monthly`. An alert fires when a region's
revenue per day moves 5% or more from its trailing three-month average, or when its rank
changes together with a one-point move in revenue share.
