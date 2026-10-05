# Databricks notebook source
# MAGIC %md
# MAGIC ### Shared setup
# MAGIC Included by every notebook with `%run ./_common`. Defines the widgets and the `cfg` object.
# MAGIC Changing workspace or environment = changing widget values (or job parameters), never code.

# COMMAND ----------

import os
import sys

# Make src/ importable whether the repo is a Git folder or deployed as a bundle.
sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..", "src")))

from tpch_lakehouse import Config  # noqa: E402

dbutils.widgets.text("catalog", "workspace", "Target catalog")
dbutils.widgets.dropdown("env", "dev", ["dev", "preprod", "prod"], "Environment")
dbutils.widgets.text("source_catalog", "samples", "Source catalog")
dbutils.widgets.text("source_schema", "tpch", "Source schema")

cfg = Config(
    catalog=dbutils.widgets.get("catalog"),
    env=dbutils.widgets.get("env"),
    source_catalog=dbutils.widgets.get("source_catalog"),
    source_schema=dbutils.widgets.get("source_schema"),
)
print(f"source : {cfg.source('<table>')}")
for layer in ("bronze", "silver", "gold", "ops"):
    print(f"{layer:<7}: {cfg.schema(layer)}")
