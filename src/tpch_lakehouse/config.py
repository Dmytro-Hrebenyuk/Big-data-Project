"""Single place for every name the pipeline uses.

Nothing else in the code base hard-codes a catalog, schema or table name, so moving
to another workspace / environment means changing widget values, not code.

Naming convention:  <catalog>.<project>_<env>_<layer>.<table>
    e.g.            workspace.tpch_dev_silver.customer
"""

from __future__ import annotations

from dataclasses import dataclass

# Load order matters for silver: parents before children (foreign keys).
TABLES = ["region", "nation", "customer", "supplier", "part", "partsupp", "orders", "lineitem"]

LAYERS = ("bronze", "silver", "gold", "ops")


@dataclass(frozen=True)
class Config:
    catalog: str | None = "workspace"      # None -> two-level names (local Spark / hive_metastore)
    env: str = "dev"                       # dev | preprod | prod
    project: str = "tpch"
    source_catalog: str | None = "samples"
    source_schema: str = "tpch"
    table_format: str = "delta"            # "parquet" for local tests (no Delta jars needed)
    apply_constraints: bool = True         # Delta / Unity Catalog constraints (off locally)

    # --- business thresholds (monitoring) -------------------------------------------
    volume_change_threshold: float = 0.05  # |revenue per day vs trailing 3-month average|
    share_shift_threshold: float = 0.01    # |revenue share vs trailing 3-month average| (1 pp)

    def schema(self, layer: str) -> str:
        if layer not in LAYERS:
            raise ValueError(f"Unknown layer {layer!r}; expected one of {LAYERS}")
        name = f"{self.project}_{self.env}_{layer}"
        return f"{self.catalog}.{name}" if self.catalog else name

    def table(self, layer: str, table: str) -> str:
        return f"{self.schema(layer)}.{table}"

    def source(self, table: str) -> str:
        prefix = f"{self.source_catalog}." if self.source_catalog else ""
        return f"{prefix}{self.source_schema}.{table}"

    def names(self) -> dict[str, str]:
        """Placeholders for SQL templates: {bronze}, {silver}, {gold}, {ops}."""
        return {layer: self.schema(layer) for layer in LAYERS}


def create_schemas(spark, cfg: Config) -> None:
    for layer in LAYERS:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.schema(layer)}")


def write_table(df, cfg: Config, layer: str, table: str, mode: str = "overwrite") -> None:
    writer = df.write.format(cfg.table_format).mode(mode)
    if mode == "overwrite" and cfg.table_format == "delta":
        writer = writer.option("overwriteSchema", "true")
    writer.saveAsTable(cfg.table(layer, table))
