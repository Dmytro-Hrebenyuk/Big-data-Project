"""Silver: typed, cleanly named, 3NF tables with enforced data quality.

Every table is described declaratively in ``SPECS`` (columns, primary key, NOT NULL,
CHECK rules, foreign keys). One generic function turns a spec into two tables:

    <silver>.<table>              rows that pass every rule
    <silver>.<table>_quarantine   rows that fail, with the list of rules they failed

Nothing is silently dropped: bronze rows = silver rows + quarantine rows (validated later).

Foreign keys are checked against the *clean* silver parent, in dependency order, so a
child of a quarantined parent is quarantined too. That is what makes the inner joins
in gold safe.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from tpch_lakehouse.config import TABLES, Config, create_schemas, write_table


@dataclass(frozen=True)
class ForeignKey:
    columns: tuple[str, ...]
    parent: str
    parent_columns: tuple[str, ...]

    @property
    def name(self) -> str:
        return f"fk_{'_'.join(self.columns)}__{self.parent}"


@dataclass(frozen=True)
class TableSpec:
    # source column -> (silver column, type)
    columns: dict[str, tuple[str, str]]
    primary_key: tuple[str, ...]
    not_null: tuple[str, ...] = ()
    checks: dict[str, str] = field(default_factory=dict)   # rule name -> SQL boolean
    foreign_keys: tuple[ForeignKey, ...] = ()


MONEY = "decimal(18,2)"

SPECS: dict[str, TableSpec] = {
    "region": TableSpec(
        columns={
            "r_regionkey": ("region_key", "bigint"),
            "r_name": ("name", "string"),
            "r_comment": ("comment", "string"),
        },
        primary_key=("region_key",),
        not_null=("name",),
        checks={"name_not_blank": "length(name) > 0"},
    ),
    "nation": TableSpec(
        columns={
            "n_nationkey": ("nation_key", "bigint"),
            "n_name": ("name", "string"),
            "n_regionkey": ("region_key", "bigint"),
            "n_comment": ("comment", "string"),
        },
        primary_key=("nation_key",),
        not_null=("name", "region_key"),
        checks={"name_not_blank": "length(name) > 0"},
        foreign_keys=(ForeignKey(("region_key",), "region", ("region_key",)),),
    ),
    "customer": TableSpec(
        columns={
            "c_custkey": ("customer_key", "bigint"),
            "c_name": ("name", "string"),
            "c_address": ("address", "string"),
            "c_nationkey": ("nation_key", "bigint"),
            "c_phone": ("phone", "string"),
            "c_acctbal": ("account_balance", MONEY),
            "c_mktsegment": ("market_segment", "string"),
            "c_comment": ("comment", "string"),
        },
        primary_key=("customer_key",),
        not_null=("name", "nation_key", "account_balance"),
        foreign_keys=(ForeignKey(("nation_key",), "nation", ("nation_key",)),),
    ),
    "supplier": TableSpec(
        columns={
            "s_suppkey": ("supplier_key", "bigint"),
            "s_name": ("name", "string"),
            "s_address": ("address", "string"),
            "s_nationkey": ("nation_key", "bigint"),
            "s_phone": ("phone", "string"),
            "s_acctbal": ("account_balance", MONEY),
            "s_comment": ("comment", "string"),
        },
        primary_key=("supplier_key",),
        not_null=("name", "nation_key"),
        foreign_keys=(ForeignKey(("nation_key",), "nation", ("nation_key",)),),
    ),
    "part": TableSpec(
        columns={
            "p_partkey": ("part_key", "bigint"),
            "p_name": ("name", "string"),
            "p_mfgr": ("manufacturer", "string"),
            "p_brand": ("brand", "string"),
            "p_type": ("type", "string"),
            "p_size": ("size", "int"),
            "p_container": ("container", "string"),
            "p_retailprice": ("retail_price", MONEY),
            "p_comment": ("comment", "string"),
        },
        primary_key=("part_key",),
        not_null=("name", "retail_price"),
        checks={"retail_price_positive": "retail_price > 0"},
    ),
    "partsupp": TableSpec(
        columns={
            "ps_partkey": ("part_key", "bigint"),
            "ps_suppkey": ("supplier_key", "bigint"),
            "ps_availqty": ("available_quantity", "int"),
            "ps_supplycost": ("supply_cost", MONEY),
            "ps_comment": ("comment", "string"),
        },
        primary_key=("part_key", "supplier_key"),
        checks={"supply_cost_not_negative": "supply_cost >= 0"},
        foreign_keys=(
            ForeignKey(("part_key",), "part", ("part_key",)),
            ForeignKey(("supplier_key",), "supplier", ("supplier_key",)),
        ),
    ),
    "orders": TableSpec(
        columns={
            "o_orderkey": ("order_key", "bigint"),
            "o_custkey": ("customer_key", "bigint"),
            "o_orderstatus": ("order_status", "string"),
            "o_totalprice": ("total_price", MONEY),
            "o_orderdate": ("order_date", "date"),
            "o_orderpriority": ("order_priority", "string"),
            "o_clerk": ("clerk", "string"),
            "o_shippriority": ("ship_priority", "int"),
            "o_comment": ("comment", "string"),
        },
        primary_key=("order_key",),
        not_null=("customer_key", "order_date"),
        foreign_keys=(ForeignKey(("customer_key",), "customer", ("customer_key",)),),
    ),
    "lineitem": TableSpec(
        columns={
            "l_orderkey": ("order_key", "bigint"),
            "l_linenumber": ("line_number", "int"),
            "l_partkey": ("part_key", "bigint"),
            "l_suppkey": ("supplier_key", "bigint"),
            "l_quantity": ("quantity", MONEY),
            "l_extendedprice": ("extended_price", MONEY),
            "l_discount": ("discount", MONEY),
            "l_tax": ("tax", MONEY),
            "l_returnflag": ("return_flag", "string"),
            "l_linestatus": ("line_status", "string"),
            "l_shipdate": ("ship_date", "date"),
            "l_commitdate": ("commit_date", "date"),
            "l_receiptdate": ("receipt_date", "date"),
            "l_shipinstruct": ("ship_instructions", "string"),
            "l_shipmode": ("ship_mode", "string"),
            "l_comment": ("comment", "string"),
        },
        primary_key=("order_key", "line_number"),
        not_null=("part_key", "supplier_key", "quantity", "extended_price", "discount"),
        checks={
            "quantity_positive": "quantity > 0",
            "extended_price_positive": "extended_price > 0",
            "discount_between_0_and_1": "discount >= 0 AND discount <= 1",
        },
        foreign_keys=(
            ForeignKey(("order_key",), "orders", ("order_key",)),
            # Composite FK: a line item must reference a (part, supplier) pair that exists.
            ForeignKey(("part_key", "supplier_key"), "partsupp", ("part_key", "supplier_key")),
        ),
    ),
}


# --------------------------------------------------------------------------------------
# Transformation
# --------------------------------------------------------------------------------------
def _typed(bronze: DataFrame, spec: TableSpec) -> DataFrame:
    """Rename + cast. Strings are trimmed; nothing else about the values changes."""
    cols = []
    for source_col, (name, dtype) in spec.columns.items():
        col = F.col(source_col)
        col = F.trim(col) if dtype == "string" else col.cast(dtype)
        cols.append(col.alias(name))
    return bronze.select(*cols, F.col("_ingested_at"))


def evaluate_rules(spark, cfg: Config, table: str, bronze_df: DataFrame | None = None) -> DataFrame:
    """Return the typed rows plus ``_failed_rules`` (empty array = clean row).

    ``bronze_df`` lets a caller evaluate the rules on other input than the bronze table
    (used to demonstrate the rules on deliberately broken rows).
    """
    spec = SPECS[table]
    df = _typed(bronze_df if bronze_df is not None else spark.table(cfg.table("bronze", table)), spec)

    rules: list[tuple[str, F.Column]] = []   # (rule name, condition that must be TRUE)

    # 1. primary key: not null + unique (keep the latest ingested copy, quarantine the rest)
    for col in spec.primary_key:
        rules.append((f"pk_not_null_{col}", F.col(col).isNotNull()))
    latest_first = Window.partitionBy(*spec.primary_key).orderBy(F.col("_ingested_at").desc())
    df = df.withColumn("_rn", F.row_number().over(latest_first))
    rules.append(("pk_unique", F.col("_rn") == 1))

    # 2. NOT NULL and CHECK rules
    for col in spec.not_null:
        rules.append((f"not_null_{col}", F.col(col).isNotNull()))
    for name, expr in spec.checks.items():
        rules.append((name, F.coalesce(F.expr(expr), F.lit(False))))

    # 3. foreign keys against the already-built clean silver parent
    for i, fk in enumerate(spec.foreign_keys):
        hit = f"_fk{i}_hit"
        parent = spark.table(cfg.table("silver", fk.parent)).select(
            *[F.col(p).alias(f"_fk{i}_{p}") for p in fk.parent_columns],
            F.lit(True).alias(hit),
        )
        cond = [df[c] == parent[f"_fk{i}_{p}"] for c, p in zip(fk.columns, fk.parent_columns)]
        df = df.join(parent, on=cond, how="left")   # parent key is unique -> no fan-out
        rules.append((fk.name, F.col(hit).isNotNull()))

    failed = F.filter(
        F.array(*[F.when(~cond, F.lit(name)) for name, cond in rules]),
        lambda x: x.isNotNull(),
    )
    silver_cols = [name for name, _ in spec.columns.values()]
    return df.select(*silver_cols, failed.alias("_failed_rules"))


def build_table(spark, cfg: Config, table: str) -> None:
    evaluated = evaluate_rules(spark, cfg, table)
    is_clean = F.size("_failed_rules") == 0

    write_table(evaluated.filter(is_clean).drop("_failed_rules"), cfg, "silver", table)
    write_table(
        evaluated.filter(~is_clean).withColumn("_quarantined_at", F.current_timestamp()),
        cfg, "silver", f"{table}_quarantine",
    )


# --------------------------------------------------------------------------------------
# Constraints (Delta / Unity Catalog)
# --------------------------------------------------------------------------------------
def constraint_statements(cfg: Config) -> list[str]:
    """DDL that documents and enforces the model on the tables themselves.

    NOT NULL and CHECK are enforced by Delta on every future write.
    PRIMARY KEY / FOREIGN KEY are informational in Unity Catalog (not enforced by the
    engine) - they drive the ER diagram in Catalog Explorer; the pipeline rules above
    are what actually enforce them.
    """
    stmts: list[str] = []
    t = lambda name: cfg.table("silver", name)  # noqa: E731

    # Drop old FKs / PKs first so the function is re-runnable.
    for table in reversed(TABLES):
        for fk in SPECS[table].foreign_keys:
            stmts.append(f"ALTER TABLE {t(table)} DROP CONSTRAINT IF EXISTS {fk.name}")
    for table in TABLES:
        stmts.append(f"ALTER TABLE {t(table)} DROP CONSTRAINT IF EXISTS pk_{table}")

    for table in TABLES:
        spec = SPECS[table]
        for col in (*spec.primary_key, *spec.not_null):
            stmts.append(f"ALTER TABLE {t(table)} ALTER COLUMN {col} SET NOT NULL")
        for name, expr in spec.checks.items():
            stmts.append(f"ALTER TABLE {t(table)} DROP CONSTRAINT IF EXISTS chk_{name}")
            stmts.append(f"ALTER TABLE {t(table)} ADD CONSTRAINT chk_{name} CHECK ({expr})")
        stmts.append(
            f"ALTER TABLE {t(table)} ADD CONSTRAINT pk_{table} "
            f"PRIMARY KEY ({', '.join(spec.primary_key)})"
        )
    for table in TABLES:
        for fk in SPECS[table].foreign_keys:
            stmts.append(
                f"ALTER TABLE {t(table)} ADD CONSTRAINT {fk.name} "
                f"FOREIGN KEY ({', '.join(fk.columns)}) "
                f"REFERENCES {t(fk.parent)} ({', '.join(fk.parent_columns)})"
            )
    return stmts


def apply_constraints(spark, cfg: Config) -> list[tuple[str, str]]:
    """Run every constraint statement; report instead of failing.

    PK/FK need Unity Catalog. On a workspace without it those statements are reported
    as SKIPPED - the data is still protected by the pipeline rules and validation.
    """
    results = []
    for stmt in constraint_statements(cfg):
        try:
            spark.sql(stmt)
            results.append((stmt, "OK"))
        except Exception as exc:  # noqa: BLE001 - report every failure, keep going
            results.append((stmt, f"SKIPPED: {str(exc).splitlines()[0][:200]}"))
    return results


def run(spark, cfg: Config) -> list[tuple[str, str]]:
    create_schemas(spark, cfg)
    for table in TABLES:
        build_table(spark, cfg, table)
        print(f"silver: built {cfg.table('silver', table)} (+ _quarantine)")
    return apply_constraints(spark, cfg) if cfg.apply_constraints else []


def summary(spark, cfg: Config) -> DataFrame:
    """Row counts per table: bronze vs silver vs quarantine."""
    rows = []
    for table in TABLES:
        rows.append((
            table,
            spark.table(cfg.table("bronze", table)).count(),
            spark.table(cfg.table("silver", table)).count(),
            spark.table(cfg.table("silver", f"{table}_quarantine")).count(),
        ))
    return spark.createDataFrame(rows, "table string, bronze_rows long, silver_rows long, quarantined_rows long")
