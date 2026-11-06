"""Entry points. Databricks job tasks call these through the wheel's `lakehouse` command."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
from dataclasses import asdict
from importlib import resources

from pyspark.sql import SparkSession

from lakehouse.generate import SourceSpec, write_landing
from lakehouse.pipeline import run_batch
from lakehouse.tables import DeltaTables, LocalTables, Tables


def spark_session(local: bool, delta: bool, warehouse: str | None = None) -> SparkSession:
    if not local:
        return SparkSession.builder.getOrCreate()  # Databricks supplies the session
    builder = (
        SparkSession.builder.master("local[2]")
        .appName("lakehouse")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
    )
    if warehouse:
        builder = builder.config("spark.sql.warehouse.dir", warehouse)
    if delta:
        from delta import configure_spark_with_delta_pip

        builder = configure_spark_with_delta_pip(
            builder.config(
                "spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"
            ).config(
                "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
            )
        )
    return builder.getOrCreate()


def make_tables(spark: SparkSession, backend: str, catalog: str | None, root: str | None) -> Tables:
    if backend == "delta":
        return DeltaTables(spark, catalog)
    if not root:
        raise SystemExit("--tables-root is required for the local backend")
    return LocalTables(spark, root)


def spec_from(args: argparse.Namespace) -> SourceSpec:
    return SourceSpec(
        start=args.start,
        days=args.days,
        transactions_per_day=args.rows,
        customers=args.customers,
        merchants=args.merchants,
    )


MAINTAINED_TABLES = (
    "silver.transactions",
    "silver.customers",
    "gold.fct_transactions",
    "gold.merchant_daily",
)


def governance_statements(catalog: str) -> list[str]:
    """The packaged governance SQL, one statement per blank-line-separated block."""
    if not catalog.replace("_", "").isalnum():
        raise SystemExit(f"Catalog name {catalog!r} must be letters, digits and underscores.")
    text = resources.files("lakehouse").joinpath("sql/governance.sql").read_text()
    statements = []
    for block in text.split("\n\n"):
        lines = [line for line in block.splitlines() if not line.lstrip().startswith("--")]
        statement = "\n".join(lines).strip()
        if statement:
            statements.append(statement.replace("{catalog}", catalog))
    return statements


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
    parser = argparse.ArgumentParser(prog="lakehouse")
    commands = parser.add_subparsers(dest="command", required=True)

    def source_args(p: argparse.ArgumentParser) -> None:
        p.add_argument("--start", type=dt.date.fromisoformat, default=dt.date(2026, 1, 1))
        p.add_argument("--days", type=int, default=5)
        p.add_argument("--rows", type=int, default=20_000, help="transactions per day")
        p.add_argument("--customers", type=int, default=5_000)
        p.add_argument("--merchants", type=int, default=2_000)

    def table_args(p: argparse.ArgumentParser) -> None:
        p.add_argument("--backend", choices=["local", "delta"], default="local")
        p.add_argument("--catalog", help="Unity Catalog catalog for the delta backend")
        p.add_argument("--tables-root", help="folder for the local backend")
        p.add_argument(
            "--local-spark", action="store_true", help="start Spark here, not on Databricks"
        )

    gen = commands.add_parser("generate", help="write synthetic landing files")
    gen.add_argument("--landing", required=True)
    source_args(gen)
    gen.add_argument("--local-spark", action="store_true")

    batch = commands.add_parser("run-batch", help="process one batch date")
    batch.add_argument("--landing", required=True)
    batch.add_argument("--date", type=dt.date.fromisoformat, required=True)
    table_args(batch)

    demo = commands.add_parser("local-demo", help="generate and process a small window locally")
    demo.add_argument("--dir", default="data/local")
    demo.add_argument("--backend", choices=["local", "delta"], default="local")
    source_args(demo)

    gov = commands.add_parser("apply-governance", help="apply column masks and row filters")
    gov.add_argument("--catalog", required=True)

    maint = commands.add_parser("maintain", help="compact files and remove old ones")
    maint.add_argument("--catalog", required=True)

    args = parser.parse_args()
    if args.command == "apply-governance":
        spark = spark_session(local=False, delta=True)
        for statement in governance_statements(args.catalog):
            spark.sql(statement)
    elif args.command == "maintain":
        spark = spark_session(local=False, delta=True)
        for table in MAINTAINED_TABLES:
            name = f"{args.catalog}.{table}"
            if spark.catalog.tableExists(name):
                spark.sql(f"OPTIMIZE {name}")
                spark.sql(f"VACUUM {name}")  # default 7-day retention keeps time travel working
    elif args.command == "generate":
        spark = spark_session(args.local_spark, delta=False)
        spec = spec_from(args)
        for day in spec.dates():
            write_landing(spark, spec, args.landing, day)
    elif args.command == "run-batch":
        spark = spark_session(args.local_spark, delta=args.backend == "delta")
        tables = make_tables(spark, args.backend, args.catalog, args.tables_root)
        print(json.dumps(asdict(run_batch(spark, tables, args.landing, args.date)), default=str))
    elif args.command == "local-demo":
        spark = spark_session(
            True, delta=args.backend == "delta", warehouse=f"{args.dir}/warehouse"
        )
        spec = spec_from(args)
        landing = f"{args.dir}/landing"
        tables = make_tables(spark, args.backend, None, f"{args.dir}/tables")
        for day in spec.dates():
            write_landing(spark, spec, landing, day)
            print(json.dumps(asdict(run_batch(spark, tables, landing, day)), default=str))


if __name__ == "__main__":
    main()
