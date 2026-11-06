"""Runs one batch date through bronze, silver and gold. Rerunning a date changes nothing."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from lakehouse import quality
from lakehouse.layers import (
    CUSTOMER_TRACKED,
    conform_customers,
    dedupe_transactions,
    fact_transactions,
    merchant_daily,
    read_landing,
    to_bronze,
    validate_transactions,
)
from lakehouse.tables import Tables

log = logging.getLogger(__name__)
MAX_REJECTED_SHARE = 0.02


class QualityGateError(RuntimeError):
    def __init__(self, stage: str, failures: list[quality.Failure]) -> None:
        self.failures = failures
        detail = "; ".join(f"{f.check}: {f.detail}" for f in failures)
        super().__init__(f"{stage} blocked: {detail}")


@dataclass
class BatchReport:
    batch_date: date
    bronze_rows: dict[str, int] = field(default_factory=dict)
    valid: int = 0
    rejected: int = 0
    affected_event_dates: list[date] = field(default_factory=list)
    customer_changes: int = 0
    gold_published: bool = False


def _landing_exists(spark: SparkSession, root: str, feed: str, batch_date: date) -> bool:
    path = f"{root}/{feed}/batch_date={batch_date.isoformat()}"
    if "://" not in root:
        return Path(path).exists()
    jvm = spark._jvm  # noqa: SLF001
    hadoop_path = jvm.org.apache.hadoop.fs.Path(path)  # type: ignore[union-attr]
    fs = hadoop_path.getFileSystem(spark._jsc.hadoopConfiguration())  # noqa: SLF001
    return bool(fs.exists(hadoop_path))


def ingest_bronze(
    spark: SparkSession, tables: Tables, landing: str, batch_date: date
) -> dict[str, int]:
    counts = {}
    for feed in ("transactions", "customers", "merchants"):
        if not _landing_exists(spark, landing, feed, batch_date):
            continue
        rows = to_bronze(read_landing(spark, landing, feed, batch_date), batch_date)
        tables.replace_partitions(f"bronze.{feed}", rows, "_batch_date", [batch_date])
        counts[feed] = (
            tables.read(f"bronze.{feed}").where(F.col("_batch_date") == batch_date).count()
        )
    return counts


def build_silver(
    spark: SparkSession, tables: Tables, batch_date: date, report: BatchReport
) -> None:
    batch = F.col("_batch_date") == batch_date
    valid, rejected = validate_transactions(tables.read("bronze.transactions").where(batch))
    valid, rejected = valid.cache(), rejected.cache()
    report.valid, report.rejected = valid.count(), rejected.count()
    failures = quality.rejected_share(report.valid, report.rejected, MAX_REJECTED_SHARE)
    if failures:
        raise QualityGateError("silver", failures)

    tables.replace_partitions("silver.transactions_rejected", rejected, "_batch_date", [batch_date])
    deduped = dedupe_transactions(valid)
    report.affected_event_dates = sorted(
        r.event_date for r in deduped.select("event_date").distinct().collect()
    )
    tables.upsert_newer("silver.transactions", deduped, "transaction_id", "version")
    valid.unpersist()
    rejected.unpersist()

    if tables.exists("bronze.customers"):
        changes = conform_customers(tables.read("bronze.customers").where(batch)).cache()
        report.customer_changes = changes.count()
        if report.customer_changes:
            tables.apply_scd2(
                "silver.customers", changes, "customer_id", CUSTOMER_TRACKED, "updated_at"
            )
        changes.unpersist()

    if tables.exists("bronze.merchants"):
        merchants = tables.read("bronze.merchants").where(batch)
        if merchants.limit(1).count():
            tables.overwrite(
                "silver.merchants",
                merchants.select("merchant_id", "merchant_name", "mcc", "state").dropDuplicates(
                    ["merchant_id"]
                ),
            )


def build_gold(tables: Tables, report: BatchReport) -> None:
    """Write, audit, publish: gold is built in a staging schema and copied only if it passes."""
    dates = report.affected_event_dates
    if not dates:
        return
    on_dates = F.col("event_date").isin(dates)
    silver = tables.read("silver.transactions")
    customers = tables.read("silver.customers")
    fact = fact_transactions(silver, customers, tables.read("silver.merchants"), dates)
    tables.replace_partitions("gold_staging.fct_transactions", fact, "event_date", dates)
    staged_fact = tables.read("gold_staging.fct_transactions").where(on_dates)
    tables.replace_partitions(
        "gold_staging.merchant_daily", merchant_daily(staged_fact), "event_date", dates
    )
    staged_daily = tables.read("gold_staging.merchant_daily").where(on_dates)

    approved_silver = silver.where(on_dates & (F.col("status") == "approved"))
    failures = (
        quality.unique(staged_fact, ["transaction_id"], "fct_transactions")
        + quality.not_null(staged_fact, "customer_version_sk", "fct_transactions")
        + quality.unique(staged_daily, ["event_date", "merchant_id"], "merchant_daily")
        + quality.reconciles(
            staged_daily,
            approved_silver,
            "amount_usd_cents",
            "net_sales_usd_cents",
            "merchant_daily",
        )
        + quality.scd2_integrity(customers, "customer_id", "silver.customers")
    )
    if failures:
        raise QualityGateError("gold", failures)
    tables.replace_partitions("gold.fct_transactions", staged_fact, "event_date", dates)
    tables.replace_partitions("gold.merchant_daily", staged_daily, "event_date", dates)
    report.gold_published = True


def run_batch(spark: SparkSession, tables: Tables, landing: str, batch_date: date) -> BatchReport:
    report = BatchReport(batch_date=batch_date)
    report.bronze_rows = ingest_bronze(spark, tables, landing, batch_date)
    if "transactions" not in report.bronze_rows:
        log.warning("%s: no transaction file landed, so nothing to process", batch_date)
        return report
    build_silver(spark, tables, batch_date, report)
    build_gold(tables, report)
    log.info("%s: %s", batch_date, report)
    return report
