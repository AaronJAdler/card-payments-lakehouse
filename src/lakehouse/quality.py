"""Checks that gate publishing. Each returns a list of failures; an empty list means it passed."""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


@dataclass(frozen=True)
class Failure:
    check: str
    detail: str


def unique(df: DataFrame, columns: list[str], name: str) -> list[Failure]:
    duplicates = df.groupBy(*columns).count().where("count > 1").count()
    if duplicates:
        return [Failure(f"{name} unique on {', '.join(columns)}", f"{duplicates} duplicate keys")]
    return []


def not_null(df: DataFrame, column: str, name: str) -> list[Failure]:
    missing = df.where(F.col(column).isNull()).count()
    return [Failure(f"{name}.{column} not null", f"{missing} null rows")] if missing else []


def reconciles(
    gold: DataFrame, silver: DataFrame, measure: str, gold_measure: str, name: str
) -> list[Failure]:
    """Gold totals must equal silver totals for the same rows, to the cent."""
    silver_total = silver.agg(F.coalesce(F.sum(measure), F.lit(0))).first()
    gold_total = gold.agg(F.coalesce(F.sum(gold_measure), F.lit(0))).first()
    expected = silver_total[0] if silver_total else 0
    actual = gold_total[0] if gold_total else 0
    if expected != actual:
        return [Failure(f"{name} reconciles to silver", f"silver {expected}, gold {actual}")]
    return []


def rejected_share(valid: int, rejected: int, limit: float) -> list[Failure]:
    total = valid + rejected
    if total and rejected / total > limit:
        return [
            Failure("rejected share", f"{rejected} of {total} rows rejected, limit {limit:.0%}")
        ]
    return []


def scd2_integrity(dim: DataFrame, key: str, name: str) -> list[Failure]:
    failures = []
    open_counts = dim.groupBy(key).agg(F.sum(F.col("is_current").cast("int")).alias("open"))
    bad = open_counts.where("open <> 1").count()
    if bad:
        failures.append(Failure(f"{name} one open version per key", f"{bad} keys"))
    a, b = dim.alias("a"), dim.alias("b")
    overlaps = a.join(
        b,
        (F.col(f"a.{key}") == F.col(f"b.{key}"))
        & (F.col("a.valid_from") < F.col("b.valid_from"))
        & (F.col("a.valid_to") > F.col("b.valid_from")),
    ).count()
    if overlaps:
        failures.append(Failure(f"{name} versions do not overlap", f"{overlaps} overlaps"))
    return failures
