"""Behavior both storage backends must share. With LAKEHOUSE_TEST_DELTA=1 each test runs twice."""

import datetime as dt

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from lakehouse.tables import Tables

D1, D2, D3 = dt.date(2026, 1, 1), dt.date(2026, 1, 2), dt.date(2026, 1, 3)


def rows(spark: SparkSession, data: list[tuple[object, ...]], schema: str) -> DataFrame:
    return spark.createDataFrame(data, schema)


def as_set(df: DataFrame, *cols: str) -> set[tuple[object, ...]]:
    return {tuple(r) for r in df.select(*cols).collect()}


def test_replace_partitions_touches_only_named_dates(spark: SparkSession, tables: Tables) -> None:
    schema = "d date, v int"
    tables.replace_partitions("s.t", rows(spark, [(D1, 1), (D2, 2)], schema), "d", [D1, D2])
    tables.replace_partitions("s.t", rows(spark, [(D2, 20), (D3, 30)], schema), "d", [D2, D3])
    assert as_set(tables.read("s.t"), "d", "v") == {(D1, 1), (D2, 20), (D3, 30)}


def test_upsert_newer_ignores_stale_versions(spark: SparkSession, tables: Tables) -> None:
    schema = "k string, version int, status string"
    tables.upsert_newer(
        "s.u", rows(spark, [("a", 1, "approved"), ("b", 2, "reversed")], schema), "k", "version"
    )
    tables.upsert_newer(
        "s.u",
        rows(spark, [("a", 2, "reversed"), ("b", 1, "approved"), ("c", 1, "approved")], schema),
        "k",
        "version",
    )
    tables.upsert_newer("s.u", rows(spark, [("a", 2, "reversed")], schema), "k", "version")
    assert as_set(tables.read("s.u"), "k", "version", "status") == {
        ("a", 2, "reversed"),
        ("b", 2, "reversed"),
        ("c", 1, "approved"),
    }


def customer(spark: SparkSession, data: list[tuple[str, str, str]]) -> DataFrame:
    return rows(spark, data, "customer_id string, state string, updated_at string").withColumn(
        "updated_at", F.col("updated_at").cast("timestamp")
    )


def scd2(tables: Tables, changes: DataFrame) -> None:
    tables.apply_scd2("s.c", changes, "customer_id", ["state"], "updated_at")


def history(tables: Tables) -> set[tuple[object, ...]]:
    df = tables.read("s.c").select(
        "customer_id",
        "state",
        F.date_format("valid_from", "yyyy-MM-dd").alias("f"),
        F.date_format("valid_to", "yyyy-MM-dd").alias("t"),
        "is_current",
    )
    return as_set(df, "customer_id", "state", "f", "t", "is_current")


def test_scd2_closes_old_versions_and_ignores_replays(spark: SparkSession, tables: Tables) -> None:
    day1 = customer(spark, [("a", "CA", "2026-01-01 06:00:00"), ("b", "TX", "2026-01-01 06:00:00")])
    day2 = customer(spark, [("a", "NV", "2026-01-02 06:00:00"), ("c", "OR", "2026-01-02 06:00:00")])
    scd2(tables, day1)
    scd2(tables, day2)
    expected = {
        ("a", "CA", "1900-01-01", "2026-01-02", False),
        ("a", "NV", "2026-01-02", "9999-12-31", True),
        ("b", "TX", "1900-01-01", "9999-12-31", True),
        ("c", "OR", "1900-01-01", "9999-12-31", True),
    }
    assert history(tables) == expected
    scd2(tables, day2)  # replaying a batch must change nothing
    scd2(tables, day1)  # nor may replaying an older one
    assert history(tables) == expected


def test_scd2_ignores_records_with_no_change(spark: SparkSession, tables: Tables) -> None:
    scd2(tables, customer(spark, [("a", "CA", "2026-01-01 06:00:00")]))
    scd2(tables, customer(spark, [("a", "CA", "2026-01-03 06:00:00")]))
    assert history(tables) == {("a", "CA", "1900-01-01", "9999-12-31", True)}
