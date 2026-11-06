"""Table storage behind one contract, with two implementations.

DeltaTables is what runs on Databricks: Delta Lake MERGE and replaceWhere, so each write is one
atomic commit. LocalTables lets the pipeline run on a laptop with no Delta jars. It writes each new
table version to its own folder and then switches a pointer file, which keeps writes atomic the
same way a table format does, at the cost of rewriting the whole table.

tests/test_tables_contract.py runs the same behavioral tests against both, so the local stand-in
cannot drift from what production does.
"""

from __future__ import annotations

import os
import uuid
from collections.abc import Sequence
from datetime import date
from pathlib import Path
from typing import Protocol

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

SCD2_COLUMNS = ("valid_from", "valid_to", "is_current")
OPEN_VALID_TO = "9999-12-31 00:00:00"


class Tables(Protocol):
    spark: SparkSession

    def exists(self, name: str) -> bool: ...

    def read(self, name: str) -> DataFrame: ...

    def overwrite(self, name: str, df: DataFrame) -> None: ...

    def replace_partitions(
        self, name: str, df: DataFrame, column: str, values: Sequence[date]
    ) -> None: ...

    def upsert_newer(self, name: str, df: DataFrame, key: str, version: str) -> None: ...

    def apply_scd2(
        self, name: str, changes: DataFrame, key: str, tracked: Sequence[str], effective: str
    ) -> None: ...


def _differs(tracked: Sequence[str], left: str, right: str) -> Column:
    """True when any tracked column differs, treating two nulls as equal."""
    return ~F.expr(" AND ".join(f"{left}.`{c}` <=> {right}.`{c}`" for c in tracked))


def scd2_staged_changes(
    changes: DataFrame, current: DataFrame | None, key: str, tracked: Sequence[str], effective: str
) -> DataFrame:
    """Builds the MERGE source for an SCD type 2 update.

    Each changed key appears twice. The row with merge_key set closes the open version, and the
    row with a null merge_key never matches, so it inserts the new version. New keys appear once.

    Changes no newer than the open version are dropped, so replaying a batch changes nothing.
    """
    latest = (
        changes.withColumn(
            "_rank", F.row_number().over(Window.partitionBy(key).orderBy(F.col(effective).desc()))
        )
        .where("_rank = 1")
        .drop("_rank")
    )
    if current is None:
        return latest.withColumn("merge_key", F.col(key))
    open_rows = current.where("is_current").alias("t")
    joined = latest.alias("s").join(open_rows, F.col(f"s.{key}") == F.col(f"t.{key}"), "left")
    is_new = F.col(f"t.{key}").isNull()
    is_change = (
        F.col(f"t.{key}").isNotNull()
        & (F.col(f"s.{effective}") > F.col("t.valid_from"))
        & _differs(tracked, "s", "t")
    )
    source_columns = [F.col(f"s.`{c}`") for c in latest.columns]
    relevant = joined.where(is_new | is_change)
    closes = relevant.select(*source_columns, F.col(f"s.{key}").alias("merge_key"))
    inserts = relevant.where(is_change).select(
        *source_columns, F.lit(None).cast("string").alias("merge_key")
    )
    return closes.unionByName(inserts)


def scd2_new_row(staged: DataFrame, effective: str, first_load: bool) -> DataFrame:
    """The inserted version for each staged row, as the target table stores it."""
    valid_from = F.col(effective)
    if first_load:
        # A customer's first record is treated as valid for all earlier history.
        valid_from = F.lit("1900-01-01 00:00:00").cast("timestamp")
    return staged.drop("merge_key").withColumns(
        {
            "valid_from": valid_from.cast("timestamp"),
            "valid_to": F.lit(OPEN_VALID_TO).cast("timestamp"),
            "is_current": F.lit(True),
        }
    )


class LocalTables:
    """Parquet folders with an atomic pointer to the current version of each table."""

    def __init__(self, spark: SparkSession, root: str) -> None:
        self.spark = spark
        self.root = Path(root)

    def _pointer(self, name: str) -> Path:
        return self.root / name.replace(".", "/") / "_current"

    def exists(self, name: str) -> bool:
        return self._pointer(name).exists()

    def read(self, name: str) -> DataFrame:
        version = self._pointer(name).read_text().strip()
        return self.spark.read.parquet(str(self._pointer(name).parent / version))

    def overwrite(self, name: str, df: DataFrame) -> None:
        pointer = self._pointer(name)
        pointer.parent.mkdir(parents=True, exist_ok=True)
        version = f"v-{uuid.uuid4().hex[:12]}"
        df.write.parquet(str(pointer.parent / version))
        tmp = pointer.with_suffix(".tmp")
        tmp.write_text(version)
        os.replace(tmp, pointer)  # readers see the old version or the new one, never a mix

    def replace_partitions(
        self, name: str, df: DataFrame, column: str, values: Sequence[date]
    ) -> None:
        if not self.exists(name):
            self.overwrite(name, df)
            return
        kept = self.read(name).where(~F.col(column).isin(list(values)))
        self.overwrite(name, kept.unionByName(df, allowMissingColumns=True))

    def upsert_newer(self, name: str, df: DataFrame, key: str, version: str) -> None:
        if not self.exists(name):
            self.overwrite(name, df)
            return
        target = self.read(name)
        newer = (
            df.alias("s")
            .join(target.alias("t"), key, "left")
            .where(F.col(f"t.{version}").isNull() | (F.col(f"s.{version}") > F.col(f"t.{version}")))
            .select("s.*")
        )
        kept = target.join(newer.select(key), key, "left_anti")
        self.overwrite(name, kept.unionByName(newer))

    def apply_scd2(
        self, name: str, changes: DataFrame, key: str, tracked: Sequence[str], effective: str
    ) -> None:
        first_load = not self.exists(name)
        current = None if first_load else self.read(name)
        staged = scd2_staged_changes(changes, current, key, tracked, effective).cache()
        if current is None:
            self.overwrite(name, scd2_new_row(staged, effective, first_load=True))
            return
        closing = staged.where(F.col("merge_key").isNotNull()).select(
            F.col("merge_key").alias(key), F.col(effective).alias("_closed_at")
        )
        updated = (
            current.alias("t")
            .join(closing.alias("c"), key, "left")
            .withColumn("_close", F.col("t.is_current") & F.col("c._closed_at").isNotNull())
            .withColumn(
                "valid_to",
                F.when(F.col("_close"), F.col("_closed_at")).otherwise(F.col("valid_to")),
            )
            .withColumn(
                "is_current", F.when(F.col("_close"), F.lit(False)).otherwise(F.col("is_current"))
            )
            .select(*current.columns)
        )
        open_keys = current.where("is_current").select(key)
        brand_new = staged.where(F.col("merge_key").isNotNull()).join(open_keys, key, "left_anti")
        inserts = scd2_new_row(staged.where(F.col("merge_key").isNull()), effective, False)
        firsts = scd2_new_row(brand_new, effective, first_load=True)
        self.overwrite(name, updated.unionByName(inserts).unionByName(firsts))
        staged.unpersist()


class DeltaTables:
    """Delta Lake tables addressed as schema.table, inside a Unity Catalog catalog if given."""

    def __init__(self, spark: SparkSession, catalog: str | None = None) -> None:
        self.spark = spark
        self.catalog = catalog

    def _full(self, name: str) -> str:
        return f"{self.catalog}.{name}" if self.catalog else name

    def exists(self, name: str) -> bool:
        return bool(self.spark.catalog.tableExists(self._full(name)))

    def read(self, name: str) -> DataFrame:
        return self.spark.read.table(self._full(name))

    def _create(self, name: str, df: DataFrame) -> None:
        schema = self._full(name).rsplit(".", 1)[0]
        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        df.write.format("delta").saveAsTable(self._full(name))

    def overwrite(self, name: str, df: DataFrame) -> None:
        if not self.exists(name):
            self._create(name, df)
            return
        df.write.format("delta").mode("overwrite").option("overwriteSchema", "false").saveAsTable(
            self._full(name)
        )

    def replace_partitions(
        self, name: str, df: DataFrame, column: str, values: Sequence[date]
    ) -> None:
        if not self.exists(name):
            self._create(name, df)
            return
        literals = ", ".join(f"DATE'{v.isoformat()}'" for v in values)
        (
            df.write.format("delta")
            .mode("overwrite")
            .option("replaceWhere", f"{column} IN ({literals})")
            .saveAsTable(self._full(name))
        )

    def upsert_newer(self, name: str, df: DataFrame, key: str, version: str) -> None:
        if not self.exists(name):
            self._create(name, df)
            return
        from delta.tables import DeltaTable

        (
            DeltaTable.forName(self.spark, self._full(name))
            .alias("t")
            .merge(df.alias("s"), f"t.{key} = s.{key}")
            .whenMatchedUpdateAll(condition=f"s.{version} > t.{version}")
            .whenNotMatchedInsertAll()
            .execute()
        )

    def apply_scd2(
        self, name: str, changes: DataFrame, key: str, tracked: Sequence[str], effective: str
    ) -> None:
        if not self.exists(name):
            staged = scd2_staged_changes(changes, None, key, tracked, effective)
            self._create(name, scd2_new_row(staged, effective, first_load=True))
            return
        from delta.tables import DeltaTable

        target = DeltaTable.forName(self.spark, self._full(name))
        staged = scd2_staged_changes(changes, target.toDF(), key, tracked, effective)
        # Keys with no open version must start at the beginning of history, like the first load.
        valid_from = (
            f"CASE WHEN s.merge_key IS NULL THEN s.{effective} "
            "ELSE TIMESTAMP'1900-01-01 00:00:00' END"
        )
        insert_values: dict[str, str | Column] = {c: f"s.`{c}`" for c in changes.columns}
        insert_values |= {
            "valid_from": valid_from,
            "valid_to": f"TIMESTAMP'{OPEN_VALID_TO}'",
            "is_current": "true",
        }
        (
            target.alias("t")
            .merge(staged.alias("s"), f"t.{key} = s.merge_key AND t.is_current")
            .whenMatchedUpdate(set={"is_current": "false", "valid_to": f"s.{effective}"})
            .whenNotMatchedInsert(values=insert_values)
            .execute()
        )
