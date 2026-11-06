"""One Spark session for the whole run. Set LAKEHOUSE_TEST_DELTA=1 to also test the Delta backend.

Delta's jars come from Maven Central when the session starts, so the Delta run needs network
access to it. CI has that access; the plain run needs nothing beyond the Python packages.
"""

from __future__ import annotations

import os
from collections.abc import Iterator
from pathlib import Path

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from lakehouse.cli import spark_session
from lakehouse.tables import DeltaTables, LocalTables, Tables

WITH_DELTA = os.environ.get("LAKEHOUSE_TEST_DELTA") == "1"
BACKENDS = ["local", "delta"] if WITH_DELTA else ["local"]


@pytest.fixture(scope="session")
def spark(tmp_path_factory: pytest.TempPathFactory) -> Iterator[SparkSession]:
    warehouse = str(tmp_path_factory.mktemp("warehouse"))
    session = spark_session(local=True, delta=WITH_DELTA, warehouse=warehouse)
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture(params=BACKENDS)
def tables(request: pytest.FixtureRequest, spark: SparkSession, tmp_path: Path) -> Iterator[Tables]:
    if request.param == "local":
        yield LocalTables(spark, str(tmp_path / "tables"))
        return
    # Each test gets its own schemas, so Delta tests cannot see each other's tables.
    prefix = f"t{abs(hash(request.node.nodeid)) % 10**8}"
    yield PrefixedDelta(spark, prefix)
    for db in spark.catalog.listDatabases():
        if db.name.startswith(prefix):
            spark.sql(f"DROP SCHEMA {db.name} CASCADE")


class PrefixedDelta(DeltaTables):
    """Maps bronze.x to <prefix>_bronze.x so tests stay isolated in one metastore."""

    def __init__(self, spark: SparkSession, prefix: str) -> None:
        super().__init__(spark, None)
        self.prefix = prefix

    def _full(self, name: str) -> str:
        return f"{self.prefix}_{name}"


def fingerprint(df: DataFrame, ignore: tuple[str, ...] = ("_ingested_at",)) -> tuple[int, int]:
    """Row count and an order-independent content hash."""
    columns = [c for c in df.columns if c not in ignore]
    row = df.select(
        F.count("*"),
        F.coalesce(F.sum(F.pmod(F.xxhash64(*columns), F.lit(1_000_000_007))), F.lit(0)),
    ).first()
    assert row is not None
    return int(row[0]), int(row[1])
