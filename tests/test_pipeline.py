"""End-to-end: three batch days, a rerun, and a blocked bad batch."""

import datetime as dt
import shutil
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from lakehouse.generate import SourceSpec, write_landing
from lakehouse.pipeline import QualityGateError, run_batch
from lakehouse.tables import Tables
from tests.conftest import fingerprint

SPEC = SourceSpec(
    start=dt.date(2026, 1, 1),
    days=3,
    transactions_per_day=4_000,
    customers=1_500,
    merchants=300,
    daily_customer_change_rate=0.02,
)
TABLES = [
    "bronze.transactions",
    "bronze.customers",
    "silver.transactions",
    "silver.customers",
    "silver.transactions_rejected",
    "gold.fct_transactions",
    "gold.merchant_daily",
]


@pytest.fixture
def landing(spark: SparkSession, tmp_path: Path) -> str:
    root = str(tmp_path / "landing")
    for day in SPEC.dates():
        write_landing(spark, SPEC, root, day)
    return root


def snapshot(tables: Tables) -> dict[str, tuple[int, int]]:
    return {name: fingerprint(tables.read(name)) for name in TABLES}


def test_rerunning_any_day_changes_nothing(
    spark: SparkSession, tables: Tables, landing: str
) -> None:
    for day in SPEC.dates():
        run_batch(spark, tables, landing, day)
    before = snapshot(tables)
    assert before["silver.customers"][0] > SPEC.customers  # some customers really changed
    for day in SPEC.dates():
        run_batch(spark, tables, landing, day)
        assert snapshot(tables) == before, f"rerunning {day} changed the warehouse"


def test_gold_has_one_row_per_transaction(
    spark: SparkSession, tables: Tables, landing: str
) -> None:
    for day in SPEC.dates():
        run_batch(spark, tables, landing, day)
    fact = tables.read("gold.fct_transactions")
    assert fact.count() == fact.select("transaction_id").distinct().count()
    assert fact.count() == tables.read("silver.transactions").count()


def test_a_bad_batch_never_reaches_gold(spark: SparkSession, tables: Tables, landing: str) -> None:
    run_batch(spark, tables, landing, SPEC.start)
    published = fingerprint(tables.read("gold.merchant_daily"))
    bad_day = SPEC.dates()[1]
    folder = Path(landing) / "transactions" / f"batch_date={bad_day.isoformat()}"
    shutil.rmtree(folder)
    folder.mkdir(parents=True)
    (folder / "part-0.json").write_text('{"transaction_id": "T1", "amount": "N/A"}\n' * 50)
    with pytest.raises(QualityGateError, match="rejected share"):
        run_batch(spark, tables, landing, bad_day)
    assert fingerprint(tables.read("gold.merchant_daily")) == published
