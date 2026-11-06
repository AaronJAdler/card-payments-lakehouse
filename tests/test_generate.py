import datetime as dt

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from lakehouse.generate import SourceSpec, customer_changes, transactions
from tests.conftest import fingerprint

SPEC = SourceSpec(
    start=dt.date(2026, 1, 1), days=3, transactions_per_day=40_000, customers=4_000, merchants=2_000
)
DAY2 = dt.date(2026, 1, 2)


def test_same_inputs_give_identical_data(spark: SparkSession) -> None:
    first = fingerprint(transactions(spark, SPEC, DAY2))
    assert fingerprint(transactions(spark, SPEC, DAY2)) == first
    assert fingerprint(transactions(spark, SPEC, DAY2).repartition(7)) == first


def test_defects_arrive_at_roughly_their_configured_rates(spark: SparkSession) -> None:
    rows = transactions(spark, SPEC, DAY2).cache()
    resent = rows.groupBy("transaction_id", "version").count().where("count > 1").count()
    late = rows.where(F.to_date("event_ts") < F.lit(DAY2)).where("version = '1'").count()
    malformed = rows.where("amount = 'N/A'").count()
    reversals = rows.where("status = 'reversed'").count()
    n = SPEC.transactions_per_day
    assert 0.003 * n < resent < 0.007 * n
    assert 0.015 * n < late < 0.025 * n
    assert 0.0005 * n < malformed < 0.002 * n
    assert 0.005 * n < reversals < 0.015 * n


def test_merchant_traffic_is_skewed(spark: SparkSession) -> None:
    counts = transactions(spark, SPEC, DAY2).groupBy("merchant_id").count()
    top = counts.agg(F.max("count")).first()
    assert top is not None
    # Uniform traffic would give each of 2,000 merchants about 0.05%.
    assert top[0] / SPEC.transactions_per_day > 0.05


def test_pii_fields_cannot_be_real(spark: SparkSession) -> None:
    people = customer_changes(spark, SPEC, SPEC.start)
    assert people.where(~F.col("ssn").rlike(r"^9\d{2}-\d{2}-\d{4}$")).count() == 0
    assert people.where(~F.col("email").endswith("@example.com")).count() == 0


def test_only_changed_customers_arrive_after_the_first_day(spark: SparkSession) -> None:
    assert customer_changes(spark, SPEC, SPEC.start).count() == SPEC.customers
    later = customer_changes(spark, SPEC, DAY2).count()
    assert 0 < later < SPEC.customers * 0.02
