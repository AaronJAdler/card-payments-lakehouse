import datetime as dt

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from lakehouse.layers import (
    dedupe_transactions,
    fact_transactions,
    merchant_daily,
    validate_transactions,
)

BATCH = dt.date(2026, 1, 2)


def raw(spark: SparkSession, rows: list[dict[str, str | None]]):  # type: ignore[no-untyped-def]
    base = {
        "transaction_id": "T000000000001",
        "customer_id": "C00000001",
        "card_id": "K00000001",
        "merchant_id": "M000001",
        "amount": "10.00",
        "currency": "USD",
        "channel": "online",
        "status": "approved",
        "event_ts": "2026-01-02T10:00:00Z",
        "version": "1",
        "_corrupt_record": None,
    }
    full = [{**base, **r} for r in rows]
    df = spark.createDataFrame(full, schema=", ".join(f"{k} string" for k in base))
    return df.withColumns(
        {
            "_batch_date": F.lit(BATCH),
            "_source_file": F.lit("part-0.json"),
            "_ingested_at": F.lit("2026-01-02 06:00:00").cast("timestamp"),
        }
    )


def test_each_defect_is_rejected_with_its_reason(spark: SparkSession) -> None:
    valid, rejected = validate_transactions(
        raw(
            spark,
            [
                {},
                {"transaction_id": "T1"},
                {"amount": "N/A"},
                {"amount": "-5.00"},
                {"currency": "EUR"},
                {"event_ts": "yesterday"},
                {"status": "pending"},
                {"merchant_id": None},
                {"_corrupt_record": "{bad json"},
            ],
        )
    )
    assert valid.count() == 1
    assert sorted(r.rejection_reason for r in rejected.collect()) == sorted(
        [
            "bad transaction_id",
            "amount is not a number",
            "amount is not positive",
            "unknown currency",
            "bad event_ts",
            "unknown status",
            "missing customer or merchant",
            "unparseable record",
        ]
    )


def test_amounts_convert_to_usd_cents(spark: SparkSession) -> None:
    valid, _ = validate_transactions(raw(spark, [{"amount": "100.00", "currency": "CAD"}]))
    assert valid.first().amount_usd_cents == 7300  # type: ignore[union-attr]


def test_dedupe_keeps_the_highest_version(spark: SparkSession) -> None:
    valid, _ = validate_transactions(
        raw(
            spark,
            [
                {},
                {},
                {"version": "2", "status": "reversed"},
            ],
        )
    )
    rows = dedupe_transactions(valid).collect()
    assert [(r.version, r.status) for r in rows] == [(2, "reversed")]


def test_fact_uses_the_customer_version_current_at_event_time(spark: SparkSession) -> None:
    valid, _ = validate_transactions(
        raw(
            spark,
            [
                {"transaction_id": "T000000000001", "event_ts": "2026-01-02T05:00:00Z"},
                {"transaction_id": "T000000000002", "event_ts": "2026-01-02T07:00:00Z"},
            ],
        )
    )
    customers = spark.createDataFrame(
        [
            ("C00000001", "CA", "standard", "1900-01-01 00:00:00", "2026-01-02 06:00:00"),
            ("C00000001", "TX", "premium", "2026-01-02 06:00:00", "9999-12-31 00:00:00"),
        ],
        "customer_id string, state string, segment string, valid_from string, valid_to string",
    ).select(
        "customer_id",
        "state",
        "segment",
        F.col("valid_from").cast("timestamp").alias("valid_from"),
        F.col("valid_to").cast("timestamp").alias("valid_to"),
    )
    merchants = spark.createDataFrame([("M000001", "5411")], "merchant_id string, mcc string")
    fact = fact_transactions(valid, customers, merchants, [dt.date(2026, 1, 2)])
    states = {r.transaction_id: r.customer_state for r in fact.collect()}
    assert states == {"T000000000001": "CA", "T000000000002": "TX"}


def test_merchant_daily_counts_final_status_and_net_sales(spark: SparkSession) -> None:
    fact = spark.createDataFrame(
        [
            ("2026-01-02", "M1", "5411", "K1", "approved", 1000),
            ("2026-01-02", "M1", "5411", "K2", "declined", 500),
            ("2026-01-02", "M1", "5411", "K1", "reversed", 700),
        ],
        "event_date string, merchant_id string, mcc string, card_id string, status string, "
        "amount_usd_cents long",
    )
    row = merchant_daily(fact).first()
    assert row is not None
    assert (row.transactions, row.approved, row.declined, row.reversed) == (3, 1, 1, 1)
    assert row.net_sales_usd_cents == 1000
    assert row.approval_rate == 0.3333
