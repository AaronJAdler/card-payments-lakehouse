"""Deterministic synthetic source system for a fictional card issuer.

The data is invented, so it carries no real person's information. It is shaped to exercise what a
real payments feed does to a pipeline:

- Merchant popularity follows a log-uniform (Zipf-like) curve, so a few merchants take a large
  share of transactions. That skew is what the tuning work later fixes.
- Some transactions are resent (duplicates), arrive up to 3 days late, or are later reversed,
  which arrives as a higher version of the same transaction.
- A small share of records are malformed, so validation has something to reject.
- Customers change state and segment over time, which feeds the SCD type 2 dimension.
- Customer records carry PII-like fields. SSNs use the 900 to 999 area range, which is never
  issued, and emails use the reserved example.com domain.

Every value comes from hashing (seed, row id), never from a random generator, so the same inputs
produce the same files on any cluster size.
"""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

STATES = ["CA", "TX", "FL", "NY", "WA", "AZ", "NV", "OR", "IL", "CO"]
SEGMENTS = ["standard", "rewards", "premium", "business"]
CHANNELS = ["card_present", "online", "contactless", "recurring"]
CURRENCIES = ["USD"] * 18 + ["CAD", "MXN"]
MCCS = ["5411", "5812", "5541", "5999", "4121", "5732", "7011", "5311", "5814", "4900"]
FIRST = [
    "Avery",
    "Jordan",
    "Riley",
    "Casey",
    "Morgan",
    "Quinn",
    "Rowan",
    "Sage",
    "Emerson",
    "Parker",
]
LAST = ["Alvarez", "Brooks", "Chen", "Diaz", "Ellis", "Foster", "Garcia", "Hayes", "Ito", "Nguyen"]


@dataclass(frozen=True)
class SourceSpec:
    start: dt.date
    days: int
    transactions_per_day: int
    customers: int
    merchants: int
    seed: int = 7
    duplicate_rate: float = 0.005
    late_rate: float = 0.02
    reversal_rate: float = 0.01
    malformed_rate: float = 0.001
    daily_customer_change_rate: float = 0.004

    def dates(self) -> list[dt.date]:
        return [self.start + dt.timedelta(days=i) for i in range(self.days)]


def _unit(seed: int, salt: str, *cols: Column) -> Column:
    """A uniform value in [0, 1) from hashing, stable for the same seed and columns."""
    hashed = F.xxhash64(F.lit(seed), F.lit(salt), *cols)
    return F.pmod(hashed, F.lit(1_000_003)).cast("double") / F.lit(1_000_003.0)


def _pick(values: list[str], u: Column) -> Column:
    array = F.array(*[F.lit(v) for v in values])
    return F.element_at(array, (F.floor(u * len(values)) + 1).cast("int"))


def merchants(spark: SparkSession, spec: SourceSpec) -> DataFrame:
    ids = spark.range(1, spec.merchants + 1).withColumnRenamed("id", "rank")
    return ids.select(
        F.format_string("M%06d", "rank").alias("merchant_id"),
        F.concat(F.lit("Merchant "), F.col("rank").cast("string")).alias("merchant_name"),
        _pick(MCCS, _unit(spec.seed, "mcc", F.col("rank"))).alias("mcc"),
        _pick(STATES, _unit(spec.seed, "mstate", F.col("rank"))).alias("state"),
    )


def customer_snapshot(spark: SparkSession, spec: SourceSpec, day: dt.date) -> DataFrame:
    """Customer records as of a day. A customer's state and segment change on hashed days."""
    ids = spark.range(1, spec.customers + 1).withColumnRenamed("id", "n")
    # The day a customer changes, counted from day 1. Most never change within the window.
    change_offset = F.floor(
        _unit(spec.seed, "change", F.col("n")) / F.lit(spec.daily_customer_change_rate)
    ) + F.lit(1)
    changed = change_offset <= F.lit((day - spec.start).days)
    version_salt = F.when(changed, F.lit("v2")).otherwise(F.lit("v1"))
    return ids.select(
        F.format_string("C%08d", "n").alias("customer_id"),
        F.concat_ws(
            " ",
            _pick(FIRST, _unit(spec.seed, "first", F.col("n"))),
            _pick(LAST, _unit(spec.seed, "last", F.col("n"))),
        ).alias("full_name"),
        F.format_string("customer%08d@example.com", "n").alias("email"),
        F.format_string(
            "%03d-%02d-%04d",
            (F.lit(900) + F.floor(_unit(spec.seed, "ssn1", F.col("n")) * 100)).cast("int"),
            (F.lit(10) + F.floor(_unit(spec.seed, "ssn2", F.col("n")) * 90)).cast("int"),
            F.floor(_unit(spec.seed, "ssn3", F.col("n")) * 10_000).cast("int"),
        ).alias("ssn"),
        F.date_add(
            F.lit(dt.date(1950, 1, 1)),
            F.floor(_unit(spec.seed, "dob", F.col("n")) * 18_000).cast("int"),
        ).alias("date_of_birth"),
        _pick(STATES, _unit(spec.seed, "state", F.col("n"), version_salt)).alias("state"),
        _pick(SEGMENTS, _unit(spec.seed, "segment", F.col("n"), version_salt)).alias("segment"),
        F.when(changed, F.date_add(F.lit(spec.start), change_offset.cast("int")))
        .otherwise(F.lit(spec.start))
        .alias("effective_date"),
    )


def customer_changes(spark: SparkSession, spec: SourceSpec, day: dt.date) -> DataFrame:
    """The customer feed for one day: everyone on the first day, then only changed records."""
    snapshot = customer_snapshot(spark, spec, day)
    if day == spec.start:
        changes = snapshot
    else:
        changes = snapshot.where(F.col("effective_date") == F.lit(day))
    return changes.withColumn(
        "updated_at", F.to_timestamp(F.concat(F.lit(day.isoformat()), F.lit(" 06:00:00")))
    ).drop("effective_date")


def _clean_transactions(spark: SparkSession, spec: SourceSpec, day: dt.date) -> DataFrame:
    """One business day's valid, first-version transactions, with hash draws for defects."""
    day_index = (day - spec.start).days
    base = spark.range(spec.transactions_per_day).withColumnRenamed("id", "seq")
    n = F.col("seq") + F.lit(day_index * spec.transactions_per_day)
    u_late = _unit(spec.seed, "late", n)
    # Late events belong to an earlier business day but arrive in this day's file.
    late_days = F.when(u_late < spec.late_rate, F.floor(_unit(spec.seed, "lag", n) * 3) + 1)
    event_ts = F.timestamp_seconds(
        F.unix_timestamp(F.lit(day.isoformat()), "yyyy-MM-dd")
        - F.coalesce(late_days, F.lit(0)).cast("long") * 86_400
        + F.floor(_unit(spec.seed, "second", n) * 86_400).cast("long")
    )
    # Log-uniform rank: merchant 1 is the most popular, with a long tail behind it.
    merchant_rank = F.least(
        F.floor(F.exp(_unit(spec.seed, "merchant", n) * F.log(F.lit(float(spec.merchants))))),
        F.lit(spec.merchants),
    ).cast("int")
    customer_n = (F.floor(_unit(spec.seed, "customer", n) * spec.customers) + 1).cast("int")
    amount = F.round(F.exp(_unit(spec.seed, "amount", n) * F.log(F.lit(2_000.0))) + 0.5, 2)
    return base.select(
        F.format_string("T%012d", n).alias("transaction_id"),
        F.format_string("C%08d", customer_n).alias("customer_id"),
        F.format_string("K%08d", customer_n).alias("card_id"),
        F.format_string("M%06d", merchant_rank).alias("merchant_id"),
        F.format_string("%.2f", amount).alias("amount"),
        _pick(CURRENCIES, _unit(spec.seed, "currency", n)).alias("currency"),
        _pick(CHANNELS, _unit(spec.seed, "channel", n)).alias("channel"),
        F.when(_unit(spec.seed, "decline", n) < 0.06, F.lit("declined"))
        .otherwise(F.lit("approved"))
        .alias("status"),
        F.date_format(event_ts, "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("event_ts"),
        F.lit("1").alias("version"),
        _unit(spec.seed, "dup", n).alias("_u_dup"),
        _unit(spec.seed, "reverse", n).alias("_u_reverse"),
        _unit(spec.seed, "malformed", n).alias("_u_bad"),
    )


def transactions(spark: SparkSession, spec: SourceSpec, day: dt.date) -> DataFrame:
    """The transaction file delivered for one batch day, including its defects.

    Columns are strings, as an upstream JSON export would deliver them.
    """
    rows = _clean_transactions(spark, spec, day)
    malformed = rows.where(F.col("_u_bad") < spec.malformed_rate).withColumn("amount", F.lit("N/A"))
    clean = rows.where(F.col("_u_bad") >= spec.malformed_rate)
    duplicates = clean.where(F.col("_u_dup") < spec.duplicate_rate)
    out = clean.unionByName(malformed).unionByName(duplicates)
    if day > spec.start:
        # Reversals of the previous day's approvals arrive as version 2 of the same transaction.
        previous = _clean_transactions(spark, spec, day - dt.timedelta(days=1))
        reversals = (
            previous.where(
                (F.col("_u_bad") >= spec.malformed_rate)
                & (F.col("status") == "approved")
                & (F.col("_u_reverse") < spec.reversal_rate)
            )
            .withColumn("status", F.lit("reversed"))
            .withColumn("version", F.lit("2"))
        )
        out = out.unionByName(reversals)
    return out.drop("_u_dup", "_u_reverse", "_u_bad")


def write_landing(spark: SparkSession, spec: SourceSpec, root: str, day: dt.date) -> None:
    """Writes one day's files the way the upstream system would drop them."""
    folder = f"batch_date={day.isoformat()}"
    transactions(spark, spec, day).write.mode("overwrite").json(f"{root}/transactions/{folder}")
    customer_changes(spark, spec, day).write.mode("overwrite").json(f"{root}/customers/{folder}")
    if day == spec.start:
        merchants(spark, spec).write.mode("overwrite").json(f"{root}/merchants/{folder}")
