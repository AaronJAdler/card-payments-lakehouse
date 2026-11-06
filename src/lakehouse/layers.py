"""Bronze, silver and gold transformations. Every function here is DataFrame in, DataFrame out."""

from __future__ import annotations

from datetime import date

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

# Everything lands as text, so a bad value is kept and reported instead of failing the read.
RAW_TRANSACTION = T.StructType(
    [
        T.StructField(c, T.StringType())
        for c in (
            "transaction_id",
            "customer_id",
            "card_id",
            "merchant_id",
            "amount",
            "currency",
            "channel",
            "status",
            "event_ts",
            "version",
        )
    ]
    + [T.StructField("_corrupt_record", T.StringType())]
)
RAW_CUSTOMER = T.StructType(
    [
        T.StructField(c, T.StringType())
        for c in (
            "customer_id",
            "full_name",
            "email",
            "ssn",
            "date_of_birth",
            "state",
            "segment",
            "updated_at",
        )
    ]
)
RAW_MERCHANT = T.StructType(
    [T.StructField(c, T.StringType()) for c in ("merchant_id", "merchant_name", "mcc", "state")]
)

# Fixed reference rates keep the demo reproducible. A real pipeline would load daily rates.
FX_TO_USD = {"USD": 1.0, "CAD": 0.73, "MXN": 0.055}
STATUSES = ("approved", "declined", "reversed")
CUSTOMER_TRACKED = ("full_name", "email", "ssn", "date_of_birth", "state", "segment")


# ---------- Bronze ----------


def read_landing(spark: SparkSession, root: str, feed: str, batch_date: date) -> DataFrame:
    schema = {"transactions": RAW_TRANSACTION, "customers": RAW_CUSTOMER, "merchants": RAW_MERCHANT}
    return (
        spark.read.schema(schema[feed])
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .json(f"{root}/{feed}/batch_date={batch_date.isoformat()}")
    )


def to_bronze(raw: DataFrame, batch_date: date) -> DataFrame:
    """Raw records plus where and when they arrived. Nothing is dropped or changed."""
    return raw.withColumns(
        {
            "_batch_date": F.lit(batch_date).cast("date"),
            "_source_file": F.col("_metadata.file_name"),
            "_ingested_at": F.current_timestamp(),
        }
    )


# ---------- Silver ----------


def _rejection(conditions: list[tuple[Column, str]]) -> Column:
    reason = F.lit(None).cast("string")
    for failed, label in reversed(conditions):
        reason = F.when(failed, F.lit(label)).otherwise(reason)
    return reason


def validate_transactions(bronze: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Types the batch and splits it into valid rows and rejected rows with a reason."""
    fx = F.create_map(*[x for k, v in FX_TO_USD.items() for x in (F.lit(k), F.lit(v))])
    typed = bronze.withColumns(
        {
            "amount_value": F.expr("try_cast(amount AS DECIMAL(12, 2))"),
            "event_at": F.expr("try_to_timestamp(event_ts, \"yyyy-MM-dd'T'HH:mm:ss'Z'\")"),
            "version_number": F.expr("try_cast(version AS INT)"),
            "fx_rate": fx[F.col("currency")],
        }
    )
    reason = _rejection(
        [
            (F.col("_corrupt_record").isNotNull(), "unparseable record"),
            (
                ~F.coalesce(F.col("transaction_id").rlike(r"^T\d{12}$"), F.lit(False)),
                "bad transaction_id",
            ),
            (
                F.col("customer_id").isNull() | F.col("merchant_id").isNull(),
                "missing customer or merchant",
            ),
            (F.col("amount_value").isNull(), "amount is not a number"),
            (F.col("amount_value") <= 0, "amount is not positive"),
            (F.col("fx_rate").isNull(), "unknown currency"),
            (F.col("event_at").isNull(), "bad event_ts"),
            (~F.col("status").isin(*STATUSES), "unknown status"),
            (F.col("version_number").isNull(), "bad version"),
        ]
    )
    checked = typed.withColumn("rejection_reason", reason)
    rejected = checked.where(F.col("rejection_reason").isNotNull()).select(
        *bronze.columns, "rejection_reason"
    )
    valid = checked.where(F.col("rejection_reason").isNull()).select(
        "transaction_id",
        "customer_id",
        "card_id",
        "merchant_id",
        F.col("amount_value").alias("amount"),
        "currency",
        F.round(F.col("amount_value") * F.col("fx_rate") * 100)
        .cast("long")
        .alias("amount_usd_cents"),
        "channel",
        "status",
        "event_at",
        F.to_date("event_at").alias("event_date"),
        F.col("version_number").alias("version"),
        "_batch_date",
        "_ingested_at",
    )
    return valid, rejected


def latest_version(df: DataFrame, key: str, order: list[Column]) -> DataFrame:
    """One row per key: the highest version, so resent duplicates and older versions drop out."""
    window = Window.partitionBy(key).orderBy(*order)
    return df.withColumn("_rank", F.row_number().over(window)).where("_rank = 1").drop("_rank")


def dedupe_transactions(valid: DataFrame) -> DataFrame:
    return latest_version(
        valid, "transaction_id", [F.col("version").desc(), F.col("_ingested_at").desc()]
    )


def conform_customers(bronze: DataFrame) -> DataFrame:
    typed = bronze.select(
        "customer_id",
        "full_name",
        F.lower("email").alias("email"),
        "ssn",
        F.to_date("date_of_birth").alias("date_of_birth"),
        F.upper("state").alias("state"),
        "segment",
        F.to_timestamp("updated_at").alias("updated_at"),
    ).where(F.col("customer_id").isNotNull() & F.col("updated_at").isNotNull())
    return latest_version(typed, "customer_id", [F.col("updated_at").desc()])


# ---------- Gold ----------


def fact_transactions(
    silver_txn: DataFrame, customers_scd2: DataFrame, merchants: DataFrame, dates: list[date]
) -> DataFrame:
    """One row per transaction on the given event dates, joined to the customer version
    that was current when the transaction happened."""
    txn = silver_txn.where(F.col("event_date").isin(dates)).alias("f")
    versions = customers_scd2.select(
        "customer_id",
        "valid_from",
        "valid_to",
        "state",
        "segment",
        F.sha2(F.concat_ws("|", "customer_id", F.col("valid_from").cast("string")), 256).alias(
            "customer_version_sk"
        ),
    ).alias("c")
    joined = txn.join(
        versions,
        (F.col("f.customer_id") == F.col("c.customer_id"))
        & (F.col("f.event_at") >= F.col("c.valid_from"))
        & (F.col("f.event_at") < F.col("c.valid_to")),
        "left",
    )
    return joined.join(
        F.broadcast(merchants.select("merchant_id", "mcc").alias("m")), "merchant_id", "left"
    ).select(
        "f.transaction_id",
        "f.event_date",
        "f.event_at",
        "c.customer_version_sk",
        F.col("f.customer_id"),
        F.col("c.state").alias("customer_state"),
        F.col("c.segment").alias("customer_segment"),
        "f.card_id",
        "merchant_id",
        "m.mcc",
        "f.channel",
        "f.status",
        "f.amount_usd_cents",
        "f.version",
    )


def merchant_daily(fact: DataFrame) -> DataFrame:
    """One row per merchant per event date. Counts use each transaction's final status."""
    approved = F.col("status") == "approved"
    return (
        fact.groupBy("event_date", "merchant_id", "mcc")
        .agg(
            F.count("*").alias("transactions"),
            F.sum(approved.cast("int")).alias("approved"),
            F.sum((F.col("status") == "declined").cast("int")).alias("declined"),
            F.sum((F.col("status") == "reversed").cast("int")).alias("reversed"),
            F.sum(F.when(approved, F.col("amount_usd_cents")).otherwise(0)).alias(
                "net_sales_usd_cents"
            ),
            F.approx_count_distinct("card_id").alias("distinct_cards_approx"),
        )
        .withColumn("approval_rate", F.round(F.col("approved") / F.col("transactions"), 4))
    )
