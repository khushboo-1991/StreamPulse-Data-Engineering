# ============================================================
# 04_delta_live_tables.py
# StreamPulse - Delta Live Tables Pipeline
# Declarative pipeline for automated table management
# ============================================================

import dlt
from pyspark.sql.functions import (
    col, to_date, year, month, dayofmonth,
    hour, dayofweek, current_timestamp,
    lit, when, monotonically_increasing_id,
    round as spark_round
)

# ── Source: Silver OBT ───────────────────────────────────────
@dlt.table(
    name="silver_watch_obt",
    comment="Source: cleaned watch events from silver layer"
)
def silver_source():
    return (
        spark.read
        .format("delta")
        .load("/mnt/streampulse/silver/ride_obt")
    )


# ── Dimension: dim_content ───────────────────────────────────
@dlt.table(
    name="dim_content",
    comment="Content dimension — shows and movies catalogue"
)
@dlt.expect_or_drop(
    "valid_content_id",
    "content_id IS NOT NULL"
)
@dlt.expect_or_drop(
    "valid_content_title",
    "content_title IS NOT NULL"
)
def dim_content():
    return (
        dlt.read("silver_watch_obt")
        .select(
            "content_id",
            "content_title",
            "content_type",
            "genre",
            "language"
        )
        .distinct()
        .withColumn(
            "created_at",
            current_timestamp()
        )
    )


# ── Dimension: dim_users ─────────────────────────────────────
@dlt.table(
    name="dim_users",
    comment="Users dimension — subscriber details"
)
@dlt.expect_or_drop(
    "valid_user_id",
    "user_id IS NOT NULL"
)
def dim_users():
    return (
        dlt.read("silver_watch_obt")
        .select(
            "user_id",
            "age_group",
            "subscription_plan",
            "subscription_price"
        )
        .distinct()
        .withColumn(
            "created_at",
            current_timestamp()
        )
    )


# ── Dimension: dim_devices ───────────────────────────────────
@dlt.table(
    name="dim_devices",
    comment="Devices dimension — viewing devices"
)
def dim_devices():
    return (
        dlt.read("silver_watch_obt")
        .select(
            "device_type",
            "device_os",
            "device_category"
        )
        .distinct()
        .withColumn(
            "created_at",
            current_timestamp()
        )
    )


# ── Dimension: dim_locations ─────────────────────────────────
@dlt.table(
    name="dim_locations",
    comment="Locations dimension — cities and regions"
)
@dlt.expect_or_drop(
    "valid_city",
    "city IS NOT NULL"
)
def dim_locations():
    return (
        dlt.read("silver_watch_obt")
        .select(
            "city",
            "state",
            "region"
        )
        .distinct()
        .withColumn(
            "created_at",
            current_timestamp()
        )
    )


# ── Dimension: dim_date ──────────────────────────────────────
@dlt.table(
    name="dim_date",
    comment="Date dimension — time attributes"
)
def dim_date():
    return (
        dlt.read("silver_watch_obt")
        .select(
            to_date("event_timestamp").alias("date")
        )
        .distinct()
        .withColumn("year",
            year("date"))
        .withColumn("month",
            month("date"))
        .withColumn("day",
            dayofmonth("date"))
        .withColumn("day_of_week",
            dayofweek("date"))
        .withColumn("is_weekend",
            when(
                dayofweek("date").isin([1, 7]),
                lit(1)
            ).otherwise(lit(0))
        )
        .withColumn("month_name",
            when(col("month") == 1,  "January")
            .when(col("month") == 2,  "February")
            .when(col("month") == 3,  "March")
            .when(col("month") == 4,  "April")
            .when(col("month") == 5,  "