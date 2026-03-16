# ============================================================
# 02_silver_transformation.py
# StreamPulse - Silver Layer
# Cleans and transforms Bronze data
# Creates One Big Table (OBT) with enriched data
# Author: Khushboo Patel
# ============================================================

# ── Cell 1: Import Libraries ─────────────────────────────────
from pyspark.sql.functions import (
    col, upper, lower, trim, round as spark_round,
    to_timestamp, when, lit, current_timestamp,
    year, month, dayofmonth, hour, dayofweek,
    concat_ws, regexp_replace
)
from pyspark.sql.types import (
    StructType, StructField, StringType,
    DoubleType, IntegerType, TimestampType
)
from delta.tables import DeltaTable

print("✅ Libraries imported successfully")

# ── Cell 2: Read Bronze Data ─────────────────────────────────
print("📥 Reading Bronze layer data...")

# Read bulk historical data
bulk_bronze_df = spark.read\
    .format("delta")\
    .load("/mnt/streampulse/bronze/bulk_history")

print(f"✅ Bulk records: {bulk_bronze_df.count():,}")

# Read streaming data if exists
try:
    stream_bronze_df = spark.read\
        .format("delta")\
        .load("/mnt/streampulse/bronze/stream_events")
    print(f"✅ Stream records: {stream_bronze_df.count():,}")

    # Combine both sources
    bronze_df = bulk_bronze_df.union(stream_bronze_df)
    print(f"✅ Total combined: {bronze_df.count():,}")

except Exception as e:
    print("⚠️ No streaming data yet — using bulk only")
    bronze_df = bulk_bronze_df
    print(f"✅ Using bulk records: {bronze_df.count():,}")

# ── Cell 3: Data Quality Check Before Cleaning ───────────────
print("\n📊 Data Quality Report — BEFORE cleaning:")
print("="*50)

total_records = bronze_df.count()
print(f"Total records: {total_records:,}")

# Check nulls on key columns
from pyspark.sql.functions import count, when, isnan, isnull

null_counts = bronze_df.select([
    count(
        when(isnull(c), c)
    ).alias(c)
    for c in [
        "event_id", "user_id", "content_id",
        "content_title", "city", "device_type",
        "subscription_plan", "completion_pct"
    ]
])

print("\nNull counts per column:")
null_counts.show()

# Check duplicates
duplicate_count = (
    total_records - bronze_df
    .select("event_id")
    .distinct()
    .count()
)
print(f"Duplicate event_ids: {duplicate_count:,}")

# ── Cell 4: Clean and Transform Data ────────────────────────
print("\n🔄 Cleaning and transforming data...")

silver_df = (
    bronze_df

    # ── Remove duplicates ──────────────────────────────────
    .dropDuplicates(["event_id"])

    # ── Remove nulls on critical columns ──────────────────
    .dropna(subset=[
        "event_id",
        "user_id",
        "content_id",
        "content_title"
    ])

    # ── Standardize text columns ───────────────────────────
    .withColumn("content_title",
        trim(col("content_title")))
    .withColumn("genre",
        trim(upper(col("genre"))))
    .withColumn("language",
        trim(upper(col("language"))))
    .withColumn("device_type",
        trim(upper(col("device_type"))))
    .withColumn("subscription_plan",
        trim(upper(col("subscription_plan"))))
    .withColumn("action",
        trim(upper(col("action"))))
    .withColumn("city",
        trim(col("city")))
    .withColumn("state",
        trim(col("state")))
    .withColumn("region",
        trim(upper(col("region"))))

    # ── Fix data types ─────────────────────────────────────
    .withColumn("completion_pct",
        spark_round(col("completion_pct"), 2))
    .withColumn("event_timestamp",
        to_timestamp(col("event_timestamp")))

    # ── Add derived columns ────────────────────────────────
    # Completion category
    .withColumn("completion_category",
        when(col("completion_pct") >= 90, "COMPLETED")
        .when(col("completion_pct") >= 50, "MAJORITY")
        .when(col("completion_pct") >= 25, "PARTIAL")
        .when(col("completion_pct") >= 10, "MINIMAL")
        .otherwise("DROPPED")
    )

    # Engagement score (0-10)
    .withColumn("engagement_score",
        spark_round(col("completion_pct") / 10, 1))

    # Revenue per view
    .withColumn("revenue_per_view",
        spark_round(
            col("subscription_price") / 30, 2
        )  # daily rate
    )

    # Device category
    .withColumn("device_category",
        when(col("device_type") == "SMARTTV", "TV")
        .when(col("device_type").isin(
            "MOBILE", "TABLET"), "MOBILE")
        .otherwise("DESKTOP")
    )

    # Time of day category
    .withColumn("time_of_day",
        when(
            (col("event_hour") >= 5) &
            (col("event_hour") < 12), "MORNING")
        .when(
            (col("event_hour") >= 12) &
            (col("event_hour") < 17), "AFTERNOON")
        .when(
            (col("event_hour") >= 17) &
            (col("event_hour") < 21), "EVENING")
        .otherwise("NIGHT")
    )

    # Add silver ingestion timestamp
    .withColumn(
        "silver_timestamp",
        current_timestamp()
    )
)

print(f"✅ Cleaned records: {silver_df.count():,}")

# ── Cell 5: Data Quality Check After Cleaning ────────────────
print("\n📊 Data Quality Report — AFTER cleaning:")
print("="*50)

cleaned_total = silver_df.count()
removed = total_records - cleaned_total

print(f"Records before: {total_records:,}")
print(f"Records after:  {cleaned_total:,}")
print(f"Records removed: {removed:,}")
print(f"Data quality: "
      f"{(cleaned_total/total_records*100):.1f}%")

# ── Cell 6: Show Sample Transformed Data ────────────────────
print("\n📋 Sample Silver Data:")
silver_df.select(
    "event_id",
    "content_title",
    "completion_pct",
    "completion_category",
    "engagement_score",
    "device_category",
    "time_of_day",
    "city"
).show(5, truncate=False)

# ── Cell 7: Write to Silver Delta Table ─────────────────────
SILVER_PATH = "/mnt/streampulse/silver/ride_obt"

print(f"\n💾 Writing to Silver layer...")

silver_df.write\
    .format("delta")\
    .mode("overwrite")\
    .option("overwriteSchema", "true")\
    .save(SILVER_PATH)

print("✅ Silver OBT written successfully!")

# ── Cell 8: Register Silver Table ───────────────────────────
spark.sql(
    "CREATE DATABASE IF NOT EXISTS streampulse_silver"
)

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS
    streampulse_silver.watch_obt
    USING DELTA
    LOCATION '{SILVER_PATH}'
""")

print("✅ Silver table registered: streampulse_silver.watch_obt")

# ── Cell 9: Silver Layer Analytics ──────────────────────────
print("\n" + "="*50)
print("📊 SILVER LAYER SUMMARY")
print("="*50)

print("\n1️⃣ Completion Categories:")
spark.sql("""
    SELECT
        completion_category,
        COUNT(*) as count,
        ROUND(COUNT(*) * 100.0 /
        SUM(COUNT(*)) OVER(), 2) as percentage
    FROM streampulse_silver.watch_obt
    GROUP BY completion_category
    ORDER BY count DESC
""").show()

print("\n2️⃣ Viewing By Time Of Day:")
spark.sql("""
    SELECT
        time_of_day,
        COUNT(*) as views,
        ROUND(AVG(completion_pct), 2) as avg_completion
    FROM streampulse_silver.watch_obt
    GROUP BY time_of_day
    ORDER BY views DESC
""").show()

print("\n3️⃣ Device Category Performance:")
spark.sql("""
    SELECT
        device_category,
        COUNT(*) as views,
        ROUND(AVG(engagement_score), 2) as avg_engagement,
        ROUND(AVG(completion_pct), 2) as avg_completion
    FROM streampulse_silver.watch_obt
    GROUP BY device_category
    ORDER BY views DESC
""").show()

print("\n4️⃣ Top Cities By Views:")
spark.sql("""
    SELECT
        city,
        state,
        COUNT(*) as total_views,
        ROUND(AVG(completion_pct), 2) as avg_completion
    FROM streampulse_silver.watch_obt
    GROUP BY city, state
    ORDER BY total_views DESC
    LIMIT 8
""").show()

print("\n✅ Silver layer complete!")
print("🚀 Ready for Gold Star Schema!")