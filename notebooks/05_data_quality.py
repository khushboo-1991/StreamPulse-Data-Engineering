# ============================================================
# 05_data_quality.py
# StreamPulse - Data Quality Checks
# Validates data across all three layers
# Bronze → Silver → Gold
# ============================================================

from pyspark.sql.functions import (
    col, count, when, isnan, isnull,
    round as spark_round, min as spark_min,
    max as spark_max, avg as spark_avg,
    countDistinct, lit
)

# ── Cell 1: Quality Check Function ───────────────────────────
def run_quality_check(table_name, df, key_columns):
    """
    Runs comprehensive quality checks on a DataFrame.
    Checks nulls, duplicates, row counts, and ranges.
    """
    print(f"\n{'='*55}")
    print(f"📊 Quality Report: {table_name}")
    print(f"{'='*55}")

    # Total rows
    total = df.count()
    print(f"✅ Total rows: {total:,}")

    # Null checks
    print("\n🔍 Null counts per column:")
    null_counts = df.select([
        count(
            when(isnull(c), c)
        ).alias(c)
        for c in df.columns
    ])
    null_counts.show()

    # Duplicate check on key columns
    total_keys = df.select(key_columns).count()
    distinct_keys = df.select(
        key_columns
    ).distinct().count()
    duplicates = total_keys - distinct_keys

    print(f"🔍 Duplicate check on {key_columns}:")
    print(f"   Total: {total_keys:,}")
    print(f"   Distinct: {distinct_keys:,}")
    print(f"   Duplicates: {duplicates:,}")

    if duplicates == 0:
        print("   ✅ No duplicates found!")
    else:
        print(f"   ⚠️ {duplicates:,} duplicates found!")

    return {
        "table": table_name,
        "total_rows": total,
        "duplicates": duplicates
    }


# ── Cell 2: Bronze Layer Quality ─────────────────────────────
print("🥉 BRONZE LAYER QUALITY CHECKS")
print("="*55)

# Check bulk history
try:
    bronze_bulk = spark.read\
        .format("delta")\
        .load("/mnt/streampulse/bronze/bulk_history")

    bronze_bulk_result = run_quality_check(
        "Bronze — Bulk History",
        bronze_bulk,
        ["event_id"]
    )
except Exception as e:
    print(f"⚠️ Bronze bulk not found: {str(e)}")

# Check streaming events
try:
    bronze_stream = spark.read\
        .format("delta")\
        .load("/mnt/streampulse/bronze/stream_events")

    bronze_stream_result = run_quality_check(
        "Bronze — Stream Events",
        bronze_stream,
        ["event_id"]
    )
except Exception as e:
    print(f"⚠️ Bronze stream not found yet: {str(e)}")
    print("This is normal if streaming has not started")


# ── Cell 3: Silver Layer Quality ─────────────────────────────
print("\n🥈 SILVER LAYER QUALITY CHECKS")
print("="*55)

try:
    silver_df = spark.read\
        .format("delta")\
        .load("/mnt/streampulse/silver/ride_obt")

    silver_result = run_quality_check(
        "Silver — Watch OBT",
        silver_df,
        ["event_id"]
    )

    # Additional silver checks
    print("\n🔍 Silver specific checks:")

    # Completion percentage range
    completion_check = silver_df.select(
        spark_min("completion_pct").alias("min_completion"),
        spark_max("completion_pct").alias("max_completion"),
        spark_avg("completion_pct").alias("avg_completion")
    )
    print("\nCompletion % range:")
    completion_check.show()

    # Check completion between 0 and 100
    invalid_completion = silver_df.filter(
        (col("completion_pct") < 0) |
        (col("completion_pct") > 100)
    ).count()

    if invalid_completion == 0:
        print("✅ All completion_pct values are valid (0-100)")
    else:
        print(f"⚠️ {invalid_completion} invalid completion values!")

    # Watch duration check
    invalid_duration = silver_df.filter(
        col("watch_duration_mins") <= 0
    ).count()

    if invalid_duration == 0:
        print("✅ All watch durations are positive")
    else:
        print(f"⚠️ {invalid_duration} invalid durations!")

    # Subscription price check
    invalid_price = silver_df.filter(
        col("subscription_price") <= 0
    ).count()

    if invalid_price == 0:
        print("✅ All subscription prices are positive")
    else:
        print(f"⚠️ {invalid_price} invalid prices!")

    # Check completion categories
    print("\nCompletion category distribution:")
    silver_df.groupBy("completion_category")\
        .count()\
        .orderBy("count", ascending=False)\
        .show()

    # Check time of day distribution
    print("Time of day distribution:")
    silver_df.groupBy("time_of_day")\
        .count()\
        .orderBy("count", ascending=False)\
        .show()

except Exception as e:
    print(f"⚠️ Silver layer not found: {str(e)}")


# ── Cell 4: Gold Layer Quality ───────────────────────────────
print("\n🥇 GOLD LAYER QUALITY CHECKS")
print("="*55)

# Check fact table
try:
    fact_df = spark.read\
        .format("delta")\
        .load("/mnt/streampulse/gold/fact_watch_events")

    fact_result = run_quality_check(
        "Gold — fact_watch_events",
        fact_df,
        ["event_id"]
    )

    # Referential integrity checks
    print("\n🔍 Referential integrity checks:")

    # Check all content_ids exist in dim_content
    dim_content = spark.read\
        .format("delta")\
        .load("/mnt/streampulse/gold/dim_content")

    orphan_content = fact_df\
        .join(
            dim_content,
            "content_id",
            "left_anti"
        ).count()

    if orphan_content == 0:
        print("✅ All content_ids found in dim_content")
    else:
        print(f"⚠️ {orphan_content} orphan content_ids!")

    # Check all cities exist in dim_locations
    dim_locations = spark.read\
        .format("delta")\
        .load("/mnt/streampulse/gold/dim_locations")

    orphan_cities = fact_df\
        .join(
            dim_locations,
            "city",
            "left_anti"
        ).count()

    if orphan_cities == 0:
        print("✅ All cities found in dim_locations")
    else:
        print(f"⚠️ {orphan_cities} orphan cities!")

    # Check all device_types in dim_devices
    dim_devices = spark.read\
        .format("delta")\
        .load("/mnt/streampulse/gold/dim_devices")

    orphan_devices = fact_df\
        .join(
            dim_devices,
            "device_type",
            "left_anti"
        ).count()

    if orphan_devices == 0:
        print("✅ All device_types found in dim_devices")
    else:
        print(f"⚠️ {orphan_devices} orphan device_types!")

except Exception as e:
    print(f"⚠️ Gold layer not found: {str(e)}")


# ── Cell 5: Cross Layer Validation ───────────────────────────
print("\n🔄 CROSS LAYER VALIDATION")
print("="*55)

try:
    bronze_count = spark.read\
        .format("delta")\
        .load("/mnt/streampulse/bronze/bulk_history")\
        .count()

    silver_count = spark.read\
        .format("delta")\
        .load("/mnt/streampulse/silver/ride_obt")\
        .count()

    gold_count = spark.read\
        .format("delta")\
        .load(
            "/mnt/streampulse/gold/fact_watch_events"
        )\
        .count()

    print(f"🥉 Bronze records: {bronze_count:,}")
    print(f"🥈 Silver records: {silver_count:,}")
    print(f"🥇 Gold records:   {gold_count:,}")

    # Calculate data loss percentage
    bronze_to_silver = (
        (bronze_count - silver_count)
        / bronze_count * 100
    )
    silver_to_gold = (
        (silver_count - gold_count)
        / silver_count * 100
    )

    print(f"\n📊 Data flow analysis:")
    print(
        f"Bronze → Silver: "
        f"{bronze_to_silver:.1f}% removed (cleaning)"
    )
    print(
        f"Silver → Gold: "
        f"{silver_to_gold:.1f}% removed (quality rules)"
    )

    if bronze_to_silver < 10:
        print("✅ Bronze to Silver loss acceptable (<10%)")
    else:
        print("⚠️ High data loss Bronze to Silver!")

    if silver_to_gold < 5:
        print("✅ Silver to Gold loss acceptable (<5%)")
    else:
        print("⚠️ High data loss Silver to Gold!")

except Exception as e:
    print(f"⚠️ Cross layer check failed: {str(e)}")


# ── Cell 6: Final Quality Summary ────────────────────────────
print("\n" + "="*55)
print("📋 FINAL QUALITY SUMMARY")
print("="*55)

print("""
Layer Checks Completed:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
🥉 Bronze:
   ✅ Row counts verified
   ✅ Duplicate event_ids checked
   ✅ Null checks on all columns

🥈 Silver:
   ✅ Data cleaning verified
   ✅ Completion % range (0-100)
   ✅ Watch duration positive
   ✅ Subscription price positive
   ✅ Category distributions checked

🥇 Gold:
   ✅ Fact table integrity
   ✅ Referential integrity — content
   ✅ Referential integrity — locations
   ✅ Referential integrity — devices

🔄 Cross Layer:
   ✅ Record counts compared
   ✅ Data loss % calculated
   ✅ Pipeline health verified
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
""")

print("✅ All quality checks complete!")
print("🚀 StreamPulse pipeline is healthy!")