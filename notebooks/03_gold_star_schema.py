# ============================================================
# 03_gold_star_schema.py
# StreamPulse - Gold Layer
# Creates Star Schema with Fact and Dimension tables
# This is the analytics ready layer
# Author: Khushboo Patel
# ============================================================

# ── Cell 1: Import Libraries ─────────────────────────────────
from pyspark.sql.functions import (
    col, monotonically_increasing_id,
    to_date, year, month, dayofmonth,
    hour, dayofweek, current_timestamp,
    lit, when, row_number, dense_rank
)
from pyspark.sql.window import Window
from delta.tables import DeltaTable

print("✅ Libraries imported successfully")
print("🏗️ Building Gold Star Schema...")

# ── Cell 2: Read Silver Data ─────────────────────────────────
print("\n📥 Reading Silver layer data...")

silver_df = spark.read\
    .format("delta")\
    .load("/mnt/streampulse/silver/ride_obt")

print(f"✅ Silver records: {silver_df.count():,}")
print("\nSilver Schema:")
silver_df.printSchema()

# ── Cell 3: Create dim_content ───────────────────────────────
print("\n📦 Creating dim_content...")

dim_content = (
    silver_df
    .select(
        "content_id",
        "content_title",
        "content_type",
        "genre",
        "language"
    )
    .distinct()
    .withColumn(
        "content_key",
        monotonically_increasing_id()
    )
    .withColumn(
        "created_at",
        current_timestamp()
    )
)

# Write dim_content
dim_content.write\
    .format("delta")\
    .mode("overwrite")\
    .save("/mnt/streampulse/gold/dim_content")

spark.sql(
    "CREATE DATABASE IF NOT EXISTS streampulse_gold"
)

spark.sql("""
    CREATE TABLE IF NOT EXISTS
    streampulse_gold.dim_content
    USING DELTA
    LOCATION '/mnt/streampulse/gold/dim_content'
""")

print(f"✅ dim_content: "
      f"{dim_content.count()} records")
dim_content.show(5, truncate=False)

# ── Cell 4: Create dim_users ─────────────────────────────────
print("\n📦 Creating dim_users...")

dim_users = (
    silver_df
    .select(
        "user_id",
        "age_group",
        "subscription_plan",
        "subscription_price"
    )
    .distinct()
    .withColumn(
        "user_key",
        monotonically_increasing_id()
    )
    .withColumn(
        "created_at",
        current_timestamp()
    )
)

# Write dim_users
dim_users.write\
    .format("delta")\
    .mode("overwrite")\
    .save("/mnt/streampulse/gold/dim_users")

spark.sql("""
    CREATE TABLE IF NOT EXISTS
    streampulse_gold.dim_users
    USING DELTA
    LOCATION '/mnt/streampulse/gold/dim_users'
""")

print(f"✅ dim_users: "
      f"{dim_users.count()} records")
dim_users.show(5, truncate=False)

# ── Cell 5: Create dim_devices ───────────────────────────────
print("\n📦 Creating dim_devices...")

dim_devices = (
    silver_df
    .select(
        "device_type",
        "device_os",
        "device_category"
    )
    .distinct()
    .withColumn(
        "device_key",
        monotonically_increasing_id()
    )
    .withColumn(
        "created_at",
        current_timestamp()
    )
)

# Write dim_devices
dim_devices.write\
    .format("delta")\
    .mode("overwrite")\
    .save("/mnt/streampulse/gold/dim_devices")

spark.sql("""
    CREATE TABLE IF NOT EXISTS
    streampulse_gold.dim_devices
    USING DELTA
    LOCATION '/mnt/streampulse/gold/dim_devices'
""")

print(f"✅ dim_devices: "
      f"{dim_devices.count()} records")
dim_devices.show(truncate=False)

# ── Cell 6: Create dim_locations ─────────────────────────────
print("\n📦 Creating dim_locations...")

dim_locations = (
    silver_df
    .select(
        "city",
        "state",
        "region"
    )
    .distinct()
    .withColumn(
        "location_key",
        monotonically_increasing_id()
    )
    .withColumn(
        "created_at",
        current_timestamp()
    )
)

# Write dim_locations
dim_locations.write\
    .format("delta")\
    .mode("overwrite")\
    .save("/mnt/streampulse/gold/dim_locations")

spark.sql("""
    CREATE TABLE IF NOT EXISTS
    streampulse_gold.dim_locations
    USING DELTA
    LOCATION '/mnt/streampulse/gold/dim_locations'
""")

print(f"✅ dim_locations: "
      f"{dim_locations.count()} records")
dim_locations.show(truncate=False)

# ── Cell 7: Create dim_date ──────────────────────────────────
print("\n📦 Creating dim_date...")

dim_date = (
    silver_df
    .select(
        to_date("event_timestamp").alias("date")
    )
    .distinct()
    .withColumn("date_key",
        monotonically_increasing_id())
    .withColumn("year",
        year("date"))
    .withColumn("month",
        month("date"))
    .withColumn("day",
        dayofmonth("date"))
    .withColumn("hour",
        hour("date"))
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
        .when(col("month") == 5,  "May")
        .when(col("month") == 6,  "June")
        .when(col("month") == 7,  "July")
        .when(col("month") == 8,  "August")
        .when(col("month") == 9,  "September")
        .when(col("month") == 10, "October")
        .when(col("month") == 11, "November")
        .otherwise("December")
    )
    .withColumn(
        "created_at",
        current_timestamp()
    )
)

# Write dim_date
dim_date.write\
    .format("delta")\
    .mode("overwrite")\
    .save("/mnt/streampulse/gold/dim_date")

spark.sql("""
    CREATE TABLE IF NOT EXISTS
    streampulse_gold.dim_date
    USING DELTA
    LOCATION '/mnt/streampulse/gold/dim_date'
""")

print(f"✅ dim_date: "
      f"{dim_date.count()} records")
dim_date.show(5, truncate=False)

# ── Cell 8: Create fact_watch_events ────────────────────────
print("\n📦 Creating fact_watch_events...")

fact_watch_events = (
    silver_df
    .select(
        # Keys
        "event_id",
        "user_id",
        "content_id",
        "device_type",
        "city",
        to_date("event_timestamp").alias("date"),

        # Measures
        "watch_duration_mins",
        "total_duration_mins",
        "completion_pct",
        "engagement_score",
        "revenue_per_view",
        "subscription_price",

        # Flags
        "is_completed",
        "is_dropped",
        "is_prime_time",
        "is_weekend",

        # Categories
        "completion_category",
        "device_category",
        "time_of_day",
        "action",

        # Timestamps
        "event_timestamp",
        "silver_timestamp"
    )
    .withColumn(
        "fact_key",
        monotonically_increasing_id()
    )
    .withColumn(
        "gold_timestamp",
        current_timestamp()
    )
)

# Write fact table
fact_watch_events.write\
    .format("delta")\
    .mode("overwrite")\
    .option("overwriteSchema", "true")\
    .save("/mnt/streampulse/gold/fact_watch_events")

spark.sql("""
    CREATE TABLE IF NOT EXISTS
    streampulse_gold.fact_watch_events
    USING DELTA
    LOCATION '/mnt/streampulse/gold/fact_watch_events'
""")

print(f"✅ fact_watch_events: "
      f"{fact_watch_events.count():,} records")
fact_watch_events.show(3, truncate=False)

# ── Cell 9: Gold Layer Analytics ─────────────────────────────
print("\n" + "="*50)
print("📊 GOLD LAYER ANALYTICS")
print("="*50)

print("\n1️⃣ Top Content By Views:")
spark.sql("""
    SELECT
        c.content_title,
        c.genre,
        c.language,
        COUNT(f.event_id) as total_views,
        ROUND(AVG(f.completion_pct), 2)
            as avg_completion,
        ROUND(AVG(f.engagement_score), 2)
            as avg_engagement
    FROM streampulse_gold.fact_watch_events f
    JOIN streampulse_gold.dim_content c
        ON f.content_id = c.content_id
    GROUP BY
        c.content_title,
        c.genre,
        c.language
    ORDER BY total_views DESC
""").show(truncate=False)

print("\n2️⃣ Revenue By Subscription Plan:")
spark.sql("""
    SELECT
        u.subscription_plan,
        COUNT(f.event_id) as total_views,
        ROUND(SUM(f.revenue_per_view), 2)
            as total_revenue,
        ROUND(AVG(f.completion_pct), 2)
            as avg_completion
    FROM streampulse_gold.fact_watch_events f
    JOIN streampulse_gold.dim_users u
        ON f.user_id = u.user_id
    GROUP BY u.subscription_plan
    ORDER BY total_revenue DESC
""").show()

print("\n3️⃣ Viewing By Location:")
spark.sql("""
    SELECT
        l.city,
        l.region,
        COUNT(f.event_id) as total_views,
        ROUND(AVG(f.completion_pct), 2)
            as avg_completion,
        ROUND(AVG(f.engagement_score), 2)
            as avg_engagement
    FROM streampulse_gold.fact_watch_events f
    JOIN streampulse_gold.dim_locations l
        ON f.city = l.city
    GROUP BY l.city, l.region
    ORDER BY total_views DESC
""").show()

print("\n4️⃣ Device Performance:")
spark.sql("""
    SELECT
        d.device_type,
        d.device_category,
        COUNT(f.event_id) as total_views,
        ROUND(AVG(f.completion_pct), 2)
            as avg_completion,
        ROUND(AVG(f.engagement_score), 2)
            as avg_engagement
    FROM streampulse_gold.fact_watch_events f
    JOIN streampulse_gold.dim_devices d
        ON f.device_type = d.device_type
    GROUP BY
        d.device_type,
        d.device_category
    ORDER BY total_views DESC
""").show()

print("\n5️⃣ Prime Time Analysis:")
spark.sql("""
    SELECT
        time_of_day,
        is_prime_time,
        COUNT(*) as total_views,
        ROUND(AVG(completion_pct), 2)
            as avg_completion,
        ROUND(AVG(engagement_score), 2)
            as avg_engagement
    FROM streampulse_gold.fact_watch_events
    GROUP BY time_of_day, is_prime_time
    ORDER BY total_views DESC
""").show()

print("\n✅ Gold layer complete!")
print("⭐ Star Schema successfully created!")
print("\n📋 Tables created:")
print("  ✅ dim_content")
print("  ✅ dim_users")
print("  ✅ dim_devices")
print("  ✅ dim_locations")
print("  ✅ dim_date")
print("  ✅ fact_watch_events")
print("\n🚀 Ready for Delta Live Tables!")