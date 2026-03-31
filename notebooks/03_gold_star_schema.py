# ============================================================
# 03_gold_star_schema.py
# StreamPulse - Gold Layer (Star Schema - Unity Catalog Bypass)
# Author: Khushboo Barot
# ============================================================

# ── Cell 1: Imports & Configuration ─────────────────────────
from pyspark.sql.functions import (
    col, monotonically_increasing_id, to_date,
    year, month, dayofmonth, hour, dayofweek,
    current_timestamp, lit, when
)

# Credentials
STORAGE_ACCOUNT = "streampulsedatalake"
STORAGE_KEY = "YOUR_STORAGE_KEY_HERE" 

spark.conf.set(f"fs.azure.account.key.{STORAGE_ACCOUNT}.dfs.core.windows.net", STORAGE_KEY)

# Paths
SILVER_PATH = f"abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net/watch_obt"
GOLD_BASE   = f"abfss://gold@{STORAGE_ACCOUNT}.dfs.core.windows.net"

print("📥 Reading Silver layer...")
silver_df = spark.read.format("delta").load(SILVER_PATH)
print(f"✅ Silver records loaded: {silver_df.count():,}")

# ── Helper: Save to Azure & Create Temp View ────────────────
def save_gold(df, table_name):
    path = f"{GOLD_BASE}/{table_name}"
    
    # 1. Physically save Delta files to Azure (The "Hard" Work)
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(path)
    
    # 2. Create a Temp View (The "Bypass" for Analytics)
    df.createOrReplaceTempView(table_name)
    print(f"✅ {table_name} → Saved to Azure & View Created ({df.count():,} rows)")

# ── Cell 2: Build Dimensions ────────────────────────────────
print("\n🏗️ Building Star Schema Dimensions...")

# dim_content
dim_content = silver_df.select(
    "content_id", "content_title", "genre", "language"
).distinct().withColumn("content_key", monotonically_increasing_id())
save_gold(dim_content, "dim_content")

# dim_users
dim_users = silver_df.select(
    "user_id", "subscription_plan", "subscription_price"
).distinct().withColumn("user_key", monotonically_increasing_id())
save_gold(dim_users, "dim_users")

# dim_device
dim_device = silver_df.select(
    "device_type", "device_category"
).distinct().withColumn("device_key", monotonically_increasing_id())
save_gold(dim_device, "dim_device")

# dim_location
dim_location = silver_df.select(
    "city", "state"
).distinct().withColumn("location_key", monotonically_increasing_id())
save_gold(dim_location, "dim_location")

# ── Cell 3: Build Fact Table ────────────────────────────────
print("\n🏗️ Building fact_watch_events...")
fact_watch_events = silver_df.select(
    "event_id", "event_timestamp", "user_id", "content_id", 
    "device_type", "city", "watch_duration_mins", "completion_pct",
    "subscription_price"
).withColumn("fact_key", monotonically_increasing_id()) \
 .withColumn("gold_timestamp", current_timestamp())
save_gold(fact_watch_events, "fact_watch_events")

# ── Cell 4: Analytics (SQL Queries) ──────────────────────────
print("\n" + "="*50)
print("📊 GOLD LAYER ANALYTICS (Using Star Schema Joins)")
print("="*50)

# 1. Top Content by Views
print("\n1️⃣ Top Genres by Views:")
spark.sql("""
    SELECT c.genre, 
           COUNT(f.event_id) as total_views,
           ROUND(AVG(f.completion_pct), 2) as avg_completion
    FROM fact_watch_events f
    JOIN dim_content c ON f.content_id = c.content_id
    GROUP BY c.genre
    ORDER BY total_views DESC
""").show()

# 2. Revenue Analysis
print("\n2️⃣ Revenue by Subscription Plan:")
spark.sql("""
    SELECT u.subscription_plan,
           COUNT(f.event_id) as total_views,
           ROUND(SUM(f.subscription_price), 2) as total_revenue
    FROM fact_watch_events f
    JOIN dim_users u ON f.user_id = u.user_id
    GROUP BY u.subscription_plan
    ORDER BY total_revenue DESC
""").show()

print("\n✅ GOLD LAYER")