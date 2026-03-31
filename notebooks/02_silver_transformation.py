# ============================================================
# 02_silver_transformation.py
# StreamPulse - Silver Layer (Cleaned & Enriched)
# Author: Khushboo Barot
# ============================================================


# Run in 00_connection_test notebook

silver_df = spark.read.format("delta").load(

"abfss://silver@streampulsedatalake.dfs.core.windows.net/watch_obt"

)

silver_df.printSchema()

print(f"Rows: {silver_df.count():,}")






# ── Cell 1: Config ───────────────────────────────────────────
from pyspark.sql.functions import (
    col, upper, trim, to_timestamp,
    when, lit, current_timestamp
)

STORAGE_ACCOUNT = "streampulsedatalake"
STORAGE_KEY = "YOUR_STORAGE_KEY_HERE" 

spark.conf.set(
    f"fs.azure.account.key.{STORAGE_ACCOUNT}.dfs.core.windows.net", STORAGE_KEY
)
sc._jsc.hadoopConfiguration().set(
    f"fs.azure.account.key.{STORAGE_ACCOUNT}.dfs.core.windows.net", STORAGE_KEY
)

BRONZE_PATH = f"abfss://bronze@{STORAGE_ACCOUNT}.dfs.core.windows.net/github_events/"
SILVER_PATH = f"abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net/watch_obt"

print("✅ Config complete")

# ── Cell 2: Read Bronze ──────────────────────────────────────
print("📥 Reading Bronze...")
bronze_df = spark.read.format("delta").load(BRONZE_PATH)
print(f"✅ Bronze records: {bronze_df.count():,}")
bronze_df.printSchema()

# ── Cell 3: Transform ────────────────────────────────────────
print("\n🔄 Transforming...")

silver_df = (
    bronze_df
    .dropDuplicates(["event_id"])
    .withColumn("event_timestamp",  to_timestamp(col("event_timestamp")))
    .withColumn("genre",            trim(upper(col("genre"))))
    .withColumn("device_type",      trim(upper(col("device_type"))))
    .withColumn("content_title",    trim(col("content_title")))
    .withColumn("completion_category",
        when(col("completion_pct") >= 90, "COMPLETED")
       .when(col("completion_pct") >= 50, "MAJORITY")
       .otherwise("PARTIAL")
    )
    .withColumn("device_category",
        when(col("device_type").isin("MOBILE", "TABLET"), "MOBILE")
       .when(col("device_type").isin("SMARTTV", "ROKU"),  "TV")
       .otherwise("DESKTOP")
    )
    .withColumn("silver_timestamp", current_timestamp())
)

print(f"✅ Silver records: {silver_df.count():,}")

# ── Cell 4: Save ─────────────────────────────────────────────
print(f"\n💾 Writing to: {SILVER_PATH}")

silver_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(SILVER_PATH)

silver_df.createOrReplaceTempView("watch_obt_view")
print("✅ Silver OBT saved!")

# ── Cell 5: Verify ───────────────────────────────────────────
print("\n📊 SILVER SUMMARY")
print(f"Total rows : {silver_df.count():,}")
print(f"Columns    : {len(silver_df.columns)}")

spark.sql("""
    SELECT completion_category,
           COUNT(*)                    as total,
           ROUND(AVG(completion_pct),2) as avg_pct
    FROM watch_obt_view
    GROUP BY 1
    ORDER BY 2 DESC
""").show()

display(silver_df.limit(5))