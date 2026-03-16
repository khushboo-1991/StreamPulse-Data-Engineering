# ============================================================
# 01_bronze_ingestion.py
# StreamPulse - Bronze Layer
# Ingests raw data from two sources:
# Source 1: Bulk historical data from GitHub JSON
# Source 2: Live streaming events from Event Hubs
# Author: Khushboo Patel
# ============================================================

# ── Install Required Libraries ───────────────────────────────
# Run this in Databricks before executing notebook
# %pip install azure-eventhub

# ── Cell 1: Setup and Configuration ─────────────────────────
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.types import (
    StructType, StructField, StringType,
    DoubleType, IntegerType, TimestampType
)
import json

# Storage configuration
# These values come from Azure Key Vault
STORAGE_ACCOUNT = dbutils.secrets.get(
    scope="streampulse-scope",
    key="storage-account-name"
)
STORAGE_KEY = dbutils.secrets.get(
    scope="streampulse-scope",
    key="storage-account-key"
)

# Set Spark configuration for ADLS access
spark.conf.set(
    f"fs.azure.account.key.{STORAGE_ACCOUNT}.dfs.core.windows.net",
    STORAGE_KEY
)

print("✅ Configuration complete")
print(f"Storage Account: {STORAGE_ACCOUNT}")

# ── Cell 2: Mount ADLS Containers ───────────────────────────
def mount_container(container_name):
    """
    Mounts an ADLS Gen2 container to Databricks.
    Checks if already mounted before mounting.
    """
    mount_point = f"/mnt/streampulse/{container_name}"

    # Check if already mounted
    if any(
        mount.mountPoint == mount_point
        for mount in dbutils.fs.mounts()
    ):
        print(f"✅ Already mounted: {mount_point}")
        return

    # Mount the container
    dbutils.fs.mount(
        source=f"abfss://{container_name}@{STORAGE_ACCOUNT}"
                f".dfs.core.windows.net/",
        mount_point=mount_point,
        extra_configs={
            f"fs.azure.account.key.{STORAGE_ACCOUNT}"
            f".dfs.core.windows.net": STORAGE_KEY
        }
    )
    print(f"✅ Mounted: {mount_point}")

# Mount all three layers
mount_container("bronze")
mount_container("silver")
mount_container("gold")

print("\n✅ All containers mounted successfully!")

# ── Cell 3: Define Schema ────────────────────────────────────
# Defining schema explicitly is better than inferring
# It is faster and more reliable

stream_event_schema = StructType([
    StructField("event_id",             StringType(),  True),
    StructField("event_timestamp",      StringType(),  True),
    StructField("event_hour",           IntegerType(), True),
    StructField("event_day",            StringType(),  True),
    StructField("event_month",          IntegerType(), True),
    StructField("is_prime_time",        IntegerType(), True),
    StructField("is_weekend",           IntegerType(), True),
    StructField("user_id",              StringType(),  True),
    StructField("age_group",            StringType(),  True),
    StructField("content_id",           StringType(),  True),
    StructField("content_title",        StringType(),  True),
    StructField("content_type",         StringType(),  True),
    StructField("genre",                StringType(),  True),
    StructField("language",             StringType(),  True),
    StructField("action",               StringType(),  True),
    StructField("watch_duration_mins",  IntegerType(), True),
    StructField("total_duration_mins",  IntegerType(), True),
    StructField("completion_pct",       DoubleType(),  True),
    StructField("is_completed",         IntegerType(), True),
    StructField("is_dropped",           IntegerType(), True),
    StructField("device_type",          StringType(),  True),
    StructField("device_os",            StringType(),  True),
    StructField("city",                 StringType(),  True),
    StructField("state",                StringType(),  True),
    StructField("region",               StringType(),  True),
    StructField("subscription_plan",    StringType(),  True),
    StructField("subscription_price",   IntegerType(), True),
])

print("✅ Schema defined successfully")
print(f"Total fields: {len(stream_event_schema.fields)}")

# ── Cell 4: Load Bulk Historical Data ───────────────────────
# Source 1 — GitHub JSON file
# This loads 1000 historical records

GITHUB_URL = (
    "https://raw.githubusercontent.com/"
    "khushboo-1991/StreamPulse-Data-Engineering/"
    "main/data/streaming_history.json"
)

print("📥 Loading bulk historical data from GitHub...")

# Read JSON from GitHub using pandas
import pandas as pd
bulk_df_pandas = pd.read_json(GITHUB_URL)

# Convert to Spark DataFrame
bulk_df = spark.createDataFrame(
    bulk_df_pandas,
    schema=stream_event_schema
)

# Add ingestion metadata
bulk_df = bulk_df.withColumn(
    "ingestion_timestamp",
    current_timestamp()
).withColumn(
    "source",
    col("event_id").cast(StringType()) # placeholder
).withColumn(
    "source",
    lit("BULK_HISTORICAL")
)

print(f"✅ Loaded {bulk_df.count():,} historical records")
print("\nSample data:")
bulk_df.show(3, truncate=False)

# ── Cell 5: Write Bulk Data to Bronze ───────────────────────
BRONZE_PATH = "/mnt/streampulse/bronze/bulk_history"

# Only write if table does not exist
if not spark.catalog.tableExists(
    "streampulse_bronze.bulk_history"
):
    bulk_df.write\
        .format("delta")\
        .mode("overwrite")\
        .save(BRONZE_PATH)

    print("✅ Bulk data written to Bronze Delta table")
else:
    print("⚠️ Table already exists — skipping bulk load")
    print("This prevents duplicate data on re-runs")

# ── Cell 6: Register Bronze Table ───────────────────────────
spark.sql("CREATE DATABASE IF NOT EXISTS streampulse_bronze")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS streampulse_bronze.bulk_history
    USING DELTA
    LOCATION '{BRONZE_PATH}'
""")

print("✅ Bronze bulk_history table registered")

# Verify
spark.sql("""
    SELECT
        COUNT(*) as total_records,
        MIN(event_timestamp) as earliest_event,
        MAX(event_timestamp) as latest_event
    FROM streampulse_bronze.bulk_history
""").show()

# ── Cell 7: Streaming Ingestion from Event Hubs ─────────────
# Source 2 — Live events from Azure Event Hubs
# Reads real time watch events as they come in

EVENTHUB_CONNECTION = dbutils.secrets.get(
    scope="streampulse-scope",
    key="eventhub-connection-string"
)
EVENTHUB_NAME = "streampulse-events"

# Event Hubs Kafka endpoint
BOOTSTRAP_SERVERS = (
    "streampulse-ns.servicebus.windows.net:9093"
)

SASL_CONFIG = (
    "org.apache.kafka.common.security.plain"
    ".PlainLoginModule required "
    f'username="$ConnectionString" '
    f'password="{EVENTHUB_CONNECTION}";'
)

print("🔄 Starting Event Hubs streaming ingestion...")

# Read stream from Event Hubs via Kafka protocol
raw_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
    .option("subscribe", EVENTHUB_NAME)
    .option("kafka.security.protocol", "SASL_SSL")
    .option("kafka.sasl.mechanism", "PLAIN")
    .option("kafka.sasl.jaas.config", SASL_CONFIG)
    .option("startingOffsets", "latest")
    .load()
)

# Parse JSON from Event Hubs
from pyspark.sql.functions import from_json, lit

parsed_stream = (
    raw_stream
    .select(
        from_json(
            col("value").cast("string"),
            stream_event_schema
        ).alias("data"),
        current_timestamp().alias("ingestion_timestamp")
    )
    .select("data.*", "ingestion_timestamp")
    .withColumn("source", lit("LIVE_STREAM"))
)

# Write stream to Bronze Delta table
STREAM_BRONZE_PATH = "/mnt/streampulse/bronze/stream_events"

streaming_query = (
    parsed_stream.writeStream
    .format("delta")
    .outputMode("append")
    .option(
        "checkpointLocation",
        "/mnt/streampulse/bronze/_checkpoints/stream_events"
    )
    .start(STREAM_BRONZE_PATH)
)

print("✅ Streaming pipeline started!")
print(f"Stream status: {streaming_query.status}")

# ── Cell 8: Register Streaming Table ────────────────────────
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS
    streampulse_bronze.stream_events
    USING DELTA
    LOCATION '{STREAM_BRONZE_PATH}'
""")

print("✅ Bronze stream_events table registered")

# ── Cell 9: Verify Bronze Layer ──────────────────────────────
print("\n" + "="*50)
print("📊 BRONZE LAYER SUMMARY")
print("="*50)

print("\n1️⃣ Bulk Historical Data:")
spark.sql("""
    SELECT
        COUNT(*) as total_records,
        COUNT(DISTINCT user_id) as unique_users,
        COUNT(DISTINCT content_id) as unique_content,
        COUNT(DISTINCT city) as unique_cities
    FROM streampulse_bronze.bulk_history
""").show()

print("\n2️⃣ Content Distribution:")
spark.sql("""
    SELECT
        content_title,
        COUNT(*) as view_count
    FROM streampulse_bronze.bulk_history
    GROUP BY content_title
    ORDER BY view_count DESC
""").show()

print("\n3️⃣ Device Distribution:")
spark.sql("""
    SELECT
        device_type,
        COUNT(*) as count
    FROM streampulse_bronze.bulk_history
    GROUP BY device_type
    ORDER BY count DESC
""").show()

print("\n✅ Bronze layer complete!")
print("🚀 Ready for Silver transformation!")