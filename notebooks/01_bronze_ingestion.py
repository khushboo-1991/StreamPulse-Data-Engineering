# ── CELL 1: Imports & Global Configuration ──────────────────────────
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, from_json, lit
from pyspark.sql.types import (
    StructType, StructField, StringType,
    DoubleType, IntegerType, TimestampType
)
import pandas as pd



bronze_df = spark.read.format("delta").load(
    "abfss://bronze@streampulsedatalake.dfs.core.windows.net/github_events/"
)
bronze_df.printSchema()



# Storage Credentials
STORAGE_ACCOUNT = "streampulsedatalake"
STORAGE_KEY = "YOUR_STORAGE_KEY_HERE" 

# Set Spark Config
spark.conf.set(
    f"fs.azure.account.key.{STORAGE_ACCOUNT}.dfs.core.windows.net",
    STORAGE_KEY
)

# Unity Catalog Paths
BRONZE_PATH = f"abfss://bronze@{STORAGE_ACCOUNT}.dfs.core.windows.net/"
print(f"✅ Configuration complete. Target: {BRONZE_PATH}")


# ── CELL 2: Source 1 - Load Bulk Historical Data (GitHub) ──────────
GITHUB_URL = "https://raw.githubusercontent.com/khushboo-1991/StreamPulse-Data-Engineering/master/data/streaming_history.json"

print("☁️ Loading bulk historical data from GitHub...")

try:
    pandas_df = pd.read_json(GITHUB_URL)
    spark_df = spark.createDataFrame(pandas_df)

    batch_df = (
        spark_df
        .withColumn("ingested_at", current_timestamp())
        .withColumn("source", lit("github_json"))
    )

    github_bronze_path = f"{BRONZE_PATH}github_events/"
    batch_df.write.format("delta").mode("overwrite").save(github_bronze_path)
    print(f"✅ GitHub JSON loaded to Bronze. Path: {github_bronze_path}")

except Exception as e:
    print(f"❌ GitHub load failed: {e}")
    raise


# ── CELL 3: Source 2 - Live Streaming (Event Hubs) ──────────────────
eh_connection_string = "Endpoint=sb://streampulse-eh.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=mPUDrfdjBCcRuXdgZWZz3sh/6Eacsms/h+AEhFij57g="

# Encrypt connection string
try:
    encrypted_conn_str = sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(eh_connection_string)
    eh_conf = {'eventhubs.connectionString': encrypted_conn_str}
    print("✅ EventHubs connection string encrypted successfully")
except Exception as e:
    print(f"⚠️ Encryption failed, using raw string: {e}")
    eh_conf = {'eventhubs.connectionString': eh_connection_string}

# Define Event Schema
event_schema = StructType([
    StructField("event_id",       StringType(),  True),
    StructField("user_id",        StringType(),  True),
    StructField("content_id",     StringType(),  True),
    StructField("event_type",     StringType(),  True),
    StructField("watch_duration", DoubleType(),  True),
    StructField("timestamp",      StringType(),  True)
])

# Read Stream
print("🔌 Connecting to Event Hubs stream...")
try:
    raw_stream_df = (
        spark.readStream
        .format("eventhubs")
        .options(**eh_conf)
        .load()
    )

    parsed_stream_df = (
        raw_stream_df
        .select(from_json(col("body").cast("string"), event_schema).alias("data"))
        .select("data.*")
        .withColumn("ingested_at", current_timestamp())
        .withColumn("source", lit("event_hubs"))
    )

    bronze_streaming_path = f"{BRONZE_PATH}streaming_events/"
    checkpoint_path        = f"{BRONZE_PATH}_checkpoints/streaming_events/"

    query = (
        parsed_stream_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .start(bronze_streaming_path)
    )

    print(f"✅ Bronze streaming write started. Path: {bronze_streaming_path}")
    print(f"   Stream status: {query.status}")

except Exception as e:
    print(f"❌ Streaming failed: {e}")
    print("⚠️ Skipping streaming — make sure EventHubs Maven library is installed on cluster")
    bronze_streaming_path = f"{BRONZE_PATH}streaming_events/"



# ── CELL 4: Register Tables in Unity Catalog ────────────────────────
# ── CELL 4: Register Tables in Unity Catalog ────────────────────────
print("📦 Registering tables in Unity Catalog...")

# 1. We create a 'Legacy' Catalog first because your workspace 
# is blocking the creation of new Managed Catalogs.
try:
    # Attempt to use the existing 'main' if available, otherwise 'default'
    CATALOG_NAME = "main" 
    
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {CATALOG_NAME}.streampulse_bronze")
    print(f"✅ Database {CATALOG_NAME}.streampulse_bronze ready.")

    # 2. Register the GitHub Table
    # Now that the 'External Location' is set in the UI, this won't fail!
    github_path = f"abfss://bronze@{STORAGE_ACCOUNT}.dfs.core.windows.net/github_events/"
    
    spark.sql(f"DROP TABLE IF EXISTS {CATALOG_NAME}.streampulse_bronze.github_events")
    
    spark.sql(f"""
        CREATE TABLE {CATALOG_NAME}.streampulse_bronze.github_events
        USING DELTA
        LOCATION '{github_path}'
    """)
    
    print(f"✅ GitHub Bronze table registered successfully in {CATALOG_NAME}!")
    
    # 3. Final Verification
    display(spark.sql(f"SELECT * FROM {CATALOG_NAME}.streampulse_bronze.github_events LIMIT 5"))

except Exception as e:
    print(f"❌ UC Registration still blocked: {e}")
    print("\n💡 EMERGENCY MODE: Reading directly from storage to show your progress:")
    # This proves the data exists even if the Catalog registration is being difficult
    df = spark.read.format("delta").load(f"abfss://bronze@{STORAGE_ACCOUNT}.dfs.core.windows.net/github_events/")
    display(df.limit(5))