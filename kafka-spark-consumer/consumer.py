import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, get_json_object, to_date

# ── Environment configuration ─────────────────────────────────────────────────
SPARK_MASTER_URL        = os.environ.get("SPARK_MASTER_URL", "spark://spark-master:7077")
KAFKA_BOOTSTRAP_SERVERS = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
KAFKA_TOPIC             = os.environ.get("KAFKA_TOPIC", "iot-sensor-data")
MINIO_ENDPOINT          = os.environ["MINIO_ENDPOINT"]
MINIO_ACCESS_KEY        = os.environ["MINIO_ACCESS_KEY"]
MINIO_SECRET_KEY        = os.environ["MINIO_SECRET_KEY"]
# Match trigger interval to producer poll rate so each batch processes
# exactly the messages emitted since the last trigger.
POLL_INTERVAL_SECONDS   = int(os.environ.get("POLL_INTERVAL_SECONDS", "60"))
RAW_BUCKET              = "s3a://raw"

spark = (
    SparkSession.builder
    .appName("kafka-raw-consumer")
    .master(SPARK_MASTER_URL)
    # MinIO / S3-compatible storage
    .config("spark.hadoop.fs.s3a.endpoint",            MINIO_ENDPOINT)
    .config("spark.hadoop.fs.s3a.access.key",          MINIO_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key",          MINIO_SECRET_KEY)
    .config("spark.hadoop.fs.s3a.path.style.access",   "true")
    .config("spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# ── Read from Kafka ───────────────────────────────────────────────────────────
# Each Kafka partition is consumed by a separate Spark task, providing
# partition-level parallelism without any additional configuration.
kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe",               KAFKA_TOPIC)
    .option("startingOffsets",         "earliest")
    .option("failOnDataLoss",          "false")
    .load()
)

# ── Extract raw JSON string and partition columns ─────────────────────────────
# Keep the full raw payload intact — no transformation here.
# Partition by ingestion_date and device so MinIO files are organized and
# downstream Spark / Trino queries can prune partitions efficiently.
raw_df = (
    kafka_df
    .selectExpr("CAST(value AS STRING) AS raw_json")
    .withColumn("ingestion_date",
                to_date(get_json_object(col("raw_json"), "$.ingestion_time")))
    .withColumn("iot_device_id",
                get_json_object(col("raw_json"), "$.iot_device_id"))
)

# ── Write to MinIO /raw (partitioned by date + device) ───────────────────────
# Output layout:
#   s3a://raw/ingestion_date=2026-04-30/iot_device_id=1/<micro-batch>.json
query = (
    raw_df.writeStream
    .format("json")
    .option("path",          f"{RAW_BUCKET}/")
    .option("checkpointLocation", f"{RAW_BUCKET}/_checkpoints/kafka-raw-consumer")
    .partitionBy("ingestion_date", "iot_device_id")
    .outputMode("append")
    .trigger(processingTime="0 seconds")
    .start()
)

query.awaitTermination()
