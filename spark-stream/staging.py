"""
Staging layer: read raw JSON files from MinIO, flatten OWM One Call 3.0 fields,
and MERGE INTO lakehouse.staging.events (Iceberg, partitioned by days(event_time)).

Trigger: availableNow=True — processes all files since last checkpoint then stops.
Orchestrated by stream.py which supplies the per-batch callback (on_batch).
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, from_json, from_unixtime, to_timestamp
from pyspark.sql.types import (
    ArrayType, DoubleType, IntegerType, LongType,
    StringType, StructField, StructType,
)

from config import (
    CHECKPOINT_STAGING,
    RAW_PATH,
    STAGING_TABLE,
)

# ── Inner JSON schema (content of the raw_json string field) ─────────────────
_INNER_SCHEMA = StructType([
    StructField("iot_device_id", IntegerType()),
    StructField("ingestion_time", StringType()),
    StructField("data", StructType([
        StructField("dt",         LongType()),
        StructField("sunrise",    LongType()),
        StructField("sunset",     LongType()),
        StructField("temp",       DoubleType()),
        StructField("feels_like", DoubleType()),
        StructField("pressure",   IntegerType()),
        StructField("humidity",   IntegerType()),
        StructField("dew_point",  DoubleType()),
        StructField("uvi",        DoubleType()),
        StructField("clouds",     IntegerType()),
        StructField("visibility", IntegerType()),
        StructField("wind_speed", DoubleType()),
        StructField("wind_deg",   IntegerType()),
        StructField("wind_gust",  DoubleType()),
        StructField("weather", ArrayType(StructType([
            StructField("id",          IntegerType()),
            StructField("main",        StringType()),
            StructField("description", StringType()),
            StructField("icon",        StringType()),
        ]))),
        StructField("rain", StructType([
            StructField("1h", DoubleType()),
        ])),
    ])),
    StructField("location", StructType([
        StructField("lat",      DoubleType()),
        StructField("lon",      DoubleType()),
        StructField("timezone", StringType()),
    ])),
])

# Outer file schema: each line is {"raw_json": "<escaped JSON string>"}
_FILE_SCHEMA = StructType([StructField("raw_json", StringType())])


def ensure_table(spark: SparkSession) -> None:
    """Create staging namespace and events table if they do not exist."""
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.staging")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {STAGING_TABLE} (
            event_time          TIMESTAMP,
            ingestion_time      TIMESTAMP,
            iot_device_id       INT,
            latitude            DOUBLE,
            longitude           DOUBLE,
            timezone            STRING,
            sunrise             TIMESTAMP,
            sunset              TIMESTAMP,
            temp                DOUBLE,
            feels_like          DOUBLE,
            pressure            INT,
            humidity            INT,
            dew_point           DOUBLE,
            uvi                 DOUBLE,
            clouds              INT,
            visibility          INT,
            wind_speed          DOUBLE,
            wind_deg            INT,
            wind_gust           DOUBLE,
            weather_id          INT,
            weather_main        STRING,
            weather_description STRING,
            rain_1h             DOUBLE
        )
        USING iceberg
        PARTITIONED BY (days(event_time))
    """)


def flatten(raw_df: DataFrame) -> DataFrame:
    """Parse raw_json string and return a flat DataFrame with OWM fields."""
    p = from_json(col("raw_json"), _INNER_SCHEMA).alias("p")
    return raw_df.select(p).select(
        from_unixtime(col("p.data.dt")).cast("timestamp").alias("event_time"),
        to_timestamp(col("p.ingestion_time")).alias("ingestion_time"),
        col("p.iot_device_id").alias("iot_device_id"),
        col("p.location.lat").alias("latitude"),
        col("p.location.lon").alias("longitude"),
        col("p.location.timezone").alias("timezone"),
        from_unixtime(col("p.data.sunrise")).cast("timestamp").alias("sunrise"),
        from_unixtime(col("p.data.sunset")).cast("timestamp").alias("sunset"),
        col("p.data.temp").alias("temp"),
        col("p.data.feels_like").alias("feels_like"),
        col("p.data.pressure").alias("pressure"),
        col("p.data.humidity").alias("humidity"),
        col("p.data.dew_point").alias("dew_point"),
        col("p.data.uvi").alias("uvi"),
        col("p.data.clouds").alias("clouds"),
        col("p.data.visibility").alias("visibility"),
        col("p.data.wind_speed").alias("wind_speed"),
        col("p.data.wind_deg").alias("wind_deg"),
        col("p.data.wind_gust").alias("wind_gust"),
        col("p.data.weather").getItem(0).getField("id").alias("weather_id"),
        col("p.data.weather").getItem(0).getField("main").alias("weather_main"),
        col("p.data.weather").getItem(0).getField("description").alias("weather_description"),
        col("p.data.rain").getField("1h").alias("rain_1h"),
    )


def merge(spark: SparkSession, batch_df: DataFrame) -> None:
    """MERGE batch_df into the staging table (skip duplicate event_time+device rows)."""
    batch_df.createOrReplaceGlobalTempView("_stg_batch")
    spark.sql(f"""
        MERGE INTO {STAGING_TABLE} t
        USING global_temp._stg_batch s
        ON t.event_time = s.event_time AND t.iot_device_id = s.iot_device_id
        WHEN NOT MATCHED THEN INSERT *
    """)


def run(spark: SparkSession, on_batch: callable) -> None:
    """
    Set up file-based streaming read from s3a://raw/, flatten each micro-batch,
    and forward to on_batch(flat_df, batch_id).  Blocks until availableNow=True completes.
    """
    ensure_table(spark)

    raw_stream = (
        spark.readStream
        .format("json")
        .schema(_FILE_SCHEMA)
        .load(RAW_PATH)
    )

    flat_stream = flatten(raw_stream)

    query = (
        flat_stream.writeStream
        .foreachBatch(on_batch)
        .option("checkpointLocation", CHECKPOINT_STAGING)
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination()
