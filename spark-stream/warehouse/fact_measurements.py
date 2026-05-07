"""
fact_measurements: append-only fact table.

At write time:
  - joins against current dim_iot_device + dim_country to resolve surrogate keys
  - carries iot_device_id + country_code as denormalized columns (join-free Grafana queries)
  - computes date_key (YYYYMMDD) for dim_date FK
  - assigns a UUID surrogate key per row

Table is created with Iceberg table properties for Grafana/Trino query performance:
  snappy compression, metadata cleanup, 128 MB Trino splits, sorted on (event_time, iot_device_id).
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, dayofmonth, expr, month, year,
)

from config import (
    DIM_COUNTRY_IS_CURRENT,
    DIM_COUNTRY_TABLE,
    DIM_DEVICE_IS_CURRENT,
    DIM_DEVICE_TABLE,
    FACT_MEASUREMENT_FIELDS,
    FACT_TABLE,
)

# Weather condition columns live in staging but are not in FACT_MEASUREMENT_FIELDS
# (which covers only numeric measurements).  Include them explicitly.
_WEATHER_COLS = ["weather_id", "weather_main", "weather_description"]


def _ensure_table(spark: SparkSession) -> None:
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.warehouse")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {FACT_TABLE} (
            fact_sk             STRING,
            date_key            INT,
            device_sk           STRING,
            country_sk          STRING,
            iot_device_id       INT,
            country_code        STRING,
            event_time          TIMESTAMP,
            ingestion_time      TIMESTAMP,
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
            rain_1h             DOUBLE,
            weather_id          INT,
            weather_main        STRING,
            weather_description STRING
        )
        USING iceberg
        PARTITIONED BY (days(event_time))
        TBLPROPERTIES (
            'write.parquet.compression-codec'            = 'snappy',
            'write.metadata.delete-after-commit.enabled' = 'true',
            'write.metadata.previous-versions-max'       = '10',
            'read.split.target-size'                     = '134217728'
        )
    """)


def insert(spark: SparkSession, batch_df: DataFrame) -> None:
    """Resolve FKs, denormalize key columns, and append new rows to fact_measurements."""
    _ensure_table(spark)

    dim_device = spark.sql(
        f"SELECT device_sk, iot_device_id"
        f" FROM {DIM_DEVICE_TABLE} WHERE {DIM_DEVICE_IS_CURRENT} = true"
    )
    dim_country = spark.sql(
        f"SELECT country_sk, country_code, iot_device_id"
        f" FROM {DIM_COUNTRY_TABLE} WHERE {DIM_COUNTRY_IS_CURRENT} = true"
    )

    fact_df = (
        batch_df
        .join(dim_device,  "iot_device_id", "left")
        .join(dim_country.select("country_sk", "country_code", "iot_device_id"),
              "iot_device_id", "left")
        .withColumn("date_key",
                    year("event_time") * 10000 +
                    month("event_time") * 100 +
                    dayofmonth("event_time"))
        .withColumn("fact_sk", expr("uuid()"))
        .select(
            "fact_sk", "date_key", "device_sk", "country_sk",
            "iot_device_id", "country_code",
            "event_time", "ingestion_time",
            *FACT_MEASUREMENT_FIELDS,
            *_WEATHER_COLS,
        )
    )

    fact_df.writeTo(FACT_TABLE).append()
