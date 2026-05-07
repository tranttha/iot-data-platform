"""
fact_hourly_agg: pre-aggregated hourly rollup from fact_measurements.

MERGE upsert by (hour_bucket, iot_device_id) — recomputes partial hours so the
last few hours are always accurate even when back-filled data arrives.

Scoped to the last 2 hours to keep each run cheap; historical correctness is
maintained because the MERGE overwrites existing rows.
"""

from pyspark.sql import SparkSession

from config import FACT_HOURLY_TABLE, FACT_TABLE


def _ensure_table(spark: SparkSession) -> None:
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.warehouse")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {FACT_HOURLY_TABLE} (
            hour_bucket    TIMESTAMP,
            iot_device_id  INT,
            country_code   STRING,
            avg_temp       DOUBLE,
            avg_feels_like DOUBLE,
            avg_humidity   DOUBLE,
            avg_pressure   DOUBLE,
            avg_wind_speed DOUBLE,
            avg_uvi        DOUBLE,
            avg_clouds     DOUBLE,
            avg_visibility DOUBLE,
            avg_rain_1h    DOUBLE
        )
        USING iceberg
        PARTITIONED BY (days(hour_bucket))
        TBLPROPERTIES (
            'write.parquet.compression-codec'            = 'snappy',
            'write.metadata.delete-after-commit.enabled' = 'true',
            'write.metadata.previous-versions-max'       = '10'
        )
    """)


def merge(spark: SparkSession) -> None:
    """Recompute hourly aggregates for the last 2 hours and MERGE into fact_hourly_agg."""
    _ensure_table(spark)

    spark.sql(f"""
        MERGE INTO {FACT_HOURLY_TABLE} t
        USING (
            SELECT
                date_trunc('hour', event_time) AS hour_bucket,
                iot_device_id,
                country_code,
                AVG(temp)       AS avg_temp,
                AVG(feels_like) AS avg_feels_like,
                AVG(humidity)   AS avg_humidity,
                AVG(pressure)   AS avg_pressure,
                AVG(wind_speed) AS avg_wind_speed,
                AVG(uvi)        AS avg_uvi,
                AVG(clouds)     AS avg_clouds,
                AVG(visibility) AS avg_visibility,
                AVG(rain_1h)    AS avg_rain_1h
            FROM {FACT_TABLE}
            WHERE event_time >= date_trunc('hour', current_timestamp() - INTERVAL '2' HOUR)
            GROUP BY 1, 2, 3
        ) s
        ON  t.hour_bucket   = s.hour_bucket
        AND t.iot_device_id = s.iot_device_id
        WHEN MATCHED     THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
