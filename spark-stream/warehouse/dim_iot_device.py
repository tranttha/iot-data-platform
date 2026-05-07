"""
dim_iot_device: SCD Type 2 — tracks lat/lon/timezone changes per device.
Delegates merge logic to warehouse.scd2.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, concat_ws, current_timestamp, md5, row_number
from pyspark.sql.window import Window

from config import (
    DIM_DEVICE_EFFECTIVE_END,
    DIM_DEVICE_EFFECTIVE_START,
    DIM_DEVICE_IS_CURRENT,
    DIM_DEVICE_NATURAL_KEY,
    DIM_DEVICE_TABLE,
    DIM_DEVICE_TRACKED,
)
from warehouse.scd2 import merge as _scd2


def _ensure_table(spark: SparkSession) -> None:
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.warehouse")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {DIM_DEVICE_TABLE} (
            device_sk       STRING,
            iot_device_id   INT,
            latitude        DOUBLE,
            longitude       DOUBLE,
            timezone        STRING,
            effective_start TIMESTAMP,
            effective_end   TIMESTAMP,
            is_current      BOOLEAN
        )
        USING iceberg
    """)


def scd2_merge(spark: SparkSession, batch_df: DataFrame) -> None:
    """SCD Type 2 merge for dim_iot_device."""
    _ensure_table(spark)

    w = Window.partitionBy(DIM_DEVICE_NATURAL_KEY).orderBy(col("event_time").desc())
    latest = (
        batch_df
        .withColumn("_rn", row_number().over(w))
        .filter(col("_rn") == 1)
        .drop("_rn")
        .select("iot_device_id", "latitude", "longitude", "timezone")
        .withColumn(DIM_DEVICE_EFFECTIVE_START, current_timestamp())
        .withColumn(
            "device_sk",
            md5(concat_ws("||",
                col("iot_device_id").cast("string"),
                col(DIM_DEVICE_EFFECTIVE_START).cast("string")))
        )
    )

    change_cond = " OR ".join(f"t.{a} != s.{a}" for a in DIM_DEVICE_TRACKED)

    _scd2(
        spark           = spark,
        stage_df        = latest,
        table           = DIM_DEVICE_TABLE,
        join_cond       = f"t.{DIM_DEVICE_NATURAL_KEY} = s.iot_device_id",
        is_current      = DIM_DEVICE_IS_CURRENT,
        change_cond     = change_cond,
        effective_start = DIM_DEVICE_EFFECTIVE_START,
        effective_end   = DIM_DEVICE_EFFECTIVE_END,
        insert_cols     = f"device_sk, iot_device_id, latitude, longitude, timezone, "
                          f"{DIM_DEVICE_EFFECTIVE_START}, {DIM_DEVICE_EFFECTIVE_END}, {DIM_DEVICE_IS_CURRENT}",
        select_expr     = f"s.device_sk, s.iot_device_id, s.latitude, s.longitude, s.timezone, "
                          f"s.{DIM_DEVICE_EFFECTIVE_START}, NULL, true",
        no_current_filter = f"t.{DIM_DEVICE_NATURAL_KEY} = s.iot_device_id "
                            f"AND t.{DIM_DEVICE_IS_CURRENT} = true",
        view_name       = "_stage_device",
    )

