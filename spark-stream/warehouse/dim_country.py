"""
dim_country: SCD Type 2 — tracks country assignment per device.
Country is derived from lat/lon via bounding-box lookup.
Delegates merge logic to warehouse.scd2.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, concat_ws, current_timestamp, md5, row_number, udf
from pyspark.sql.types import StringType, StructField, StructType
from pyspark.sql.window import Window

from config import (
    DIM_COUNTRY_EFFECTIVE_END,
    DIM_COUNTRY_EFFECTIVE_START,
    DIM_COUNTRY_IS_CURRENT,
    DIM_COUNTRY_TABLE,
    DIM_COUNTRY_TRACKED,
)
from utils.country_lookup import lookup as _py_lookup
from warehouse.scd2 import merge as _scd2

_COUNTRY_RETURN = StructType([
    StructField("country_code", StringType()),
    StructField("country_name", StringType()),
    StructField("city",         StringType()),
])


@udf(returnType=_COUNTRY_RETURN)
def _country_udf(lat, lon):
    code, name, city = _py_lookup(lat, lon)
    return (code, name, city)


def _ensure_table(spark: SparkSession) -> None:
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.warehouse")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {DIM_COUNTRY_TABLE} (
            country_sk      STRING,
            iot_device_id   INT,
            country_code    STRING,
            country_name    STRING,
            city            STRING,
            effective_start TIMESTAMP,
            effective_end   TIMESTAMP,
            is_current      BOOLEAN
        )
        USING iceberg
    """)


def scd2_merge(spark: SparkSession, batch_df: DataFrame) -> None:
    """SCD Type 2 merge for dim_country."""
    _ensure_table(spark)

    w = Window.partitionBy("iot_device_id").orderBy(col("event_time").desc())
    latest = (
        batch_df
        .withColumn("_rn", row_number().over(w))
        .filter(col("_rn") == 1)
        .drop("_rn")
        .select("iot_device_id", "latitude", "longitude")
        .withColumn("_c", _country_udf(col("latitude"), col("longitude")))
        .withColumn("country_code", col("_c.country_code"))
        .withColumn("country_name", col("_c.country_name"))
        .withColumn("city",         col("_c.city"))
        .drop("_c", "latitude", "longitude")
        .withColumn(DIM_COUNTRY_EFFECTIVE_START, current_timestamp())
        .withColumn(
            "country_sk",
            md5(concat_ws("||",
                col("iot_device_id").cast("string"),
                col("country_code"),
                col(DIM_COUNTRY_EFFECTIVE_START).cast("string")))
        )
    )

    change_cond = " OR ".join(f"t.{a} != s.{a}" for a in DIM_COUNTRY_TRACKED)

    _scd2(
        spark           = spark,
        stage_df        = latest,
        table           = DIM_COUNTRY_TABLE,
        join_cond       = "t.iot_device_id = s.iot_device_id",
        is_current      = DIM_COUNTRY_IS_CURRENT,
        change_cond     = change_cond,
        effective_start = DIM_COUNTRY_EFFECTIVE_START,
        effective_end   = DIM_COUNTRY_EFFECTIVE_END,
        insert_cols     = f"country_sk, iot_device_id, country_code, country_name, city, "
                          f"{DIM_COUNTRY_EFFECTIVE_START}, {DIM_COUNTRY_EFFECTIVE_END}, {DIM_COUNTRY_IS_CURRENT}",
        select_expr     = f"s.country_sk, s.iot_device_id, s.country_code, s.country_name, s.city, "
                          f"s.{DIM_COUNTRY_EFFECTIVE_START}, NULL, true",
        no_current_filter = f"t.iot_device_id = s.iot_device_id "
                            f"AND t.{DIM_COUNTRY_IS_CURRENT} = true",
        view_name       = "_stage_country",
    )

