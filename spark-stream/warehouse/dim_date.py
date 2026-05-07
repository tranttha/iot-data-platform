"""
dim_date: calendar date dimension (insert-only — dates are immutable, no SCD).
Upserts by date_key (YYYYMMDD integer).
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, date_format, dayofmonth, dayofweek, lit,
    month, pmod, quarter, to_date, weekofyear, year,
)

from config import DIM_DATE_TABLE


def _ensure_table(spark: SparkSession) -> None:
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.warehouse")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {DIM_DATE_TABLE} (
            date_key     INT,
            full_date    DATE,
            year         INT,
            quarter      INT,
            month        INT,
            month_name   STRING,
            week_of_year INT,
            day_of_month INT,
            day_of_week  INT,
            day_name     STRING,
            is_weekend   BOOLEAN
        )
        USING iceberg
    """)


def upsert(spark: SparkSession, batch_df: DataFrame) -> None:
    """Insert any new calendar dates found in batch_df.event_time."""
    _ensure_table(spark)

    dates = batch_df.select(to_date(col("event_time")).alias("d")).distinct()

    # Spark dayofweek: 1=Sun … 7=Sat → remap to Mon=1 … Sun=7
    _dow = (pmod(dayofweek("d") - 2, 7) + 1)

    enriched = dates.select(
        (year("d") * 10000 + month("d") * 100 + dayofmonth("d")).alias("date_key"),
        col("d").alias("full_date"),
        year("d").alias("year"),
        quarter("d").alias("quarter"),
        month("d").alias("month"),
        date_format("d", "MMMM").alias("month_name"),
        weekofyear("d").alias("week_of_year"),
        dayofmonth("d").alias("day_of_month"),
        _dow.alias("day_of_week"),
        date_format("d", "EEEE").alias("day_name"),
        (_dow >= lit(6)).alias("is_weekend"),   # Sat=6, Sun=7
    )

    enriched.createOrReplaceGlobalTempView("_new_dates")
    spark.sql(f"""
        MERGE INTO {DIM_DATE_TABLE} t
        USING global_temp._new_dates s ON t.date_key = s.date_key
        WHEN NOT MATCHED THEN INSERT *
    """)
