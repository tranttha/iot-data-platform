"""
spark-stream entry point.

Execution order per run (trigger=availableNow → processes all pending files then stops):
  1.  Streaming read of s3a://raw/**/*.json  (with checkpoint)
  2.  Flatten + parse OWM fields  (staging.flatten)

  For each micro-batch (foreachBatch):
  3.  dropDuplicates(event_time, iot_device_id)  — within-batch dedup
  4.  MERGE INTO staging.events                  — cross-run dedup
  5.  Upsert dim_date
  6.  SCD2 merge dim_iot_device
  7.  SCD2 merge dim_country
  8.  INSERT INTO fact_measurements  (denorm cols + FK join)

  After all micro-batches complete:
  9.  MERGE INTO fact_hourly_agg    (recompute last 2 hours)
  10. Compact small files in fact_measurements (last 2 days)
"""

import config as C
import staging
from warehouse import dim_country, dim_date, dim_iot_device, fact_hourly_agg, fact_measurements

from pyspark.sql import SparkSession


def _build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("spark-stream")
        .master(C.SPARK_MASTER_URL)
        # ── S3A / MinIO ──────────────────────────────────────────────────────
        .config("spark.hadoop.fs.s3a.endpoint",          C.MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key",        C.MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key",        C.MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        # ── Iceberg ──────────────────────────────────────────────────────────
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(f"spark.sql.catalog.{C.CATALOG_NAME}",
                "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{C.CATALOG_NAME}.type",      C.CATALOG_TYPE)
        .config(f"spark.sql.catalog.{C.CATALOG_NAME}.uri",        C.HIVE_METASTORE_URI)
        .config(f"spark.sql.catalog.{C.CATALOG_NAME}.warehouse",  C.CATALOG_WAREHOUSE)
        .getOrCreate()
    )


def _make_batch_fn(spark: SparkSession):
    """Return a foreachBatch function that runs the full warehouse pipeline."""
    def _process_batch(batch_df, batch_id):
        deduped = batch_df.dropDuplicates(C.STAGING_DEDUP_KEYS)
        if deduped.isEmpty():
            return

        deduped.cache()
        try:
            staging.merge(spark, deduped)
            dim_date.upsert(spark, deduped)
            dim_iot_device.scd2_merge(spark, deduped)
            dim_country.scd2_merge(spark, deduped)
            fact_measurements.insert(spark, deduped)
        finally:
            deduped.unpersist()

    return _process_batch


def main() -> None:
    spark = _build_spark()
    spark.sparkContext.setLogLevel("WARN")

    # Ensure fact_measurements table exists before fact_hourly_agg.merge() runs,
    # even if the stream processes no new batches (e.g. empty or already-checkpointed data).
    fact_measurements.ensure_table(spark)

    # Run streaming job — blocks until availableNow=True query completes
    staging.run(spark, on_batch=_make_batch_fn(spark))

    # Post-run: recompute hourly aggregates once (after all batches committed)
    fact_hourly_agg.merge(spark)

    # Compact small files written in the last 2 days
    spark.sql(f"""
        CALL {C.CATALOG_NAME}.system.rewrite_data_files(
            table  => '{C.FACT_TABLE}',
            `where` => 'event_time >= date_sub(current_date(), 2)'
        )
    """)

    spark.stop()


if __name__ == "__main__":
    main()
