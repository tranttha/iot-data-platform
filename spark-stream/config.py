import os
from pathlib import Path

import yaml

# ─────────────────────────────────────────────────────────────────────────────
# Load static config from YAML
# ─────────────────────────────────────────────────────────────────────────────
_yaml_path = Path(__file__).parent / "config.yaml"
with _yaml_path.open() as _f:
    _cfg = yaml.safe_load(_f)

# ─────────────────────────────────────────────────────────────────────────────
# Runtime values from environment (secrets / hostnames never go in YAML)
# ─────────────────────────────────────────────────────────────────────────────
SPARK_MASTER_URL  = os.environ.get("SPARK_MASTER_URL", "spark://spark-master:7077")
MINIO_ENDPOINT    = os.environ["MINIO_ENDPOINT"]
MINIO_ACCESS_KEY  = os.environ["MINIO_ACCESS_KEY"]
MINIO_SECRET_KEY  = os.environ["MINIO_SECRET_KEY"]
POLL_INTERVAL_SECONDS = int(
    os.environ.get("POLL_INTERVAL_SECONDS", _cfg["defaults"]["poll_interval_seconds"])
)

# ─────────────────────────────────────────────────────────────────────────────
# Storage paths  (straight from YAML — no secrets)
# ─────────────────────────────────────────────────────────────────────────────
_storage = _cfg["storage"]

RAW_PATH       = _storage["raw_path"]
STAGING_PATH   = _storage["staging_path"]
WAREHOUSE_PATH = _storage["warehouse_path"]

CHECKPOINT_STAGING = _storage["checkpoints"]["staging"]

# ─────────────────────────────────────────────────────────────────────────────
# Iceberg catalog
# ─────────────────────────────────────────────────────────────────────────────
_iceberg = _cfg["iceberg"]

CATALOG_NAME = _iceberg["catalog_name"]
CATALOG_TYPE = _iceberg["catalog_type"]
# catalog.hadoop.warehouse is constructed at runtime so it picks up WAREHOUSE_PATH
CATALOG_WAREHOUSE = WAREHOUSE_PATH

# ─────────────────────────────────────────────────────────────────────────────
# Staging layer
# ─────────────────────────────────────────────────────────────────────────────
_staging = _cfg["staging"]

STAGING_TABLE      = _staging["table"]
STAGING_PARTITION  = _staging["partition_by"]
STAGING_DEDUP_KEYS = _staging["dedup_keys"]             # list[str]
STAGING_FIELDS     = _staging["fields"]                 # list[{name, source, type}]

# Convenience: field names only (for select / schema definitions)
STAGING_FIELD_NAMES = [f["name"] for f in STAGING_FIELDS]

# ─────────────────────────────────────────────────────────────────────────────
# Warehouse — dimension tables
# ─────────────────────────────────────────────────────────────────────────────
_wh = _cfg["warehouse"]

# dim_date
DIM_DATE_TABLE       = _wh["dim_date"]["table"]
DIM_DATE_NATURAL_KEY = _wh["dim_date"]["natural_key"]

# dim_iot_device  (SCD Type 2)
DIM_DEVICE_TABLE          = _wh["dim_iot_device"]["table"]
DIM_DEVICE_NATURAL_KEY    = _wh["dim_iot_device"]["natural_key"]
DIM_DEVICE_TRACKED        = _wh["dim_iot_device"]["tracked_attributes"]
DIM_DEVICE_SK             = _wh["dim_iot_device"]["surrogate_key"]
DIM_DEVICE_EFFECTIVE_START = _wh["dim_iot_device"]["effective_start_col"]
DIM_DEVICE_EFFECTIVE_END   = _wh["dim_iot_device"]["effective_end_col"]
DIM_DEVICE_IS_CURRENT      = _wh["dim_iot_device"]["is_current_col"]

# dim_country  (SCD Type 2)
DIM_COUNTRY_TABLE          = _wh["dim_country"]["table"]
DIM_COUNTRY_NATURAL_KEY    = _wh["dim_country"]["natural_key"]   # list[str]
DIM_COUNTRY_TRACKED        = _wh["dim_country"]["tracked_attributes"]
DIM_COUNTRY_SK             = _wh["dim_country"]["surrogate_key"]
DIM_COUNTRY_EFFECTIVE_START = _wh["dim_country"]["effective_start_col"]
DIM_COUNTRY_EFFECTIVE_END   = _wh["dim_country"]["effective_end_col"]
DIM_COUNTRY_IS_CURRENT      = _wh["dim_country"]["is_current_col"]

# fact_measurements
FACT_TABLE               = _wh["fact_measurements"]["table"]
FACT_SK                  = _wh["fact_measurements"]["surrogate_key"]
FACT_MEASUREMENT_FIELDS  = _wh["fact_measurements"]["measurement_fields"]  # list[str]

# fact_hourly_agg
FACT_HOURLY_TABLE       = _wh["fact_hourly_agg"]["table"]
FACT_HOURLY_NATURAL_KEY = _wh["fact_hourly_agg"]["natural_key"]   # list[str]
FACT_HOURLY_AGG_FIELDS  = _wh["fact_hourly_agg"]["agg_fields"]    # list[str]

# ─────────────────────────────────────────────────────────────────────────────
# Country bounding-box lookup table
# ─────────────────────────────────────────────────────────────────────────────
COUNTRY_LOOKUP = _cfg["country_lookup"]   # list[dict] — used by utils/country_lookup.py
