# spark-stream — Implementation Plan

## 1. File Structure

```
spark-stream/
├── Dockerfile
├── requirement.txt
├── config.yaml                ← static config: paths, table names, field lists, SCD keys,
│                                 country lookup — version-controlled, no secrets
├── config.py                  ← loader: reads config.yaml, overlays env vars (secrets/hosts),
│                                 exposes flat constants to all other modules
├── stream.py                  ← entry point: SparkSession + orchestrates all layers
├── staging.py                 ← flatten raw JSON + dedup → Iceberg staging table
├── warehouse/
│   ├── dim_date.py            ← upsert by date_key (dates are immutable, no SCD)
│   ├── dim_iot_device.py      ← SCD Type 2 merge on (iot_device_id, lat, lon, timezone)
│   ├── dim_country.py         ← SCD Type 2 merge on (iot_device_id, country_code)
│   ├── fact_measurements.py   ← append-only insert with FK lookups + denorm columns
│   └── fact_hourly_agg.py     ← hourly MERGE rollup from fact_measurements
└── utils/
    └── country_lookup.py      ← bounding-box lat/lon → (country_code, country_name, city)
```

---

## 1a. Config Architecture

```
┌─────────────────────────────────────┐    ┌──────────────────────────────┐
│           config.yaml               │    │      Environment (.env)      │
│  (static, version-controlled)       │    │  (secrets, hostnames)        │
│                                     │    │                              │
│  storage:     paths, checkpoints    │    │  MINIO_ENDPOINT              │
│  iceberg:     catalog name/type     │    │  MINIO_ACCESS_KEY            │
│  staging:     table, fields, dedup  │    │  MINIO_SECRET_KEY            │
│  warehouse:   tables, SCD keys,     │    │  SPARK_MASTER_URL            │
│               FK mappings           │    │  POLL_INTERVAL_SECONDS       │
│  country_lookup: bounding boxes     │    │                              │
└──────────────┬──────────────────────┘    └──────────────┬───────────────┘
               │                                          │
               └──────────────────┬───────────────────────┘
                                  ▼
                         ┌────────────────┐
                         │   config.py    │  ← single import for all modules
                         │  (loader)      │
                         │                │
                         │  RAW_PATH      │
                         │  STAGING_TABLE │
                         │  DIM_DEVICE_*  │
                         │  FACT_TABLE    │
                         │  COUNTRY_LOOKUP│
                         │  MINIO_*       │
                         │  ...           │
                         └───────┬────────┘
                                 │
               ┌─────────────────┼──────────────────┐
               ▼                 ▼                   ▼
          staging.py      warehouse/*.py        stream.py
```

**Rule:** no module except `config.py` ever calls `os.environ` or opens `config.yaml`.

---

## 2. Java Dependencies (`--packages` on spark-submit)

| JAR | Purpose |
|---|---|
| `org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2` | Iceberg catalog, MERGE INTO, table DDL |
| `org.apache.hadoop:hadoop-aws:3.3.4` | S3A filesystem for MinIO |
| `com.amazonaws:aws-java-sdk-bundle:1.12.262` | AWS auth for S3-compatible API |

Iceberg catalog type: **Hadoop catalog** (file-based, no metastore service needed).
Both Spark and Trino point to the same `s3a://warehouse/` path.

---

## 3. Staging Layer — `lakehouse.staging.events`

**Source:** `s3a://raw/**/*.json` — file-based streaming read with checkpoint.

**Trigger:** `availableNow=True` — processes all new files since last checkpoint then stops.
Scheduled by Docker restart policy (`restart: on-failure` with a sleep between runs).

### Flat schema after transform

Source JSON structure: `raw_json` string containing `{iot_device_id, ingestion_time, data: {OWM current block}, location: {lat, lon, timezone}}`

| Column | Type | Source (inside `raw_json`) |
|---|---|---|
| `event_time` | TIMESTAMP | `data.dt` (epoch → `from_unixtime`) |
| `ingestion_time` | TIMESTAMP | `ingestion_time` |
| `iot_device_id` | INT | `iot_device_id` |
| `latitude` | DOUBLE | `location.lat` |
| `longitude` | DOUBLE | `location.lon` |
| `timezone` | STRING | `location.timezone` |
| `sunrise` | TIMESTAMP | `data.sunrise` (epoch → `from_unixtime`) |
| `sunset` | TIMESTAMP | `data.sunset` (epoch → `from_unixtime`) |
| `temp` | DOUBLE | `data.temp` |
| `feels_like` | DOUBLE | `data.feels_like` |
| `pressure` | INT | `data.pressure` |
| `humidity` | INT | `data.humidity` |
| `dew_point` | DOUBLE | `data.dew_point` |
| `uvi` | DOUBLE | `data.uvi` |
| `clouds` | INT | `data.clouds` |
| `visibility` | INT | `data.visibility` |
| `wind_speed` | DOUBLE | `data.wind_speed` |
| `wind_deg` | INT | `data.wind_deg` |
| `wind_gust` | DOUBLE | `data.wind_gust` |
| `weather_id` | INT | `data.weather[0].id` |
| `weather_main` | STRING | `data.weather[0].main` |
| `weather_description` | STRING | `data.weather[0].description` |
| `rain_1h` | DOUBLE | `data.rain.1h` (nullable) |

**Deduplication key:** `(event_time, iot_device_id)`
- Within each batch: `dropDuplicates(["event_time", "iot_device_id"])`
- Against existing staging table: Iceberg MERGE — skip rows where key already exists

**Partitioning:** `days(event_time)`

---

## 4. Dimension Tables

### 4.1 `dim_date` — `lakehouse.warehouse.dim_date`

No SCD needed (calendar dates are immutable). Insert-only upsert by `date_key`.

| Column | Type | Notes |
|---|---|---|
| `date_key` | INT | `YYYYMMDD` — natural + surrogate key |
| `full_date` | DATE | |
| `year` | INT | |
| `quarter` | INT | 1–4 |
| `month` | INT | 1–12 |
| `month_name` | STRING | "January" … "December" |
| `week_of_year` | INT | ISO week number |
| `day_of_month` | INT | |
| `day_of_week` | INT | 1 = Monday … 7 = Sunday |
| `day_name` | STRING | "Monday" … "Sunday" |
| `is_weekend` | BOOLEAN | |

---

### 4.2 `dim_iot_device` — `lakehouse.warehouse.dim_iot_device` (SCD Type 2)

Tracks changes in device physical attributes. If a device is re-deployed with a new
lat/lon, the old row is closed and a new row is opened.

**Natural key:** `iot_device_id`
**SCD trigger attributes:** `latitude`, `longitude`, `timezone`
**Surrogate key:** `device_sk = MD5(iot_device_id || effective_start)`

| Column | Type | Notes |
|---|---|---|
| `device_sk` | STRING | Surrogate PK |
| `iot_device_id` | INT | Natural key |
| `latitude` | DOUBLE | `location.lat` |
| `longitude` | DOUBLE | `location.lon` |
| `timezone` | STRING | `location.timezone` |
| `effective_start` | TIMESTAMP | When this version became active |
| `effective_end` | TIMESTAMP | NULL = currently active |
| `is_current` | BOOLEAN | |

**SCD Type 2 MERGE logic:**
```
WHEN matched AND is_current = true AND (lat/lon/timezone changed):
  → UPDATE: set effective_end = now(), is_current = false
  → INSERT: new row with new values, effective_start = now(), is_current = true
WHEN not matched:
  → INSERT: first-seen device
WHEN matched AND nothing changed:
  → no-op
```

---

### 4.3 `dim_country` — `lakehouse.warehouse.dim_country` (SCD Type 2)

Country/region a device is located in, derived from lat/lon via bounding-box lookup.
If the device's coordinates move to a different country, a new version is recorded.

**Natural key:** `(iot_device_id, country_code)` — country is scoped per device
**Surrogate key:** `country_sk = MD5(iot_device_id || country_code || effective_start)`

| Column | Type | Notes |
|---|---|---|
| `country_sk` | STRING | Surrogate PK |
| `iot_device_id` | INT | Links to device |
| `country_code` | STRING | ISO 3166-1 alpha-2 |
| `country_name` | STRING | |
| `city` | STRING | Nearest major city |
| `effective_start` | TIMESTAMP | |
| `effective_end` | TIMESTAMP | NULL = currently active |
| `is_current` | BOOLEAN | |

**Country bounding-box lookup table (`country_lookup.py`):**

| Lat range | Lon range | country_code | country_name | city |
|---|---|---|---|---|
| 47.3 – 55.1 | 5.9 – 15.0 | DE | Germany | Berlin |
| 41.3 – 51.2 | -5.2 – 9.6 | FR | France | Paris |
| fallback | | XX | Unknown | Unknown |

---

## 5. Fact Table — `lakehouse.warehouse.fact_measurements` (append-only)

FK resolution: at write time, join new staging rows against
`dim_iot_device WHERE is_current = true` and `dim_country WHERE is_current = true`
to retrieve the current surrogate keys.

**Partition:** `days(event_time)` — Trino prunes to day files on time-range queries.
**Sort order:** `(event_time, iot_device_id)` — enables min/max file skipping within partitions.
**Denormalized columns** (`iot_device_id`, `country_code`) carried at write time so Grafana panels require no joins for 95% of queries.

| Column | Type | Notes |
|---|---|---|
| `fact_sk` | STRING | `UUID()` per row — surrogate PK |
| `date_key` | INT | FK → `dim_date.date_key` |
| `device_sk` | STRING | FK → `dim_iot_device.device_sk` |
| `country_sk` | STRING | FK → `dim_country.country_sk` |
| `iot_device_id` | INT | Denormalized — join-free device filter |
| `country_code` | STRING | Denormalized — join-free country filter |
| `event_time` | TIMESTAMP | Original measurement timestamp (`data.dt`) |
| `ingestion_time` | TIMESTAMP | When the event entered the platform |
| `sunrise` | TIMESTAMP | `data.sunrise` |
| `sunset` | TIMESTAMP | `data.sunset` |
| `temp` | DOUBLE | `data.temp` |
| `feels_like` | DOUBLE | `data.feels_like` |
| `pressure` | INT | `data.pressure` |
| `humidity` | INT | `data.humidity` |
| `dew_point` | DOUBLE | `data.dew_point` |
| `uvi` | DOUBLE | `data.uvi` |
| `clouds` | INT | `data.clouds` |
| `visibility` | INT | `data.visibility` |
| `wind_speed` | DOUBLE | `data.wind_speed` |
| `wind_deg` | INT | `data.wind_deg` |
| `wind_gust` | DOUBLE | `data.wind_gust` |
| `weather_id` | INT | `data.weather[0].id` |
| `weather_main` | STRING | `data.weather[0].main` |
| `weather_description` | STRING | `data.weather[0].description` |
| `rain_1h` | DOUBLE | `data.rain.1h` (nullable) |

---

## 5a. Hourly Aggregate Table — `lakehouse.warehouse.fact_hourly_agg` (MERGE upsert)

Pre-aggregated rollup computed from `fact_measurements` each run.
Powers overview dashboard panels (avg temp per station/country per hour) without full fact scans.

**Natural key:** `(hour_bucket, iot_device_id)` — MERGE upsert to recompute partial hours.
**Partition:** `days(hour_bucket)`.

| Column | Type | Notes |
|---|---|---|
| `hour_bucket` | TIMESTAMP | `date_trunc('hour', event_time)` |
| `iot_device_id` | INT | Natural key component |
| `country_code` | STRING | Denormalized from fact |
| `avg_temp` | DOUBLE | |
| `avg_feels_like` | DOUBLE | |
| `avg_humidity` | DOUBLE | |
| `avg_pressure` | DOUBLE | |
| `avg_wind_speed` | DOUBLE | |
| `avg_uvi` | DOUBLE | |
| `avg_clouds` | DOUBLE | |
| `avg_visibility` | DOUBLE | |
| `avg_rain_1h` | DOUBLE | null when no rain events in hour |

---

## 6. Execution Flow per Run

```
stream.py (spark-submit, trigger availableNow=True)
│
├─ 1. Read s3a://raw/**/*.json  (file stream + checkpoint)
├─ 2. Flatten data.* fields, cast types (dt/sunrise/sunset → from_unixtime)
├─ 3. dropDuplicates(event_time, iot_device_id)  ← within-batch dedup
├─ 4. MERGE INTO staging.events                  ← cross-run dedup
│
└─ foreachBatch(process_batch):
    ├─ 5. upsert dim_date          ← INSERT where date_key not exists
    ├─ 6. SCD2 merge dim_iot_device
    ├─ 7. SCD2 merge dim_country
    ├─ 8. INSERT INTO fact_measurements  ← append, resolve SKs + carry denorm columns
    ├─ 9. MERGE INTO fact_hourly_agg     ← recompute partial hour from fact_measurements
    └─ 10. OPTIMIZE fact_measurements   ← compact small files (last 2 days only)
           WHERE event_time >= current_date - INTERVAL '2' DAY
```

---

## 7. Environment Variables (added to `.env` / docker-compose `spark-stream` service)

| Variable | Value | Notes |
|---|---|---|
| `SPARK_MASTER_URL` | `spark://spark-master:7077` | Already in `.env` |
| `MINIO_ENDPOINT` | `http://minio:9000` | Already in `.env` |
| `MINIO_ACCESS_KEY` | `admin` | Already in `.env` |
| `MINIO_SECRET_KEY` | `password123` | Already in `.env` |
| `POLL_INTERVAL_SECONDS` | `18` | Reused — sleep between runs |

---

## 8. Implementation Order

- [x] `config.yaml` — static config (paths, tables, field lists, SCD keys, country lookup)
- [x] `config.py` — loader: reads YAML, overlays env vars, exposes flat constants
- [ ] `utils/country_lookup.py` — pure Python bounding-box lookup using `COUNTRY_LOOKUP`
- [ ] `staging.py` — flatten, dedup, MERGE INTO staging using `STAGING_*` constants
- [ ] `warehouse/dim_date.py`
- [ ] `warehouse/dim_iot_device.py` — SCD Type 2 using `DIM_DEVICE_*` constants
- [ ] `warehouse/dim_country.py` — SCD Type 2 using `DIM_COUNTRY_*` constants
- [ ] `warehouse/fact_measurements.py` — append + FK join + denorm columns; set partition/sort/table_properties at CREATE
- [ ] `warehouse/fact_hourly_agg.py` — MERGE upsert hourly rollup from `fact_measurements`
- [ ] `stream.py` — wire everything together; add OPTIMIZE call after fact insert
- [ ] `Dockerfile` + `requirement.txt`
- [ ] Update `docker-compose.yaml` `spark-stream` environment block
- [ ] Update `trino/catalog/iceberg.properties` — add metadata cache settings
- [ ] Update `trino/config.properties`
