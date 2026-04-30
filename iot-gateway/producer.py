import json
import logging
import os
import time
from datetime import datetime, timezone

import requests
from confluent_kafka import Producer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

# ── Environment configuration ─────────────────────────────────────────────────
DEVICE_ID               = os.environ["DEVICE_ID"]
LATITUDE                = float(os.environ["LATITUDE"])
LONGITUDE               = float(os.environ["LONGITUDE"])
KAFKA_BOOTSTRAP_SERVERS = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
KAFKA_TOPIC             = os.environ.get("KAFKA_TOPIC", "iot-sensor-data")
POLL_INTERVAL_SECONDS   = int(os.environ.get("POLL_INTERVAL_SECONDS", "60"))

# Open-Meteo free-tier rate limits shared across all gateway containers.
# Binding constraint is the daily quota (10,000/day).
OPENMETEO_MAX_CALLS_PER_MIN = int(os.environ.get("OPENMETEO_MAX_CALLS_PER_MIN", "600"))
NUM_GATEWAY_CONTAINERS       = int(os.environ.get("NUM_GATEWAY_CONTAINERS", "1"))

_min_by_minute = (NUM_GATEWAY_CONTAINERS * 60)      / 600    # per-minute quota
_min_by_hour   = (NUM_GATEWAY_CONTAINERS * 3600)    / 5000   # per-hour quota
_min_by_day    = (NUM_GATEWAY_CONTAINERS * 86400)   / 10000  # per-day quota  ← binding
_min_interval  = max(_min_by_minute, _min_by_hour, _min_by_day)

if POLL_INTERVAL_SECONDS < _min_interval:
    raise ValueError(
        f"POLL_INTERVAL_SECONDS={POLL_INTERVAL_SECONDS} would exceed the Open-Meteo rate limit. "
        f"With {NUM_GATEWAY_CONTAINERS} container(s) the minimum safe interval is "
        f"{_min_interval:.1f}s (bound by daily quota of 10,000 calls/day)."
    )

# ── Open-Meteo API ────────────────────────────────────────────────────────────
API_URL = "https://api.open-meteo.com/v1/forecast"
API_PARAMS = {
    "latitude": LATITUDE,
    "longitude": LONGITUDE,
    "current": [
        "temperature_2m",
        "relative_humidity_2m",
        "apparent_temperature",
        "is_day",
        "precipitation",
        "rain",
        "showers",
        "snowfall",
        "weather_code",
        "cloud_cover",
        "pressure_msl",
        "surface_pressure",
        "wind_speed_10m",
        "wind_direction_10m",
        "wind_gusts_10m",
    ],
}


def fetch_weather() -> dict:
    response = requests.get(API_URL, params=API_PARAMS, timeout=10)
    response.raise_for_status()
    return response.json()


def build_event(raw: dict) -> dict:
    """Enrich the raw API response with device metadata."""
    raw["iot_device_id"] = DEVICE_ID
    raw["ingestion_time"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    return raw


def delivery_report(err, msg):
    if err:
        logger.error("Delivery failed for key %s: %s", msg.key(), err)
    else:
        logger.info(
            "Delivered to %s [partition %d] offset %d",
            msg.topic(), msg.partition(), msg.offset(),
        )


def main():
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})

    logger.info(
        "IoT Gateway started | device=%s lat=%.4f lon=%.4f topic=%s poll=%ds",
        DEVICE_ID, LATITUDE, LONGITUDE, KAFKA_TOPIC, POLL_INTERVAL_SECONDS,
    )

    while True:
        try:
            raw = fetch_weather()
            event = build_event(raw)
            payload = json.dumps(event).encode("utf-8")

            producer.produce(
                topic=KAFKA_TOPIC,
                key=DEVICE_ID.encode("utf-8"),
                value=payload,
                callback=delivery_report,
            )
            producer.poll(0)
            logger.info("Published event for device=%s", DEVICE_ID)

        except requests.RequestException as exc:
            logger.error("API request failed: %s", exc)
        except Exception as exc:
            logger.error("Unexpected error: %s", exc)

        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
