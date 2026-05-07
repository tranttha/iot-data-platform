import json
import logging
import math
import random
import sys
import time
from datetime import datetime, timezone

from confluent_kafka import Producer

import config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

# ── Weather condition catalogue (OWM codes, main, description) ────────────────
_WEATHER_CONDITIONS = [
    (800, "Clear",        "clear sky"),
    (801, "Clouds",       "few clouds: 11-25%"),
    (802, "Clouds",       "scattered clouds: 25-50%"),
    (803, "Clouds",       "broken clouds: 51-84%"),
    (804, "Clouds",       "overcast clouds: 85-100%"),
    (500, "Rain",         "light rain"),
    (501, "Rain",         "moderate rain"),
    (502, "Rain",         "heavy intensity rain"),
    (300, "Drizzle",      "light intensity drizzle"),
    (600, "Snow",         "light snow"),
    (601, "Snow",         "snow"),
    (701, "Mist",         "mist"),
    (721, "Haze",         "haze"),
    (741, "Fog",          "fog"),
    (200, "Thunderstorm", "thunderstorm with light rain"),
    (201, "Thunderstorm", "thunderstorm with rain"),
]

# ── Timezone offset table keyed by approximate longitude band ─────────────────
def _timezone_for(lat: float, lon: float) -> str:
    """Very rough lon→timezone string (good enough for fake data)."""
    offsets = [
        (-180, -157, "America/Honolulu"),
        (-157, -127, "America/Los_Angeles"),
        (-127,  -97, "America/Denver"),
        ( -97,  -67, "America/New_York"),
        ( -67,  -30, "America/Sao_Paulo"),
        ( -30,   15, "Europe/London"),
        (  15,   37, "Europe/Berlin"),
        (  37,   60, "Asia/Dubai"),
        (  60,   90, "Asia/Kolkata"),
        (  90,  120, "Asia/Bangkok"),
        ( 120,  145, "Asia/Tokyo"),
        ( 145,  180, "Australia/Sydney"),
    ]
    for lo, hi, tz in offsets:
        if lo <= lon < hi:
            return tz
    return "UTC"


def _fake_weather(station: dict) -> dict:
    """Generate a fake OWM One Call 3.0 `current` block + envelope."""
    now = int(time.time())
    lat, lon = station["lat"], station["lon"]

    # Base temperature from latitude (rough climate model)
    base_temp = 25.0 - abs(lat) * 0.55 + random.gauss(0, 4)
    temp        = round(base_temp, 2)
    feels_like  = round(temp + random.uniform(-3, 3), 2)
    dew_point   = round(temp - random.uniform(5, 15), 2)

    condition = random.choice(_WEATHER_CONDITIONS)
    weather_id, weather_main, weather_desc = condition

    clouds = random.randint(0, 100)
    is_rainy = weather_main in ("Rain", "Drizzle", "Thunderstorm")

    # Approximate sunrise/sunset: solar noon ≈ lon/15 offset from UTC noon
    solar_offset = int(lon / 15 * 3600)
    today_noon   = (now // 86400) * 86400 + 43200  # UTC noon today
    sunrise = today_noon - 21600 + solar_offset    # ~6 h before noon
    sunset  = today_noon + 21600 + solar_offset    # ~6 h after noon

    current = {
        "dt":          now,
        "sunrise":     sunrise,
        "sunset":      sunset,
        "temp":        temp,
        "feels_like":  feels_like,
        "pressure":    random.randint(990, 1030),
        "humidity":    random.randint(20, 95),
        "dew_point":   dew_point,
        "uvi":         round(random.uniform(0, 12), 2),
        "clouds":      clouds,
        "visibility":  random.randint(1000, 10000),
        "wind_speed":  round(random.uniform(0, 15), 2),
        "wind_deg":    random.randint(0, 359),
        "wind_gust":   round(random.uniform(0, 20), 2),
        "weather": [
            {
                "id":          weather_id,
                "main":        weather_main,
                "description": weather_desc,
                "icon":        "01d",
            }
        ],
    }
    if is_rainy:
        current["rain"] = {"1h": round(random.uniform(0.1, 10.0), 2)}

    return {
        "lat":      lat,
        "lon":      lon,
        "timezone": _timezone_for(lat, lon),
        "current":  current,
    }


def build_event(raw: dict, station: dict) -> dict:
    """Wrap the fake OWM payload with station/device metadata (identical schema)."""
    current = raw.get("current", {})
    return {
        "iot_device_id":  station["id"],
        "ingestion_time": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "data":           current,
        "location": {
            "lat":      raw.get("lat"),
            "lon":      raw.get("lon"),
            "timezone": raw.get("timezone"),
        },
    }


def delivery_report(err, msg):
    if err:
        logger.error("Delivery failed for key %s: %s", msg.key(), err)
    else:
        logger.debug(
            "Delivered to %s [partition %d] offset %d",
            msg.topic(), msg.partition(), msg.offset(),
        )


def main():
    producer = Producer({"bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS})
    # deadline = time.monotonic() + config.KILL_AFTER_SECONDS

    logger.info(
        "IoT Gateway started (faker mode) | stations=%d topic=%s ", \
        # "poll=%ds kill_after=%ds",
        len(config.STATIONS), config.KAFKA_TOPIC
        # ,
        # config.POLL_INTERVAL_SECONDS, config.KILL_AFTER_SECONDS,
    )

    # ── Continuous loop: 1 station/second, 60 stations → 1 full round/minute ──
    round_num = 0
    while True:
        # if time.monotonic() >= deadline:
        #     logger.info(
        #         "Kill timer expired after %ds. Flushing and shutting down.",
        #         config.KILL_AFTER_SECONDS,
        #     )
        #     producer.flush()
        #     sys.exit(0)

        round_num += 1
        for station in config.STATIONS:
            # if time.monotonic() >= deadline:
            #     break
            partition = (station["id"] - 1) % config.KAFKA_NUM_PARTITIONS
            try:
                raw   = _fake_weather(station)
                event = build_event(raw, station)
                producer.produce(
                    topic=config.KAFKA_TOPIC,
                    partition=partition,
                    key=str(station["id"]).encode("utf-8"),
                    value=json.dumps(event).encode("utf-8"),
                    callback=delivery_report,
                )
                producer.poll(0)
                logger.info(
                    "Published | round=%d device=%d partition=%d temp=%.1f weather=%s",
                    round_num, station["id"], partition,
                    event["data"].get("temp"), event["data"]["weather"][0]["main"],
                )
            except Exception as exc:
                logger.error("Error for device %d: %s", station["id"], exc)

            time.sleep(1)  # 1 station/second → 1 full round per minute

        logger.info("Round %d complete (%d stations).", round_num, len(config.STATIONS))


if __name__ == "__main__":
    main()

