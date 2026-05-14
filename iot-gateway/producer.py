import json
import logging
import random
import time
from datetime import datetime, timezone

import requests
from confluent_kafka import Producer

import config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

# ── Per-device metric state (last emitted `current` block per device_id) ───────
# Populated on first OWM call; used as the basis for mock fluctuation rounds.
_device_state: dict[int, dict] = {}

# ── Last real OWM snapshot per device ────────────────────────────────────────
# Updated ONLY on successful OWM API calls.  Mock rounds always fluctuate
# against this snapshot so drift is bounded to ±2 % of the last real reading.
_owm_snapshot: dict[int, dict] = {}

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

# ── Physical bounds for clamping after fluctuation ────────────────────────────
# (min, max) — None means unbounded in that direction
_BOUNDS: dict[str, tuple] = {
    "temp":        (-60.0,  60.0),
    "feels_like":  (-65.0,  65.0),
    "dew_point":   (-65.0,  55.0),
    "pressure":    (870,   1084),
    "humidity":    (0,      100),
    "uvi":         (0.0,    12.0),
    "clouds":      (0,      100),
    "visibility":  (100,   10000),
    "wind_speed":  (0.0,    None),
    "wind_gust":   (0.0,    None),
}
# Integer fields (rounded after fluctuation)
_INT_FIELDS = {"pressure", "humidity", "clouds", "visibility"}

# ── Timezone offset table keyed by approximate longitude band ─────────────────
def _timezone_for(lat: float, lon: float) -> str:
    """Very rough lon→timezone string (good enough for fake/mock data)."""
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


# ── Fluctuation helper ─────────────────────────────────────────────────────────

def _fluctuate(value: float, lo, hi) -> float:
    """
    Apply a random walk of at most ±2 % of the current value.
    A minimum absolute step of 0.01 prevents zero-valued metrics from freezing.
    Result is clamped to [lo, hi].
    """
    pct = random.uniform(-0.02, 0.02)
    delta = value * pct if abs(value) > 0.01 else random.uniform(-0.01, 0.01)
    new = value + delta
    if lo is not None:
        new = max(lo, new)
    if hi is not None:
        new = min(hi, new)
    return new


# ── Cold-start seed (used only when OWM is unreachable and state is empty) ────

def _seed_weather(station: dict) -> dict:
    """Generate a latitude-aware seed OWM payload with no previous state."""
    now = int(time.time())
    lat, lon = station["lat"], station["lon"]

    base_temp  = 25.0 - abs(lat) * 0.55 + random.gauss(0, 4)
    temp       = round(base_temp, 2)
    feels_like = round(temp + random.uniform(-3, 3), 2)
    dew_point  = round(temp - random.uniform(5, 15), 2)

    condition = random.choice(_WEATHER_CONDITIONS)
    weather_id, weather_main, weather_desc = condition
    clouds   = random.randint(0, 100)
    is_rainy = weather_main in ("Rain", "Drizzle", "Thunderstorm")

    solar_offset = int(lon / 15 * 3600)
    today_noon   = (now // 86400) * 86400 + 43200
    sunrise = today_noon - 21600 + solar_offset
    sunset  = today_noon + 21600 + solar_offset

    current: dict = {
        "dt":         now,
        "sunrise":    sunrise,
        "sunset":     sunset,
        "temp":       temp,
        "feels_like": feels_like,
        "pressure":   random.randint(990, 1030),
        "humidity":   random.randint(20, 95),
        "dew_point":  dew_point,
        "uvi":        round(random.uniform(0, 12), 2),
        "clouds":     clouds,
        "visibility": random.randint(1000, 10000),
        "wind_speed": round(random.uniform(0, 15), 2),
        "wind_deg":   random.randint(0, 359),
        "wind_gust":  round(random.uniform(0, 20), 2),
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


# ── Real OWM fetch ─────────────────────────────────────────────────────────────

def _fetch_owm(station: dict) -> dict:
    """
    Call OWM One Call 3.0 for a single station.
    Returns the raw OWM response dict (lat/lon/timezone/current).
    Raises requests.HTTPError on non-2xx responses.
    """
    resp = requests.get(
        config.OWM_API_URL,
        params={
            "lat":     station["lat"],
            "lon":     station["lon"],
            "appid":   config.OWM_API_KEY,
            "units":   "metric",
            "exclude": "minutely,hourly,daily,alerts",
        },
        timeout=10,
    )
    resp.raise_for_status()
    return resp.json()


# ── Mock fluctuation from stored state ────────────────────────────────────────

def _mock_from_state(station: dict) -> dict:
    """
    Derive the next reading by applying ≤2 % random fluctuation to every
    numeric metric in the device's last known state.

    Non-numeric / slowly changing fields (weather condition, sunrise, sunset)
    are retained unchanged.  `dt` is always updated to the current epoch.
    wind_deg wraps modulo 360.
    """
    device_id = station["id"]
    prev   = _owm_snapshot.get(device_id) or _device_state[device_id]
    prev_c = prev["current"]
    now    = int(time.time())

    current: dict = {
        "dt":      now,
        "sunrise": prev_c["sunrise"],
        "sunset":  prev_c["sunset"],
        "weather": prev_c["weather"],           # condition only changes on OWM rounds
    }

    # Fluctuate each bounded numeric metric
    for key, (lo, hi) in _BOUNDS.items():
        prev_val = prev_c.get(key)
        if prev_val is None:
            continue
        new_val = _fluctuate(float(prev_val), lo, hi)
        current[key] = int(round(new_val)) if key in _INT_FIELDS else round(new_val, 2)

    # wind_deg: fluctuate ±3 % then wrap 0-359
    if "wind_deg" in prev_c:
        new_deg = _fluctuate(float(prev_c["wind_deg"]), None, None) % 360
        current["wind_deg"] = int(round(new_deg))

    # dew_point must stay ≤ temp after fluctuation
    if "dew_point" in prev_c:
        lo_dp, hi_dp = _BOUNDS["dew_point"]
        new_dp = _fluctuate(float(prev_c["dew_point"]), lo_dp, hi_dp)
        current["dew_point"] = round(min(new_dp, current.get("temp", new_dp)), 2)

    # Optional rain: fluctuate if present in previous state
    if "rain" in prev_c and "1h" in prev_c["rain"]:
        current["rain"] = {"1h": round(_fluctuate(float(prev_c["rain"]["1h"]), 0.0, None), 2)}

    return {
        "lat":      prev["lat"],
        "lon":      prev["lon"],
        "timezone": prev["timezone"],
        "current":  current,
    }


# ── Kafka event envelope ───────────────────────────────────────────────────────

def build_event(raw: dict, station: dict) -> dict:
    """Wrap an OWM raw payload with station/device metadata."""
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


# ── Main loop ──────────────────────────────────────────────────────────────────

def main():
    producer = Producer({"bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS})

    logger.info(
        "IoT Gateway started | stations=%d topic=%s | OWM every 10th round, mock ±2%% otherwise",
        len(config.STATIONS), config.KAFKA_TOPIC,
    )

    # Strategy
    # ─────────
    # Rounds 1, 11, 21, … (i.e. round_num % 10 == 1): call OWM API for all 60 devices
    # Rounds 2-10, 12-20, …: apply ≤2% minute-over-minute mock fluctuation per device
    #
    # Cadence: 1 station/second → 60 stations = 1 full round/minute.
    # On an OWM round, each device call takes ~0.1 s for the HTTP request; the
    # remaining time is consumed by the 1-second sleep so the overall pace is the same.

    round_num = 0
    while True:
        round_num += 1
        use_real_api = (round_num % 10 == 1)   # True on rounds 1, 11, 21, …

        if use_real_api:
            logger.info("Round %d — OWM API round (calling live data for all %d devices)",
                        round_num, len(config.STATIONS))
        else:
            logger.info("Round %d — mock fluctuation round (±2%% from previous state)",
                        round_num)

        for station in config.STATIONS:
            tick_start = time.monotonic()
            device_id = station["id"]
            partition = (device_id - 1) % config.KAFKA_NUM_PARTITIONS

            try:
                # ── Decide data source ─────────────────────────────────────
                if use_real_api or device_id not in _device_state:
                    # OWM round, or first-ever call for this device
                    try:
                        raw = _fetch_owm(station)
                        source = "owm"
                    except Exception as owm_err:
                        logger.warning(
                            "OWM fetch failed for device=%d (%s), falling back to %s",
                            device_id, owm_err,
                            "mock" if device_id in _device_state else "seed",
                        )
                        raw = (_mock_from_state(station)
                               if device_id in _device_state
                               else _seed_weather(station))
                        source = "fallback"
                else:
                    # Mock round: fluctuate previous state
                    raw    = _mock_from_state(station)
                    source = "mock"

                # ── Update per-device state ────────────────────────────────
                _device_state[device_id] = raw
                if source == "owm":
                    _owm_snapshot[device_id] = raw

                # ── Publish to Kafka ───────────────────────────────────────
                event = build_event(raw, station)
                producer.produce(
                    topic=config.KAFKA_TOPIC,
                    partition=partition,
                    key=str(device_id).encode("utf-8"),
                    value=json.dumps(event).encode("utf-8"),
                    callback=delivery_report,
                )
                producer.poll(0)

                logger.info(
                    "Published | round=%d [%s] device=%d part=%d temp=%.1f weather=%s",
                    round_num, source, device_id, partition,
                    event["data"].get("temp", 0),
                    event["data"]["weather"][0]["main"],
                )

            except Exception as exc:
                logger.error("Error for device %d: %s", device_id, exc)

            # Sleep for the remainder of the 1-second slot, accounting for
            # OWM HTTP latency and processing time so every round stays ~60 s.
            elapsed = time.monotonic() - tick_start
            sleep_for = max(0.0, 1.0 - elapsed)
            if sleep_for > 0:
                time.sleep(sleep_for)

        logger.info("Round %d complete (%d stations).", round_num, len(config.STATIONS))


if __name__ == "__main__":
    main()

