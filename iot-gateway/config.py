import os
from pathlib import Path

import yaml

# ── Load static config from YAML ──────────────────────────────────────────────
_yaml_path = Path(__file__).parent / "config.yaml"
with _yaml_path.open() as _f:
    _cfg = yaml.safe_load(_f)

_defaults    = _cfg["defaults"]
# _owm         = _cfg["openweathermap"]
# _rate_limits = _cfg["rate_limits"]

# ── Runtime values from environment ───────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
KAFKA_TOPIC             = os.environ.get("KAFKA_TOPIC",           _defaults["kafka_topic"])
KAFKA_NUM_PARTITIONS    = int(os.environ.get("KAFKA_NUM_PARTITIONS", "3"))
# POLL_INTERVAL_SECONDS   = int(os.environ.get("POLL_INTERVAL_SECONDS", _defaults["poll_interval_seconds"]))
# KILL_AFTER_SECONDS      = int(os.environ.get("KILL_AFTER_SECONDS",    _defaults["kill_after_seconds"]))
OWM_API_KEY             = os.environ["OWM_API_KEY"]
OWM_API_URL             = "https://api.openweathermap.org/data/3.0/onecall"

# ── Station list (defined entirely in config.yaml) ─────────────────────────────
STATIONS = _cfg["stations"]  # list of {device_id, city, lat, lon}

# # ── Rate-limit validation ─────────────────────────────────────────────────────
# # batches_per_run = kill_after ÷ poll_interval
# # calls_per_run   = stations × batches_per_run  ≤  max_calls_per_day
# _batches_per_run = KILL_AFTER_SECONDS / POLL_INTERVAL_SECONDS
# _calls_per_run   = len(STATIONS) * _batches_per_run
# if _calls_per_run > _rate_limits["max_calls_per_day"]:
#     raise ValueError(
#         f"{len(STATIONS)} stations × {_batches_per_run:.0f} batches "
#         f"= {_calls_per_run:.0f} calls/run exceeds OWM limit of "
#         f"{_rate_limits['max_calls_per_day']} calls/day."
#     )

# ── OpenWeatherMap One Call 3.0 API ───────────────────────────────────────────
# API_URL = _owm["url"]
# # lat/lon are per-station; appid, units, exclude are constant
# API_BASE_PARAMS = {
#     "appid":   OWM_API_KEY,
#     "units":   _owm["units"],
#     "exclude": _owm["exclude"],
# }
