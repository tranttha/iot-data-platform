"""
Pure-Python bounding-box lat/lon → (country_code, country_name, city).

The lookup table is embedded here so this module is fully self-contained and
can be serialised by cloudpickle and executed on Spark workers without any
external imports (no yaml, no config.py, no filesystem access).

Entries are evaluated top-to-bottom: more specific / overlapping bounding
boxes must appear before larger containing regions (e.g. Singapore before
Malaysia, Hong Kong before China, Norway before Russia).
"""

# fmt: off
# Ordering rules: more specific / overlapping bounding boxes MUST appear before
# larger containing ones (e.g. SG before MY before ID, HK before VN, NO before
# SE with narrowed lon, MA before ES, CA/MX tightened to not swallow US cities).
_LOOKUP = [
    # ── Americas ──────────────────────────────────────────────────────────────
    # CA lat_min raised to 43.0 so Chicago (41.85°N) falls through to US.
    {"lat_min": 43.0,   "lat_max": 83.34,  "lon_min": -141.0,  "lon_max":  -52.32, "country_code": "CA", "country_name": "Canada",               "city": "Toronto"},
    # MX lat_max lowered to 32.5 (Tijuana ≈32.53°N) and lon_max tightened to
    # -98.5 so Houston (-95.37), San Antonio (-98.49) and San Diego (32.72°N)
    # all fall through to US.
    {"lat_min": 14.38,  "lat_max": 32.5,   "lon_min": -118.6,  "lon_max":  -98.5,  "country_code": "MX", "country_name": "Mexico",               "city": "Mexico City"},
    {"lat_min":  -4.23, "lat_max": 16.05,  "lon_min":  -82.12, "lon_max":  -66.85, "country_code": "CO", "country_name": "Colombia",             "city": "Bogota"},
    {"lat_min": 24.0,   "lat_max": 49.5,   "lon_min": -125.0,  "lon_max":  -66.9,  "country_code": "US", "country_name": "United States",        "city": "New York"},
    {"lat_min": -56.73, "lat_max": -17.5,  "lon_min": -109.68, "lon_max":  -66.08, "country_code": "CL", "country_name": "Chile",                "city": "Santiago"},
    {"lat_min": -20.2,  "lat_max":  -0.04, "lon_min":  -84.64, "lon_max":  -68.65, "country_code": "PE", "country_name": "Peru",                 "city": "Lima"},
    {"lat_min": -55.19, "lat_max": -21.78, "lon_min":  -73.56, "lon_max":  -53.64, "country_code": "AR", "country_name": "Argentina",            "city": "Buenos Aires"},
    {"lat_min": -33.87, "lat_max":   5.27, "lon_min":  -73.98, "lon_max":  -28.63, "country_code": "BR", "country_name": "Brazil",               "city": "Sao Paulo"},
    # ── Europe ────────────────────────────────────────────────────────────────
    {"lat_min": 49.67,  "lat_max": 61.06,  "lon_min":  -14.02, "lon_max":    2.09, "country_code": "GB", "country_name": "United Kingdom",       "city": "London"},
    {"lat_min": 50.75,  "lat_max": 53.74,  "lon_min":    3.36, "lon_max":    7.23, "country_code": "NL", "country_name": "Netherlands",          "city": "Amsterdam"},
    {"lat_min": 46.37,  "lat_max": 49.02,  "lon_min":    9.53, "lon_max":   17.16, "country_code": "AT", "country_name": "Austria",              "city": "Vienna"},
    {"lat_min": 47.27,  "lat_max": 55.1,   "lon_min":    5.87, "lon_max":   15.04, "country_code": "DE", "country_name": "Germany",              "city": "Berlin"},
    {"lat_min": 35.29,  "lat_max": 47.09,  "lon_min":    6.63, "lon_max":   18.78, "country_code": "IT", "country_name": "Italy",                "city": "Rome"},
    # MA before ES: Casablanca (33.59°N, -7.62°E) overlaps the ES bbox.
    {"lat_min": 20.67,  "lat_max": 36.0,   "lon_min":  -17.32, "lon_max":   -0.99, "country_code": "MA", "country_name": "Morocco",              "city": "Casablanca"},
    {"lat_min": 27.43,  "lat_max": 43.99,  "lon_min":  -18.39, "lon_max":    4.59, "country_code": "ES", "country_name": "Spain",                "city": "Madrid"},
    {"lat_min": 41.3,   "lat_max": 51.31,  "lon_min":   -5.2,  "lon_max":    9.56, "country_code": "FR", "country_name": "France",               "city": "Paris"},
    {"lat_min": 49.0,   "lat_max": 55.04,  "lon_min":   14.07, "lon_max":   24.15, "country_code": "PL", "country_name": "Poland",               "city": "Warsaw"},
    # NO before SE, lon_max narrowed to 15.0°E so Oslo (10.75°E) → NO while
    # Stockholm (18.07°E) misses NO and falls to SE.
    {"lat_min": 57.8,   "lat_max": 71.3,   "lon_min":    4.09, "lon_max":   15.0,  "country_code": "NO", "country_name": "Norway",               "city": "Oslo"},
    {"lat_min": 55.14,  "lat_max": 69.06,  "lon_min":   10.59, "lon_max":   24.18, "country_code": "SE", "country_name": "Sweden",               "city": "Stockholm"},
    # ── Africa ────────────────────────────────────────────────────────────────
    {"lat_min": 21.99,  "lat_max": 31.83,  "lon_min":   24.65, "lon_max":   37.12, "country_code": "EG", "country_name": "Egypt",                "city": "Cairo"},
    {"lat_min":  4.07,  "lat_max": 13.89,  "lon_min":    2.68, "lon_max":   14.68, "country_code": "NG", "country_name": "Nigeria",              "city": "Lagos"},
    {"lat_min":  -4.9,  "lat_max":  4.62,  "lon_min":   33.91, "lon_max":   41.91, "country_code": "KE", "country_name": "Kenya",                "city": "Nairobi"},
    {"lat_min": -47.18, "lat_max": -22.13, "lon_min":   16.33, "lon_max":   38.29, "country_code": "ZA", "country_name": "South Africa",         "city": "Johannesburg"},
    # ── Middle East ───────────────────────────────────────────────────────────
    {"lat_min": 22.63,  "lat_max": 26.15,  "lon_min":   51.42, "lon_max":   56.6,  "country_code": "AE", "country_name": "United Arab Emirates", "city": "Dubai"},
    {"lat_min": 16.29,  "lat_max": 32.15,  "lon_min":   34.46, "lon_max":   55.67, "country_code": "SA", "country_name": "Saudi Arabia",         "city": "Riyadh"},
    {"lat_min": 24.84,  "lat_max": 39.78,  "lon_min":   44.03, "lon_max":   63.33, "country_code": "IR", "country_name": "Iran",                 "city": "Tehran"},
    {"lat_min": 35.81,  "lat_max": 42.3,   "lon_min":   25.57, "lon_max":   44.82, "country_code": "TR", "country_name": "Turkey",               "city": "Istanbul"},
    {"lat_min": 41.19,  "lat_max": 82.06,  "lon_min":   19.64, "lon_max":  180.0,  "country_code": "RU", "country_name": "Russia",               "city": "Moscow"},
    # ── Asia ──────────────────────────────────────────────────────────────────
    {"lat_min":  6.55,  "lat_max": 35.67,  "lon_min":   67.95, "lon_max":   97.4,  "country_code": "IN", "country_name": "India",                "city": "Mumbai"},
    # SG tiny bbox must precede MY and ID (both contain SG's coordinates).
    {"lat_min":  1.13,  "lat_max":  1.51,  "lon_min":  103.57, "lon_max":  104.57, "country_code": "SG", "country_name": "Singapore",            "city": "Singapore"},
    # MY before ID: Kuala Lumpur (3.14°N) is inside ID's large bbox.
    {"lat_min":  0.85,  "lat_max":  8.38,  "lon_min":   98.74, "lon_max":  119.47, "country_code": "MY", "country_name": "Malaysia",             "city": "Kuala Lumpur"},
    {"lat_min": -11.21, "lat_max":  6.27,  "lon_min":   94.77, "lon_max":  141.02, "country_code": "ID", "country_name": "Indonesia",            "city": "Jakarta"},
    {"lat_min":  5.61,  "lat_max": 20.46,  "lon_min":   97.34, "lon_max":  105.64, "country_code": "TH", "country_name": "Thailand",             "city": "Bangkok"},
    # HK tiny bbox before VN: Hong Kong (22.32°N, 114.17°E) is inside VN bbox.
    {"lat_min": 22.14,  "lat_max": 22.57,  "lon_min":  113.82, "lon_max":  114.5,  "country_code": "HK", "country_name": "Hong Kong",            "city": "Hong Kong"},
    {"lat_min":  7.69,  "lat_max": 23.39,  "lon_min":  102.14, "lon_max":  114.86, "country_code": "VN", "country_name": "Vietnam",              "city": "Ho Chi Minh City"},
    {"lat_min": 10.33,  "lat_max": 26.44,  "lon_min":  114.29, "lon_max":  122.33, "country_code": "TW", "country_name": "Taiwan",               "city": "Taipei"},
    {"lat_min": 32.91,  "lat_max": 38.62,  "lon_min":  124.37, "lon_max":  132.12, "country_code": "KR", "country_name": "South Korea",          "city": "Seoul"},
    {"lat_min": 20.21,  "lat_max": 45.71,  "lon_min":  122.71, "lon_max":  154.21, "country_code": "JP", "country_name": "Japan",                "city": "Tokyo"},
    {"lat_min":  8.67,  "lat_max": 53.56,  "lon_min":   73.5,  "lon_max":  134.78, "country_code": "CN", "country_name": "China",                "city": "Beijing"},
    # ── Oceania ───────────────────────────────────────────────────────────────
    {"lat_min": -47.29, "lat_max": -34.0,  "lon_min":  166.43, "lon_max":  178.55, "country_code": "NZ", "country_name": "New Zealand",          "city": "Auckland"},
    {"lat_min": -43.74, "lat_max": -10.07, "lon_min":  113.16, "lon_max":  153.64, "country_code": "AU", "country_name": "Australia",            "city": "Sydney"},
]
# fmt: on


def lookup(lat: float, lon: float) -> tuple:
    """Return (country_code, country_name, city) for the given coordinates.

    Falls back to ('XX', 'Unknown', 'Unknown') if no bounding box matches.
    """
    if lat is None or lon is None:
        return "XX", "Unknown", "Unknown"
    for entry in _LOOKUP:
        if (entry["lat_min"] <= lat <= entry["lat_max"] and
                entry["lon_min"] <= lon <= entry["lon_max"]):
            return entry["country_code"], entry["country_name"], entry["city"]
    return "XX", "Unknown", "Unknown"
