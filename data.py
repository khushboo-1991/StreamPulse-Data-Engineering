# ============================================================
# data.py
# StreamPulse - Event Generator
# Generates fake streaming watch events
# Author: Khushboo Patel
# ============================================================

import random
import uuid
from datetime import datetime
from faker import Faker

fake = Faker('en_IN')  # Indian locale

# ── Content Library ──────────────────────────────────────────
CONTENT_MAPPING = [
    {"content_id": "CNT-001", "title": "Sacred Games",
     "type": "Series", "genre": "Thriller",
     "language": "Hindi", "avg_duration": 52},
    {"content_id": "CNT-002", "title": "Mirzapur",
     "type": "Series", "genre": "Crime",
     "language": "Hindi", "avg_duration": 48},
    {"content_id": "CNT-003", "title": "Panchayat",
     "type": "Series", "genre": "Comedy",
     "language": "Hindi", "avg_duration": 30},
    {"content_id": "CNT-004", "title": "RRR",
     "type": "Movie", "genre": "Action",
     "language": "Telugu", "avg_duration": 182},
    {"content_id": "CNT-005", "title": "KGF Chapter 2",
     "type": "Movie", "genre": "Action",
     "language": "Kannada", "avg_duration": 168},
    {"content_id": "CNT-006", "title": "Scam 1992",
     "type": "Series", "genre": "Drama",
     "language": "Hindi", "avg_duration": 55},
    {"content_id": "CNT-007", "title": "The Family Man",
     "type": "Series", "genre": "Action",
     "language": "Hindi", "avg_duration": 45},
    {"content_id": "CNT-008", "title": "Delhi Crime",
     "type": "Series", "genre": "Crime",
     "language": "Hindi", "avg_duration": 50},
    {"content_id": "CNT-009", "title": "Rocket Boys",
     "type": "Series", "genre": "Drama",
     "language": "Hindi", "avg_duration": 48},
    {"content_id": "CNT-010", "title": "Drishyam 2",
     "type": "Movie", "genre": "Thriller",
     "language": "Hindi", "avg_duration": 152},
]

# ── Device Mapping ───────────────────────────────────────────
DEVICE_MAPPING = [
    {"device_type": "SmartTV",  "os": "Android TV"},
    {"device_type": "Mobile",   "os": "Android"},
    {"device_type": "Mobile",   "os": "iOS"},
    {"device_type": "Laptop",   "os": "Windows"},
    {"device_type": "Tablet",   "os": "iOS"},
]

# ── Subscription Mapping ─────────────────────────────────────
SUBSCRIPTION_MAPPING = [
    {"plan": "BASIC",    "price": 149},
    {"plan": "STANDARD", "price": 299},
    {"plan": "PREMIUM",  "price": 649},
]

# ── Location Mapping ─────────────────────────────────────────
LOCATION_MAPPING = [
    {"city": "Mumbai",    "state": "Maharashtra", "region": "West"},
    {"city": "Delhi",     "state": "Delhi",       "region": "North"},
    {"city": "Bangalore", "state": "Karnataka",   "region": "South"},
    {"city": "Hyderabad", "state": "Telangana",   "region": "South"},
    {"city": "Chennai",   "state": "Tamil Nadu",  "region": "South"},
    {"city": "Pune",      "state": "Maharashtra", "region": "West"},
    {"city": "Kolkata",   "state": "West Bengal", "region": "East"},
    {"city": "Ahmedabad", "state": "Gujarat",     "region": "West"},
]

# ── Action Mapping ───────────────────────────────────────────
ACTION_MAPPING = [
    "PLAY", "PLAY", "PLAY",  # weighted more toward PLAY
    "PAUSE", "STOP", "RESUME", "COMPLETE"
]

# ── Age Groups ───────────────────────────────────────────────
AGE_GROUPS = ["18-24", "25-34", "35-44", "45-54", "55+"]


# ── Main Event Generator Function ───────────────────────────
def generate_stream_event():
    """
    Generates a single fake streaming watch event.
    This simulates what happens when a user presses
    play on StreamPulse platform.
    """
    content      = random.choice(CONTENT_MAPPING)
    device       = random.choice(DEVICE_MAPPING)
    location     = random.choice(LOCATION_MAPPING)
    subscription = random.choice(SUBSCRIPTION_MAPPING)
    action       = random.choice(ACTION_MAPPING)
    event_time   = datetime.utcnow()

    total_duration = content["avg_duration"]
    watch_duration = random.randint(1, total_duration)
    completion_pct = round((watch_duration / total_duration) * 100, 2)

    return {
        # Event Details
        "event_id":             str(uuid.uuid4()),
        "event_timestamp":      event_time.isoformat(),
        "event_hour":           event_time.hour,
        "event_day":            event_time.strftime("%A"),
        "event_month":          event_time.month,
        "is_prime_time":        1 if 18 <= event_time.hour <= 23 else 0,
        "is_weekend":           1 if event_time.weekday() >= 5 else 0,

        # User Details
        "user_id":              f"USR-{random.randint(1000, 9999)}",
        "age_group":            random.choice(AGE_GROUPS),

        # Content Details
        "content_id":           content["content_id"],
        "content_title":        content["title"],
        "content_type":         content["type"],
        "genre":                content["genre"],
        "language":             content["language"],

        # Watch Details
        "action":               action,
        "watch_duration_mins":  watch_duration,
        "total_duration_mins":  total_duration,
        "completion_pct":       completion_pct,
        "is_completed":         1 if completion_pct >= 90 else 0,
        "is_dropped":           1 if completion_pct < 10 else 0,

        # Device Details
        "device_type":          device["device_type"],
        "device_os":            device["os"],

        # Location Details
        "city":                 location["city"],
        "state":                location["state"],
        "region":               location["region"],

        # Subscription Details
        "subscription_plan":    subscription["plan"],
        "subscription_price":   subscription["price"],
    }