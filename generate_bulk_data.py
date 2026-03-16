# ============================================================
# generate_bulk_data.py
# StreamPulse - Bulk Historical Data Generator
# Generates 1000 historical viewing records
# Saves to data/streaming_history.json
# Author: Khushboo Patel
# ============================================================

import json
import random
import uuid
from datetime import datetime, timedelta

# Seed for reproducibility
random.seed(42)

# ── Content Library ──────────────────────────────────────────
CONTENTS = [
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

DEVICES = [
    {"device_type": "SmartTV",  "os": "Android TV"},
    {"device_type": "Mobile",   "os": "Android"},
    {"device_type": "Mobile",   "os": "iOS"},
    {"device_type": "Laptop",   "os": "Windows"},
    {"device_type": "Tablet",   "os": "iOS"},
]

SUBSCRIPTIONS = [
    {"plan": "BASIC",    "price": 149},
    {"plan": "STANDARD", "price": 299},
    {"plan": "PREMIUM",  "price": 649},
]

LOCATIONS = [
    {"city": "Mumbai",    "state": "Maharashtra", "region": "West"},
    {"city": "Delhi",     "state": "Delhi",       "region": "North"},
    {"city": "Bangalore", "state": "Karnataka",   "region": "South"},
    {"city": "Hyderabad", "state": "Telangana",   "region": "South"},
    {"city": "Chennai",   "state": "Tamil Nadu",  "region": "South"},
    {"city": "Pune",      "state": "Maharashtra", "region": "West"},
    {"city": "Kolkata",   "state": "West Bengal", "region": "East"},
    {"city": "Ahmedabad", "state": "Gujarat",     "region": "West"},
]

ACTIONS = [
    "PLAY", "PLAY", "PLAY",
    "PAUSE", "STOP", "RESUME", "COMPLETE"
]

AGE_GROUPS = ["18-24", "25-34", "35-44", "45-54", "55+"]

# ── Generate Records ─────────────────────────────────────────
def generate_bulk_records(num_records=1000):
    """
    Generates historical viewing records.
    These represent past viewing data —
    like 3 months of history already in the system.
    """
    records = []
    start_date = datetime(2025, 12, 1)  # 3 months of history

    for i in range(num_records):
        content      = random.choice(CONTENTS)
        device       = random.choice(DEVICES)
        location     = random.choice(LOCATIONS)
        subscription = random.choice(SUBSCRIPTIONS)
        action       = random.choice(ACTIONS)

        # Random date in last 3 months
        event_date = start_date + timedelta(
            days=random.randint(0, 90),
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59)
        )

        total_duration = content["avg_duration"]
        watch_duration = random.randint(1, total_duration)
        completion_pct = round(
            (watch_duration / total_duration) * 100, 2
        )

        records.append({
            # Event Details
            "event_id":            str(uuid.uuid4()),
            "event_timestamp":     event_date.strftime(
                                   "%Y-%m-%d %H:%M:%S"),
            "event_hour":          event_date.hour,
            "event_day":           event_date.strftime("%A"),
            "event_month":         event_date.month,
            "is_prime_time":       1 if 18 <= event_date.hour
                                   <= 23 else 0,
            "is_weekend":          1 if event_date.weekday()
                                   >= 5 else 0,

            # User Details
            "user_id":             f"USR-{random.randint(1000, 9999)}",
            "age_group":           random.choice(AGE_GROUPS),

            # Content Details
            "content_id":          content["content_id"],
            "content_title":       content["title"],
            "content_type":        content["type"],
            "genre":               content["genre"],
            "language":            content["language"],

            # Watch Details
            "action":              action,
            "watch_duration_mins": watch_duration,
            "total_duration_mins": total_duration,
            "completion_pct":      completion_pct,
            "is_completed":        1 if completion_pct >= 90
                                   else 0,
            "is_dropped":          1 if completion_pct < 10
                                   else 0,

            # Device Details
            "device_type":         device["device_type"],
            "device_os":           device["os"],

            # Location Details
            "city":                location["city"],
            "state":               location["state"],
            "region":              location["region"],

            # Subscription Details
            "subscription_plan":   subscription["plan"],
            "subscription_price":  subscription["price"],
        })

    return records


# ── Save To JSON ─────────────────────────────────────────────
def save_to_json(records, filepath):
    """
    Saves records to JSON file.
    This file goes on GitHub and is used
    as bulk historical data source in Bronze layer.
    """
    with open(filepath, 'w') as f:
        json.dump(records, f, indent=2)
    print(f"✅ Saved {len(records):,} records to {filepath}")


# ── Main ─────────────────────────────────────────────────────
if __name__ == "__main__":
    print("🎬 StreamPulse - Generating bulk historical data...")
    print("=" * 50)

    # Generate 1000 records
    records = generate_bulk_records(num_records=1000)

    # Save to data folder
    filepath = "data/streaming_history.json"
    save_to_json(records, filepath)

    # Print summary
    print("\n📊 Data Summary:")
    print(f"Total records: {len(records):,}")

    # Count by content
    from collections import Counter
    titles = [r["content_title"] for r in records]
    print("\nTop 5 most watched:")
    for title, count in Counter(titles).most_common(5):
        print(f"  {title}: {count} views")

    # Count by city
    cities = [r["city"] for r in records]
    print("\nTop 5 cities:")
    for city, count in Counter(cities).most_common(5):
        print(f"  {city}: {count} views")

    # Count by device
    devices = [r["device_type"] for r in records]
    print("\nDevice breakdown:")
    for device, count in Counter(devices).most_common():
        print(f"  {device}: {count} views")

    print("\n✅ Bulk data generation complete!")
    print(f"📁 File location: {filepath}")
    print("🚀 Ready to upload to Azure Data Lake!")