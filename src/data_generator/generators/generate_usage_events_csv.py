import uuid
import random
import csv
import json
import pyodbc
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

OUTPUT_FILE = "usage_events.csv"
TARGET_EVENTS = 1_000_000

DEVICES = ["mobile", "web", "tablet"]
PLATFORMS = ["iOS", "Android", "Web"]
EVENT_TYPES = ["login", "content_view", "feature_used", "session_end"]


# ----------------------------
# LOAD SEED DATA
# ----------------------------
def load_seed_subscriptions():
    conn = pyodbc.connect(os.getenv("AZURE_SQL_CONN_STR"))
    cur = conn.cursor()

    cur.execute("""
        SELECT subscription_id, user_id, plan_id
        FROM subscriptions
        WHERE status = 'active'
    """)

    subs = [
        {
            "subscription_id": r[0],
            "user_id": r[1],
            "plan_id": r[2]
        }
        for r in cur.fetchall()
    ]

    cur.close()
    conn.close()

    return subs


# ----------------------------
# PLAN BEHAVIOR
# ----------------------------
def plan_behavior(plan_id):
    if plan_id == "free":
        return {"sessions": (1, 2), "views": (1, 3), "feature_prob": 0.2}
    elif plan_id == "pro":
        return {"sessions": (2, 5), "views": (3, 8), "feature_prob": 0.6}
    else:
        return {"sessions": (4, 8), "views": (5, 12), "feature_prob": 0.8}


# ----------------------------
# SEASONALITY
# ----------------------------
def seasonal_multiplier(dt: datetime):
    m = 1.0

    # summer spike
    if dt.month in [6, 7, 8]:
        m *= 1.4

    # weekend spike
    if dt.weekday() >= 5:
        m *= 1.25

    # evening spike
    if 18 <= dt.hour <= 23:
        m *= 1.1

    return m


def random_time():
    base = datetime(2025, 1, 1)
    return base + timedelta(seconds=random.randint(0, 60 * 60 * 24 * 365))


# ----------------------------
# EVENT CREATION (UPDATED)
# ----------------------------
def create_event(sub, session_id, event_type, timestamp):
    return {
        "event_id": str(uuid.uuid4()),
        "user_id": sub["user_id"],
        "subscription_id": sub["subscription_id"],
        "session_id": session_id,
        "event_type": event_type,
        "event_timestamp": timestamp.isoformat(),
        "content_id": random.randint(1, 1000) if event_type == "content_view" else None,
        "feature_name": "search" if event_type == "feature_used" else None,
        "device_type": random.choice(DEVICES),
        "platform": random.choice(PLATFORMS),
        "event_properties": {
            "duration_sec": random.randint(10, 300),
            "referrer": random.choice(["homepage", "search", "ads"])
        }
    }


# ----------------------------
# SESSION GENERATION
# ----------------------------
def generate_session(sub):
    session_id = str(uuid.uuid4())
    base_time = random_time()

    profile = plan_behavior(sub["plan_id"])
    multiplier = seasonal_multiplier(base_time)

    events = []

    events.append(create_event(sub, session_id, "login", base_time))

    num_views = random.randint(*profile["views"])

    for i in range(num_views):
        ts = base_time + timedelta(seconds=10 * (i + 1))
        if random.random() < multiplier:
            events.append(create_event(sub, session_id, "content_view", ts))

    if random.random() < profile["feature_prob"] * multiplier:
        events.append(create_event(
            sub,
            session_id,
            "feature_used",
            base_time + timedelta(seconds=60)
        ))

    events.append(create_event(
        sub,
        session_id,
        "session_end",
        base_time + timedelta(seconds=120)
    ))

    return events, base_time


# ----------------------------
# MAIN GENERATION
# ----------------------------
def run():
    subs = load_seed_subscriptions()

    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)

        # IMPORTANT: 2 columns now
        writer.writerow(["data", "created_at"])

        total = 0

        while total < TARGET_EVENTS:
            sub = random.choice(subs)

            events, session_time = generate_session(sub)

            for event in events:
                writer.writerow([
                    json.dumps(event),
                    session_time.isoformat()
                ])

                total += 1

                if total % 100_000 == 0:
                    print(f"[INFO] Generated {total}")

                if total >= TARGET_EVENTS:
                    break

    print("[DONE]", total)


if __name__ == "__main__":
    run()