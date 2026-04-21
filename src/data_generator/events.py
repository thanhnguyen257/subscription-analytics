import uuid
import random
import time
import requests
from datetime import datetime, timedelta
import pyodbc
from dotenv import load_dotenv
import os

load_dotenv()

API_URL = "http://event_ingest_api:8000/events"

DEVICES = ["mobile", "web", "tablet"]
PLATFORMS = ["iOS", "Android", "Web"]

def load_active_subscriptions():
    conn = pyodbc.connect(
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={os.getenv('AZURE_SQL_SERVER')};"
        f"DATABASE={os.getenv('AZURE_SQL_DATABASE')};"
        f"UID={os.getenv('AZURE_SQL_USER')};"
        f"PWD={os.getenv('AZURE_SQL_PASSWORD')};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )
    cur = conn.cursor()

    cur.execute(f"""
        SELECT subscription_id, user_id, plan_id
        FROM subscriptions
        WHERE status = 'active'
    """)

    rows = cur.fetchall()
    cur.close()
    conn.close()

    return [
        {
            "subscription_id": r[0],
            "user_id": r[1],
            "plan_id": r[2]
        }
        for r in rows
    ]


def create_event(sub, session_id, event_type, timestamp):
    device = random.choice(DEVICES)
    platform = random.choice(PLATFORMS)

    return {
        "event_id": str(uuid.uuid4()),
        "user_id": sub["user_id"],
        "subscription_id": sub["subscription_id"],
        "session_id": session_id,
        "event_type": event_type,
        "event_timestamp": timestamp.isoformat(),
        "content_id": random.randint(1, 1000) if event_type == "content_view" else None,
        "feature_name": "search" if event_type == "feature_used" else None,
        "device_type": device,
        "platform": platform,
        "event_properties": {
            "duration_sec": random.randint(10, 300),
            "referrer": random.choice(["homepage", "search", "ads"])
        },
        "created_at": datetime.now().isoformat()
    }


def generate_session(sub):
    session_id = str(uuid.uuid4())
    base_time = datetime.now()

    events = []

    # login
    events.append(create_event(sub, session_id, "login", base_time))

    # content views
    for i in range(random.randint(1, 5)):
        events.append(create_event(
            sub,
            session_id,
            "content_view",
            base_time + timedelta(seconds=10 * (i + 1))
        ))

    # feature used
    if random.random() < 0.7:
        events.append(create_event(
            sub,
            session_id,
            "feature_used",
            base_time + timedelta(seconds=60)
        ))

    # session end
    events.append(create_event(
        sub,
        session_id,
        "session_end",
        base_time + timedelta(seconds=120)
    ))

    return events


def send_event(event):
    try:
        requests.post(API_URL, json=event, timeout=1)
    except:
        pass


def run():
    subs = load_active_subscriptions()

    while True:
        sub = random.choice(subs)
        session = generate_session(sub)

        for event in session:
            send_event(event)
            time.sleep(0.2)  # simulate streaming

        time.sleep(random.uniform(0.5, 2))


if __name__ == "__main__":
    run()