from fastapi import FastAPI, HTTPException
import json
import psycopg2
from dotenv import load_dotenv
import os

load_dotenv()

app = FastAPI()

@app.post("/events")
def ingest_event(event: dict):
    try:
        conn = psycopg2.connect(
            host=os.getenv('POSTGRES_HOST'),
            port=os.getenv('POSTGRES_PORT'),
            database=os.getenv('POSTGRES_DB'),
            user=os.getenv('POSTGRES_USER'),
            password=os.getenv('POSTGRES_PASSWORD')
        )
        cur = conn.cursor()

        cur.execute(
            f"INSERT INTO {os.getenv('POSTGRES_SCHEMA_LANDING')}.usage_events_raw (data) VALUES (%s)",
            [json.dumps(event)]
        )

        conn.commit()
        cur.close()
        conn.close()

        return {"status": "accepted"}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))