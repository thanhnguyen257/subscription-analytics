FROM python:3.10-slim

WORKDIR /app

COPY src/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/events_api/main.py ./app/main.py

RUN apt-get update && apt-get install -y curl

CMD ["sh", "-c", "uvicorn app.main:app --host 0.0.0.0 --port 8000"]