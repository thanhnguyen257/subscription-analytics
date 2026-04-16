FROM python:3.10-slim

WORKDIR /app

COPY src/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/data_generator/events.py ./generator/events.py

CMD ["sh", "-c", "python generator/events.py"]