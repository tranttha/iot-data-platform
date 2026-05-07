FROM python:3.11-slim

WORKDIR /app

COPY requirement.txt .
RUN pip install --no-cache-dir -r requirement.txt

COPY producer.py .
COPY config.py .
COPY config.yaml .

# Runtime values supplied via docker-compose environment section.
ENV KAFKA_BOOTSTRAP_SERVERS=kafka:9092 \
    KAFKA_TOPIC=iot-sensor-data

CMD ["python", "producer.py"]
