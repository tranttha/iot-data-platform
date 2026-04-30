FROM python:3.11-slim

WORKDIR /app

COPY requirement.txt .
RUN pip install --no-cache-dir -r requirement.txt

COPY producer.py .

# Supplied at runtime via docker-compose environment 
# (DEVICE_ID, LATITUDE, LONGITUDE, KAFKA_BOOTSTRAP_SERVERS). 
# Defaults are placeholders only.
ENV DEVICE_ID=device_unknown \
    LATITUDE=0.0 \
    LONGITUDE=0.0 \
    KAFKA_BOOTSTRAP_SERVERS=kafka:9092 \
    KAFKA_TOPIC=iot-sensor-data

CMD ["python", "producer.py"]
