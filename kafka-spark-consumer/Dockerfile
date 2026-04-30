FROM bitnami/spark:3.5

# Switch to root to install OS packages
USER root

WORKDIR /app

COPY requirement.txt .
RUN pip install --no-cache-dir -r requirement.txt

COPY consumer.py .

# spark-submit is on PATH from the base image.
# Jars (kafka connector, hadoop-aws) are downloaded at first run via spark.jars.packages
# and cached in /root/.ivy2 — mount a volume in dev to speed up restarts.
CMD [
  "spark-submit",
  "--master", "spark://spark-master:7077",
  "--deploy-mode", "client",
  "--name", "kafka-raw-consumer",
  "--conf", "spark.driver.host=kafka-spark-consumer",
  "consumer.py"
]
