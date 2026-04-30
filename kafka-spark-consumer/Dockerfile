FROM bitnamilegacy/spark:3.5

# Switch to root to install OS packages
USER root

WORKDIR /app

COPY requirement.txt .
RUN pip install --no-cache-dir -r requirement.txt

COPY consumer.py .

# spark-submit is on PATH from the base image.
# Jars (kafka connector, hadoop-aws) are downloaded at first run via spark.jars.packages
# and cached in /root/.ivy2 — mount a volume in dev to speed up restarts.
CMD spark-submit \
    --master $SPARK_MASTER_URL \
    --deploy-mode client \
    --name kafka-raw-consumer \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
    --conf spark.driver.host=kafka-spark-consumer \
    consumer.py
