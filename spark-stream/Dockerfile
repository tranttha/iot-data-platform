FROM bitnamilegacy/spark:3.5

USER root

WORKDIR /app

COPY requirement.txt .
RUN apt-get update && apt-get install -y --no-install-recommends zip && rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir -r requirement.txt

COPY . .

# Make all sub-packages importable from /app (driver side)
ENV PYTHONPATH=/app

# Run once per poll cycle: process all available raw files then sleep.
# Resources: controlled via STREAM_TOTAL_EXECUTOR_CORES / STREAM_EXECUTOR_MEMORY / STREAM_DRIVER_MEMORY.
# --conf spark.executor.cores=$STREAM_TOTAL_EXECUTOR_CORES collapses all cores into a single
# executor so the job stays within the memory budget.
# Bundle local packages so Spark workers can import them (PYTHONPATH only covers the driver).
CMD /bin/sh -c " \
    while true; do \
        cd /app && zip -qr /tmp/app_libs.zip utils/ warehouse/ config.py staging.py && \
        spark-submit \
            --master $SPARK_MASTER_URL \
            --deploy-mode client \
            --name spark-stream \
            --total-executor-cores ${STREAM_TOTAL_EXECUTOR_CORES:-6} \
            --executor-memory ${STREAM_EXECUTOR_MEMORY:-512m} \
            --driver-memory ${STREAM_DRIVER_MEMORY:-512m} \
            --conf spark.executor.cores=${STREAM_TOTAL_EXECUTOR_CORES:-6} \
            --conf spark.driver.host=spark-stream \
            --py-files /tmp/app_libs.zip \
            --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
            /app/stream.py; \
        echo 'spark-stream run complete, sleeping ${POLL_INTERVAL_SECONDS}s...'; \
        sleep ${POLL_INTERVAL_SECONDS:-18}; \
    done \
"
