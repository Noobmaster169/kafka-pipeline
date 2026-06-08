# Baked Spark image: fit3182/pyspark + the A3 Python deps pre-installed, so a recreated
# container needs no `pip install` step. Build once, then run with the code bind-mounted.
#
#   docker build -t awas/pyspark -f deployment/Dockerfile .
#   docker run -d --name spark --network kafka-net -p 8888:8888 -p 4040:4040 \
#       -e MONGO_HOST=mongo -e KAFKA_BOOTSTRAP_SERVERS=kafka:9092 \
#       -v "$PWD":/app -v "$PWD/deployment/.cache/ivy2":/root/.ivy2 \
#       awas/pyspark jupyter notebook
#
# (Build context = the module root, so requirements.txt is at ./requirements.txt.)
FROM fit3182/pyspark

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

WORKDIR /app/src
