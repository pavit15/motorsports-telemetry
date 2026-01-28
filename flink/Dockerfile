FROM apache/flink:1.17.2-scala_2.12

USER root

# Install Python and tools
RUN apt-get update && \
    apt-get install -y python3 python3-pip curl && \
    ln -s /usr/bin/python3 /usr/bin/python && \
    pip3 install --no-cache-dir apache-flink==1.17.2 boto3 && \
    rm -rf /var/lib/apt/lists/*

# ------------------------------------------------------------------
# Flink Kafka connector
# ------------------------------------------------------------------
RUN curl -L \
  https://repo1.maven.org/maven2/org/apache/flink/flink-connector-kafka/1.17.2/flink-connector-kafka-1.17.2.jar \
  -o /opt/flink/lib/flink-connector-kafka.jar

# ------------------------------------------------------------------
# Kafka client dependency (required)
# ------------------------------------------------------------------
RUN curl -L \
  https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.6.0/kafka-clients-3.6.0.jar \
  -o /opt/flink/lib/kafka-clients.jar

USER flink
