from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import WatermarkStrategy

import json
import boto3
import uuid


def write_to_minio(value: str):
    # -------------------------------------------------
    # Guard against empty / invalid messages
    # -------------------------------------------------
    if value is None:
        return

    value = value.strip()
    if not value:
        return

    try:
        data = json.loads(value)
    except json.JSONDecodeError:
        # Skip malformed records safely
        return

    s3 = boto3.client(
        "s3",
        endpoint_url="http://minio:9000",
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin",
        region_name="us-east-1",
    )

    key = f"telemetry/{uuid.uuid4()}.json"

    s3.put_object(
        Bucket="telemetry-data",
        Key=key,
        Body=json.dumps(data),
    )


env = StreamExecutionEnvironment.get_execution_environment()

kafka_source = (
    KafkaSource.builder()
    .set_bootstrap_servers("kafka:9092")
    .set_topics("telemetry")
    .set_group_id("flink-consumer")
    .set_value_only_deserializer(SimpleStringSchema())
    .build()
)

stream = env.from_source(
    source=kafka_source,
    watermark_strategy=WatermarkStrategy.no_watermarks(),
    source_name="Kafka Source",
)

stream.map(write_to_minio)

env.execute("Motorsports Telemetry Streaming Job")
