from pyflink.common.restart_strategy import RestartStrategies
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource
from pyflink.datastream.connectors.file_system import (
    StreamingFileSink,
    OutputFileConfig,
    RollingPolicy,
)
from pyflink.common.serialization import SimpleStringSchema, Encoder
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.common.typeinfo import Types

import json
from datetime import datetime


# -------------------------------------------------
# Schema validation
# -------------------------------------------------
REQUIRED_FIELDS = {
    "car_id": int,
    "speed": (int, float),
    "rpm": int,
}


def is_valid(record: dict) -> bool:
    for field, field_type in REQUIRED_FIELDS.items():
        if field not in record:
            return False
        if not isinstance(record[field], field_type):
            return False
    return True


# -------------------------------------------------
# Pure transformation (NO I/O)
# -------------------------------------------------
def parse_and_filter(value: str):
    try:
        record = json.loads(value)
        if not is_valid(record):
            return None

        record["ingest_time"] = datetime.utcnow().isoformat()
        return json.dumps(record)
    except Exception:
        return None


# -------------------------------------------------
# Flink environment
# -------------------------------------------------
env = StreamExecutionEnvironment.get_execution_environment()

env.enable_checkpointing(10_000)
env.set_parallelism(1)
env.set_restart_strategy(RestartStrategies.no_restart())


# -------------------------------------------------
# Kafka source
# -------------------------------------------------
kafka_source = (
    KafkaSource.builder()
    .set_bootstrap_servers("kafka:9092")
    .set_topics("telemetry")
    .set_group_id("flink-telemetry-consumer")
    .set_value_only_deserializer(SimpleStringSchema())
    .build()
)

stream = env.from_source(
    kafka_source,
    WatermarkStrategy.no_watermarks(),
    "Kafka Telemetry Source",
)


# -------------------------------------------------
# Transform
# -------------------------------------------------
valid_stream = (
    stream
    .map(parse_and_filter, output_type=Types.STRING())
    .filter(lambda x: x is not None)
)


# -------------------------------------------------
# S3 / MinIO Sink (PRODUCTION SAFE)
# -------------------------------------------------
sink = StreamingFileSink.for_row_format(
    "s3://telemetry-data/telemetry/",
    Encoder.simple_string_encoder("utf-8"),
).with_rolling_policy(
    RollingPolicy.default_rolling_policy()
).with_output_file_config(
    OutputFileConfig.builder()
    .with_part_prefix("telemetry")
    .with_part_suffix(".json")
    .build()
).build()

valid_stream.add_sink(sink)


# -------------------------------------------------
# Execute
# -------------------------------------------------
env.execute("Motorsports Telemetry Streaming Job")
