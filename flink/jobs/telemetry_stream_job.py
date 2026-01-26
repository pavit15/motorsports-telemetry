from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream.connectors.file_system import FileSink, OutputFileConfig
from pyflink.common.configuration import Configuration

import json

# --------------------------------------------------
# 1. Environment
# --------------------------------------------------
config = Configuration()
config.set_string("execution.checkpointing.interval", "10000")

env = StreamExecutionEnvironment.get_execution_environment(config)
env.set_parallelism(1)

# --------------------------------------------------
# 2. Kafka Source
# --------------------------------------------------
kafka_source = KafkaSource.builder() \
    .set_bootstrap_servers("127.0.0.1:9092") \
    .set_topics("telemetry.raw") \
    .set_group_id("flink-telemetry-group") \
    .set_starting_offsets(KafkaOffsetsInitializer.latest()) \
    .set_value_only_deserializer(SimpleStringSchema()) \
    .build()

stream = env.from_source(
    kafka_source,
    WatermarkStrategy.no_watermarks(),
    "Kafka Telemetry Source"
)

# --------------------------------------------------
# 3. Parse telemetry JSON
# --------------------------------------------------
def parse_event(value):
    try:
        event = json.loads(value)
        return json.dumps({
            "car_id": event["car_id"],
            "speed": event["speed"],
            "rpm": event["rpm"],
            "lap": event["lap"],
            "timestamp": event["timestamp"]
        })
    except Exception:
        return None

parsed = (
    stream
    .map(parse_event)
    .filter(lambda x: x is not None)
)

# --------------------------------------------------
# 4. MinIO (S3) Sink
# --------------------------------------------------
sink = FileSink \
    .for_row_format(
        "s3://motorsports/processed/telemetry/",
        SimpleStringSchema()
    ) \
    .with_output_file_config(
        OutputFileConfig.builder()
        .with_part_prefix("telemetry")
        .with_part_suffix(".json")
        .build()
    ) \
    .build()

parsed.sink_to(sink)

# --------------------------------------------------
# 5. Execute
# --------------------------------------------------
env.execute("Motorsports Telemetry Streaming Job")