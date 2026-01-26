from kafka import KafkaProducer
import json
import time

producer = KafkaProducer(
    bootstrap_servers="127.0.0.1:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    api_version=(3, 6, 0)  # IMPORTANT: skip auto-detection
)

while True:
    event = {
        "car_id": 44,
        "speed": 312,
        "rpm": 11800,
        "lap": 12,
        "timestamp": time.time()
    }
    producer.send("telemetry.raw", event)
    producer.flush()
    print("Sent:", event)
    time.sleep(1)
