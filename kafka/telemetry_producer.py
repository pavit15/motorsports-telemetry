from kafka import KafkaProducer
import json
import time
import random

producer = KafkaProducer(
    bootstrap_servers="localhost:29092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

while True:
    telemetry = {
        "car_id": random.randint(1, 20),
        "speed": random.uniform(120, 340),
        "rpm": random.randint(9000, 15000),
        "gear": random.randint(1, 8),
        "timestamp": time.time(),
    }

    producer.send("telemetry", telemetry)
    print("Sent:", telemetry)
    time.sleep(1)
