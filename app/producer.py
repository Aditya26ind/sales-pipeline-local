import json
import os
import time

import pandas as pd
from kafka import KafkaProducer

from consumer import main as consume_main


BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "target")
INPUT_CSV_PATH = os.getenv("INPUT_CSV_PATH", "data/target_data.csv")
PRODUCER_DELAY_SECONDS = float(os.getenv("PRODUCER_DELAY_SECONDS", "0.5"))
RUN_CONSUMER_AFTER_PRODUCE = os.getenv("RUN_CONSUMER_AFTER_PRODUCE", "false").lower() == "true"


def build_producer(retries: int = 20, sleep_seconds: int = 3) -> KafkaProducer:
    last_error = None
    for attempt in range(1, retries + 1):
        try:
            return KafkaProducer(
                bootstrap_servers=BOOTSTRAP_SERVERS,
                value_serializer=lambda value: json.dumps(value).encode("utf-8"),
            )
        except Exception as exc:
            last_error = exc
            print(f"[producer] Kafka unavailable, retry {attempt}/{retries}: {exc}")
            time.sleep(sleep_seconds)
    raise RuntimeError(f"Unable to connect to Kafka: {last_error}") from last_error


def main() -> None:
    df = pd.read_csv(INPUT_CSV_PATH)
    producer = build_producer()

    print(f"[producer] Sending {len(df)} records to topic '{TOPIC}'")
    for row in df.to_dict(orient="records"):
        producer.send(TOPIC, value=row)
        producer.flush()
        print(f"[producer] Sent Id={row['Id']}")
        time.sleep(PRODUCER_DELAY_SECONDS)

    producer.close()
    print("[producer] Completed streaming CSV rows")

    if RUN_CONSUMER_AFTER_PRODUCE:
        print("[pipeline] Starting consumer step")
        consume_main()


if __name__ == "__main__":
    main()
