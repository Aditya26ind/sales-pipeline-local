import json
import os
import time

from kafka import KafkaConsumer

from pipeline import run_pipeline


BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "target")
GROUP_ID = os.getenv("KAFKA_GROUP_ID", "target-consumer")
EXPECTED_MESSAGE_COUNT = int(os.getenv("EXPECTED_MESSAGE_COUNT", "1000"))
MAX_WAIT_SECONDS = int(os.getenv("MAX_WAIT_SECONDS", "120"))


def build_consumer(retries: int = 20, sleep_seconds: int = 3) -> KafkaConsumer:
    last_error = None
    for attempt in range(1, retries + 1):
        try:
            return KafkaConsumer(
                TOPIC,
                bootstrap_servers=BOOTSTRAP_SERVERS,
                group_id=GROUP_ID,
                auto_offset_reset="earliest",
                value_deserializer=lambda value: json.loads(value.decode("utf-8")),
            )
        except Exception as exc:
            last_error = exc
            print(f"[consumer] Kafka unavailable, retry {attempt}/{retries}: {exc}")
            time.sleep(sleep_seconds)
    raise RuntimeError(f"Unable to connect to Kafka: {last_error}") from last_error


def main() -> None:
    consumer = build_consumer()
    records: list[dict] = []
    started_at = time.time()

    print(f"[consumer] Waiting for {EXPECTED_MESSAGE_COUNT} records from '{TOPIC}'")
    while len(records) < EXPECTED_MESSAGE_COUNT and (time.time() - started_at) < MAX_WAIT_SECONDS:
        batches = consumer.poll(timeout_ms=1000)
        for _, messages in batches.items():
            for message in messages:
                records.append(message.value)
                print(f"[consumer] Consumed Id={message.value['Id']}")
                if len(records) >= EXPECTED_MESSAGE_COUNT:
                    break
            if len(records) >= EXPECTED_MESSAGE_COUNT:
                break

    consumer.close()

    if not records:
        raise RuntimeError("No Kafka messages were consumed. Pipeline cannot proceed.")
    if len(records) < EXPECTED_MESSAGE_COUNT:
        print(
            f"[consumer] Proceeding with {len(records)} records after waiting {MAX_WAIT_SECONDS} seconds"
        )

    insights = run_pipeline(records)
    print("[consumer] Pipeline complete")
    print(json.dumps(insights, indent=2))


if __name__ == "__main__":
    main()
