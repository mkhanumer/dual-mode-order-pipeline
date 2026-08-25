import json
import time
import random
import os
import sys
from kafka import KafkaProducer

# etl/ is mounted at /etl in docker (see docker-compose.yml)
sys.path.append("/etl")
from extract import extract_from_csv   # noqa: E402
from clean import clean_orders          # noqa: E402


def build_producer(bootstrap_servers: str) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
        retries=5,
        retry_backoff_ms=2000,
        request_timeout_ms=20000,
    )


def stream_orders(producer: KafkaProducer, topic: str, clean_df, delay_seconds: float = 2.0):
    print(f"[producer] Streaming from {len(clean_df)} cleaned historical orders "
          f"into topic '{topic}' every {delay_seconds}s...")

    next_order_id = int(clean_df["order_id"].max()) + 1

    while True:
        template = clean_df.sample(1).iloc[0]

        event = {
            "order_id": next_order_id,
            "customer_name": template["customer_name"],
            "product_name": template["product_name"],
            "category": template["category"],
            "unit_price": float(template["unit_price"]),
            "quantity": int(random.choice([1, 1, 2, 3])),
            "country": template["country"],
            "event_time": time.time(),
        }
        event["total_amount"] = round(event["unit_price"] * event["quantity"], 2)

        producer.send(topic, value=event)
        producer.flush()
        print(f"[producer] Sent order {event['order_id']}: "
              f"{event['product_name']} x{event['quantity']} = ${event['total_amount']}")

        next_order_id += 1
        time.sleep(delay_seconds)


if __name__ == "__main__":
    KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
    RAW_DATA_PATH = os.getenv("RAW_DATA_PATH", "/data/raw_orders_messy.csv")
    TOPIC = os.getenv("KAFKA_TOPIC", "orders_clean")

    raw_df = extract_from_csv(RAW_DATA_PATH)
    clean_df = clean_orders(raw_df)

    producer = build_producer(KAFKA_BROKER)
    stream_orders(producer, TOPIC, clean_df, delay_seconds=2.0)
