import json
import os
import random
import sys
import time

from kafka import KafkaProducer

# etl/ is mounted at /etl inside docker (see docker-compose.yml)
sys.path.append("/etl")
from extract import extract_from_csv  # noqa: E402
from clean import clean_orders  # noqa: E402


def build_producer(broker_address):
    producer = KafkaProducer(
        bootstrap_servers=broker_address,
        value_serializer=lambda event: json.dumps(event).encode("utf-8"),
        retries=5,
    )
    return producer


def make_event(order_id, template_row):
    """Build a fake live order using a real cleaned order as the base."""
    quantity = random.choice([1, 1, 2, 3])
    price = float(template_row["unit_price"])

    event = {
        "order_id": order_id,
        "customer_name": template_row["customer_name"],
        "product_name": template_row["product_name"],
        "category": template_row["category"],
        "unit_price": price,
        "quantity": quantity,
        "country": template_row["country"],
        "event_time": time.time(),
        "total_amount": round(price * quantity, 2),
    }
    return event


def stream_orders(producer, topic, clean_orders_df, delay_seconds=2.0):
    """Send one order every few seconds, forever."""
    next_order_id = int(clean_orders_df["order_id"].max()) + 1

    while True:
        # pick a random real cleaned order to use as the template
        sample = clean_orders_df.sample(n=1)
        template_row = sample.iloc[0]

        event = make_event(next_order_id, template_row)
        producer.send(topic, value=event)

        print(f"[producer] Sent order {event['order_id']}: {event['product_name']} x{event['quantity']}")

        next_order_id += 1
        time.sleep(delay_seconds)


if __name__ == "__main__":
    broker = os.getenv("KAFKA_BROKER", "kafka:9092")
    data_path = os.getenv("RAW_DATA_PATH", "/data/raw_orders_messy.csv")
    topic = os.getenv("KAFKA_TOPIC", "orders_clean")

    raw_df = extract_from_csv(data_path)
    clean_df = clean_orders(raw_df)

    producer = build_producer(broker)
    stream_orders(producer, topic, clean_df)
