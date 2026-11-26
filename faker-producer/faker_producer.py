"""Produce fake food-delivery orders to the 'faker' Kafka topic."""
import json
import os
import time

from faker import Faker
from kafka import KafkaProducer

KAFKA_BROKER_URL = os.environ.get("KAFKA_BROKER_URL")
TOPIC_NAME = os.environ.get("TOPIC_NAME")
SLEEP_TIME = int(os.environ.get("SLEEP_TIME", 5))

CITIES = ["Hanoi", "Ho Chi Minh City"]
ITEM_CATEGORIES = ["Rice", "Noodles", "Coffee", "Bubble tea", "Banh mi", "Pizza"]
PAYMENT_METHODS = ["cash", "card", "e-wallet"]

fake = Faker("vi_VN")


def get_order():
    """Build one fake delivery order.

    The keys must match the columns in cassandra/schema-faker.cql and the
    fakersink mapping in kafka/connect/create-cassandra-sink.sh.
    """
    ordered_at = fake.date_time_between(start_date="-1m", end_date="now")
    return {
        "order_id": fake.uuid4(),
        "ordered_at": ordered_at.strftime("%Y-%m-%d %H:%M:%S"),
        "city": fake.random_element(CITIES),
        "customer_name": fake.name(),
        "address": fake.street_address(),
        "item_category": fake.random_element(ITEM_CATEGORIES),
        "order_amount_vnd": fake.random_int(min=30, max=500) * 1000,
        "payment_method": fake.random_element(PAYMENT_METHODS),
        "delivery_minutes": fake.random_int(min=10, max=60),
        "customer_rating": fake.random_int(min=1, max=5),
    }


def run():
    iterator = 0
    print("Setting up Faker producer at {}".format(KAFKA_BROKER_URL))
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER_URL],
        # Encode every order as a JSON string
        value_serializer=lambda x: json.dumps(x).encode("ascii"),
    )

    while True:
        order = get_order()
        print("Sending new faker order iteration - {}".format(iterator))
        producer.send(TOPIC_NAME, value=order)
        print("New faker order sent")
        time.sleep(SLEEP_TIME)
        print("Waking up!")
        iterator += 1


if __name__ == "__main__":
    run()