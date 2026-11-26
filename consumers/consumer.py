"""Read messages from one Kafka topic and print them.

The topic comes from the TOPIC_NAME environment variable, so one script serves
the weather, faker and airquality consumers. Saving to Cassandra is done by the
Kafka Connect sinks, so this consumer only prints what it receives.
"""
import json
import os

from kafka import KafkaConsumer

TOPIC_NAME = os.environ.get("TOPIC_NAME")
KAFKA_BROKER_URL = os.environ.get("KAFKA_BROKER_URL", "localhost:9092")


if __name__ == "__main__":
    print("Starting consumer for topic {}".format(TOPIC_NAME))
    print("Setting up Kafka consumer at {}".format(KAFKA_BROKER_URL))
    consumer = KafkaConsumer(TOPIC_NAME, bootstrap_servers=[KAFKA_BROKER_URL])

    print("Waiting for msg...")
    for msg in consumer:
        print(json.loads(msg.value.decode("ascii")))