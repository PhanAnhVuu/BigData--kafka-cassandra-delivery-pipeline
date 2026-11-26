"""Produce Open-Meteo air quality readings to the 'airquality' Kafka topic."""
import json
import os
import time

import requests
from kafka import KafkaProducer

KAFKA_BROKER_URL = os.environ.get("KAFKA_BROKER_URL")
TOPIC_NAME = os.environ.get("TOPIC_NAME")
SLEEP_TIME = int(os.environ.get("SLEEP_TIME", 60))

API_URL = "https://air-quality-api.open-meteo.com/v1/air-quality"
MEASURES = ["pm10", "pm2_5", "us_aqi", "european_aqi"]

# City name -> (latitude, longitude). The names match the weather producer.
LOCATIONS = {
    "Hanoi": (21.0285, 105.8542),
    "Ho Chi Minh City": (10.8231, 106.6297),
}


def get_air_quality(city):
    """Ask Open-Meteo for the current air quality of one city and return one message."""
    latitude, longitude = LOCATIONS[city]
    response = requests.get(
        API_URL,
        params={
            "latitude": latitude,
            "longitude": longitude,
            "current": ",".join(MEASURES),
        },
        timeout=10,
    )
    response.raise_for_status()
    current = response.json()["current"]

    message = {"location": city}
    # Stamp the message with the time we fetched it (UTC), like the weather producer
    message["report_time"] = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
    for measure in MEASURES:
        message[measure] = current[measure]
    return message


def run():
    cities = list(LOCATIONS)
    iterator = 0
    repeat_request = SLEEP_TIME / len(cities)
    print("Setting up Air Quality producer at {}".format(KAFKA_BROKER_URL))
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER_URL],
        # Encode every message as a JSON string
        value_serializer=lambda x: json.dumps(x).encode("ascii"),
    )

    while True:
        city = cities[iterator % len(cities)]
        print("Sending new air quality report iteration - {}".format(iterator))
        try:
            producer.send(TOPIC_NAME, value=get_air_quality(city))
            print("New air quality report sent for {}".format(city))
        except requests.RequestException as error:
            # One failed request should not stop the producer
            print("Request for {} failed: {}".format(city, error))
        time.sleep(repeat_request)
        print("Waking up!")
        iterator += 1


if __name__ == "__main__":
    run()