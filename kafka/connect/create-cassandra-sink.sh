#!/bin/sh
# Registers the three Cassandra sinks with Kafka Connect.
# Each sink is retried until Connect really has it, so a slow start-up no longer fails silently.

CONNECT_URL="http://localhost:8083/connectors"

# True when Connect already knows a connector with this name (HTTP 200).
sink_exists() {
    [ "$(curl -s -o /dev/null -w '%{http_code}' "$CONNECT_URL/$1")" = "200" ]
}

# create_sink <name> <topic> <table> <column=value.field mapping>
create_sink() {
    name="$1"
    topic="$2"
    table="$3"
    mapping="$4"

    json='{
  "name": "'"$name"'",
  "config": {
    "connector.class": "com.datastax.oss.kafka.sink.CassandraSinkConnector",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "false",
    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable": "false",
    "tasks.max": "1",
    "topics": "'"$topic"'",
    "contactPoints": "cassandradb",
    "loadBalancing.localDc": "datacenter1",
    "topic.'"$topic"'.kafkapipeline.'"$table"'.mapping": "'"$mapping"'",
    "topic.'"$topic"'.kafkapipeline.'"$table"'.consistencyLevel": "LOCAL_QUORUM"
  }
}'

    echo "Starting $name"
    attempt=1
    while [ "$attempt" -le 30 ]; do
        if sink_exists "$name"; then
            echo "$name: ready"
            return 0
        fi
        code=$(curl -s -o /dev/null -w '%{http_code}' -X POST "$CONNECT_URL" \
               -H "Content-Type: application/json" -d "$json")
        echo "$name: POST returned HTTP $code (attempt $attempt of 30)"
        attempt=$((attempt + 1))
        sleep 5
    done
    echo "$name: FAILED after 30 attempts"
    return 1
}

create_sink weathersink weather weatherreport \
    "location=value.location, forecastdate=value.report_time, description=value.description, temp=value.temp, feels_like=value.feels_like, temp_min=value.temp_min, temp_max=value.temp_max, pressure=value.pressure, humidity=value.humidity, wind=value.wind, sunrise=value.sunrise, sunset=value.sunset"

create_sink fakersink faker fakerdata \
    "order_id=value.order_id, ordered_at=value.ordered_at, city=value.city, customer_name=value.customer_name, address=value.address, item_category=value.item_category, order_amount_vnd=value.order_amount_vnd, payment_method=value.payment_method, delivery_minutes=value.delivery_minutes, customer_rating=value.customer_rating"

create_sink airqualitysink airquality airquality \
    "location=value.location, report_time=value.report_time, pm10=value.pm10, pm2_5=value.pm2_5, us_aqi=value.us_aqi, european_aqi=value.european_aqi"

echo "Done."
