# Assignment 1: Streaming pipeline for food-delivery conditions in Hanoi and Ho Chi Minh City

Big Data for Engineering (EEET2574), RMIT Vietnam
Student: Vu Phan Anh, sID s3926005

## 1. What this project does

A food-delivery platform operating in Hanoi and Ho Chi Minh City wants to see its orders next to the weather and the air quality that its riders work in. This project streams three data sources through Kafka into Cassandra and analyses them in Jupyter.

| Task | Data source | Kafka topic | Cassandra table |
|---|---|---|---|
| 1 | OpenWeatherMap API (Hanoi, Ho Chi Minh City) | `weather` | `weatherreport` |
| 2 | Faker library, synthetic delivery orders (10 fields, Vietnamese locale) | `faker` | `fakerdata` |
| 3 | Open-Meteo Air Quality API (no key needed) | `airquality` | `airquality` |
| 4 | Jupyter notebook with three plots and analysis | reads all three tables | |

### Architecture

```
OpenWeatherMap API --> owm-producer ------\
Faker library ------> faker-producer ------+--> Kafka (broker) --> consumers (print each topic)
Open-Meteo API -----> airquality-producer -/            |
                                                        v
                                         Kafka Connect (3 Cassandra sinks)
                                                        |
                                                        v
                                         Cassandra (keyspace kafkapipeline)
                                                        |
                                                        v
                                         Jupyter Lab (analysis.ipynb)
```

Two Docker networks connect the parts: `kafka-network` (Kafka, producers, consumers, Kafka Connect) and `cassandra-network` (Cassandra, Kafka Connect, Jupyter).

## 2. Project structure

```
cassandra/            Dockerfile, keyspace.cql, schema.cql (weatherreport),
                      schema-faker.cql (fakerdata), schema-airquality.cql (airquality),
                      bootstrap.sh (creates keyspace and tables on first start)
kafka/                docker-compose.yml (Zookeeper, broker, Kafka Manager, Kafka Connect),
                      connect/create-cassandra-sink.sh (creates the three sinks)
owm-producer/         Task 1: OpenWeatherMap producer
faker-producer/       Task 2: Faker producer (10 fields)
airquality-producer/  Task 3: Open-Meteo Air Quality producer
consumers/            One generic consumer, started once per topic
data-vis/             Jupyter Lab, python/analysis.ipynb, python/cassandrautils.py
```

## 3. Requirements

- Docker Desktop (with docker-compose v2) running. Tested on Windows 11 with WSL 2.
- A free OpenWeatherMap API key (https://openweathermap.org/api).
- Free ports: 8083 (Kafka Connect), 8888 (Jupyter), 9000 (Kafka Manager).

## 4. How to replicate

Run every command from the project root folder. The commands below are for Windows Command Prompt.

### 4.1 Add your API key

The key file is not part of the repository. Copy the example and put your own key in it:

```
copy owm-producer\openweathermap_service.cfg.example owm-producer\openweathermap_service.cfg
```

Then open `owm-producer\openweathermap_service.cfg` and replace `YOUR_API_KEY_HERE` with your key. The file looks like this:

```
[openweathermap_api_credential]
access_token = YOUR_API_KEY_HERE
```

### 4.2 Build the images (once)

```
docker-compose -f cassandra/docker-compose.yml build
docker-compose -f kafka/docker-compose.yml build
docker-compose -f owm-producer/docker-compose.yml build
docker-compose -f faker-producer/docker-compose.yml build
docker-compose -f airquality-producer/docker-compose.yml build
docker-compose -f consumers/docker-compose.yml build
docker-compose -f data-vis/docker-compose.yml build
```

The first build takes about 20 to 25 minutes because of the base image downloads.

### 4.3 Start the pipeline

1. Create the networks:

   ```
   docker network create kafka-network
   docker network create cassandra-network
   ```

2. Start Cassandra and wait about 90 seconds. The first start creates the keyspace `kafkapipeline` and the three tables:

   ```
   docker-compose -f cassandra/docker-compose.yml up -d
   docker exec cassandra cqlsh --cqlversion=3.4.4 -k kafkapipeline -e "DESCRIBE TABLES" 127.0.0.1
   ```

   Expected: `fakerdata  airquality  weatherreport`.

3. Start Kafka and wait about 2 minutes. The broker creates the topics `weather`, `faker` and `airquality`, and Kafka Connect creates the three sinks:

   ```
   docker-compose -f kafka/docker-compose.yml up -d
   curl -s http://localhost:8083/connectors
   ```

   Expected: `["airqualitysink","weathersink","fakersink"]`. Check that each sink reports `RUNNING`:

   ```
   curl -s http://localhost:8083/connectors/weathersink/status
   curl -s http://localhost:8083/connectors/fakersink/status
   curl -s http://localhost:8083/connectors/airqualitysink/status
   ```

   Optional: open Kafka Manager at http://localhost:9000 (login `admin` / `bigbang`), add a cluster with Zookeeper host `zookeeper:2181`, and look at the Topics page.

4. Start the producers, each in its own Command Prompt window (they print live):

   ```
   docker-compose -f owm-producer/docker-compose.yml up
   docker-compose -f faker-producer/docker-compose.yml up
   docker-compose -f airquality-producer/docker-compose.yml up
   ```

5. Start the consumers, each in its own window:

   ```
   docker-compose -f consumers/docker-compose.yml up weatherconsumer
   docker-compose -f consumers/docker-compose.yml up fakerconsumer
   docker-compose -f consumers/docker-compose.yml up airqualityconsumer
   ```

6. Check that data reached Cassandra:

   ```
   docker exec cassandra cqlsh --cqlversion=3.4.4 -k kafkapipeline -e "SELECT * FROM weatherreport;" 127.0.0.1
   docker exec cassandra cqlsh --cqlversion=3.4.4 -k kafkapipeline -e "SELECT * FROM airquality;" 127.0.0.1
   docker exec cassandra cqlsh --cqlversion=3.4.4 -k kafkapipeline -e "SELECT * FROM fakerdata LIMIT 10;" 127.0.0.1
   docker exec cassandra cqlsh --cqlversion=3.4.4 -k kafkapipeline -e "SELECT COUNT(*) FROM fakerdata;" 127.0.0.1
   ```

7. Start Jupyter Lab, then open http://localhost:8888 (no token), open `analysis.ipynb` and choose Run, Run All Cells:

   ```
   docker-compose -f data-vis/docker-compose.yml up -d
   ```

   Let the producers run for a few minutes first, so the plots have enough data.

### 4.4 Stop and reset

```
docker-compose -f data-vis/docker-compose.yml down
docker-compose -f consumers/docker-compose.yml down
docker-compose -f airquality-producer/docker-compose.yml down
docker-compose -f faker-producer/docker-compose.yml down
docker-compose -f owm-producer/docker-compose.yml down
docker-compose -f kafka/docker-compose.yml down
docker-compose -f cassandra/docker-compose.yml down
docker network rm kafka-network cassandra-network
rmdir /s /q cassandra\data
```

`down` keeps the images, so nothing has to be rebuilt. Deleting `cassandra\data` makes the next start begin with empty tables, because the bootstrap script runs only on the first start.

## 5. The tasks in detail

### Task 1: OpenWeatherMap, two new cities

`owm-producer/openweathermap_producer.py` calls the OpenWeatherMap API for **Hanoi** and **Ho Chi Minh City**, alternating between them every 30 seconds, and sends each report as JSON to the `weather` topic. The `weathersink` connector writes it to `weatherreport` (temperature, feels-like, min and max, pressure, humidity, wind, description, sunrise and sunset).

### Task 2: Faker, 10 fields

`faker-producer/faker_producer.py` uses `Faker("vi_VN")` to generate one synthetic delivery order every 5 seconds. Each order has 10 fields:

| Field | Meaning |
|---|---|
| `order_id` | Unique id (UUID) |
| `ordered_at` | Order time (within the last month, stored in UTC) |
| `city` | Hanoi or Ho Chi Minh City |
| `customer_name` | Vietnamese name |
| `address` | Vietnamese street address |
| `item_category` | Rice, Noodles, Coffee, Bubble tea, Banh mi or Pizza |
| `order_amount_vnd` | Order value in VND (30,000 to 500,000) |
| `payment_method` | cash, card or e-wallet |
| `delivery_minutes` | Delivery time in minutes (10 to 60) |
| `customer_rating` | Rating from 1 to 5 |

The `fakersink` connector writes the orders to `fakerdata`.

### Task 3: Open-Meteo Air Quality API

`airquality-producer/airquality_producer.py` calls the [Open-Meteo Air Quality API](https://open-meteo.com/en/docs/air-quality-api) (`https://air-quality-api.open-meteo.com/v1/air-quality`). It needs no API key. For each city it requests the current `pm10`, `pm2_5`, `us_aqi` and `european_aqi` values and sends them, with the city and report time, to the `airquality` topic. The `airqualitysink` connector writes them to `airquality`.

Why this API: air quality is directly relevant to riders who work outdoors, it pairs naturally with the weather, and it is free and keyless, so anyone can replicate the project.

### Task 4: Plots and analysis (`data-vis/python/analysis.ipynb`)

The notebook loads the three Cassandra tables into pandas DataFrames, prints each one, and draws three plots.

**Plot A: temperature and US air quality index over time, per city.** A two-panel line chart, one line per city, with a reference line at AQI 100 (unhealthy for sensitive groups). It is useful because it shows when riders work in heat or poor air, so the platform can plan rest breaks or a rider bonus, and it shows which city is worse at a given time.

**Plot B: orders and revenue per item category, by city.** Grouped bar charts. They are useful for menu planning: which categories drive order volume and which drive revenue in each city, so the platform knows what to promote and stock.

**Plot C: orders by US AQI category.** Each order is joined to the nearest earlier air quality reading (`pd.merge_asof`) and orders are counted per US AQI category (Good, Moderate, Unhealthy for sensitive groups, and so on). It is useful for forecasting demand and rider supply when air quality changes.

The written analysis of each plot is in the notebook, under each plot, and was written from the data of the demo run.

**Caveat:** the orders are synthetic, so Plot B and Plot C show the method rather than real behaviour. With real order data, the same notebook would show whether bad air increases or reduces orders.

## 6. Practical implication

A food-delivery company could run this pipeline continuously to watch rider conditions and demand in near real time: raise rider incentives or add riders when the air quality index or temperature becomes unhealthy, adjust delivery-time promises, and compare the two cities. The design is easy to extend, because adding another source needs one producer, one topic, one table and one sink.

## 7. Troubleshooting

- **`Connection refused` from cqlsh:** Cassandra is still starting. Wait another minute and retry.
- **`curl .../connectors` returns `[]` or an empty reply:** Kafka Connect is still starting. Wait 30 seconds and retry. If it stays empty, run `docker-compose -f kafka/docker-compose.yml down` and then `up -d` again. Do not recreate only the broker, because Connect then loses its offsets.
- **Weather producer prints errors:** check that your key is in `owm-producer/openweathermap_service.cfg`. A new OpenWeatherMap key can take up to two hours to activate.
- **Notebook shows no or few rows:** let the producers run a few minutes longer, then run all cells again.
- **Windows line endings:** `.gitattributes` forces LF line endings for `.sh` files. If a script fails with `\r` errors after a clone, make sure the files were checked out with LF endings.
- **Docker pulls hang:** restart Docker Desktop (or the computer) and run the build again.

## AI use declaration

I used Claude (Anthropic) as an AI assistant while working on this assignment. It helped with:

- Choosing the Task 3 API (Open-Meteo Air Quality) and the project use case (food delivery in Hanoi and Ho Chi Minh City).
- Designing the Task 2 Faker fields and the three plots.
- Drafting and debugging code: the producers and consumer, the Cassandra schema files, the Kafka Connect sink script, the Dockerfiles and compose files, and the analysis notebook.
- Drafting this README and the demo video script.
- Step-by-step guidance for running and troubleshooting the Docker pipeline.

I ran, tested and recorded the pipeline myself, I checked the results shown in the video, and I wrote the final analysis of the plots from my own run. I am responsible for the submitted work