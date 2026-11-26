"""Helpers that read the pipeline's Cassandra tables into pandas DataFrames."""
import os

import pandas as pd
from cassandra.cluster import Cluster
from cassandra.query import dict_factory

CASSANDRA_HOST = os.environ.get("CASSANDRA_HOST", "localhost")
CASSANDRA_KEYSPACE = os.environ.get("CASSANDRA_KEYSPACE", "kafkapipeline")

WEATHER_TABLE = os.environ.get("WEATHER_TABLE", "weatherreport")
FAKER_TABLE = os.environ.get("FAKER_TABLE", "fakerdata")
AIRQUALITY_TABLE = os.environ.get("AIRQUALITY_TABLE", "airquality")

ALLOWED_TABLES = (WEATHER_TABLE, FAKER_TABLE, AIRQUALITY_TABLE)


def getDF(source_table):
    """Read a whole table into a DataFrame. Returns None for an unknown table name."""
    if source_table not in ALLOWED_TABLES:
        return None

    cluster = Cluster([CASSANDRA_HOST])
    session = cluster.connect(CASSANDRA_KEYSPACE)
    session.row_factory = dict_factory
    rows = session.execute("SELECT * FROM " + source_table + ";")
    dataframe = pd.DataFrame(rows)
    cluster.shutdown()
    return dataframe


def getWeatherDF():
    return getDF(WEATHER_TABLE)


def getFakerDF():
    return getDF(FAKER_TABLE)


def getAirQualityDF():
    return getDF(AIRQUALITY_TABLE)
