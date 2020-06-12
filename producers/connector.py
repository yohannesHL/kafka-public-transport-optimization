"""Configures a Kafka Connector for Postgres Station data"""
import json
import logging

import requests


logger = logging.getLogger(__name__)

# TODO: move to .env file
KAFKA_CONNECT_URL = "http://connect:8083/connectors"
CONNECTOR_NAME = "stations"
POSTGRES_USER =  "cta_admin"
POSTGRES_PASSWORD = "chicago"
POSTGRES_DB = "cta"

def configure_connector():
    """Starts and configures the Kafka Connect connector"""
    logging.debug("creating or updating kafka connect connector...")

    resp = requests.get(f"{KAFKA_CONNECT_URL}/{CONNECTOR_NAME}")
    if resp.status_code == 200:
        logging.debug("connector already created skipping recreation")
        return

    resp = requests.post(
       KAFKA_CONNECT_URL,
       headers={"Content-Type": "application/json"},
       data=json.dumps({
           "name": CONNECTOR_NAME,
           "config": {
               "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
               "key.converter": "org.apache.kafka.connect.json.JsonConverter",
               "key.converter.schemas.enable": "false",
               "value.converter": "org.apache.kafka.connect.json.JsonConverter",
               "value.converter.schemas.enable": "false",
               "batch.max.rows": "500",
               "connection.url": "jdbc:postgresql://postgres_1:5432/stations",
               "connection.user": "cta_admin",
               "connection.password": "chicago",
               "table.whitelist": "stations",
               "mode": "incrementing",
               "incrementing.column.name": "stop_id",
               "topic.prefix": "cta.",
               "poll.interval.ms": "300000",
           }
       }),
    )

    try:
        resp.raise_for_status()
    except:
        print('Error', resp.text)

        logging.debug("error creating connector", resp.text)

    logging.debug("connector created successfully")


if __name__ == "__main__":
    configure_connector()
