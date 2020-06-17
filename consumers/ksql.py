"""Configures KSQL to combine station and turnstile data"""
import json
import logging

import requests

import topic_check


logger = logging.getLogger(__name__)


KSQL_URL = "http://ksql:8088"

KSQL_STATEMENT = """
CREATE TABLE turnstile (
    timestamp VARCHAR,
    station_id INTEGER,
    station_name VARCHAR,
    line VARCHAR
) WITH (
    KAFKA_TOPIC='org.chicago.cta.trainstation.visits',
    VALUE_FORMAT='avro',
    KEY='timestamp'
);

CREATE TABLE turnstile_summary 
WITH ( 
    KAFKA_TOPIC='org.chicago.cta.trainstation.visits-summary',
    VALUE_FORMAT='JSON') AS 
    SELECT COUNT(station_id) as count, station_id
    FROM turnstile
    GROUP BY station_id;
"""


def execute_statement():
    """Executes the KSQL statement against the KSQL API"""
    if topic_check.topic_exists("TURNSTILE_SUMMARY") is True:
        return

    logging.debug("executing ksql statement...")

    resp = requests.post(
        f"{KSQL_URL}/ksql",
        headers={"Content-Type": "application/vnd.ksql.v1+json"},
        data=json.dumps(
            {
                "ksql": KSQL_STATEMENT,
                "streamsProperties": {"ksql.streams.auto.offset.reset": "earliest"},
            }
        ),
    )

    # Ensure that a 2XX status code was returned
    resp.raise_for_status()


if __name__ == "__main__":
    execute_statement()
