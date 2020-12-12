"""Configures KSQL to combine station and turnstile data"""
import json
import logging
import logging.config
from pathlib import Path

import requests

import topic_check

# Import logging before models to ensure configuration is picked up
logging.config.fileConfig(f"{Path(__file__).parents[0]}/logging.ini")
logger = logging.getLogger(__name__)


KSQL_URL = "http://localhost:8088"

KSQL_STATEMENT = """
CREATE TABLE turnstile (
    station_id int,
    station_name VARCHAR,
    line VARCHAR
) WITH (
    KAFKA_TOPIC = 'com.udacity.turnstile.event',
    VALUE_FORMAT = 'AVRO',
    KEY = 'station_id'
);

CREATE TABLE turnstile_summary
WITH (
    VALUE_FORMAT = 'json'
) AS
    SELECT
        station_id,
        COUNT(station_id) AS count
    FROM turnstile
    GROUP BY station_id;
"""


def execute_statement():
    """Executes the KSQL statement against the KSQL API"""
    if topic_check.topic_exists("TURNSTILE_SUMMARY") is True:
        return

    logger.debug("executing ksql statement...")

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
