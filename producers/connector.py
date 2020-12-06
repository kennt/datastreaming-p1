"""Configures a Kafka Connector for Postgres Station data"""
import json
import logging
import logging.config
from pathlib import Path

import requests

# Import logging before models to ensure configuration is picked up
logging.config.fileConfig(f"{Path(__file__).parents[0]}/logging.ini")

logger = logging.getLogger(__name__)


KAFKA_CONNECT_URL = "http://localhost:8083/connectors"
CONNECTOR_NAME = "stations"

def configure_connector():
    """Starts and configures the Kafka Connect connector"""
    logger.info("creating or updating kafka connect connector...")

    resp = requests.get(f"{KAFKA_CONNECT_URL}/{CONNECTOR_NAME}")
    if resp.status_code == 200:
        logger.info("connector already created skipping recreation")
        return
    logger.info(f"need to create the kafka connector for {CONNECTOR_NAME}...")

    # TODO: Complete the Kafka Connect Config below.
    # Directions: Use the JDBC Source Connector to connect to Postgres. Load the `stations` table
    # using incrementing mode, with `stop_id` as the incrementing column name.
    # Make sure to think about what an appropriate topic prefix would be, and how frequently Kafka
    # Connect should run this connector (hint: not very often!)
    #logger.info("connector code not completed skipping connector creation")
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
                # TODO
                "connection.url": "jdbc:postgresql://postgres:5432/cta",
                # TODO
               "connection.user": "cta_admin",
                # TODO
                "connection.password": "chicago",
                # TODO
                "table.whitelist": "stations",
                # TODO
                "mode": "incrementing",
                # TODO
                "incrementing.column.name": "stop_id",
                # TODO
                "topic.prefix": "com.udacity.connect-",
                # TODO
                # Poll once an hour
                "poll.interval.ms": "3600000",
            }
        }),
    )

    ## Ensure a healthy response was given
    resp.raise_for_status()
    logger.info("connector created successfully")


if __name__ == "__main__":
    configure_connector()
