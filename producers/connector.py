"""Configures a Kafka Connector for Postgres Station data"""
import json
import logging
import logging.config
from pathlib import Path

import requests

import topic_check

# Import logging before models to ensure configuration is picked up
logging.config.fileConfig(f"{Path(__file__).parents[0]}/logging.ini")

logger = logging.getLogger(__name__)


KAFKA_CONNECT_URL = "http://localhost:8083/connectors"
CONNECTOR_NAME = "stations"
CONNECTOR_TOPIC_PREFIX="com.udacity.connect-"
CONNECTOR_TOPIC_NAME=f"{CONNECTOR_TOPIC_PREFIX}{CONNECTOR_NAME}"

def configure_connector():
    """Starts and configures the Kafka Connect connector"""
    logger.info("creating or updating kafka connect connector...")

    resp = requests.get(f"{KAFKA_CONNECT_URL}/{CONNECTOR_NAME}")
    if resp.status_code == 200:
        logger.info("connector already created skipping recreation")
        return
    logger.info(f"need to create the kafka connector for {CONNECTOR_NAME}...")

    # Ensure that the topic has been created
    if not topic_check.topic_exists(CONNECTOR_TOPIC_NAME):
        topic_check.topic_create(CONNECTOR_TOPIC_NAME)
        logger.info(f"Topic created:{CONNECTOR_TOPIC_NAME}")
    else:
        logger.info(f"Using existingt topic:{CONNECTOR_TOPIC_NAME}")

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
                "connection.url": "jdbc:postgresql://postgres:5432/cta",
                "connection.user": "cta_admin",
                "connection.password": "chicago",
                "table.whitelist": "stations",
                "mode": "incrementing",
                "incrementing.column.name": "stop_id",
                "topic.prefix": CONNECTOR_TOPIC_PREFIX,
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
