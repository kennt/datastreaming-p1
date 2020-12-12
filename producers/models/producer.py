"""Producer base-class providing common utilites and functionality"""
import logging
import time

from confluent_kafka import avro
from confluent_kafka import Consumer
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer, CachedSchemaRegistryClient

import topic_check

logger = logging.getLogger(__name__)


class Producer:
    """Defines and provides common functionality amongst Producers"""

    BROKER_URL="PLAINTEXT://localhost:9092"
    SCHEMA_REGISTRY_URL="http://localhost:8081"

    # Tracks existing topics across all Producer instances
    existing_topics = set([])

    def __init__(
        self,
        topic_name,
        key_schema,
        value_schema=None,
        num_partitions=1,
        num_replicas=1,
    ):
        """Initializes a Producer object with basic settings"""
        self.topic_name = topic_name
        self.key_schema = key_schema
        self.value_schema = value_schema
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas

        # See Lesson 3.19 : Integrating the Schema Registry
        self.schema_registry = CachedSchemaRegistryClient(Producer.SCHEMA_REGISTRY_URL)


        # See Lesson 2.24 : Configure a Producer
        self.broker_properties = {
            "bootstrap.servers": Producer.BROKER_URL,
            "client.id": "project1-server-1",
            #"linger.ms": "",
            #"batch.num_messages": "",
            #"compression.type": "",
        }

        # If the topic does not already exist, try to create it
        if self.topic_name not in Producer.existing_topics:
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)

        self.producer = AvroProducer(
            self.broker_properties,
            schema_registry=self.schema_registry
        )

    def create_topic(self):
        """Creates the producer topic if it does not already exist"""
        if not topic_check.topic_exists(self.topic_name):
            topic_check.topic_create(self.topic_name,
                                     num_partitions = self.num_partitions,
                                     replication_factor = self.num_replicas)
            logger.info(f"Creating topic:{self.topic_name}")


    def time_millis(self):
        return int(round(time.time() * 1000))

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        self.producer.flush()

    def time_millis(self):
        """Use this function to get the key for Kafka Events"""
        return int(round(time.time() * 1000))
