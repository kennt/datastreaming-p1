"""Defines core consumer functionality"""
import logging
import logging.config
from pathlib import Path
import random

# Import logging before models to ensure configuration is picked up
logging.config.fileConfig(f"{Path(__file__).parents[0]}/logging.ini")

import confluent_kafka
from confluent_kafka import Consumer
from confluent_kafka.avro import AvroConsumer
from confluent_kafka.avro.serializer import SerializerError
from tornado import gen

logger = logging.getLogger(__name__)


class KafkaConsumer:
    """Defines the base kafka consumer class"""

    BROKER_URL="PLAINTEXT://localhost:9092"
    SCHEMA_REGISTRY_URL="http://localhost:8081"

    def __init__(
        self,
        topic_name_pattern,
        message_handler,
        is_avro=True,
        offset_earliest=False,
        sleep_secs=1.0,
        consume_timeout=0.1,
    ):
        """Creates a consumer object for asynchronous use"""
        self.topic_name_pattern = topic_name_pattern
        self.message_handler = message_handler
        self.sleep_secs = sleep_secs
        self.consume_timeout = consume_timeout
        self.offset_earliest = offset_earliest

        self.broker_properties = {
            "bootstrap.servers": KafkaConsumer.BROKER_URL,
            "group.id": f"group{random.randint(0,1000000)}",
            "default.topic.config": {
                "auto.offset.reset": "earliest"
            },
         }

        if self.offset_earliest:
            self.broker_properties['auto.offset.reset'] = 'earliest'
        if is_avro is True:
            self.broker_properties["schema.registry.url"] = KafkaConsumer.SCHEMA_REGISTRY_URL
            self.consumer = AvroConsumer(self.broker_properties)
        else:
            self.consumer = Consumer(self.broker_properties)

        logger.debug(f"Consumer subscribing to {self.topic_name_pattern}")
        self.consumer.subscribe([self.topic_name_pattern],
                                on_assign=self.on_assign
                                )

    def on_assign(self, consumer, partitions):
        """Callback for when topic assignment takes place"""

        if self.offset_earliest:
            for partition in partitions:
                partition.offset = confluent_kafka.OFFSET_BEGINNING

        consumer.assign(partitions)

    async def consume(self):
        """Asynchronously consumes data from kafka topic"""
        try:
            while True:
                num_results = 1
                while num_results > 0:
                    num_results = self._consume()
                await gen.sleep(self.sleep_secs)
        except:
            traceback.print_exc()

    def _consume(self):
        """Polls for a message. Returns 1 if a message was received, 0 otherwise"""
        #
        #
        # TODO: Poll Kafka for messages. Make sure to handle any errors or exceptions.
        # Additionally, make sure you return 1 when a message is processed, and 0 when no message
        # is retrieved.
        #
        #
        try:
            message = self.consumer.poll(self.consume_timeout)
            if message is None:
                # Not an error, we didn't receive any messages
                # in this polling period
                pass
            elif message.error() is not None:
                logger.info(f"Error during polling: {message.error()}")
            else:
                # Not an error, we received a message
                # This is the only case where we should return 1
                self.message_handler(message)
                return 1

        except SerializerError as e:
            logger.info("Message deserialization failed")
        except KeyError as e:
            logger.info(f"Failed to unpack message: {e}")
        except Exception as e:
            traceback.print_exc()

        return 0

    def close(self):
        """Cleans up any open kafka consumers"""
        self.consumer.unsubscribe()
        self.consumer.close()
