"""Defines core consumer functionality"""
import logging

import confluent_kafka
from confluent_kafka import Consumer, OFFSET_BEGINNING
from confluent_kafka.avro import AvroConsumer
from confluent_kafka.avro.serializer import SerializerError
from tornado import gen


logger = logging.getLogger(__name__)

BROKER_URL = "PLAINTEXT://localhost:9092"

class KafkaConsumer:
    """Defines the base kafka consumer class"""

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

        #
        #
        # TODO: Configure the broker properties below. Make sure to reference the project README
        # and use the Host URL for Kafka and Schema Registry!
        #
        #
        self.broker_properties = {
            #
            # TODO
            "bootstrap.servers": BROKER_URL,
            "group.id": self.topic_name_pattern,
            "auto.offset.reset": "earliest" if offset_earliest else "latest"
        }

        logger.info(f"Got consumer for topic: {topic_name_pattern}")
        
        # TODO: Create the Consumer, using the appropriate type.
        if is_avro is True:
            self.broker_properties["schema.registry.url"] = "http://localhost:8081"
            self.consumer = AvroConsumer(self.broker_properties)
        else:
            self.consumer = Consumer(self.broker_properties)

        #
        #
        # TODO: Configure the AvroConsumer and subscribe to the topics. Make sure to think about
        # how the `on_assign` callback should be invoked.
        #
        #
        logger.info(f"Subscribing to topic {self.topic_name_pattern}")
        self.consumer.subscribe([self.topic_name_pattern])
        logger.info(f"Subscribed to topic {self.topic_name_pattern}")

    def on_assign(self, consumer, partitions):
        """Callback for when topic assignment takes place"""
        # TODO: If the topic is configured to use `offset_earliest` set the partition offset to
        # the beginning or earliest
#         logger.info("on_assign is incomplete - skipping")
        print("on_assign method")
        print(f"Got consumer topic: {self.topic_name_pattern}")
        for partition in partitions:
            if self.offset_earliest:
                partition.offset = OFFSET_BEGINNING
            #
            #
            # TODO
            

        logger.info("partitions assigned for %s", self.topic_name_pattern)
        consumer.assign(partitions)

    async def consume(self):
        """Asynchronously consumes data from kafka topic"""
        while True:
            num_results = 1
            while num_results > 0:
#                 print(num_results)
                num_results = self._consume()
#                 print(num_results)
                
            await gen.sleep(self.sleep_secs)

    def _consume(self):
        """Polls for a message. Returns 1 if a message was received, 0 otherwise"""
        #
        #
        # TODO: Poll Kafka for messages. Make sure to handle any errors or exceptions.
        # Additionally, make sure you return 1 when a message is processed, and 0 when no message
        # is retrieved.
        while True:
            message = self.consumer.poll(self.consume_timeout)
#             print(message)
            if message is None:
                logger.warn("no message received by consumer")
                return 0
            elif message.error() is not None:
                logger.error(f"error from consumer {message.error()}")
                return 0
            else:
                logger.info("message consumed successfully")
                self.message_handler(message)
                return 1
                

    def close(self):
        """Cleans up any open kafka consumers"""
        #
        #
        # TODO: Cleanup the kafka consumer
        self.consumer.close()
