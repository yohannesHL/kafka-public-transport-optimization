"""Producer base-class providing common utilites and functionality"""
import logging
import time
import os
from io import BytesIO
from confluent_kafka import avro
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer, CachedSchemaRegistryClient
from fastavro import parse_schema, writer

KAFKA_BROKER_URL = os.getenv('KAFKA_BROKER_URL', "PLAINTEXT://localhost:9092") 
SCHEMA_REGISTRY_URL = os.getenv('SCHEMA_REGISTRY_URL', "http://localhost:8081") 

logger = logging.getLogger(__name__)


class Producer:
    """Defines and provides common functionality amongst Producers"""

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

        self.broker_properties = {
            "bootstrap.servers": KAFKA_BROKER_URL
        }

        # If the topic does not already exist, try to create it
        if self.topic_name not in Producer.existing_topics:
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)

        self.producer = AvroProducer(
            {
                **self.broker_properties,
                "schema.registry.url": SCHEMA_REGISTRY_URL
            },
            default_key_schema = self.key_schema,
            default_value_schema = self.value_schema
        )

    def create_topic(self):
        """Creates the producer topic if it does not already exist"""
        client = AdminClient(self.broker_properties)
        meta = client.list_topics()

        if self.topic_name in meta.topics:
            logger.info(f"topic {self.topic_name} already exists in kafka broker - skipping topic creation")
            return

        topic = NewTopic(
            self.topic_name, 
            num_partitions=self.num_partitions, 
            replication_factor=self.num_replicas,
            config = { 
                "retention.ms": -1
            })

        client.create_topics([topic])

    def send(self, topic=None, key=None, value=None):
        """Send event to kafka broker

        Keyword Arguments:
            topic {[type]} -- [description] (default: {None})
            key {[type]} -- [description] (default: {None})
            value {[type]} -- [description] (default: {None})
        """
        out = BytesIO()
        writer(out, parse_schema(self.value_schema), [value])

        return self.producer.produce(topic=topic,key=key,value=out.getvalue())

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        self.producer.flush()

    def time_millis(self):
        """Use this function to get the key for Kafka Events"""
        return int(round(time.time() * 1000))
