from confluent_kafka.admin import AdminClient
import os

KAFKA_BROKER_URL = os.getenv('KAFKA_BROKER_URL', "PLAINTEXT://localhost:9092") 

def topic_exists(topic):
    """Checks if the given topic exists in Kafka"""
    client = AdminClient({ "bootstrap.servers": KAFKA_BROKER_URL })
    topic_metadata = client.list_topics(timeout=5)
    return topic in set(t.topic for t in iter(topic_metadata.topics.values()))
