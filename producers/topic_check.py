from confluent_kafka.admin import AdminClient, NewTopic

BOOTSTRAP_SERVER = "PLAINTEXT://localhost:9092"

def topic_exists(topic):
    """Checks if the given topic exists in Kafka"""
    client = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVER})
    topic_metadata = client.list_topics(timeout=5)
    return topic in set(t.topic for t in iter(topic_metadata.topics.values()))


def topic_create(topic_name, num_partitions=1, replication_factor=1):
    """Create a topic in Kafka"""
    client = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVER})
    futures = client.create_topics(
        [NewTopic(topic=topic_name,
                  num_partitions=num_partitions,
                  replication_factor=replication_factor)]
    )
    for _, future in futures.items():
        try:
            future.result()
        except Exception as e:
            traceback.print_exc()
