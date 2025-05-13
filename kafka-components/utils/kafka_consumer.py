from kafka import KafkaConsumer
from datetime import datetime, timedelta
import time

def create_kafka_consumer(topic: str, group_id: str) -> KafkaConsumer:
    """
    Creates and returns a KafkaConsumer instance subscribed to a specified topic, 
    with automatic reconnection retry logic.

    This function attempts to establish a connection to a Kafka broker and subscribe 
    to the given topic. If the connection fails (e.g., broker not yet available), 
    it will continuously retry every 3 seconds until successful. The returned consumer 
    starts reading messages from the earliest available offset by default.

    Parameters
    ----------
    topic : str
        The Kafka topic to subscribe to for consuming messages.
    group_id : str
        The consumer group ID to associate with the Kafka consumer. This allows 
        coordination and offset tracking among multiple consumers in the same group.

    Returns
    -------
    KafkaConsumer
        An instance of `kafka.KafkaConsumer` connected to the specified Kafka topic 
        and configured for the given consumer group.

    Notes
    -----
    - The Kafka broker is assumed to be available at `kafka:9092`. This hostname 
      typically refers to the Kafka service within a Docker Compose network.
    - The consumer is configured with `auto_offset_reset="earliest"`, which ensures 
      it reads messages from the beginning of the topic if no committed offset exists.
    - If Kafka is not ready at the time of execution, the function retries the connection 
      every 3 seconds until successful. This is useful in containerized or distributed setups 
      where services may start asynchronously.

    Examples
    --------
    >>> consumer = create_kafka_consumer(topic="bus.passenger.predictions", group_id="analytics-service")
    >>> for message in consumer:
    ...     print(message.value)
    """
    
    # Retry loop for Kafka connection
    consumer = None
    while consumer is None:
        try:
            consumer = KafkaConsumer(
                topic, 
                bootstrap_servers="kafka:9092",
                auto_offset_reset="earliest",
                group_id=group_id,
                consumer_timeout_ms=5000,
                enable_auto_commit=True
            )
            print("Kafka consumer connected.")
        except Exception as e:
            print(f"Kafka not ready, retrying in 3 seconds... ({e})")
            time.sleep(3)
    return consumer