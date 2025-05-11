from kafka import KafkaProducer
import json
import time

def create_kafka_producer():
    """
    Creates and returns a KafkaProducer instance with JSON serialization, 
    including retry logic to ensure connection to the Kafka broker.

    This function initializes a Kafka producer that connects to a Kafka broker 
    at `kafka:9092`. The producer is configured to serialize Python dictionaries 
    or objects into JSON-formatted byte strings before sending them as message payloads. 
    If the broker is not available at the time of execution, the function will retry 
    the connection every 3 seconds until successful.

    Returns
    -------
    KafkaProducer
        An instance of `kafka.KafkaProducer` configured with a JSON value serializer 
        and connected to the specified Kafka broker.

    Notes
    -----
    - The Kafka broker is assumed to be available at the hostname `kafka:9092`, 
      which is typically used within a Docker Compose or container network setup.
    - The producer uses a JSON serializer: values sent to Kafka must be serializable 
      to JSON, or an exception will occur.
    - The retry mechanism is blocking and will indefinitely attempt to establish 
      the connection, making it suitable for systems where Kafka startup may be delayed.

    Examples
    --------
    >>> producer = create_kafka_producer()
    >>> payload = {"key": "value", "timestamp": "2025-01-01T12:00:00"}
    >>> producer.send("example-topic", value=payload)
    """
    
    producer = None
    while producer is None:
        try:
            producer = KafkaProducer(
                bootstrap_servers='kafka:9092',
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            print("Kafka producer connected.")
        except Exception as e:
            print(f"Kafka not ready, retrying in 3 seconds... ({e})")
            time.sleep(3)
    return producer