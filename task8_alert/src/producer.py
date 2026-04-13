from confluent_kafka import Producer
import pandas as pd
import time
import json

CHUNKSIZE: int = 10000


def delivery_report(err, msg) -> None:
    """
    Callback function to report message delivery status.

    Args:
        err: Error information if delivery failed, otherwise None.
        msg: The Kafka message.

    Returns:
        None.
    """
    if err is not None:
        print(f"Delivery failed: {err}")


def log_producer(filename: str) -> None:
    """
    Reads a large CSV file in chunks and sends each row to Kafka.

    Args:
        filename: Path to the CSV file.
        :returns: None.

    Returns:
        None.
    """

    conf = {
        'bootstrap.servers': 'kafka:9092',
        'acks': 'all',  # strongest durability
        'retries': 5,  # retry on transient errors
        'compression.type': 'snappy',  # faster network usage
    }

    producer = Producer(conf)
    topic_name = 'raw-mobile-logs'

    print("Starting csv ingestion...")
    total_sent = 0

    try:
        # Read the massive CSV in chunks of 5,000 rows
        for chunk in pd.read_csv(filename, chunksize=CHUNKSIZE):

            # Iterate through the rows in this chunk
            for index, row in chunk.iterrows():
                # Convert the Pandas row to a Python dictionary
                record = row.to_dict()

                # Kafka requires bytes.
                # Convert dict -> JSON string -> UTF-8 bytes
                json_data = json.dumps(record).encode("utf-8")

                # Push the record to Kafka
                producer.produce(topic_name, value=json_data, callback=delivery_report)

                # This triggers the delivery_report and keeps the producer's memory clean
                producer.poll(0)

            # Wait for the batch to be fully transmitted over the network
            producer.flush()

            total_sent += len(chunk)
            print(f"Successfully sent {total_sent} records so far...")
            time.sleep(1)
        print("All data is sent to Kafka successfully!")

    except Exception as e:
        print(f"A fatal error occurred while reading the CSV: {e}")


if __name__ == "__main__":
    """
    Entry point for running the producer as a standalone script.
    Builds the CSV file path and starts the producer.
    """

    from pathlib import Path

    print("Starting standalone producer...")

    # Safely build the path to the CSV file
    project_root = Path(__file__).resolve().parent.parent
    csv_path = project_root / 'data' / 'data.csv'

    log_producer(str(csv_path))
