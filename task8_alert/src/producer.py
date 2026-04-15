import csv

from confluent_kafka import Producer
import json

BATCH_SIZE: int = 10000


def delivery_report(err, msg) -> None:
    """
    Callback function to report message delivery status.
    Guarantees that callback bugs never crash the main producer loop.

    Args:
        err: Error information if delivery failed, otherwise None.
        msg: The Kafka message.

    Returns:
        None.
    """

    try:
        if err is not None:
            print(f"Delivery failed: {err}")
    except Exception as e:
        print(f" CRITICAL ERROR in delivery: {e}")


def log_producer(filename: str) -> None:
    """
    Reads a large CSV file and sends each row to Kafka.

    Args:
        filename: Path to the CSV file.
        :returns: None.

    Returns:
        None.
    """

    conf = {
        "bootstrap.servers": "kafka:9092",
        "compression.type": "snappy",  # faster network usage
        "acks": "all",  # strongest durability
        "enable.idempotence": True,  # Prevents duplicates on retries
        "linger.ms": 50,  # Wait up to 50ms to build a batch
        "delivery.timeout.ms": 120000,  # Give up after 2 minutes of retry attempts
        "batch.size": 65536,  # 64KB batches for better compression
    }

    producer = Producer(conf)
    topic_name = "raw-mobile-logs"

    print("Starting csv ingestion...")
    total_sent = 0

    try:
        # Open the file as a raw text stream
        with open(filename, mode="r", encoding="utf-8") as file:
            # DictReader maps the header row to keys automatically
            csv_reader = csv.DictReader(file)

            for row in csv_reader:
                json_data = json.dumps(row).encode("utf-8")

                # Handle queue full
                while True:
                    try:
                        producer.produce(
                            topic_name, value=json_data, callback=delivery_report
                        )
                        break
                    except BufferError:
                        # Wait 0.5 seconds to let the background thread send data over the network
                        producer.poll(0.5)
                total_sent += 1

                # Poll periodically to clear the RAM buffer
                if total_sent % BATCH_SIZE == 0:
                    producer.poll(0)
                    print(f"Successfully sent {total_sent} records so far...")

        # Block Python and wait for the background thread to finish emptying the buffer
        producer.flush()
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
    csv_path = project_root / "data" / "data.csv"

    log_producer(str(csv_path))
