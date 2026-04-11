import json
import pandas as pd
from confluent_kafka import Consumer, KafkaError

BATCH_SIZE_LIMIT = 5000

def run_alert_engine() -> None:
    conf = {
        'bootstrap.servers': 'kafka:9092',
        'group.id': 'alert_group_1',
        'auto.offset.reset': 'earliest'
    }
    consumer = Consumer(conf)
    topic_name = 'raw-mobile-logs'

    consumer.subscribe([topic_name])
    print(f"Subscribed to {topic_name}. Waiting for messages...")

    batch = []
    total_logs_processed = 0
    total_errors_found = 0

    try:
        while True:
            # Poll for a message, wait at most 1.0 seconds
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                # If no message arrived in the last second, but we have some in our batch,
                # process them anyway so alerts aren't delayed
                if len(batch) > 0:
                    logs_count, errors_count = process_batch(batch)

                    total_logs_processed += logs_count
                    total_errors_found += errors_count
                    print(f"--- RUNNING TOTAL: {total_errors_found}"
                          f" errors out of {total_logs_processed} logs ---")

                    batch = []
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event (not a real error)
                    continue
                else:
                    print(f"Consumer error: {msg.error()}")
                    continue
            # Decode the message: Bytes -> String -> Dictionary
            try:
                record_dict = json.loads(msg.value().decode('utf-8'))
                batch.append(record_dict)
            except json.decoder.JSONDecodeError:
                print("Failed to decode JSON. Skipping corrupted message.")

            # If our batch hits 5,000, process it immediately
            if len(batch) >= BATCH_SIZE_LIMIT:
                logs_count, errors_count = process_batch(batch)

                total_logs_processed += logs_count
                total_errors_found += errors_count
                print(f"--- RUNNING TOTAL: {total_errors_found} errors out of {total_logs_processed} logs ---")

                batch = []

    except KeyboardInterrupt:
        print("Shutting down consumer...")
    finally:
        consumer.close()


def process_batch(batch: list[dict]) -> tuple[int, int]:
    """Takes a list of dictionaries, converts to Pandas, and finds errors."""
    # Convert to Pandas instantly
    df = pd.DataFrame(batch)
    batch_logs_count = len(df)

    errors_df = df[df['2'] == 'Error']

    batch_errors_count = len(errors_df)

    if batch_errors_count == 0:
        print(f"Batch clear: Processed {batch_logs_count} logs. No errors found.")
    else:
        print(f"Batch alert: Processed {batch_logs_count} logs. Found {batch_errors_count} errors!")
        # TODO: Pass errors_df to apply rules and send Telegram notifications here

    return batch_logs_count, batch_errors_count


if __name__ == "__main__":
    run_alert_engine()
