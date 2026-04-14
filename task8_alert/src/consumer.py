import json
import pandas as pd
from confluent_kafka import Consumer, KafkaError
from rules import ACTIVE_RULES
import os

from notifier import TelegramNotifier

BATCH_SIZE_LIMIT = 10000
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')
LOCALIZATION_MAP = {
    r'PG': 'AM',  # Malay AM
    r'PT': 'PM',  # Malay PM
    r'ص': 'AM',  # Arabic AM
    r'م': 'PM',  # Arabic PM
    r'ق\.ظ\.': 'AM',  # Arabic AM
    r'ب\.ظ\.': 'PM',  # Arabic PM
    r'PMG': 'PM',
    r'SA': 'AM',  # Vietnamese AM
    r'CH': 'PM',  # Vietnamese PM
    r'г': '',  # Cyrillic "year"
    r'上午': 'AM',  # Chinese AM
    r'下午': 'PM',  # Chinese PM
    r'π\.μ\.': 'AM',  # Greek AM
    r'μ\.μ\.': 'PM',  # Greek PM
}


def run_alert_engine() -> None:
    """
    Consumes messages from Kafka, processes them in batches,
    and evaluates alert rules.

    Returns:
        None.
    """

    notifier = TelegramNotifier(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID)

    conf = {
        'bootstrap.servers': 'kafka:9092',
        'group.id': 'alert_group_1',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False,  # Prevent data loss on crash
        'max.poll.interval.ms': 300000,  # Give Pandas 5 mins max to process a batch
        'fetch.min.bytes': 100000,  # Wait for at least 100KB of data
        'fetch.wait.max.ms': 500  # Or 500ms, whichever comes first
    }
    consumer = Consumer(conf)
    topic_name = 'raw-mobile-logs'

    consumer.subscribe([topic_name])
    print(f"Subscribed to {topic_name}. Waiting for messages...")

    error_history_df = pd.DataFrame()

    try:
        while True:
            # Let the C-library build the batch of rows.
            # It returns immediately if it hits batch size, or waits up to 1 sec.
            msgs = consumer.consume(num_messages=BATCH_SIZE_LIMIT, timeout=1.0)

            # If no messages arrived, loop again
            if not msgs:
                continue

            batch = []

            # Process the C-array into a Python list
            for msg in msgs:
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue  # Normal behavior
                    else:
                        print(f"Consumer error: {msg.error()}")
                        continue
                # Decode the message: Bytes -> String -> Dictionary
                try:
                    record_dict = json.loads(msg.value().decode('utf-8'))
                    batch.append(record_dict)
                except json.decoder.JSONDecodeError:
                    print("Failed to decode JSON. Skipping corrupted message.")
            # If we successfully parsed valid messages, evaluate them
            if batch:
                error_history_df = process_and_evaluate(batch, error_history_df, notifier)

            # Commit offsets ONLY after successful processing
            # asynchronous=False ensures we don't fetch more data until the commit is verified
            consumer.commit(asynchronous=False)
            print(f"Successfully processed and committed batch of {len(batch)} records.")
    except KeyboardInterrupt:
        print("Shutting down consumer...")
    except Exception as e:
        print(f"Fatal pipeline error: {e}")
    finally:
        consumer.close()


def process_and_evaluate(batch: list[dict], history_df: pd.DataFrame,
                         tg_notifier: TelegramNotifier) -> pd.DataFrame:
    """
    Converts batch to DataFrame, cleans data, updates history,
    and evaluates alert rules.

    Args:
        batch: List of records from Kafka.
        history_df: Existing error history.
        tg_notifier: Telegram notifier instance.

    Returns:
        Updated history DataFrame.
    """

    df = pd.DataFrame(batch)
    # Clean the raw data
    clean_df = clean_timestamp(df)
    # Update the rolling memory
    new_history_df = update_history(clean_df, history_df)
    # Evaluate rules and trigger alerts
    evaluate_and_alert(new_history_df, tg_notifier)

    # Return the updated state to the Kafka loop
    return new_history_df


def clean_timestamp(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardizes global timestamps and parses them into Datetime objects.

    Args:
        df: Raw DataFrame.

    Returns:
        DataFrame with parsed timestamps.
    """

    # Ensure everything is strictly a string before cleaning
    df['timestamp'] = df['12'].astype(str)
    # Unify date separators
    df['timestamp'] = df['timestamp'].str.replace(r'[/-]', '.', regex=True)
    # Fix the "European Dot" anomalies
    df['timestamp'] = df['timestamp'].str.replace(r'\.\s+', ' ', regex=True)
    df['timestamp'] = df['timestamp'].str.replace(r'\s(\d{1,2})\.(\d{2})\.(\d{2})', r' \1:\2:\3', regex=True)
    # String replacement loop
    for unknown_chars, english_chars in LOCALIZATION_MAP.items():
        df['timestamp'] = df['timestamp'].str.replace(unknown_chars, english_chars, regex=True)
    # Fix the AM/PM placement for East Asian formats
    df['timestamp'] = df['timestamp'].str.replace(r'\s(AM|PM)\s(\d{1,2}:\d{2}:\d{2})', r' \2 \1', regex=True)

    # Parse clean strings
    df['timestamp'] = pd.to_datetime(df['timestamp'], format='mixed', dayfirst=True)
    return df


def update_history(current_batch_df: pd.DataFrame, history_df: pd.DataFrame) -> pd.DataFrame:
    """
    Filters errors from the batch, appends them to history,
    and prunes data older than 1 hour.

    Args:
        current_batch_df: Cleaned batch DataFrame.
        history_df: Previous history.

    Returns:
        Updated history DataFrame.
    """

    # Extract only the errors from this incoming batch
    errors = current_batch_df[current_batch_df['2'] == 'Error']
    # If there are no new errors, just return the existing history to save CPU
    if errors.empty:
        return history_df
    # Append the new errors to rolling history
    updated_history_df = pd.concat([history_df, errors], ignore_index=True)

    # Find the newest timestamp and draw a line 1 hour behind it
    latest_time = updated_history_df['timestamp'].max()
    one_hour_ago = latest_time - pd.Timedelta(hours=1)
    return updated_history_df[updated_history_df['timestamp'] >= one_hour_ago]


def evaluate_and_alert(history_df: pd.DataFrame, tg_notifier: TelegramNotifier) -> None:
    """
     Runs alert rules on history and sends alerts.

    Args:
        history_df: DataFrame with recent errors.
        tg_notifier: Telegram notifier instance.

    Returns:
        None.
    """

    if history_df.empty:
        return None

    # Dynamic rule engine
    for rule_function in ACTIVE_RULES:
        triggered_alerts = rule_function(history_df)

        for alert_message, alert_id in triggered_alerts:
            print(alert_message)
            tg_notifier.send_alert(alert_message, alert_id=alert_id)

    return None


if __name__ == "__main__":
    run_alert_engine()
