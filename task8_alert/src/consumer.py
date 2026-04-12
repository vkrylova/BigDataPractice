import json
import pandas as pd
from confluent_kafka import Consumer, KafkaError
from rules import ACTIVE_RULES
import os

from notifier import TelegramNotifier

BATCH_SIZE_LIMIT = 5000
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
    notifier = TelegramNotifier(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID)

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

    error_history_df = pd.DataFrame()

    try:
        while True:
            # Poll for a message, wait at most 1.0 seconds
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                # If no message arrived in the last second, but we have some in our batch,
                # process them anyway so alerts aren't delayed
                if len(batch) > 0:
                    error_history_df = process_and_evaluate(batch, error_history_df, notifier)
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
                error_history_df = process_and_evaluate(batch, error_history_df, notifier)
                batch = []

    except KeyboardInterrupt:
        print("Shutting down consumer...")
    finally:
        consumer.close()


def process_and_evaluate(batch: list[dict], history_df: pd.DataFrame, tg_notifier: TelegramNotifier) -> pd.DataFrame:
    """Processes a batch, updates the rolling history, and checks alert rules."""
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
    """Standardizes global timestamps and parses them into Datetime objects."""
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
    """Filters errors from the batch, appends them to history, and prunes data older than 1 hour."""

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
