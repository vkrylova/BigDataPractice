import logging
from typing import Any

import boto3
import pandas as pd
import os
import json
from decimal import Decimal
import concurrent.futures

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)

LOCALSTACK_URL = f"http://{os.getenv('LOCALSTACK_HOSTNAME', 'localstack-main')}:4566"


def get_boto3_client(service_name: str) -> Any:
    """
    Returns a Boto3 client configured for the LocalStack endpoint.

    Args:
        service_name (str): The AWS service to create a client for (e.g., "s3").

    Returns:
        Any: A configured Boto3 client object.
    """

    # LocalStack injects LOCALSTACK_HOSTNAME automatically
    host = os.getenv("LOCALSTACK_HOSTNAME", "localstack-main")
    endpoint = f"http://{host}:4566"

    return boto3.client(
        service_name,
        endpoint_url=endpoint,
        region_name=os.getenv("AWS_REGION", "us-east-1"),
    )


def get_boto3_resource(service_name: str) -> Any:
    """
    Returns a Boto3 resource configured for the LocalStack endpoint.

    Args:
        service_name (str): The AWS service to create a resource for (e.g., "dynamodb").

    Returns:
        Any: A configured Boto3 resource object.
    """

    host = os.getenv("LOCALSTACK_HOSTNAME", "localstack-main")
    endpoint = f"http://{host}:4566"

    return boto3.resource(
        service_name,
        endpoint_url=endpoint,
        region_name=os.getenv("AWS_REGION", "us-east-1"),
    )


def check_all_files_exist(s3_client: Any, bucket: str, month_id: str) -> bool:
    """
    Validates that the main data file and both metric files exist in S3.

    Args:
        s3_client (Any): The configured Boto3 S3 client.
        bucket (str): The name of the S3 bucket.
        month_id (str): The target month identifier to check files for.

    Returns:
        bool: True if all three required files exist, False otherwise.
    """

    required_files: list[str] = [
        f"{month_id}.csv",
        f"metrics/departures_{month_id}.csv",
        f"metrics/returns_{month_id}.csv",
    ]

    for file_key in required_files:
        try:
            s3_client.head_object(Bucket=bucket, Key=file_key)
        except s3_client.exceptions.ClientError:
            return False

    return True


def load_csv_from_s3(s3_client: Any, bucket: str, file_key: str) -> pd.DataFrame:
    """
    Fetches an object from S3 and loads it directly into a Pandas DataFrame.

    Args:
        s3_client (Any): The configured Boto3 S3 client.
        bucket (str): The name of the S3 bucket.
        file_key (str): The S3 object key for the CSV file.

    Returns:
        pd.DataFrame: A Pandas DataFrame containing the CSV data.
    """

    response: dict[str, Any] = s3_client.get_object(Bucket=bucket, Key=file_key)
    return pd.read_csv(response["Body"])


def prepare_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans the raw DataFrame, standardizes column names, and handles missing data.

    Args:
        df (pd.DataFrame): The raw DataFrame loaded from S3.

    Returns:
        pd.DataFrame: A cleaned DataFrame ready for metric aggregation.
    """

    df.fillna(0, inplace=True)

    # Safely rename columns regardless of exact Kaggle capitalization
    rename_map: dict[str, str] = {
        "distance (m)": "distance",
        "duration (sec.)": "duration",
        "avg_speed (km/h)": "speed",
        "Air temperature (degC)": "temperature",
    }
    df.rename(columns=rename_map, inplace=True, errors="ignore")

    # Safety Net: If any required column is completely missing from the CSV, create it with 0.0
    for col in ["distance", "duration", "speed", "temperature"]:
        if col not in df.columns:
            df[col] = 0.0

    # Extract Date safely (coerce turns bad dates into NaT instead of crashing)
    dep_col: str = "departure" if "departure" in df.columns else "Departure"
    if dep_col in df.columns:
        df["departure_datetime"] = pd.to_datetime(df[dep_col], errors="coerce")
        # Drop corrupted rows where the datetime couldn't be parsed
        df.dropna(subset=["departure_datetime"], inplace=True)
        df["date"] = df["departure_datetime"].dt.date.astype(str)
    else:
        df["date"] = "1970-01-01"

    return df


def calculate_daily_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregates the cleaned DataFrame to compute daily averages.

    Args:
        df (pd.DataFrame): The cleaned DataFrame.

    Returns:
        pd.DataFrame: A new DataFrame grouped by date with daily average metrics.
    """

    return (
        df.groupby("date")
        .agg(
            {
                "distance": "mean",
                "duration": "mean",
                "speed": "mean",
                "temperature": "mean",
            }
        )
        .fillna(0)
        .reset_index()
    )


def save_daily_metrics(dynamodb, daily_df: pd.DataFrame) -> None:
    """
    Writes daily aggregate metrics to the DynamoDB 'BikeMetricsDaily' table in batches.

    Args:
        dynamodb (Any): The configured Boto3 DynamoDB resource.
        daily_df (pd.DataFrame): The DataFrame containing calculated daily metrics.

    Returns:
        None
    """

    table: Any = dynamodb.Table("BikeMetricsDaily")

    daily_df = daily_df.astype(str)
    records: list[dict[str, Any]] = daily_df.to_dict(orient="records")

    with table.batch_writer() as batch:
        for row in records:
            batch.put_item(
                Item={
                    "date": row["date"],
                    # Wrap the pre-calculated strings in Decimal for DynamoDB Number types
                    "avg_distance": Decimal(row["distance"]),
                    "avg_duration": Decimal(row["duration"]),
                    "avg_speed": Decimal(row["speed"]),
                    "avg_temperature": Decimal(row["temperature"]),
                }
            )

    return None


def save_monthly_metrics(dynamodb: Any, clean_df: pd.DataFrame, month_id: str) -> None:
    """
    Calculates final monthly averages from the daily DataFrame and saves them to DynamoDB.

    Args:
        dynamodb (Any): The configured Boto3 DynamoDB resource.
        clean_df (pd.DataFrame): The DataFrame containing clean data.
        month_id (str): The month identifier being processed.

    Returns:
        None
    """

    table: Any = dynamodb.Table("BikeMetricsMonthly")

    # Check length so we don't divide by zero if dataframe is empty
    if not clean_df.empty:
        metrics: pd.Series = (
            clean_df[["distance", "duration", "speed", "temperature"]].mean().fillna(0)
        )

        table.put_item(
            Item={
                "month_id": month_id,
                "avg_distance": Decimal(str(metrics["distance"])),
                "avg_duration": Decimal(str(metrics["duration"])),
                "avg_speed": Decimal(str(metrics["speed"])),
                "avg_temperature": Decimal(str(metrics["temperature"])),
            }
        )

    return None


def process_and_save_data(s3_client, dynamodb, bucket: str, month_id: str) -> None:
    """
    Coordinates the ETL pipeline, calculating metrics and saving them concurrently.

    Args:
        s3_client (Any): The configured Boto3 S3 client.
        dynamodb (Any): The configured Boto3 DynamoDB resource.
        bucket (str): The name of the S3 bucket containing the data.
        month_id (str): The target month identifier to process.

    Returns:
        None

    Raises:
        Exception: Re-raises any exceptions encountered during concurrent DynamoDB writes.
    """

    logger.info(f"Fetching data for {month_id}...")
    raw_df: pd.DataFrame = load_csv_from_s3(s3_client, bucket, f"{month_id}.csv")

    logger.info("Transforming and calculating metrics...")
    clean_df: pd.DataFrame = prepare_df(raw_df)
    daily_df: pd.DataFrame = calculate_daily_metrics(clean_df)

    logger.info("Saving to DynamoDB Concurrently...")

    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        # Create a dictionary to map futures to their tasks for better logging
        future_to_task: dict[concurrent.futures.Future, str] = {
            executor.submit(save_daily_metrics, dynamodb, daily_df): "Daily Metrics",
            executor.submit(
                save_monthly_metrics, dynamodb, clean_df, month_id
            ): "Monthly Metrics",
        }

        for future in concurrent.futures.as_completed(future_to_task):
            task_name: str = future_to_task[future]
            try:
                future.result()  # Raises the exception if one occurred
                logger.info(f"{task_name} saved successfully.")
            except Exception as e:
                logger.error(f"Error saving {task_name}: {str(e)}")
                raise e  # Fail the Lambda so SNS can retry

    logger.info("All DynamoDB saves completed successfully!")

    return None


def _extract_month_from_key(file_key: str) -> str:
    """
    Extracts the base month identifier from an S3 object key.

    Args:
        file_key (str): The S3 object key (e.g., "metrics/departures_2016-05.csv").

    Returns:
        str: The extracted month identifier (e.g., "2016-05").
    """

    filename: str = file_key.split("/")[-1]
    return (
        filename.replace("departures_", "").replace("returns_", "").replace(".csv", "")
    )


# --- THE LAMBDA ENTRYPOINT ---
def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    """
    The entrypoint for AWS Lambda to receive the SNS event and trigger the pipeline.

    Args:
        event (dict[str, Any]): The AWS event payload, expected to be an SNS notification.
        context (Any): The AWS Lambda runtime context object.

    Returns:
        dict[str, Any]: A dictionary containing the HTTP status code and a completion message.
    """

    s3_client: Any = get_boto3_client("s3")
    dynamodb: Any = get_boto3_resource("dynamodb")

    # SNS wraps the S3 event inside an SNS message, we must unpack it
    for sns_record in event.get("Records", []):
        sns_message: dict[str, Any] = json.loads(sns_record["Sns"]["Message"])

        for s3_record in sns_message.get("Records", []):
            bucket: str = s3_record["s3"]["bucket"]["name"]
            key: str = s3_record["s3"]["object"]["key"]

            logger.info(f"Triggered by file: {key}")
            month_id: str = _extract_month_from_key(key)

            if check_all_files_exist(s3_client, bucket, month_id):
                logger.info(f"All 3 files found for {month_id}. Starting Pandas ETL...")
                process_and_save_data(s3_client, dynamodb, bucket, month_id)
            else:
                logger.info(
                    f"Wait condition: Not all 3 files for {month_id} are present."
                )

    return {"statusCode": 200, "body": "Invocation complete"}
