import logging
from typing import Any

from airflow.sdk import dag, task
import pendulum
import boto3
import os
from botocore.config import Config
from pandas import DataFrame

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)


def get_localstack_s3_client(max_connections=10) -> Any:
    """
    Returns a Boto3 S3 client configured for the LocalStack endpoint.

    Args:
        max_connections (int): Maximum pool connections for the Boto3 config.

    Returns:
        Any: A configured Boto3 S3 client object.
    """

    return boto3.client(
        "s3",
        endpoint_url="http://localstack-main:4566",
        region_name=os.getenv("AWS_REGION", "us-east-1"),
        config=Config(max_pool_connections=max_connections),
    )


@dag(
    dag_id="upload_bike_data_to_s3",
    start_date=pendulum.datetime(2026, 4, 20),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["helsinki-bikes", "s3"],
)
def helsinki_bike_pipeline() -> None:
    """
    Orchestrates the Helsinki Bike data ETL from local storage to S3 and Spark processing.

    Returns:
        None
    """

    @task(task_id="upload_to_localstack")
    def upload_to_localstack() -> list[str]:
        """
        Scans a local directory for bike dataset CSV files and uploads them to S3.

        Returns:
        list[str]: A list of filenames successfully uploaded to the S3 bucket.

        Raises:
        FileNotFoundError: If the source directory
            `/opt/airflow/data/monthly_splits` does not exist.
        """

        from concurrent.futures import ThreadPoolExecutor

        local_dir: str = "/opt/airflow/data/monthly_splits"
        bucket_name: str = os.getenv("S3_BUCKET_NAME", "bike-data-2016-2020")

        def _upload_single_file(filename: str) -> str:
            """
            Uploads a single CSV file to the S3 bucket.

            Args:
                filename (str): The name of the file to upload.

            Returns:
                str: The name of the uploaded file.
            """

            filepath: str = os.path.join(local_dir, filename)
            s3_client.upload_file(filepath, bucket_name, filename)

            return filename

        # Initialize boto3 to talk to LocalStack instead of real AWS
        s3_client: Any = get_localstack_s3_client(max_connections=50)

        if not os.path.exists(local_dir):
            logger.error(f"Could not find the data directory at {local_dir}.")
            raise FileNotFoundError(
                f"Could not find the data directory at {local_dir}."
            )

        uploaded_files: list[str] = []

        files_to_upload: list[str] = [
            f for f in os.listdir(local_dir) if f.endswith(".csv")
        ]

        logger.info(f"Starting parallel upload of {len(files_to_upload)} files...")

        # Upload up to 5 files at the same time
        with ThreadPoolExecutor(max_workers=5) as executor:
            uploaded_files: list[str] = list(
                executor.map(_upload_single_file, files_to_upload)
            )

        logger.info(f"Successfully uploaded {len(uploaded_files)} files to s3.")
        return uploaded_files

    @task(task_id="calculate_metrics", max_active_tis_per_dag=1)
    def calculate_metrics(filename: str) -> str:
        """
        Triggers a Spark Connect job to compute station usage metrics for a specific file.

        Args:
            filename (str): The name of the CSV file in S3 to process.

        Returns:
            str: The name of the processed file.
        """

        from pyspark.sql import SparkSession
        import pyspark.sql.functions as F

        bucket_name: str = os.getenv("S3_BUCKET_NAME", "bike-data-2016-2020")
        input_file: str = f"s3a://{bucket_name}/{filename}"
        temp_departures_prefix: str = f"metrics/temp_departures_{filename}"
        temp_returns_prefix: str = f"metrics/temp_returns_{filename}"

        # Initialize a remote Spark Session to keep the Airflow worker lightweight
        logger.info("Sending instructions to Spark Connect...")
        spark = SparkSession.builder.remote("sc://spark-local:15002").getOrCreate()

        # Initialize df to None so the finally block doesn't crash if the read fails
        df: DataFrame | None = None

        try:
            logger.info("Spark Session built! Attempting to read from LocalStack S3...")
            df = spark.read.csv(input_file, header=True, inferSchema=True)
            df.cache()

            logger.info("File read successfully! Calculating metrics...")
            departures_df = df.groupBy("departure_name").agg(
                F.count("departure_id").alias("total_departures")
            )
            returns_df = df.groupBy("return_name").agg(
                F.count("return_id").alias("total_returns")
            )

            # Force Spark to output a single file per metric using coalesce(1)
            logger.info("Saving metrics back to S3...")
            departures_df.coalesce(1).write.mode("overwrite").csv(
                f"s3a://{bucket_name}/{temp_departures_prefix}", header=True
            )
            returns_df.coalesce(1).write.mode("overwrite").csv(
                f"s3a://{bucket_name}/{temp_returns_prefix}", header=True
            )

            logger.info(f"SUCCESS -> {filename} processed!")
        except Exception as e:
            logger.error(f"Spark processing failed for {filename}: {str(e)}")
            raise  # Re-raise the error so Airflow marks the task as FAILED

        finally:
            logger.info("Cleaning up Spark resources...")
            if df is not None:
                df.unpersist()
            spark.stop()

        return filename

    @task(task_id="rename_spark_outputs", max_active_tis_per_dag=1)
    def rename_spark_outputs(filename: str) -> None:
        """
        Cleans up Spark partitioned output directories into formatted single CSV files.

        Args:
            filename (str): The original filename used to identify the metrics directories.

        Returns:
            None
        """

        bucket_name: str = os.getenv("S3_BUCKET_NAME", "bike-data-2016-2020")
        temp_departures_prefix: str = f"metrics/temp_departures_{filename}"
        temp_returns_prefix: str = f"metrics/temp_returns_{filename}"
        final_departures_key: str = f"metrics/departures_{filename}"
        final_returns_key: str = f"metrics/returns_{filename}"

        s3_client: Any = get_localstack_s3_client()

        def _rename(spark_dir_prefix: str, final_key: str) -> None:
            """
            Locates the part-file within a Spark output folder, renames it, and deletes the temp folder.

            Args:
                spark_dir_prefix (str): The temporary S3 prefix generated by Spark.
                final_key (str): The final destination key for the cleanly named CSV.

            Returns:
                None
            """

            # List objects in the Spark output directory
            response = s3_client.list_objects_v2(
                Bucket=bucket_name, Prefix=f"{spark_dir_prefix}/"
            )
            objects = response.get("Contents", [])

            # Find the actual data part file
            part_file_key: str | None = next(
                (
                    obj["Key"]
                    for obj in objects
                    if obj["Key"].endswith(".csv") and "part-" in obj["Key"]
                ),
                None,
            )

            if part_file_key:
                # Copy the file to the clean final path
                s3_client.copy_object(
                    Bucket=bucket_name,
                    CopySource={"Bucket": bucket_name, "Key": part_file_key},
                    Key=final_key,
                )

                # Delete the original Spark metadata and part files
                objects_to_delete: list[dict[str, str]] = [
                    {"Key": obj["Key"]} for obj in objects
                ]
                if objects_to_delete:
                    s3_client.delete_objects(
                        Bucket=bucket_name, Delete={"Objects": objects_to_delete}
                    )
                else:
                    logger.warning(
                        f"No part-file found in {spark_dir_prefix}/ to rename."
                    )
            return None

        # --- RENAME TASK EXECUTION ---
        logger.info(f"Renaming files for {filename}...")
        _rename(temp_departures_prefix, final_departures_key)
        _rename(temp_returns_prefix, final_returns_key)
        logger.info(f"Cleanup complete for {filename}!")

        return None

    # --- DAG EXECUTION ---

    # 1. Upload files to S3 and get the list of uploaded file names
    uploaded_files_list: list[str] = upload_to_localstack()

    # 2. Start Spark calculations
    processed_files_list: list[str] = calculate_metrics.expand(
        filename=uploaded_files_list
    )

    # 3. Rename processed files by Spark and delete tmp data
    rename_spark_outputs.expand(filename=processed_files_list)


helsinki_bike_pipeline()
