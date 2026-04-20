from airflow.sdk import dag, task
import pendulum
import os
import boto3

@dag(
    dag_id="upload_bike_data_to_s3",
    start_date=pendulum.datetime(2026, 4, 20),
    schedule=None,
    catchup=False,
    tags=["helsinki-bikes", "s3"],
)
def helsinki_bike_pipeline():

    @task
    def upload_to_localstack():
        local_dir = "/opt/airflow/data/monthly_splits"
        bucket_name = "bike-data-2016-2020"

        # Initialize boto3 to talk to LocalStack instead of real AWS
        s3_client = boto3.client(
            "s3",
            endpoint_url="http://localstack-main:4566",
            aws_access_key_id="test",
            aws_secret_access_key="test",
            region_name="us-east-1",
        )

        if not os.path.exists(local_dir):
            raise FileNotFoundError(f"Could not find the data directory at {local_dir}.")

        uploaded_count = 0

        for filename in os.listdir(local_dir):
            if filename.endswith(".csv"):
                filepath = os.path.join(local_dir, filename)
                print(f"Uploading {filename} to s3://{bucket_name}/{filename}...")

                s3_client.upload_file(filepath, bucket_name, filename)
                uploaded_count += 1

        print(f"Uploaded {uploaded_count} files to s3.")

    upload_to_localstack()

helsinki_bike_pipeline()