from airflow.sdk import dag, task
from airflow.datasets import Dataset
from datetime import timedelta

from pathlib import Path

FILENAME = "tiktok_google_play_reviews.csv"
OUTPUT_DIR = "/opt/airflow/data/processed"

OUTPUT_FILE_PATH = Path(OUTPUT_DIR) / FILENAME

processed_file = Dataset(str(OUTPUT_FILE_PATH))


def get_reviews_collection() -> "pymongo.collection.Collection":
    """
    Retrieve the MongoDB collection used for storing reviews.

    Uses the MongoDB connection configured in Airflow (conn_id='mongo_default').

    Steps:
    1. Create a MongoHook using the Airflow connection.
    2. Obtain a MongoDB client.
    3. Select the 'airflow' database.
    4. Return the 'reviews' collection.

    :return: MongoDB collection object for 'airflow.reviews'
    """

    from airflow.providers.mongo.hooks.mongo import MongoHook
    hook = MongoHook(conn_id="mongo_default")
    client = hook.get_conn()

    # Select the database
    db = client["airflow"]

    return db["reviews"]


@dag(
    dag_id="load_to_mongo",
    schedule=[processed_file],
    tags=["assets"],
)
def load_to_mongo():
    """
    DAG triggered by the availability of the processed CSV dataset.

    Workflow:
    1. Ensure MongoDB indexes are properly configured.
    2. Load CSV data into MongoDB using bulk upsert operations.

    Trigger:
        Executed when the dataset 'processed_file' is updated.
    """

    @task(retries=3, deadline=timedelta(hours=1))
    def init_mongo() -> None:
        """
         Ensure required MongoDB indexes exist.

        Creates a unique index on the 'reviewId' field to:
        - Prevent duplicate documents
        - Enable safe upsert operations

        :return: None
        """

        reviews_collection = get_reviews_collection()
        reviews_collection.create_index("reviewId", unique=True)

        print("Indexes ensured on 'reviews' collection.")

    @task(retries=3, deadline=timedelta(minutes=10))
    def load_data() -> None:
        """
        Load CSV data into MongoDB using bulk upserts.

        Steps:
        1. Read the processed CSV file.
        2. Convert NaN values to None (MongoDB-compatible).
        3. Transform rows into dictionaries.
        4. Build UpdateOne upsert operations.
        5. Execute bulk writes in batches.

        :return: None
        """

        import pandas as pd
        from pymongo import UpdateOne

        reviews_collection = get_reviews_collection()

        # Read CSV
        df = pd.read_csv(OUTPUT_FILE_PATH)

        # Replace NaN with None for MongoDB compatibility
        df = df.where(pd.notnull(df), None)

        records = df.to_dict("records")

        # Prepare bulk upsert operations
        operations = [
            UpdateOne(
                {"reviewId": record["reviewId"]},  # filter by reviewId
                {"$set": record},  # set new data
                upsert=True,  # insert if not exists
            )
            for record in records
        ]

        # Batch processing
        batch_size = 1000
        for i in range(0, len(operations), batch_size):
            batch = operations[i: i + batch_size]
            reviews_collection.bulk_write(batch)

    init_mongo() >> load_data()


load_to_mongo()
